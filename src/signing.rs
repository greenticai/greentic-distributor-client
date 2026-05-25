//! DSSE + in-toto Ed25519 signing primitive (C2).
//!
//! This is the reusable cryptographic core for artifact signing across the
//! Greentic ecosystem: the bundle builder signs `.gtbundle` artifacts with
//! [`sign_statement`], and the distributor-client verifier (plus the deployer's
//! revision/revenue gates) verify them with [`verify_artifact_dsse`].
//!
//! Format: a [DSSE](https://github.com/secure-systems-lab/dsse) envelope wraps
//! an [in-toto Statement v1](https://in-toto.io/Statement/v1) whose `subject`
//! pins the artifact's SHA-256 digest and whose predicate is a minimal
//! `https://slsa.dev/provenance/v1` document. Phase B claims signature
//! *authenticity* only — KMS, Rekor/transparency log, and full provenance
//! policy belong to the Trust plan, so [`SlsaProvenance::tlog_entry_id`] is
//! reserved but unused here.
//!
//! Keys are plain Ed25519 in PKCS#8 PEM (private) / SPKI PEM (public). A
//! [`TrustRoot`] is an explicit allowlist of public keys addressed by a
//! `key_id` derived as the first 16 bytes of `SHA-256(raw 32-byte public key)`,
//! hex-encoded — the same derivation `packc` uses for pack-directory signing.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// DSSE `payloadType` for an in-toto Statement.
pub const DSSE_PAYLOAD_TYPE_INTOTO: &str = "application/vnd.in-toto+json";
/// in-toto Statement `_type` (v1).
pub const INTOTO_STATEMENT_TYPE: &str = "https://in-toto.io/Statement/v1";
/// SLSA provenance `predicateType` (v1).
pub const SLSA_PROVENANCE_PREDICATE_TYPE: &str = "https://slsa.dev/provenance/v1";

/// Errors from constructing, signing, or verifying a DSSE envelope.
#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    #[error("malformed DSSE envelope: {0}")]
    MalformedEnvelope(String),
    #[error("unsupported DSSE payloadType {0:?}; expected {expected:?}", expected = DSSE_PAYLOAD_TYPE_INTOTO)]
    UnsupportedPayloadType(String),
    #[error("malformed in-toto statement: {0}")]
    MalformedStatement(String),
    #[error("base64 decode error in {field}: {source}")]
    Base64 {
        field: &'static str,
        source: base64::DecodeError,
    },
    #[error("could not decode key ({0})")]
    KeyDecode(String),
    #[error("no trusted key matched any envelope signature (key ids tried: {0})")]
    NoTrustedKey(String),
    #[error("signature did not verify against trusted key {key_id}")]
    SignatureInvalid { key_id: String },
    #[error(
        "subject digest mismatch: statement pins sha256:{statement}, artifact is sha256:{artifact}"
    )]
    SubjectDigestMismatch { statement: String, artifact: String },
    #[error("statement has no subject pinning sha256")]
    NoSubjectDigest,
}

/// A DSSE envelope: `payload` is base64(std) of the in-toto Statement JSON,
/// signed under the DSSE Pre-Authentication Encoding (see [`pae`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DsseEnvelope {
    #[serde(rename = "payloadType")]
    pub payload_type: String,
    /// base64(std) of the Statement JSON.
    pub payload: String,
    pub signatures: Vec<DsseSignature>,
}

/// One DSSE signature: `sig` is base64(std) of the raw 64-byte Ed25519
/// signature over `PAE(payloadType, payload_bytes)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DsseSignature {
    pub keyid: String,
    pub sig: String,
}

/// in-toto Statement v1. The `subject` pins one or more artifact digests; the
/// `predicate` carries SLSA provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InTotoStatement {
    #[serde(rename = "_type")]
    pub type_: String,
    pub subject: Vec<Subject>,
    #[serde(rename = "predicateType")]
    pub predicate_type: String,
    pub predicate: serde_json::Value,
}

/// An in-toto subject: a named artifact and its digest(s) keyed by algorithm
/// (e.g. `{"sha256": "<hex>"}`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subject {
    pub name: String,
    pub digest: BTreeMap<String, String>,
}

/// Minimal SLSA provenance v1 predicate. Only the fields Phase B needs are
/// modeled; `tlog_entry_id` is reserved for the Trust plan (Rekor) and is not
/// populated or checked here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlsaProvenance {
    /// Identifier of the builder that produced the artifact (e.g.
    /// `greentic-bundle/<version>`).
    pub builder_id: String,
    /// Artifact-type / build-type discriminator (e.g. `gtbundle`).
    pub build_type: String,
    /// RFC3339 timestamp the artifact was signed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_at: Option<String>,
    /// Reserved for a transparency-log entry id (Rekor). Always `None` in
    /// Phase B; verification never depends on it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tlog_entry_id: Option<String>,
}

impl InTotoStatement {
    /// Build a Statement pinning `artifact_name` to `sha256_hex` with a SLSA
    /// provenance predicate. `sha256_hex` is the bare lowercase hex digest (no
    /// `sha256:` prefix).
    pub fn provenance(
        artifact_name: impl Into<String>,
        sha256_hex: &str,
        prov: SlsaProvenance,
    ) -> Self {
        let mut digest = BTreeMap::new();
        digest.insert("sha256".to_string(), sha256_hex.to_string());
        Self {
            type_: INTOTO_STATEMENT_TYPE.to_string(),
            subject: vec![Subject {
                name: artifact_name.into(),
                digest,
            }],
            predicate_type: SLSA_PROVENANCE_PREDICATE_TYPE.to_string(),
            predicate: serde_json::to_value(prov).unwrap_or(serde_json::Value::Null),
        }
    }

    /// The bare lowercase-hex sha256 digest pinned by the first subject, if any.
    pub fn subject_sha256(&self) -> Option<&str> {
        self.subject
            .iter()
            .find_map(|s| s.digest.get("sha256"))
            .map(String::as_str)
    }
}

/// A public key trusted to sign artifacts, addressed by [`key_id_for_public_key_pem`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedKey {
    pub key_id: String,
    /// Ed25519 public key as SPKI PEM (`-----BEGIN PUBLIC KEY-----`).
    pub public_key_pem: String,
}

/// Explicit allowlist of trusted signing keys. Verification requires at least
/// one envelope signature to match a key in this set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustRoot {
    pub keys: Vec<TrustedKey>,
}

impl TrustRoot {
    pub fn new(keys: Vec<TrustedKey>) -> Self {
        Self { keys }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn find(&self, key_id: &str) -> Option<&TrustedKey> {
        self.keys.iter().find(|k| k.key_id == key_id)
    }
}

/// A statement that verified against at least one trusted key.
#[derive(Clone, Debug)]
pub struct VerifiedStatement {
    pub statement: InTotoStatement,
    /// Key ids whose signatures verified.
    pub verified_key_ids: Vec<String>,
}

/// DSSE Pre-Authentication Encoding (PAE):
/// `"DSSEv1 " + len(type) + " " + type + " " + len(payload) + " " + payload`.
pub fn pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload_type.len() + payload.len() + 32);
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(payload_type.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload_type.as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}

/// Derive a key id from an Ed25519 SPKI public-key PEM: the first 16 bytes of
/// `SHA-256(raw 32-byte public key)`, lowercase hex (32 hex chars). Mirrors the
/// derivation `packc` uses, so a key id is stable across signing subsystems.
pub fn key_id_for_public_key_pem(pem: &str) -> Result<String, SigningError> {
    let vk = VerifyingKey::from_public_key_pem(pem)
        .map_err(|e| SigningError::KeyDecode(format!("public key PEM: {e}")))?;
    Ok(key_id_for_verifying_key(&vk))
}

fn key_id_for_verifying_key(vk: &VerifyingKey) -> String {
    let digest = Sha256::digest(vk.to_bytes());
    hex::encode(&digest[..16])
}

/// Sign an in-toto Statement with a PKCS#8 PEM Ed25519 private key, producing a
/// single-signature DSSE envelope. `key_id` must match the key id the verifier's
/// trust root carries for the corresponding public key (see
/// [`key_id_for_public_key_pem`]).
pub fn sign_statement(
    statement: &InTotoStatement,
    signing_key_pkcs8_pem: &str,
    key_id: &str,
) -> Result<DsseEnvelope, SigningError> {
    let sk = SigningKey::from_pkcs8_pem(signing_key_pkcs8_pem)
        .map_err(|e| SigningError::KeyDecode(format!("private key PEM: {e}")))?;
    let payload_json = serde_json::to_vec(statement)
        .map_err(|e| SigningError::MalformedStatement(e.to_string()))?;
    let signature = sk.sign(&pae(DSSE_PAYLOAD_TYPE_INTOTO, &payload_json));
    Ok(DsseEnvelope {
        payload_type: DSSE_PAYLOAD_TYPE_INTOTO.to_string(),
        payload: BASE64.encode(&payload_json),
        signatures: vec![DsseSignature {
            keyid: key_id.to_string(),
            sig: BASE64.encode(signature.to_bytes()),
        }],
    })
}

/// Verify a DSSE envelope against a trust root. Requires the payload type to be
/// in-toto and at least one signature to verify against a trusted key. Returns
/// the parsed Statement and the key ids that verified.
pub fn verify_envelope(
    envelope: &DsseEnvelope,
    trust_root: &TrustRoot,
) -> Result<VerifiedStatement, SigningError> {
    if envelope.payload_type != DSSE_PAYLOAD_TYPE_INTOTO {
        return Err(SigningError::UnsupportedPayloadType(
            envelope.payload_type.clone(),
        ));
    }
    let payload_bytes = BASE64
        .decode(envelope.payload.as_bytes())
        .map_err(|source| SigningError::Base64 {
            field: "payload",
            source,
        })?;
    let statement: InTotoStatement = serde_json::from_slice(&payload_bytes)
        .map_err(|e| SigningError::MalformedStatement(e.to_string()))?;
    if statement.type_ != INTOTO_STATEMENT_TYPE {
        return Err(SigningError::MalformedStatement(format!(
            "unexpected statement _type {:?}",
            statement.type_
        )));
    }

    let pae_bytes = pae(&envelope.payload_type, &payload_bytes);
    let mut tried = Vec::new();
    let mut verified_key_ids = Vec::new();
    for sig in &envelope.signatures {
        tried.push(sig.keyid.clone());
        let Some(trusted) = trust_root.find(&sig.keyid) else {
            continue;
        };
        let vk = VerifyingKey::from_public_key_pem(&trusted.public_key_pem)
            .map_err(|e| SigningError::KeyDecode(format!("trusted key {}: {e}", sig.keyid)))?;
        let sig_bytes =
            BASE64
                .decode(sig.sig.as_bytes())
                .map_err(|source| SigningError::Base64 {
                    field: "signature",
                    source,
                })?;
        let signature = Signature::from_slice(&sig_bytes)
            .map_err(|e| SigningError::KeyDecode(format!("signature bytes: {e}")))?;
        if vk.verify(&pae_bytes, &signature).is_err() {
            return Err(SigningError::SignatureInvalid {
                key_id: sig.keyid.clone(),
            });
        }
        verified_key_ids.push(sig.keyid.clone());
    }

    if verified_key_ids.is_empty() {
        return Err(SigningError::NoTrustedKey(tried.join(", ")));
    }
    Ok(VerifiedStatement {
        statement,
        verified_key_ids,
    })
}

/// Verify a serialized DSSE envelope and confirm its subject pins
/// `expected_sha256`. `expected_sha256` may carry a `sha256:` prefix or be a
/// bare hex digest. This is the high-level entry point used by artifact gates.
pub fn verify_artifact_dsse(
    envelope_json: &[u8],
    expected_sha256: &str,
    trust_root: &TrustRoot,
) -> Result<VerifiedStatement, SigningError> {
    let envelope: DsseEnvelope = serde_json::from_slice(envelope_json)
        .map_err(|e| SigningError::MalformedEnvelope(e.to_string()))?;
    let verified = verify_envelope(&envelope, trust_root)?;
    let expected = expected_sha256
        .strip_prefix("sha256:")
        .unwrap_or(expected_sha256);
    let pinned = verified
        .statement
        .subject_sha256()
        .ok_or(SigningError::NoSubjectDigest)?;
    if !pinned.eq_ignore_ascii_case(expected) {
        return Err(SigningError::SubjectDigestMismatch {
            statement: pinned.to_string(),
            artifact: expected.to_string(),
        });
    }
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};

    /// Deterministic keypair from a 32-byte seed -> (pkcs8 private PEM, spki
    /// public PEM, key_id).
    fn keypair(seed: u8) -> (String, String, String) {
        let sk = SigningKey::from_bytes(&[seed; 32]);
        let vk = sk.verifying_key();
        let priv_pem = sk.to_pkcs8_pem(LineEnding::LF).unwrap().to_string();
        let pub_pem = vk.to_public_key_pem(LineEnding::LF).unwrap();
        let key_id = key_id_for_verifying_key(&vk);
        (priv_pem, pub_pem, key_id)
    }

    fn statement(sha256_hex: &str) -> InTotoStatement {
        InTotoStatement::provenance(
            "customer.support_v1.2.0.gtbundle",
            sha256_hex,
            SlsaProvenance {
                builder_id: "greentic-bundle/test".into(),
                build_type: "gtbundle".into(),
                built_at: Some("2026-05-25T00:00:00Z".into()),
                tlog_entry_id: None,
            },
        )
    }

    const DIGEST: &str = "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234";

    #[test]
    fn key_id_matches_between_priv_and_pub_pem() {
        let (_priv, pub_pem, key_id) = keypair(1);
        assert_eq!(key_id_for_public_key_pem(&pub_pem).unwrap(), key_id);
        assert_eq!(key_id.len(), 32);
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        let (priv_pem, pub_pem, key_id) = keypair(2);
        let env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id: key_id.clone(),
            public_key_pem: pub_pem,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        let verified = verify_artifact_dsse(&bytes, DIGEST, &trust).unwrap();
        assert_eq!(verified.verified_key_ids, vec![key_id]);
        assert_eq!(verified.statement.subject_sha256(), Some(DIGEST));
        assert_eq!(
            verified.statement.predicate_type,
            SLSA_PROVENANCE_PREDICATE_TYPE
        );
    }

    #[test]
    fn verify_accepts_sha256_prefixed_expected() {
        let (priv_pem, pub_pem, key_id) = keypair(3);
        let env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id,
            public_key_pem: pub_pem,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        verify_artifact_dsse(&bytes, &format!("sha256:{DIGEST}"), &trust).unwrap();
    }

    #[test]
    fn untrusted_key_is_rejected() {
        let (priv_pem, _pub_pem, key_id) = keypair(4);
        let (_priv2, other_pub, other_id) = keypair(5);
        let env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        // Trust root holds a *different* key.
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id: other_id,
            public_key_pem: other_pub,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        assert!(matches!(
            verify_artifact_dsse(&bytes, DIGEST, &trust),
            Err(SigningError::NoTrustedKey(_))
        ));
    }

    #[test]
    fn tampered_payload_fails_signature() {
        let (priv_pem, pub_pem, key_id) = keypair(6);
        let mut env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        // Re-encode a different statement under the same (now wrong) signature.
        let other = serde_json::to_vec(&statement(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ))
        .unwrap();
        env.payload = BASE64.encode(&other);
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id: key_id.clone(),
            public_key_pem: pub_pem,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        assert!(matches!(
            verify_artifact_dsse(&bytes, DIGEST, &trust),
            Err(SigningError::SignatureInvalid { .. })
        ));
    }

    #[test]
    fn subject_digest_mismatch_rejected() {
        let (priv_pem, pub_pem, key_id) = keypair(7);
        let env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id,
            public_key_pem: pub_pem,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        let wrong = "0000000000000000000000000000000000000000000000000000000000000000";
        assert!(matches!(
            verify_artifact_dsse(&bytes, wrong, &trust),
            Err(SigningError::SubjectDigestMismatch { .. })
        ));
    }

    #[test]
    fn empty_trust_root_rejects_signed_envelope() {
        let (priv_pem, _pub_pem, key_id) = keypair(8);
        let env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        let bytes = serde_json::to_vec(&env).unwrap();
        assert!(matches!(
            verify_artifact_dsse(&bytes, DIGEST, &TrustRoot::default()),
            Err(SigningError::NoTrustedKey(_))
        ));
    }

    #[test]
    fn wrong_payload_type_rejected() {
        let (priv_pem, pub_pem, key_id) = keypair(9);
        let mut env = sign_statement(&statement(DIGEST), &priv_pem, &key_id).unwrap();
        env.payload_type = "application/octet-stream".into();
        let trust = TrustRoot::new(vec![TrustedKey {
            key_id,
            public_key_pem: pub_pem,
        }]);
        let bytes = serde_json::to_vec(&env).unwrap();
        assert!(matches!(
            verify_artifact_dsse(&bytes, DIGEST, &trust),
            Err(SigningError::UnsupportedPayloadType(_))
        ));
    }

    #[test]
    fn pae_encoding_is_spec_shaped() {
        assert_eq!(pae("t", b"hi"), b"DSSEv1 1 t 2 hi".to_vec());
    }
}
