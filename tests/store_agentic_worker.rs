//! Integration tests for the store agentic-worker artifact transport.
//!
//! These exercise the real blocking HTTP path against a local `httpmock`
//! server, so they only build when the `dist-client` feature (which gates the
//! `store_agentic_worker` module and pulls in `reqwest`) is enabled.
#![cfg(feature = "dist-client")]

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use greentic_distributor_client::store_agentic_worker::{
    agentic_worker_artifact_url, fetch_store_agentic_worker, fetch_store_agentic_worker_verified,
};
use greentic_distributor_client::{
    InTotoStatement, SlsaProvenance, TrustRoot, TrustedKey, key_id_for_public_key_pem,
    sign_statement,
};
use sha2::{Digest, Sha256};

/// Start a mock server, skipping the test when the environment forbids binding.
fn start_server() -> Option<httpmock::MockServer> {
    std::panic::catch_unwind(httpmock::MockServer::start).ok()
}

/// Deterministic keypair from a seed byte — mirrors the helper in `signing.rs` tests.
///
/// Returns `(pkcs8_priv_pem, spki_pub_pem, key_id)`.
fn test_keypair(seed: u8) -> (String, String, String) {
    let sk = SigningKey::from_bytes(&[seed; 32]);
    let vk = sk.verifying_key();
    let priv_pem = sk.to_pkcs8_pem(LineEnding::LF).unwrap().to_string();
    let pub_pem = vk.to_public_key_pem(LineEnding::LF).unwrap();
    let key_id = key_id_for_public_key_pem(&pub_pem).unwrap();
    (priv_pem, pub_pem, key_id)
}

/// Build an in-toto statement pinning `artifact_sha256` for a test agentic-worker pack.
fn test_artifact_statement(artifact_sha256: &str) -> InTotoStatement {
    InTotoStatement::provenance(
        "demo-agent-0.1.0.gtpack",
        artifact_sha256,
        SlsaProvenance {
            builder_id: "greentic-bundle/test".into(),
            build_type: "gtpack".into(),
            built_at: None,
            tlog_entry_id: None,
        },
    )
}

/// Serialize a signed envelope as the JSON string stored in the `dsseEnvelope` field.
fn envelope_to_metadata_body(envelope: &greentic_distributor_client::DsseEnvelope) -> String {
    let envelope_json = serde_json::to_string(envelope).unwrap();
    let metadata = serde_json::json!({"dsseEnvelope": envelope_json});
    serde_json::to_string(&metadata).unwrap()
}

// ---------------------------------------------------------------------------
// Task 1 tests (kept intact)
// ---------------------------------------------------------------------------

#[test]
fn download_verifies_and_caches_artifact() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let body = b"a-real-gtpack-archive".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/demo-agent/0.1.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let bytes = fetch_store_agentic_worker(
        &server.base_url(),
        "demo-agent",
        "0.1.0",
        cache.path(),
        false,
    )
    .expect("download should succeed");

    mock.assert();
    assert_eq!(bytes, body);

    // A second offline fetch is served from the ref-keyed cache (no network).
    let cached = fetch_store_agentic_worker(
        &server.base_url(),
        "demo-agent",
        "0.1.0",
        cache.path(),
        true,
    )
    .expect("offline hit after caching");
    assert_eq!(cached, body);
}

#[test]
fn download_bails_on_sha_mismatch() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/bad-agent/0.1.0/artifact");
        then.status(200)
            // Advertise a digest that does not match the body.
            .header("x-artifact-sha256", "deadbeef")
            .body("some-bytes");
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let err = fetch_store_agentic_worker(
        &server.base_url(),
        "bad-agent",
        "0.1.0",
        cache.path(),
        false,
    )
    .expect_err("sha mismatch should bail");

    mock.assert();
    let message = err.to_string();
    assert!(
        message.contains("integrity check failed") && message.contains("bad-agent@0.1.0"),
        "unexpected error: {message}"
    );
}

#[test]
fn url_shape_is_stable() {
    assert_eq!(
        agentic_worker_artifact_url("https://store.example/", "n", "1.0.0"),
        "https://store.example/api/v1/agentic-workers/n/1.0.0/artifact"
    );
}

// ---------------------------------------------------------------------------
// Task 2 tests — Ed25519/DSSE verification
// ---------------------------------------------------------------------------

/// A valid signature verifies and returns the artifact bytes.
#[test]
fn verified_valid_signature_returns_bytes() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };

    let body = b"verified-agent-pack-bytes".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    let (priv_pem, pub_pem, key_id) = test_keypair(42);
    let stmt = test_artifact_statement(&sha);
    let envelope = sign_statement(&stmt, &priv_pem, &key_id).unwrap();
    let metadata_body = envelope_to_metadata_body(&envelope);

    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/verified-agent/0.1.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });
    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/verified-agent/0.1.0");
        then.status(200)
            .header("content-type", "application/json")
            .body(metadata_body);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let trust = TrustRoot::new(vec![TrustedKey {
        key_id,
        public_key_pem: pub_pem,
    }]);

    let bytes = fetch_store_agentic_worker_verified(
        &server.base_url(),
        "verified-agent",
        "0.1.0",
        cache.path(),
        false,
        &trust,
    )
    .expect("valid signature should succeed");

    assert_eq!(bytes, body);
}

/// A tampered envelope signature causes the function to bail.
#[test]
fn verified_tampered_signature_bails() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };

    let body = b"tampered-agent-pack-bytes".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    let (priv_pem, pub_pem, key_id) = test_keypair(43);
    let stmt = test_artifact_statement(&sha);
    let mut envelope = sign_statement(&stmt, &priv_pem, &key_id).unwrap();

    // Corrupt the signature: replace with 64 zero bytes (invalid Ed25519 sig).
    envelope.signatures[0].sig = BASE64.encode([0u8; 64]);

    let metadata_body = envelope_to_metadata_body(&envelope);

    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/tampered-agent/0.1.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });
    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/tampered-agent/0.1.0");
        then.status(200)
            .header("content-type", "application/json")
            .body(metadata_body);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let trust = TrustRoot::new(vec![TrustedKey {
        key_id,
        public_key_pem: pub_pem,
    }]);

    let err = fetch_store_agentic_worker_verified(
        &server.base_url(),
        "tampered-agent",
        "0.1.0",
        cache.path(),
        false,
        &trust,
    )
    .expect_err("tampered signature should bail");

    let message = err.to_string();
    assert!(
        message.contains("tampered-agent"),
        "error should name the coordinate; got: {message}"
    );
}

/// An empty `TrustRoot` skips verification entirely — sha256 integrity only.
///
/// The metadata endpoint is deliberately not mocked. If the implementation
/// attempts to fetch it, `httpmock` returns 404 and the function would bail,
/// failing this test and proving the no-network-for-empty-trust contract.
#[test]
fn verified_empty_trust_root_returns_bytes_sha_only() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };

    let body = b"sha-only-agent-pack".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/sha-only-agent/0.1.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let trust = TrustRoot::default(); // empty — no trusted keys

    let bytes = fetch_store_agentic_worker_verified(
        &server.base_url(),
        "sha-only-agent",
        "0.1.0",
        cache.path(),
        false,
        &trust,
    )
    .expect("empty trust root should succeed with sha-only verification");

    assert_eq!(bytes, body);
}

/// When a trust root is configured but the store does not serve a `dsseEnvelope`,
/// the function must bail (fail-closed).
#[test]
fn verified_configured_trust_but_missing_envelope_bails() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };

    let body = b"no-sig-agent-pack".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    let (_, pub_pem, key_id) = test_keypair(44);

    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/no-sig-agent/0.1.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });
    // Metadata response WITHOUT the `dsseEnvelope` field (pre-existing version).
    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/agentic-workers/no-sig-agent/0.1.0");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"name":"no-sig-agent","version":"0.1.0"}"#);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let trust = TrustRoot::new(vec![TrustedKey {
        key_id,
        public_key_pem: pub_pem,
    }]);

    let err = fetch_store_agentic_worker_verified(
        &server.base_url(),
        "no-sig-agent",
        "0.1.0",
        cache.path(),
        false,
        &trust,
    )
    .expect_err("configured trust with absent envelope must bail");

    let message = err.to_string();
    assert!(
        message.contains("no-sig-agent") || message.contains("envelope"),
        "error should mention the coordinate or missing envelope; got: {message}"
    );
}
