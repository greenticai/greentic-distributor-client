//! Pushing artifacts to an OCI registry.
//!
//! The mirror of `oci_packs`' fetch path: whatever this module stamps on a
//! layer must be a media type that path accepts, or a pushed artifact becomes
//! unfetchable. `media_type_is_always_acceptable_to_the_fetcher` is what keeps
//! the two halves honest.

use std::str::FromStr;

use async_trait::async_trait;
use oci_client::Reference;
use oci_client::client::{Config, ImageLayer};
use oci_client::errors::OciDistributionError;

use crate::oci_packs::DefaultRegistryClient;
use crate::oci_packs::{
    PACK_LAYER_MEDIA_TYPE_OCTET_STREAM, PACK_LAYER_MEDIA_TYPE_ZIP, compute_digest,
};

/// A pushed artifact and the digest of its CONTENT.
///
/// The digest is deliberately the content digest, not the OCI manifest digest:
/// it belongs in the deployer's `bundle_digest` field (see
/// `greentic-deployer/src/cli/bundle_stage.rs`, which computes the same value
/// over the staged `.gtbundle` and fail-closes against it). `oci-client`'s
/// `PushResponse` carries neither digest — only `config_url` and
/// `manifest_url` — so this is computed locally from the same bytes that were
/// pushed.
///
/// Do NOT use this digest to pin the reference as `@sha256:<digest>`:
/// registries resolve an `@digest` suffix against the OCI **manifest**
/// digest, which is a different value from this content digest. (For
/// `DefaultRegistryClient`, `fetch_pack_to_cache` in `oci_packs.rs` sets
/// `resolved_digest` from the manifest digest whenever the registry returns
/// one, which is nearly always — the `compute_digest`-over-bytes fallback
/// there is effectively unreachable for that client.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushedPack {
    /// Fully-qualified reference the artifact was pushed to.
    pub reference: String,
    /// `sha256:<hex>` over the artifact bytes.
    pub digest: String,
}

/// Choose the layer media type for an artifact's bytes.
///
/// Only ever returns a member of `oci_packs::default_pack_layer_media_types()`
/// — the fetch path rejects anything else, and a pushed artifact that cannot be
/// fetched is worse than a failed push.
///
/// Detection is by magic bytes rather than by file extension, matching
/// `detect_bundle_archive_kind` in `greentic-start`, which prefers magic over
/// suffix. Anything unrecognised is honestly labelled `application/octet-stream`
/// rather than guessed at; `detect_bundle_archive_kind`'s magic-byte sniffing
/// covers exactly ZIP and SquashFS — which is what this path produces — and
/// falls back to media type or filename suffix for tar / tar+gzip / tar+zstd.
pub fn layer_media_type_for(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"PK\x03\x04") {
        return PACK_LAYER_MEDIA_TYPE_ZIP;
    }
    PACK_LAYER_MEDIA_TYPE_OCTET_STREAM
}

/// Errors from pushing an artifact.
#[derive(Debug, thiserror::Error)]
pub enum OciPushError {
    #[error("invalid OCI reference `{reference}`: {source}")]
    InvalidReference {
        reference: String,
        #[source]
        source: oci_client::ParseError,
    },
    #[error("registry rejected the push: {0}")]
    Registry(#[from] OciDistributionError),
}

/// Pushing half of the registry contract.
///
/// Deliberately NOT a method on `oci_packs::RegistryClient`: that trait has
/// five implementors, four of them test mocks, and every one would break.
/// `DefaultRegistryClient` implements both.
#[async_trait]
pub trait RegistryPusher: Send + Sync {
    /// Push `bytes` as a single layer of `media_type` to `reference`.
    ///
    /// Deliberately pushes with no manifest annotations (`Client::push`'s
    /// `manifest` argument is always `None` here). This is a decision, not an
    /// oversight: signing is out of scope for this epic. The FETCH side
    /// already expects annotations as signature material — see
    /// `manifest_annotations` on `oci_packs::PulledImage` /
    /// `oci_packs::ResolvedPack`, documented as carrying e.g. a
    /// `dev.greentic.dsse` signature — so a future signed push will need
    /// annotations here too. Because this signature is positional, adding
    /// them later without breaking every implementor requires either an
    /// options struct or a new trait method; do not thread an
    /// `Option<annotations>` parameter through this signature as a quick fix.
    async fn push_artifact(
        &self,
        reference: &Reference,
        bytes: &[u8],
        media_type: &str,
    ) -> Result<(), OciDistributionError>;
}

#[async_trait]
impl RegistryPusher for DefaultRegistryClient {
    async fn push_artifact(
        &self,
        reference: &Reference,
        bytes: &[u8],
        media_type: &str,
    ) -> Result<(), OciDistributionError> {
        let layers = vec![ImageLayer::new(
            bytes.to_vec(),
            media_type.to_string(),
            None,
        )];
        // An artifact, not a runnable image: the fetch path never inspects the
        // config blob's content or media type at all, so its shape is
        // irrelevant. An empty JSON object is the conventional filler for a
        // non-runnable artifact.
        let config = Config::new(
            b"{}".to_vec(),
            "application/vnd.oci.image.config.v1+json".to_string(),
            None,
        );
        self.inner_client()
            .push(reference, &layers, config, &self.registry_auth(), None)
            .await
            .map(|_| ())
    }
}

/// Push `bytes` to `reference`, returning the reference and the content digest.
///
/// `DefaultRegistryClient`'s auth is frozen at `with_basic_auth` time and held
/// for the client's lifetime, so a caller authenticating with a short-lived
/// credential (e.g. a Google Artifact Registry OAuth token) must build a
/// fresh client per push rather than reusing one across pushes.
pub async fn push_pack_with_client<P: RegistryPusher>(
    client: &P,
    reference: &str,
    bytes: &[u8],
) -> Result<PushedPack, OciPushError> {
    let parsed =
        Reference::from_str(reference).map_err(|source| OciPushError::InvalidReference {
            reference: reference.to_string(),
            source,
        })?;
    let media_type = layer_media_type_for(bytes);
    client.push_artifact(&parsed, bytes, media_type).await?;
    Ok(PushedPack {
        reference: reference.to_string(),
        digest: compute_digest(bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci_packs::default_pack_layer_media_types;

    /// Minimal ZIP header — `detect_bundle_archive_kind` in greentic-start
    /// keys off exactly these magic bytes.
    const ZIP_MAGIC: &[u8] = b"PK\x03\x04rest-of-the-archive";
    const UNRECOGNISED: &[u8] = b"\x00\x01\x02\x03 not a known archive";

    #[test]
    fn a_zip_archive_is_stamped_as_a_gtpack_zip() {
        assert_eq!(
            layer_media_type_for(ZIP_MAGIC),
            crate::oci_packs::PACK_LAYER_MEDIA_TYPE_ZIP
        );
    }

    #[test]
    fn an_unrecognised_archive_falls_back_to_octet_stream() {
        assert_eq!(
            layer_media_type_for(UNRECOGNISED),
            crate::oci_packs::PACK_LAYER_MEDIA_TYPE_OCTET_STREAM
        );
    }

    #[test]
    fn an_empty_artifact_still_yields_an_acceptable_media_type() {
        assert_eq!(
            layer_media_type_for(&[]),
            crate::oci_packs::PACK_LAYER_MEDIA_TYPE_OCTET_STREAM
        );
    }

    /// The push/pull contract. If this fails, pushed artifacts are unfetchable
    /// by the very fetcher `greentic-start` uses at container boot.
    #[test]
    fn media_type_is_always_acceptable_to_the_fetcher() {
        let accepted = default_pack_layer_media_types();
        for sample in [ZIP_MAGIC, UNRECOGNISED, &[][..]] {
            let stamped = layer_media_type_for(sample).to_string();
            assert!(
                accepted.contains(&stamped),
                "push stamps {stamped}, which the fetcher does not accept"
            );
        }
    }

    #[test]
    fn pushed_pack_carries_the_content_digest_not_the_manifest_digest() {
        // sha256 of the empty input, the one digest that is trivially checkable.
        let pack = PushedPack {
            reference: "example.test/repo/thing:abc".to_string(),
            digest: crate::oci_packs::compute_digest(&[]),
        };
        assert_eq!(
            pack.digest,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
