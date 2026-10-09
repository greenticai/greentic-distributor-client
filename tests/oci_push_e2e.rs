//! Round-trip: push an artifact, then fetch it back through the SAME fetch path
//! `greentic-start` uses at container boot, and require the bytes to match.
//!
//! Covers BOTH payload shapes `layer_media_type_for` distinguishes:
//! - a SquashFS-magic (`hsqs`) payload, which is what every real `.gtbundle`
//!   push actually is (`greentic-bundle` packs artifacts as SquashFS, not
//!   ZIP) and which falls through to the `application/octet-stream` branch;
//! - a ZIP-magic payload, which takes the dedicated gtpack-zip media type.
//!
//! Without both, only the ZIP branch is proven against a real registry and
//! the branch the product actually takes on every push is untested.
//!
//! Skipped unless `OCI_PUSH_E2E=1`, and needs a registry to talk to. Start one:
//!
//!   docker run -d --rm -p 5000:5000 --name gtc-push-e2e registry:2
//!   OCI_PUSH_E2E=1 cargo test --features pack-push --test oci_push_e2e
//!   docker rm -f gtc-push-e2e
//!
//! The registry is plain HTTP, so the client is built with the loopback host
//! registered as insecure.

#![cfg(feature = "pack-push")]

use greentic_distributor_client::oci_packs::{
    DefaultRegistryClient, OciPackFetcher, PackFetchOptions,
};
use greentic_distributor_client::oci_push::push_pack_with_client;

const REGISTRY: &str = "127.0.0.1:5000";

fn enabled() -> bool {
    std::env::var("OCI_PUSH_E2E").ok().as_deref() == Some("1")
}

/// Push `payload` to a fresh tag, fetch it back through the normal fetch
/// path, and assert both the bytes and the digest round-trip exactly.
async fn assert_round_trips(tag: &str, payload: &[u8]) {
    let reference = format!("{REGISTRY}/greentic/round-trip:{tag}");
    let client = DefaultRegistryClient::with_insecure_registries(vec![REGISTRY.to_string()]);

    let pushed = push_pack_with_client(&client, &reference, payload)
        .await
        .expect("push succeeds");

    let fetcher = OciPackFetcher::with_client(
        DefaultRegistryClient::with_insecure_registries(vec![REGISTRY.to_string()]),
        // Digest pins are enforced by default (see docs/oci_packs.md); this
        // reference is a tag on purpose, so opt in exactly as a tag-based
        // caller would.
        PackFetchOptions {
            allow_tags: true,
            ..PackFetchOptions::default()
        },
    );
    let fetched = fetcher
        .fetch_pack(&reference)
        .await
        .expect("the pushed artifact must be fetchable by the normal fetch path");

    assert_eq!(
        fetched, payload,
        "round-trip changed the bytes: push and fetch disagree about the layer"
    );

    // And the digest the push reported is the digest of what came back.
    //
    // NOTE: `hasher.finalize()` returns a `GenericArray<u8, _>`, which this
    // repo's pinned `sha2` does not implement `LowerHex` for, so this hex-encodes
    // byte-by-byte the same way `oci_packs::compute_digest` does, rather than
    // via `{:x}` on the digest value directly.
    let refetched_digest = {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        let mut hasher = Sha256::new();
        hasher.update(&fetched);
        let digest = hasher.finalize();
        let mut rendered = String::with_capacity("sha256:".len() + digest.len() * 2);
        rendered.push_str("sha256:");
        for byte in digest {
            let _ = write!(&mut rendered, "{byte:02x}");
        }
        rendered
    };
    assert_eq!(pushed.digest, refetched_digest);
}

#[tokio::test]
async fn a_squashfs_pushed_artifact_is_fetchable_byte_for_byte() {
    if !enabled() {
        eprintln!("skipping: set OCI_PUSH_E2E=1 and run a registry on {REGISTRY}");
        return;
    }

    // `hsqs` — the SquashFS magic. This is the shape every real `.gtbundle`
    // push takes (`greentic-bundle` builds bundles as SquashFS archives, see
    // `greentic-bundle/src/build/mod.rs` and `greentic-start/src/bundle_ref.rs`),
    // and it exercises the `application/octet-stream` fallback branch of
    // `layer_media_type_for` — the branch the product actually uses on every
    // push, reached by fallthrough in `select_layer` rather than preference.
    let mut payload = b"hsqs".to_vec();
    payload.extend_from_slice(b"round-trip-fixture-contents-squashfs");

    assert_round_trips("squashfs-v1", &payload).await;
}

#[tokio::test]
async fn a_zip_pushed_artifact_is_fetchable_byte_for_byte() {
    if !enabled() {
        eprintln!("skipping: set OCI_PUSH_E2E=1 and run a registry on {REGISTRY}");
        return;
    }

    // A ZIP-magic payload, so the media type taken is the dedicated gtpack-zip
    // one rather than the octet-stream fallback. Not what a real `.gtbundle`
    // push produces (those are SquashFS, see the SquashFS test above), but
    // kept to prove the ZIP branch of `layer_media_type_for` round-trips too.
    let mut payload = b"PK\x03\x04".to_vec();
    payload.extend_from_slice(b"round-trip-fixture-contents-zip");

    assert_round_trips("zip-v1", &payload).await;
}
