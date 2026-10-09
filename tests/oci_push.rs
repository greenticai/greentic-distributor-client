//! Unit tests for the push path. No network: a recording `RegistryPusher`
//! captures exactly what would have been sent.

#![cfg(feature = "pack-push")]

use std::sync::Mutex;

use async_trait::async_trait;
use greentic_distributor_client::oci_push::{RegistryPusher, push_pack_with_client};
use oci_client::Reference;
use oci_client::errors::OciDistributionError;

#[derive(Default)]
struct RecordingPusher {
    calls: Mutex<Vec<(String, Vec<u8>, String)>>,
}

#[async_trait]
impl RegistryPusher for RecordingPusher {
    async fn push_artifact(
        &self,
        reference: &Reference,
        bytes: &[u8],
        media_type: &str,
    ) -> Result<(), OciDistributionError> {
        self.calls.lock().unwrap().push((
            reference.whole(),
            bytes.to_vec(),
            media_type.to_string(),
        ));
        Ok(())
    }
}

const ZIP_BYTES: &[u8] = b"PK\x03\x04a-tiny-fake-archive";

#[tokio::test]
async fn push_sends_the_bytes_verbatim_under_an_acceptable_media_type() {
    let pusher = RecordingPusher::default();
    let pushed = push_pack_with_client(&pusher, "example.test/repo/thing:v1", ZIP_BYTES)
        .await
        .expect("push succeeds");

    let calls = pusher.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "exactly one push");
    let (reference, bytes, media_type) = &calls[0];
    assert_eq!(reference, "example.test/repo/thing:v1");
    assert_eq!(bytes.as_slice(), ZIP_BYTES, "bytes must not be re-encoded");
    assert_eq!(media_type, "application/vnd.greentic.gtpack.v1+zip");
    assert_eq!(pushed.reference, "example.test/repo/thing:v1");
}

#[tokio::test]
async fn the_returned_digest_is_over_the_pushed_bytes() {
    let pusher = RecordingPusher::default();
    let pushed = push_pack_with_client(&pusher, "example.test/repo/thing:v1", b"")
        .await
        .expect("push succeeds");
    assert_eq!(
        pushed.digest,
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[tokio::test]
async fn a_malformed_reference_is_rejected_before_any_push_is_attempted() {
    let pusher = RecordingPusher::default();
    let err = push_pack_with_client(&pusher, "not a valid reference", ZIP_BYTES)
        .await
        .expect_err("malformed reference must be rejected");
    assert!(
        format!("{err}").contains("reference"),
        "error should name the reference problem: {err}"
    );
    assert!(
        pusher.calls.lock().unwrap().is_empty(),
        "nothing may be pushed after a reference failure"
    );
}
