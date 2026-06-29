//! Integration tests for the store extension artifact transport.
//!
//! These exercise the real blocking HTTP path against a local `httpmock`
//! server, so they only build when the `dist-client` feature (which gates the
//! `store_ext` module and pulls in `reqwest`) is enabled.
#![cfg(feature = "dist-client")]

use greentic_distributor_client::store_ext::{fetch_store_extension, store_artifact_url};
use sha2::{Digest, Sha256};

/// Start a mock server, skipping the test when the environment forbids binding.
fn start_server() -> Option<httpmock::MockServer> {
    std::panic::catch_unwind(httpmock::MockServer::start).ok()
}

#[test]
fn download_verifies_and_caches_artifact() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let body = b"a-real-gtxpack-archive".to_vec();
    let sha = hex::encode(Sha256::digest(&body));

    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/extensions/demo-ext/0.4.0/artifact");
        then.status(200)
            .header("x-artifact-sha256", &sha)
            .body(&body);
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let bytes = fetch_store_extension(&server.base_url(), "demo-ext", "0.4.0", cache.path(), false)
        .expect("download should succeed");

    mock.assert();
    assert_eq!(bytes, body);

    // A second offline fetch is served from the ref-keyed cache (no network).
    let cached = fetch_store_extension(&server.base_url(), "demo-ext", "0.4.0", cache.path(), true)
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
            .path("/api/v1/extensions/bad-ext/0.4.0/artifact");
        then.status(200)
            // Advertise a digest that does not match the body.
            .header("x-artifact-sha256", "deadbeef")
            .body("some-bytes");
    });

    let cache = tempfile::tempdir().expect("tempdir");
    let err = fetch_store_extension(&server.base_url(), "bad-ext", "0.4.0", cache.path(), false)
        .expect_err("sha mismatch should bail");

    mock.assert();
    let message = err.to_string();
    assert!(
        message.contains("integrity check failed") && message.contains("bad-ext@0.4.0"),
        "unexpected error: {message}"
    );
}

#[test]
fn url_shape_is_stable() {
    assert_eq!(
        store_artifact_url("https://store.example/", "n", "1.0.0"),
        "https://store.example/api/v1/extensions/n/1.0.0/artifact"
    );
}
