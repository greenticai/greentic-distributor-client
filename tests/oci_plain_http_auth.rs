//! Transport × credentials, against a real (mock) plain-HTTP registry.
//!
//! Both `DefaultRegistryClient`s (packs and components) must honour all four
//! combinations:
//!
//! | transport        | credentials | expected                                     |
//! |------------------|-------------|----------------------------------------------|
//! | HTTPS (default)  | anonymous   | TLS attempt; never a plain-HTTP request      |
//! | HTTPS (default)  | basic       | TLS attempt; credentials never sent in clear |
//! | insecure (listed)| anonymous   | plain-HTTP pull, no `Authorization` header   |
//! | insecure (listed)| basic       | plain-HTTP pull presenting basic auth        |
//!
//! The HTTPS rows run against the same plain-HTTP mock: the pull must fail and
//! the mock must see ZERO requests. That is the proof that plain HTTP is never
//! inferred — neither from credentials nor as a fallback.

#![cfg(any(feature = "pack-fetch", feature = "oci-components"))]

use base64::Engine as _;
use greentic_distributor_client::oci_client::Reference;
use httpmock::Mock;
use httpmock::prelude::*;
use sha2::{Digest, Sha256};

const USER: &str = "robot";
const PASSWORD: &str = "pl41n-http-s3cret";
const LAYER_MEDIA_TYPE: &str = "application/vnd.greentic.test.layer.v1";
const LAYER_BYTES: &[u8] = b"plain-http-auth-layer";
const CONFIG_BYTES: &[u8] = b"{}";

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn basic_header(user: &str, password: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
    )
}

fn manifest_body() -> String {
    serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": sha256(CONFIG_BYTES),
            "size": CONFIG_BYTES.len(),
        },
        "layers": [{
            "mediaType": LAYER_MEDIA_TYPE,
            "digest": sha256(LAYER_BYTES),
            "size": LAYER_BYTES.len(),
        }],
    })
    .to_string()
}

/// Mount a mock registry serving `greentic/demo:v1` on `server`. With
/// `password: Some(..)` it behaves like `registry:2` behind htpasswd: a `Basic`
/// challenge on `/v2/` and 401 for any content request lacking the expected
/// credentials. Returns every mock so a test can count what reached it.
async fn mount_registry<'s>(server: &'s MockServer, password: Option<&str>) -> Vec<Mock<'s>> {
    let manifest = manifest_body();
    let manifest_digest = sha256(manifest.as_bytes());
    let paths = [
        (
            "/v2/greentic/demo/manifests/v1".to_string(),
            manifest.into_bytes(),
            true,
        ),
        (
            format!("/v2/greentic/demo/blobs/{}", sha256(CONFIG_BYTES)),
            CONFIG_BYTES.to_vec(),
            false,
        ),
        (
            format!("/v2/greentic/demo/blobs/{}", sha256(LAYER_BYTES)),
            LAYER_BYTES.to_vec(),
            false,
        ),
    ];

    let mut mocks = Vec::new();
    let challenge = password.is_some();
    mocks.push(
        server
            .mock_async(move |when, then| {
                when.method(GET).path("/v2/");
                if challenge {
                    then.status(401)
                        .header("WWW-Authenticate", "Basic realm=\"gtc\"");
                } else {
                    then.status(200);
                }
            })
            .await,
    );

    for (path, body, is_manifest) in paths {
        let digest = manifest_digest.clone();
        let expected = password.map(|p| basic_header(USER, p));
        let refused_path = path.clone();
        mocks.push(
            server
                .mock_async(move |when, then| {
                    let when = when.method(GET).path(path);
                    let _ = match expected {
                        Some(header) => when.header("authorization", header),
                        None => when.header_missing("authorization"),
                    };
                    let then = then.status(200).body(body);
                    if is_manifest {
                        then.header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                            .header("Docker-Content-Digest", digest);
                    }
                })
                .await,
        );
        // Anything that reached a content path without matching the mock
        // above (wrong / missing / unexpected credentials) is refused.
        mocks.push(
            server
                .mock_async(move |when, then| {
                    when.method(GET).path(refused_path);
                    then.status(401)
                        .header("WWW-Authenticate", "Basic realm=\"gtc\"");
                })
                .await,
        );
    }
    mocks
}

async fn total_hits(mocks: &[Mock<'_>]) -> usize {
    let mut total = 0;
    for mock in mocks {
        total += mock.calls_async().await;
    }
    total
}

fn registry_of(server: &MockServer) -> String {
    server.address().to_string()
}

fn reference_of(server: &MockServer) -> Reference {
    format!("{}/greentic/demo:v1", registry_of(server))
        .parse()
        .expect("reference")
}

fn assert_secret_free(err: &dyn std::fmt::Debug) {
    let rendered = format!("{err:?}");
    assert!(
        !rendered.contains(PASSWORD),
        "error must never carry the password: {rendered}"
    );
}

macro_rules! combination_tests {
    ($module:ident, $client:path, $trait:path) => {
        mod $module {
            use super::*;
            use greentic_distributor_client::oci_retry::RetryPolicy;
            use $client as Client;
            use $trait as _;

            fn quiet(client: Client) -> Client {
                client.with_retry_policy(RetryPolicy::disabled())
            }

            #[tokio::test]
            async fn anonymous_over_insecure_transport_pulls_without_credentials() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, None).await;
                let client = quiet(Client::with_insecure_registries(vec![registry_of(&server)]));
                assert!(client.uses_plain_http_for(&registry_of(&server)));
                assert!(!client.has_credentials());

                let image = client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect("anonymous plain-HTTP pull");
                assert!(image.layers.iter().any(|l| l.data == LAYER_BYTES));
                assert!(total_hits(&mocks).await > 0);
            }

            #[tokio::test]
            async fn basic_auth_over_insecure_transport_pulls_with_credentials() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, Some(PASSWORD)).await;
                let client = quiet(
                    Client::with_basic_auth(USER, PASSWORD)
                        .try_with_insecure_transport(vec![registry_of(&server)])
                        .expect("bare host:port"),
                );
                assert!(client.uses_plain_http_for(&registry_of(&server)));
                assert!(client.has_credentials());

                let image = client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect("authenticated plain-HTTP pull");
                assert!(image.layers.iter().any(|l| l.data == LAYER_BYTES));
                assert!(total_hits(&mocks).await > 0);
            }

            #[tokio::test]
            async fn wrong_password_over_insecure_transport_fails_without_echoing_it() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, Some("the-real-one")).await;
                let client = quiet(
                    Client::with_basic_auth(USER, PASSWORD)
                        .with_insecure_transport(vec![registry_of(&server)]),
                );
                let err = client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect_err("401 must surface");
                assert_secret_free(&err);
                assert!(!err.to_string().contains(PASSWORD));
                assert!(total_hits(&mocks).await > 0);
            }

            #[tokio::test]
            async fn anonymous_https_never_downgrades_to_plain_http() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, None).await;
                let client = quiet(<Client as Default>::default());
                assert!(!client.uses_plain_http_for(&registry_of(&server)));

                client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect_err("HTTPS against a plain-HTTP registry must fail");
                assert_eq!(total_hits(&mocks).await, 0);
            }

            #[tokio::test]
            async fn basic_auth_https_never_sends_credentials_in_clear() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, Some(PASSWORD)).await;
                let client = quiet(Client::with_basic_auth(USER, PASSWORD));
                assert!(!client.uses_plain_http_for(&registry_of(&server)));

                let err = client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect_err("HTTPS against a plain-HTTP registry must fail");
                assert_secret_free(&err);
                assert_eq!(total_hits(&mocks).await, 0);
            }

            #[tokio::test]
            async fn only_the_listed_registry_is_downgraded() {
                let server = MockServer::start_async().await;
                let mocks = mount_registry(&server, Some(PASSWORD)).await;
                let client = quiet(
                    Client::with_basic_auth(USER, PASSWORD)
                        .with_insecure_transport(vec!["some-other-registry:5000".into()]),
                );
                assert!(!client.uses_plain_http_for(&registry_of(&server)));

                client
                    .pull(&reference_of(&server), &[LAYER_MEDIA_TYPE])
                    .await
                    .expect_err("an unlisted registry stays on HTTPS");
                assert_eq!(total_hits(&mocks).await, 0);
            }

            #[test]
            fn a_scheme_prefixed_entry_is_refused_instead_of_silently_staying_https() {
                let err = Client::with_basic_auth(USER, PASSWORD)
                    .try_with_insecure_transport(vec!["http://registry.local:5000".into()])
                    .err()
                    .expect("scheme-prefixed entry must be refused");
                assert_eq!(err.entry(), "http://registry.local:5000");
            }
        }
    };
}

#[cfg(feature = "pack-fetch")]
combination_tests!(
    packs,
    greentic_distributor_client::oci_packs::DefaultRegistryClient,
    greentic_distributor_client::oci_packs::RegistryClient
);

#[cfg(feature = "oci-components")]
combination_tests!(
    components,
    greentic_distributor_client::oci_components::DefaultRegistryClient,
    greentic_distributor_client::oci_components::RegistryClient
);
