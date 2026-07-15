#![cfg(feature = "http-runtime")]

use greentic_distributor_client::{
    CacheInfo, ComponentDigest, ComponentStatus, DistributorClient, DistributorClientConfig,
    DistributorEnvironmentId, EnvId, HttpDistributorClient, ResolveComponentRequest,
    ResolveComponentResponse, SecretKey, SecretRequirement, SecretScope, SignatureSummary,
    TenantCtx, TenantId,
};
use httpmock::prelude::*;
use reqwest::StatusCode;
use serde_json::json;
use std::panic;
use std::time::Duration;

fn sample_request() -> ResolveComponentRequest {
    let tenant = TenantCtx::new(
        EnvId::try_from("dev").unwrap(),
        TenantId::try_from("tenant-a").unwrap(),
    );
    ResolveComponentRequest {
        tenant,
        environment_id: DistributorEnvironmentId::from("env-1"),
        pack_id: "pack-123".into(),
        component_id: "component-x".into(),
        version: "1.0.0".into(),
        extra: json!({"note": "test"}),
    }
}

fn sample_secret_requirement() -> SecretRequirement {
    let mut req = SecretRequirement::default();
    req.key = SecretKey::parse("TEST_API_KEY").unwrap();
    req.required = true;
    req.description = Some("API key".into());
    req.scope = Some(SecretScope {
        env: "dev".into(),
        tenant: "tenant-a".into(),
        team: None,
    });
    req.examples = vec!["abc123".into()];
    req
}

fn sample_response() -> ResolveComponentResponse {
    ResolveComponentResponse {
        status: ComponentStatus::Ready,
        digest: ComponentDigest(
            "sha256:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into(),
        ),
        artifact: greentic_distributor_client::ArtifactLocation::FilePath {
            path: "/tmp/component.wasm".into(),
        },
        signature: SignatureSummary {
            verified: true,
            signer: "signer".into(),
            extra: json!({"sig": "ok"}),
        },
        cache: CacheInfo {
            size_bytes: 42,
            last_used_utc: "2024-01-01T00:00:00Z".into(),
            last_refreshed_utc: "2024-01-01T00:00:00Z".into(),
        },
        secret_requirements: Some(vec![sample_secret_requirement()]),
    }
}

fn start_server() -> Option<MockServer> {
    panic::catch_unwind(MockServer::start).ok()
}

#[tokio::test]
async fn http_resolve_component_success() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/distributor-api/resolve-component")
            .json_body(json!({
                "tenant": {
                    "env": "dev",
                    "tenant": "tenant-a",
                    "tenant_id": "tenant-a",
                    "attempt": 0
                },
                "environment_id": "env-1",
                "pack_id": "pack-123",
                "component_id": "component-x",
                "version": "1.0.0",
                "extra": {"note": "test"}
            }));
        then.status(200)
            .json_body(serde_json::to_value(sample_response()).unwrap());
    });

    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: Some(Duration::from_secs(5)),
    };

    let client = HttpDistributorClient::new(config).unwrap();
    let resp = client.resolve_component(sample_request()).await.unwrap();
    mock.assert();
    assert_eq!(resp.status, ComponentStatus::Ready);
    assert!(resp.digest.is_sha256_like());
    let secrets = resp
        .secret_requirements
        .expect("secret requirements are threaded through");
    assert_eq!(secrets.len(), 1);
    assert_eq!(secrets[0].key.as_str(), "TEST_API_KEY");
}

#[tokio::test]
async fn http_resolve_component_allows_missing_secret_requirements() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mut response = serde_json::to_value(sample_response()).unwrap();
    response
        .as_object_mut()
        .expect("object response")
        .remove("secret_requirements");
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/distributor-api/resolve-component")
            .json_body(json!({
                "tenant": {
                    "env": "dev",
                    "tenant": "tenant-a",
                    "tenant_id": "tenant-a",
                    "attempt": 0
                },
                "environment_id": "env-1",
                "pack_id": "pack-123",
                "component_id": "component-x",
                "version": "1.0.0",
                "extra": {"note": "test"}
            }));
        then.status(200).json_body(response.clone());
    });

    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: Some(Duration::from_secs(5)),
    };

    let client = HttpDistributorClient::new(config).unwrap();
    let resp = client.resolve_component(sample_request()).await.unwrap();
    mock.assert();
    assert!(resp.secret_requirements.is_none());
}

#[tokio::test]
async fn http_get_pack_status_json() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/distributor-api/pack-status")
            .query_param("tenant_id", "tenant-a")
            .query_param("environment_id", "env-1")
            .query_param("pack_id", "pack-123");
        then.status(200)
            .json_body(json!({"ready": 2, "pending": 0}));
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    let resp = client
        .get_pack_status(
            &TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            &DistributorEnvironmentId::from("env-1"),
            "pack-123",
        )
        .await
        .unwrap();
    mock.assert();
    assert_eq!(resp["ready"], 2);
}

#[tokio::test]
async fn http_get_pack_status_v2_secret_requirements() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/distributor-api/pack-status-v2")
            .query_param("tenant_id", "tenant-a")
            .query_param("environment_id", "env-1")
            .query_param("pack_id", "pack-123");
        then.status(200).json_body(json!({
            "status": "ready",
            "secret_requirements": [sample_secret_requirement()],
            "extra": {"ready": 1}
        }));
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    let resp = client
        .get_pack_status_v2(
            &TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            &DistributorEnvironmentId::from("env-1"),
            "pack-123",
        )
        .await
        .unwrap();
    mock.assert();
    assert_eq!(resp.status, "ready");
    let secrets = resp
        .secret_requirements
        .expect("parsed secret requirements");
    assert_eq!(secrets.len(), 1);
    assert_eq!(secrets[0].key.as_str(), "TEST_API_KEY");
    assert_eq!(resp.extra["ready"], 1);
}

#[tokio::test]
async fn http_sets_auth_header() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/distributor-api/pack-status")
            .header("authorization", "Bearer token123")
            .query_param("tenant_id", "tenant-a")
            .query_param("environment_id", "env-1")
            .query_param("pack_id", "pack-123");
        then.status(200).json_body(json!({"ready": 1}));
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: Some("token123".into()),
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    client
        .get_pack_status(
            &TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            &DistributorEnvironmentId::from("env-1"),
            "pack-123",
        )
        .await
        .unwrap();
    mock.assert();
}

#[tokio::test]
async fn http_resolve_component_not_found_maps_error() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(POST).path("/distributor-api/resolve-component");
        then.status(404);
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    let err = client
        .resolve_component(sample_request())
        .await
        .unwrap_err();
    mock.assert();
    assert!(matches!(
        err,
        greentic_distributor_client::DistributorError::NotFound
    ));
}

#[tokio::test]
async fn http_maps_server_error_status() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(POST).path("/distributor-api/resolve-component");
        then.status(500).body("boom");
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    let err = client
        .resolve_component(sample_request())
        .await
        .unwrap_err();
    mock.assert();
    assert!(matches!(
        err,
        greentic_distributor_client::DistributorError::Status { status, .. }
        if status == StatusCode::INTERNAL_SERVER_ERROR
    ));
}

#[tokio::test]
async fn http_handles_bad_json() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    let mock = server.mock(|when, then| {
        when.method(POST).path("/distributor-api/resolve-component");
        then.status(200).body("not-json");
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::new(config).unwrap();
    let err = client
        .resolve_component(sample_request())
        .await
        .unwrap_err();
    mock.assert();
    assert!(
        matches!(
            err,
            greentic_distributor_client::DistributorError::Serde(_)
                | greentic_distributor_client::DistributorError::Http(_)
        ),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn http_with_client_uses_supplied_client() {
    let Some(server) = start_server() else {
        eprintln!("skipping: unable to bind mock server in this environment");
        return;
    };
    // A client with a distinctive user-agent: if the request carries it, the
    // supplied client (not an internally-built default) served the call — this
    // is the seam an mTLS client is injected through.
    let injected = reqwest::Client::builder()
        .user_agent("greentic-mtls-probe/1")
        .build()
        .unwrap();
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/distributor-api/pack-status")
            .header("user-agent", "greentic-mtls-probe/1")
            .query_param("tenant_id", "tenant-a")
            .query_param("environment_id", "env-1")
            .query_param("pack_id", "pack-123");
        then.status(200).json_body(json!({"ready": 3}));
    });
    let config = DistributorClientConfig {
        base_url: Some(server.base_url()),
        environment_id: DistributorEnvironmentId::from("env-1"),
        tenant: TenantCtx::new(
            EnvId::try_from("dev").unwrap(),
            TenantId::try_from("tenant-a").unwrap(),
        ),
        auth_token: None,
        extra_headers: None,
        request_timeout: None,
    };
    let client = HttpDistributorClient::with_client(injected, config);
    let resp = client
        .get_pack_status(
            &TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            &DistributorEnvironmentId::from("env-1"),
            "pack-123",
        )
        .await
        .unwrap();
    mock.assert();
    assert_eq!(resp["ready"], 3);
}

#[test]
fn component_digest_sha256_like() {
    let digest = ComponentDigest(
        "sha256:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into(),
    );
    assert!(digest.is_sha256_like());
    let bad = ComponentDigest("md5:abc".into());
    assert!(!bad.is_sha256_like());
}
