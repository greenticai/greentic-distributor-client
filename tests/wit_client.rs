use async_trait::async_trait;
use greentic_distributor_client::{
    ArtifactLocation, ComponentDigest, ComponentStatus, DistributorApiBindings, DistributorClient,
    DistributorEnvironmentId, EnvId, ResolveComponentRequest, TenantCtx, TenantId,
    WitDistributorClient,
};
use greentic_interfaces_guest::distributor_api as wit;
use serde_json::json;

#[derive(Clone)]
struct DummyBindings;

#[async_trait]
impl DistributorApiBindings for DummyBindings {
    async fn resolve_component(
        &self,
        req: wit::ResolveComponentRequest,
    ) -> Result<wit::ResolveComponentResponse, anyhow::Error> {
        assert_eq!(req.pack_id, "pack-123");
        let response = wit::ResolveComponentResponse {
            component_status: wit::ComponentStatus::Ready,
            digest: "sha256:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                .into(),
            artifact_location: wit::ArtifactLocation {
                kind: "file".into(),
                value: "/tmp/component.wasm".into(),
            },
            signature_summary: wit::SignatureSummary {
                verified: true,
                signer: "signer".into(),
                extra: "{\"sig\":\"ok\"}".into(),
            },
            cache_info: wit::CacheInfo {
                size_bytes: 12,
                last_used_utc: "2024-01-01T00:00:00Z".into(),
                last_refreshed_utc: "2024-01-01T00:00:00Z".into(),
            },
            secret_requirements: vec![wit::SecretRequirement {
                key: "TEST_API_KEY".into(),
                required: true,
                description: Some("API key".into()),
                scope: None,
                format: None,
                schema: None,
                examples: vec!["abc123".into()],
            }],
        };
        Ok(response)
    }

    async fn get_pack_status(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<String, anyhow::Error> {
        Ok(json!({
            "tenant": tenant_id,
            "env": environment_id,
            "pack": pack_id
        })
        .to_string())
    }

    async fn get_pack_status_v2(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<wit::PackStatusResponse, anyhow::Error> {
        Ok(wit::PackStatusResponse {
            status: format!("status-{pack_id}"),
            secret_requirements: vec![wit::SecretRequirement {
                key: "TEST_API_KEY".into(),
                required: true,
                description: Some("pack status key".into()),
                scope: None,
                format: None,
                schema: None,
                examples: vec![],
            }],
            extra: json!({
                "tenant": tenant_id,
                "env": environment_id
            })
            .to_string(),
        })
    }

    async fn warm_pack(
        &self,
        _tenant_id: &str,
        _environment_id: &str,
        _pack_id: &str,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn wit_resolve_component_translation() {
    let client = WitDistributorClient::new(DummyBindings);
    let resp = client
        .resolve_component(ResolveComponentRequest {
            tenant: TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            environment_id: DistributorEnvironmentId::from("env-1"),
            pack_id: "pack-123".into(),
            component_id: "comp-x".into(),
            version: "1.0.0".into(),
            extra: json!({"note": "test"}),
        })
        .await
        .unwrap();
    assert_eq!(resp.status, ComponentStatus::Ready);
    assert!(matches!(resp.artifact, ArtifactLocation::FilePath { .. }));
    assert_eq!(
        resp.digest,
        ComponentDigest(
            "sha256:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into()
        )
    );
    let reqs = resp
        .secret_requirements
        .expect("secret requirements present");
    assert_eq!(reqs.len(), 1);
    let req = &reqs[0];
    assert_eq!(req.key.as_str(), "TEST_API_KEY");
    assert!(req.required);
    assert_eq!(req.scope, None);
    assert_eq!(req.format, None);
}

#[tokio::test]
async fn wit_get_pack_status_json_round_trip() {
    let client = WitDistributorClient::new(DummyBindings);
    let value = client
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
    assert_eq!(value["tenant"], "tenant-a");
}

#[tokio::test]
async fn wit_pack_status_v2_maps_secret_requirements() {
    let client = WitDistributorClient::new(DummyBindings);
    let status = client
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
    assert_eq!(status.status, "status-pack-123");
    let secrets = status
        .secret_requirements
        .expect("secret requirements threaded through");
    assert_eq!(secrets.len(), 1);
    assert_eq!(secrets[0].key.as_str(), "TEST_API_KEY");
}

#[tokio::test]
async fn wit_warm_pack_ok() {
    let client = WitDistributorClient::new(DummyBindings);
    client
        .warm_pack(
            &TenantCtx::new(
                EnvId::try_from("dev").unwrap(),
                TenantId::try_from("tenant-a").unwrap(),
            ),
            &DistributorEnvironmentId::from("env-1"),
            "pack-123",
        )
        .await
        .unwrap();
}
