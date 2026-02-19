use crate::{ArtifactLocation, CacheInfo, SignatureSummary};
use crate::{ComponentDigest, ComponentStatus};
use crate::{
    DistributorClient, DistributorEnvironmentId, DistributorError, PackStatusResponse,
    ResolveComponentRequest, ResolveComponentResponse, SecretFormat, SecretKey, SecretRequirement,
    SecretScope, TenantCtx,
};
use anyhow::anyhow;
use async_trait::async_trait;
use greentic_interfaces_guest::distributor_api as wit;
#[cfg(target_arch = "wasm32")]
use greentic_interfaces_guest::distributor_api::DistributorApiImports;
use serde_json::Value;

#[async_trait]
pub trait DistributorApiBindings: Send + Sync {
    async fn resolve_component(
        &self,
        req: wit::ResolveComponentRequest,
    ) -> Result<wit::ResolveComponentResponse, anyhow::Error>;

    async fn get_pack_status(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<String, anyhow::Error>;

    async fn get_pack_status_v2(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<wit::PackStatusResponse, anyhow::Error>;

    async fn warm_pack(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<(), anyhow::Error>;
}

#[derive(Clone)]
pub struct WitDistributorClient<B: DistributorApiBindings> {
    bindings: B,
}

impl<B: DistributorApiBindings> WitDistributorClient<B> {
    pub fn new(bindings: B) -> Self {
        Self { bindings }
    }
}

/// Default bindings that call the generated distributor-api imports when running as a WASM guest.
#[derive(Clone, Default)]
pub struct GeneratedDistributorApiBindings;

#[async_trait]
impl DistributorApiBindings for GeneratedDistributorApiBindings {
    async fn resolve_component(
        &self,
        req: wit::ResolveComponentRequest,
    ) -> Result<wit::ResolveComponentResponse, anyhow::Error> {
        #[cfg(target_arch = "wasm32")]
        {
            let api = DistributorApiImports::new();
            Ok(api.resolve_component(&req))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = req;
            Err(anyhow!(
                "distributor-api imports are only available on wasm32 targets"
            ))
        }
    }

    async fn get_pack_status(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<String, anyhow::Error> {
        #[cfg(target_arch = "wasm32")]
        {
            let api = DistributorApiImports::new();
            Ok(api.get_pack_status(
                &tenant_id.to_string(),
                &environment_id.to_string(),
                &pack_id.to_string(),
            ))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (tenant_id, environment_id, pack_id);
            Err(anyhow!(
                "distributor-api imports are only available on wasm32 targets"
            ))
        }
    }

    async fn get_pack_status_v2(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<wit::PackStatusResponse, anyhow::Error> {
        #[cfg(target_arch = "wasm32")]
        {
            let api = DistributorApiImports::new();
            Ok(api.get_pack_status_v2(
                &tenant_id.to_string(),
                &environment_id.to_string(),
                &pack_id.to_string(),
            ))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (tenant_id, environment_id, pack_id);
            Err(anyhow!(
                "distributor-api imports are only available on wasm32 targets"
            ))
        }
    }

    async fn warm_pack(
        &self,
        tenant_id: &str,
        environment_id: &str,
        pack_id: &str,
    ) -> Result<(), anyhow::Error> {
        #[cfg(target_arch = "wasm32")]
        {
            let api = DistributorApiImports::new();
            api.warm_pack(
                &tenant_id.to_string(),
                &environment_id.to_string(),
                &pack_id.to_string(),
            );
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (tenant_id, environment_id, pack_id);
            Err(anyhow!(
                "distributor-api imports are only available on wasm32 targets"
            ))
        }
    }
}

#[async_trait]
impl<B> DistributorClient for WitDistributorClient<B>
where
    B: DistributorApiBindings,
{
    async fn resolve_component(
        &self,
        req: ResolveComponentRequest,
    ) -> Result<ResolveComponentResponse, DistributorError> {
        let wit_req = to_wit_request(req)?;
        let resp = self
            .bindings
            .resolve_component(wit_req)
            .await
            .map_err(|e| {
                // TODO: once distributor-api exposes structured errors, map to
                // NotFound/PermissionDenied instead of a generic Wit error.
                DistributorError::Wit(e.to_string())
            })?;
        from_wit_response(resp)
    }

    async fn get_pack_status(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<Value, DistributorError> {
        let payload = self
            .bindings
            .get_pack_status(tenant.tenant_id.as_str(), env.as_str(), pack_id)
            .await
            .map_err(|e| DistributorError::Wit(e.to_string()))?;
        serde_json::from_str(&payload).map_err(DistributorError::Serde)
    }

    async fn get_pack_status_v2(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<PackStatusResponse, DistributorError> {
        let payload = self
            .bindings
            .get_pack_status_v2(tenant.tenant_id.as_str(), env.as_str(), pack_id)
            .await
            .map_err(|e| DistributorError::Wit(e.to_string()))?;
        from_wit_pack_status(payload)
    }

    async fn warm_pack(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<(), DistributorError> {
        self.bindings
            .warm_pack(tenant.tenant_id.as_str(), env.as_str(), pack_id)
            .await
            .map_err(|e| DistributorError::Wit(e.to_string()))
    }
}

fn to_wit_request(
    req: ResolveComponentRequest,
) -> Result<wit::ResolveComponentRequest, DistributorError> {
    Ok(wit::ResolveComponentRequest {
        tenant_id: req.tenant.tenant_id.to_string(),
        environment_id: req.environment_id.as_str().to_string(),
        pack_id: req.pack_id,
        component_id: req.component_id,
        version: req.version,
        extra: serde_json::to_string(&req.extra)?,
    })
}

fn from_wit_response(
    resp: wit::ResolveComponentResponse,
) -> Result<ResolveComponentResponse, DistributorError> {
    let status = match resp.component_status {
        wit::ComponentStatus::Pending => ComponentStatus::Pending,
        wit::ComponentStatus::Ready => ComponentStatus::Ready,
        wit::ComponentStatus::Failed => ComponentStatus::Failed {
            reason: "failed".to_string(),
        },
    };
    let artifact = match resp.artifact_location.kind.as_str() {
        "file" | "file_path" => ArtifactLocation::FilePath {
            path: resp.artifact_location.value,
        },
        "oci" | "oci_reference" => ArtifactLocation::OciReference {
            reference: resp.artifact_location.value,
        },
        _ => ArtifactLocation::DistributorInternal {
            handle: resp.artifact_location.value,
        },
    };
    let signature = SignatureSummary {
        verified: resp.signature_summary.verified,
        signer: resp.signature_summary.signer,
        extra: serde_json::from_str(&resp.signature_summary.extra)?,
    };
    let cache = CacheInfo {
        size_bytes: resp.cache_info.size_bytes,
        last_used_utc: resp.cache_info.last_used_utc,
        last_refreshed_utc: resp.cache_info.last_refreshed_utc,
    };
    Ok(ResolveComponentResponse {
        status,
        digest: ComponentDigest(resp.digest),
        artifact,
        signature,
        cache,
        secret_requirements: from_wit_secret_requirements(resp.secret_requirements)?,
    })
}

fn from_wit_pack_status(
    resp: wit::PackStatusResponse,
) -> Result<PackStatusResponse, DistributorError> {
    Ok(PackStatusResponse {
        status: resp.status,
        secret_requirements: from_wit_secret_requirements(resp.secret_requirements)?,
        extra: serde_json::from_str(&resp.extra)?,
    })
}

fn from_wit_secret_requirements(
    reqs: impl WitSecretRequirementsExt,
) -> Result<Option<Vec<SecretRequirement>>, DistributorError> {
    reqs.into_optional_vec()
        .map(|requirements| {
            requirements
                .into_iter()
                .map(from_wit_secret_requirement)
                .collect()
        })
        .transpose()
}

trait WitSecretRequirementsExt {
    fn into_optional_vec(self) -> Option<Vec<wit::SecretRequirement>>;
}

impl WitSecretRequirementsExt for Vec<wit::SecretRequirement> {
    fn into_optional_vec(self) -> Option<Vec<wit::SecretRequirement>> {
        Some(self)
    }
}

impl WitSecretRequirementsExt for Option<Vec<wit::SecretRequirement>> {
    fn into_optional_vec(self) -> Option<Vec<wit::SecretRequirement>> {
        self
    }
}

fn from_wit_secret_requirement(
    req: wit::SecretRequirement,
) -> Result<SecretRequirement, DistributorError> {
    let key = SecretKey::parse(&req.key).map_err(|e| {
        DistributorError::InvalidResponse(format!("invalid secret key `{}`: {e}", req.key))
    })?;
    let scope = req.scope.map(|scope| SecretScope {
        env: scope.env,
        tenant: scope.tenant,
        team: scope.team,
    });
    let format = req.format.map(|format| match format as u8 {
        0 => SecretFormat::Bytes,
        1 => SecretFormat::Text,
        2 => SecretFormat::Json,
        _ => unreachable!("unexpected WIT secret format discriminant"),
    });
    let schema = match req.schema {
        Some(schema) => Some(serde_json::from_str(&schema)?),
        None => None,
    };
    let mut requirement = SecretRequirement::default();
    requirement.key = key;
    requirement.required = req.required;
    requirement.description = req.description;
    requirement.scope = scope;
    requirement.format = format;
    requirement.schema = schema;
    requirement.examples = req.examples;
    Ok(requirement)
}
