pub mod config;
pub mod error;
pub mod source;
pub mod types;

#[cfg(feature = "dist-client")]
pub mod dist;
#[cfg(feature = "dist-cli")]
pub mod dist_cli;
#[cfg(feature = "http-runtime")]
mod http;
#[cfg(feature = "oci-components")]
pub mod oci_components;
#[cfg(feature = "pack-fetch")]
pub mod oci_packs;
#[cfg(feature = "runner-api")]
pub mod runner_api;
mod wit_client;

pub use config::DistributorClientConfig;
#[cfg(feature = "dist-client")]
pub use dist::{
    DistClient, DistOptions, InjectedResolution, LockHint, ResolveRefInjector, ResolveRefRequest,
    ResolvedArtifact,
};
pub use error::DistributorError;
#[cfg(feature = "http-runtime")]
pub use http::HttpDistributorClient;
#[cfg(feature = "oci-components")]
pub use oci_components::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentError,
    OciComponentResolver, ResolvedComponent,
};
#[cfg(feature = "pack-fetch")]
pub use oci_packs::{OciPackError, OciPackFetcher, PackFetchOptions, ResolvedPack};
#[cfg(feature = "pack-fetch")]
pub use oci_packs::{fetch_pack, fetch_pack_to_cache};
pub use source::{ChainedDistributorSource, DistributorSource};
pub use types::*;
pub use wit_client::{
    DistributorApiBindings, GeneratedDistributorApiBindings, WitDistributorClient,
};

use async_trait::async_trait;

/// Trait implemented by clients that can communicate with a Distributor.
#[async_trait]
pub trait DistributorClient: Send + Sync {
    async fn resolve_component(
        &self,
        req: ResolveComponentRequest,
    ) -> Result<ResolveComponentResponse, DistributorError>;

    async fn get_pack_status(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<serde_json::Value, DistributorError>;

    async fn get_pack_status_v2(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<PackStatusResponse, DistributorError>;

    async fn warm_pack(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<(), DistributorError>;
}
