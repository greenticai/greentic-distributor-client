use crate::{
    DistributorClient, DistributorClientConfig, DistributorEnvironmentId, DistributorError,
    PackStatusResponse, ResolveComponentRequest, ResolveComponentResponse, TenantCtx,
};
use async_trait::async_trait;
use reqwest::{StatusCode, header::HeaderMap};

// Runtime HTTP JSON contract mirrors greentic-types::distributor DTOs (serde field names).
#[derive(Clone)]
pub struct HttpDistributorClient {
    http: reqwest::Client,
    config: DistributorClientConfig,
}

impl HttpDistributorClient {
    pub fn new(config: DistributorClientConfig) -> Result<Self, DistributorError> {
        let mut builder = reqwest::Client::builder();
        if let Some(timeout) = config.request_timeout {
            builder = builder.timeout(timeout);
        }
        let http = builder.build()?;
        Ok(Self { http, config })
    }

    /// Build a client from a pre-constructed [`reqwest::Client`].
    ///
    /// Use this to supply a client configured for mutual TLS — e.g. one built by
    /// `greentic-update`'s `tls::build_mtls_client` — so every distributor call
    /// travels over the mTLS update channel. The caller owns all transport
    /// configuration (client identity, root certificates, timeouts, proxy);
    /// `config.request_timeout` is intentionally **not** re-applied on top of the
    /// supplied client, since a `reqwest::Client`'s timeout is fixed at build time.
    #[must_use]
    pub fn with_client(http: reqwest::Client, config: DistributorClientConfig) -> Self {
        Self { http, config }
    }

    fn base_url(&self) -> Result<String, DistributorError> {
        self.config
            .base_url
            .as_ref()
            .cloned()
            .ok_or_else(|| DistributorError::InvalidResponse("base_url not configured".into()))
    }

    fn headers(&self) -> Result<HeaderMap, DistributorError> {
        let mut headers = HeaderMap::new();
        if let Some(token) = &self.config.auth_token {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token).parse().map_err(|e| {
                    DistributorError::InvalidResponse(format!("invalid auth header: {e}"))
                })?,
            );
        }
        if let Some(extra) = &self.config.extra_headers {
            for (k, v) in extra {
                let name: reqwest::header::HeaderName = k.parse().map_err(|e| {
                    DistributorError::InvalidResponse(format!("invalid header name {}: {e}", k))
                })?;
                let value: reqwest::header::HeaderValue = v.parse().map_err(|e| {
                    DistributorError::InvalidResponse(format!("invalid header value {}: {e}", v))
                })?;
                headers.insert(name, value);
            }
        }
        Ok(headers)
    }

    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, DistributorError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response.json::<T>().await?);
        }
        let body = response.text().await.unwrap_or_default();
        match status {
            StatusCode::NOT_FOUND => Err(DistributorError::NotFound),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(DistributorError::PermissionDenied)
            }
            _ => Err(DistributorError::Status { status, body }),
        }
    }
}

#[async_trait]
impl DistributorClient for HttpDistributorClient {
    async fn resolve_component(
        &self,
        req: ResolveComponentRequest,
    ) -> Result<ResolveComponentResponse, DistributorError> {
        let url = format!("{}/distributor-api/resolve-component", self.base_url()?);
        let response = self
            .http
            .post(url)
            .headers(self.headers()?)
            .json(&req)
            .send()
            .await?;
        self.handle_response(response).await
    }

    async fn get_pack_status(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<serde_json::Value, DistributorError> {
        let url = format!("{}/distributor-api/pack-status", self.base_url()?);
        let response = self
            .http
            .get(url)
            .headers(self.headers()?)
            .query(&[
                ("tenant_id", tenant.tenant_id.as_str()),
                ("environment_id", env.as_str()),
                ("pack_id", pack_id),
            ])
            .send()
            .await?;
        self.handle_response(response).await
    }

    async fn get_pack_status_v2(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<PackStatusResponse, DistributorError> {
        let url = format!("{}/distributor-api/pack-status-v2", self.base_url()?);
        let response = self
            .http
            .get(url)
            .headers(self.headers()?)
            .query(&[
                ("tenant_id", tenant.tenant_id.as_str()),
                ("environment_id", env.as_str()),
                ("pack_id", pack_id),
            ])
            .send()
            .await?;
        self.handle_response(response).await
    }

    async fn warm_pack(
        &self,
        tenant: &TenantCtx,
        env: &DistributorEnvironmentId,
        pack_id: &str,
    ) -> Result<(), DistributorError> {
        let url = format!("{}/distributor-api/warm-pack", self.base_url()?);
        let payload = serde_json::json!({
            "tenant_id": tenant.tenant_id,
            "environment_id": env.as_str(),
            "pack_id": pack_id
        });
        let response = self
            .http
            .post(url)
            .headers(self.headers()?)
            .json(&payload)
            .send()
            .await?;
        self.handle_response::<serde_json::Value>(response).await?;
        Ok(())
    }
}
