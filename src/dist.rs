use crate::oci_components::{ComponentResolveOptions, DefaultRegistryClient, OciComponentResolver};
use async_trait::async_trait;
use oci_distribution::Reference;
use reqwest::Url;
use sha2::{Digest, Sha256};
use std::cell::OnceCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const WASM_CONTENT_TYPE: &str = "application/wasm";

#[derive(Clone, Debug)]
pub struct DistOptions {
    pub cache_dir: PathBuf,
    pub allow_tags: bool,
    pub offline: bool,
    pub allow_insecure_local_http: bool,
    pub cache_max_bytes: u64,
    pub repo_registry_base: Option<String>,
    pub store_registry_base: Option<String>,
    #[cfg(feature = "fixture-resolver")]
    pub fixture_dir: Option<PathBuf>,
}

impl Default for DistOptions {
    fn default() -> Self {
        let offline = std::env::var("GREENTIC_DIST_OFFLINE").is_ok_and(|v| v == "1");
        let allow_insecure_local_http =
            std::env::var("GREENTIC_DIST_ALLOW_INSECURE_LOCAL_HTTP").is_ok_and(|v| v == "1");
        let cache_max_bytes = std::env::var("GREENTIC_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3 * 1024 * 1024 * 1024);
        let cache_dir = std::env::var("GREENTIC_CACHE_DIR")
            .or_else(|_| std::env::var("GREENTIC_DIST_CACHE_DIR"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_distribution_cache_root());
        Self {
            cache_dir,
            allow_tags: true,
            offline,
            allow_insecure_local_http,
            cache_max_bytes,
            repo_registry_base: std::env::var("GREENTIC_REPO_REGISTRY_BASE").ok(),
            store_registry_base: std::env::var("GREENTIC_STORE_REGISTRY_BASE").ok(),
            #[cfg(feature = "fixture-resolver")]
            fixture_dir: std::env::var("GREENTIC_FIXTURE_DIR")
                .ok()
                .map(PathBuf::from),
        }
    }
}

#[derive(Debug)]
pub struct ResolvedArtifact {
    pub resolved_digest: String,
    pub wasm_bytes: Option<Vec<u8>>,
    pub wasm_path: Option<PathBuf>,
    pub component_id: String,
    pub abi_version: Option<String>,
    pub describe_artifact_ref: Option<String>,
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    pub fetched: bool,
    pub source: ArtifactSource,
    pub digest: String,
    pub cache_path: Option<PathBuf>,
    loaded_wasm_bytes: OnceCell<Vec<u8>>,
}

impl ResolvedArtifact {
    #[allow(clippy::too_many_arguments)]
    fn from_path(
        resolved_digest: String,
        wasm_path: PathBuf,
        component_id: String,
        abi_version: Option<String>,
        describe_artifact_ref: Option<String>,
        content_length: Option<u64>,
        content_type: Option<String>,
        fetched: bool,
        source: ArtifactSource,
    ) -> Self {
        Self {
            digest: resolved_digest.clone(),
            cache_path: Some(wasm_path.clone()),
            resolved_digest,
            wasm_bytes: None,
            wasm_path: Some(wasm_path),
            component_id,
            abi_version,
            describe_artifact_ref,
            content_length,
            content_type,
            fetched,
            source,
            loaded_wasm_bytes: OnceCell::new(),
        }
    }

    pub fn validate_payload(&self) -> Result<(), DistError> {
        let has_bytes = self.wasm_bytes.is_some();
        let has_path = self.wasm_path.is_some();
        if has_bytes == has_path {
            return Err(DistError::CorruptArtifact {
                reference: self.resolved_digest.clone(),
                reason: "expected exactly one of wasm_bytes or wasm_path".into(),
            });
        }
        Ok(())
    }

    pub fn wasm_bytes(&self) -> Result<&[u8], DistError> {
        self.validate_payload()?;
        if let Some(bytes) = self.wasm_bytes.as_deref() {
            return Ok(bytes);
        }
        let path = self
            .wasm_path
            .as_ref()
            .ok_or_else(|| DistError::CorruptArtifact {
                reference: self.resolved_digest.clone(),
                reason: "missing wasm path".into(),
            })?;
        if self.loaded_wasm_bytes.get().is_none() {
            let loaded = fs::read(path).map_err(|source| DistError::CacheError {
                path: path.display().to_string(),
                source,
            })?;
            let _ = self.loaded_wasm_bytes.set(loaded);
        }
        Ok(self
            .loaded_wasm_bytes
            .get()
            .expect("loaded_wasm_bytes must be set")
            .as_slice())
    }

    fn with_resolved_digest(mut self, digest: String) -> Self {
        self.resolved_digest = digest.clone();
        self.digest = digest;
        self
    }

    pub fn lock_hint(&self, source_ref: impl Into<String>) -> LockHint {
        LockHint {
            source_ref: source_ref.into(),
            resolved_digest: self.resolved_digest.clone(),
            content_length: self.content_length,
            content_type: self.content_type.clone(),
            abi_version: self.abi_version.clone(),
            component_id: self.component_id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockHint {
    pub source_ref: String,
    pub resolved_digest: String,
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    pub abi_version: Option<String>,
    pub component_id: String,
}

#[derive(Clone, Debug)]
pub enum ArtifactSource {
    Digest,
    Http(String),
    File(PathBuf),
    Oci(String),
    Repo(String),
    Store(String),
}

pub struct DistClient {
    cache: ComponentCache,
    oci: OciComponentResolver<DefaultRegistryClient>,
    http: reqwest::Client,
    opts: DistOptions,
    injected: Option<Arc<dyn ResolveRefInjector>>,
}

#[derive(Clone, Debug)]
pub struct OciCacheInspection {
    pub digest: String,
    pub cache_dir: PathBuf,
    pub selected_media_type: String,
    pub fetched: bool,
}

#[derive(Clone, Debug)]
pub struct ResolveRefRequest {
    pub reference: String,
}

#[derive(Clone, Debug, Default)]
pub struct ResolveComponentRequest {
    pub reference: String,
    pub tenant: Option<String>,
    pub pack: Option<String>,
    pub environment: Option<String>,
}

#[derive(Clone, Debug)]
pub enum InjectedResolution {
    Redirect(String),
    WasmBytes {
        resolved_digest: String,
        wasm_bytes: Vec<u8>,
        component_id: String,
        abi_version: Option<String>,
        source: ArtifactSource,
    },
    WasmPath {
        resolved_digest: String,
        wasm_path: PathBuf,
        component_id: String,
        abi_version: Option<String>,
        source: ArtifactSource,
    },
}

#[async_trait]
pub trait ResolveRefInjector: Send + Sync {
    async fn resolve(&self, reference: &str) -> Result<Option<InjectedResolution>, DistError>;
}

impl DistClient {
    pub fn new(opts: DistOptions) -> Self {
        Self::with_injector(opts, None)
    }

    pub fn with_ref_injector(opts: DistOptions, injector: Arc<dyn ResolveRefInjector>) -> Self {
        Self::with_injector(opts, Some(injector))
    }

    fn with_injector(opts: DistOptions, injected: Option<Arc<dyn ResolveRefInjector>>) -> Self {
        let oci_opts = ComponentResolveOptions {
            allow_tags: opts.allow_tags,
            offline: opts.offline,
            cache_dir: opts.cache_dir.clone(),
            ..Default::default()
        };
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("failed to build http client");
        Self {
            cache: ComponentCache::new(opts.cache_dir.clone()),
            oci: OciComponentResolver::new(oci_opts),
            http,
            opts,
            injected,
        }
    }

    pub async fn resolve_ref(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let mut current = reference.to_string();
        for _ in 0..8 {
            if let Some(injected) = &self.injected
                && let Some(result) = injected.resolve(&current).await?
            {
                if let InjectedResolution::Redirect(next) = result {
                    current = next;
                    continue;
                }
                return self.materialize_injected(result);
            }

            return match classify_reference(&current)? {
                RefKind::Digest(digest) => {
                    let path =
                        self.cache
                            .existing_component(&digest)
                            .ok_or(DistError::NotFound {
                                reference: digest.clone(),
                            })?;
                    let resolved = ResolvedArtifact::from_path(
                        digest.clone(),
                        path,
                        digest.clone(),
                        None,
                        None,
                        file_size_if_exists(self.cache.component_path(&digest).as_path()),
                        Some(WASM_CONTENT_TYPE.to_string()),
                        false,
                        ArtifactSource::Digest,
                    );
                    resolved.validate_payload()?;
                    Ok(resolved)
                }
                RefKind::Http(url) => self.fetch_http(&url).await,
                RefKind::File(path) => self.ingest_file(&path).await,
                RefKind::Oci(reference) => self.pull_oci(&reference).await,
                RefKind::Repo(target) => self.resolve_repo_ref(&target).await,
                RefKind::Store(target) => self.resolve_store_ref(&target).await,
                #[cfg(feature = "fixture-resolver")]
                RefKind::Fixture(target) => self.resolve_fixture_ref(&target).await,
            };
        }
        Err(DistError::InvalidInput(
            "too many injected redirect hops".to_string(),
        ))
    }

    pub async fn resolve_ref_request(
        &self,
        req: ResolveRefRequest,
    ) -> Result<ResolvedArtifact, DistError> {
        self.resolve_ref(&req.reference).await
    }

    pub async fn resolve_component(
        &self,
        req: ResolveComponentRequest,
    ) -> Result<ResolvedArtifact, DistError> {
        self.resolve_ref(&req.reference).await
    }

    pub async fn ensure_cached(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let resolved = self.resolve_ref(reference).await?;
        if resolved.wasm_bytes.is_some() {
            return Ok(resolved);
        }
        if let Some(path) = &resolved.wasm_path
            && path.exists()
        {
            return Ok(resolved);
        }
        Err(DistError::NotFound {
            reference: reference.to_string(),
        })
    }

    pub async fn fetch_digest(&self, digest: &str) -> Result<PathBuf, DistError> {
        let normalized = normalize_digest(digest);
        self.cache
            .existing_component(&normalized)
            .ok_or(DistError::NotFound {
                reference: normalized,
            })
    }

    pub async fn pull_lock(&self, lock_path: &Path) -> Result<Vec<ResolvedArtifact>, DistError> {
        let contents = fs::read_to_string(lock_path).map_err(|source| DistError::CacheError {
            path: lock_path.display().to_string(),
            source,
        })?;
        let entries = parse_lockfile(&contents)?;
        let mut resolved = Vec::with_capacity(entries.len());
        for entry in entries {
            let reference = entry
                .reference
                .clone()
                .ok_or_else(|| DistError::InvalidInput("lock entry missing ref".into()))?;
            let digest = if let Some(d) = entry.digest.clone() {
                d
            } else {
                if self.opts.offline {
                    return Err(DistError::Offline {
                        reference: reference.clone(),
                    });
                }
                self.resolve_ref(&reference).await?.digest
            };

            let cache_key = entry.digest.clone().unwrap_or_else(|| reference.clone());
            let resolved_item = if let Ok(item) = self.ensure_cached(&cache_key).await {
                item
            } else {
                self.ensure_cached(&reference).await?
            };
            resolved.push(resolved_item.with_resolved_digest(digest));
        }
        Ok(resolved)
    }

    pub fn list_cache(&self) -> Vec<String> {
        self.cache.list_digests()
    }

    pub fn remove_cached(&self, digests: &[String]) -> Result<(), DistError> {
        for digest in digests {
            let dir = self.cache.component_dir(digest);
            if dir.exists() {
                fs::remove_dir_all(&dir).map_err(|source| DistError::CacheError {
                    path: dir.display().to_string(),
                    source,
                })?;
            }
        }
        Ok(())
    }

    pub fn gc(&self) -> Result<Vec<String>, DistError> {
        let mut removed = Vec::new();
        for digest in self.cache.list_digests() {
            let path = self.cache.component_path(&digest);
            if !path.exists() {
                let dir = self.cache.component_dir(&digest);
                fs::remove_dir_all(&dir).ok();
                removed.push(digest);
            }
        }
        Ok(removed)
    }

    async fn fetch_http(&self, url: &str) -> Result<ResolvedArtifact, DistError> {
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: url.to_string(),
            });
        }
        let request_url = ensure_secure_http_url(url, self.opts.allow_insecure_local_http)?;
        let bytes = self
            .http
            .get(request_url.clone())
            .send()
            .await
            .map_err(|err| DistError::Network(err.to_string()))?;
        let response = bytes
            .error_for_status()
            .map_err(|err| DistError::Network(err.to_string()))?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| Some(WASM_CONTENT_TYPE.to_string()));
        let bytes = response
            .bytes()
            .await
            .map_err(|err| DistError::Network(err.to_string()))?;
        let digest = digest_for_bytes(&bytes);
        let path = self
            .cache
            .write_component(&digest, &bytes)
            .map_err(|source| DistError::CacheError {
                path: self.cache.component_path(&digest).display().to_string(),
                source,
            })?;
        let resolved = ResolvedArtifact::from_path(
            digest,
            path,
            component_id_from_ref(&RefKind::Http(request_url.to_string())),
            None,
            None,
            Some(bytes.len() as u64),
            content_type,
            true,
            ArtifactSource::Http(request_url.to_string()),
        );
        self.enforce_cache_cap()?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn ingest_file(&self, path: &Path) -> Result<ResolvedArtifact, DistError> {
        let bytes = fs::read(path).map_err(|source| DistError::CacheError {
            path: path.display().to_string(),
            source,
        })?;
        let digest = digest_for_bytes(&bytes);
        let cached = self
            .cache
            .write_component(&digest, &bytes)
            .map_err(|source| DistError::CacheError {
                path: self.cache.component_path(&digest).display().to_string(),
                source,
            })?;
        let resolved = ResolvedArtifact::from_path(
            digest,
            cached,
            component_id_from_ref(&RefKind::File(path.to_path_buf())),
            None,
            source_sidecar_describe_ref(path),
            Some(bytes.len() as u64),
            Some(WASM_CONTENT_TYPE.to_string()),
            true,
            ArtifactSource::File(path.to_path_buf()),
        );
        self.enforce_cache_cap()?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn pull_oci(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let component_id = component_id_from_ref(&RefKind::Oci(reference.to_string()));
        self.pull_oci_with_source(
            reference,
            ArtifactSource::Oci(reference.to_string()),
            component_id,
        )
        .await
    }

    async fn pull_oci_with_source(
        &self,
        reference: &str,
        source: ArtifactSource,
        component_id: String,
    ) -> Result<ResolvedArtifact, DistError> {
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: reference.to_string(),
            });
        }
        let result = self
            .oci
            .resolve_refs(&crate::oci_components::ComponentsExtension {
                refs: vec![reference.to_string()],
                mode: crate::oci_components::ComponentsMode::Eager,
            })
            .await
            .map_err(DistError::Oci)?;
        let resolved = result
            .into_iter()
            .next()
            .ok_or_else(|| DistError::InvalidRef {
                reference: reference.to_string(),
            })?;
        let resolved = ResolvedArtifact::from_path(
            resolved.resolved_digest,
            resolved.path.clone(),
            resolve_component_id_from_cache(&resolved.path, &component_id),
            resolve_abi_version_from_cache(&resolved.path),
            resolve_describe_artifact_ref_from_cache(&resolved.path),
            file_size_if_exists(&resolved.path),
            Some(normalize_content_type(
                Some(&resolved.media_type),
                WASM_CONTENT_TYPE,
            )),
            resolved.fetched_from_network,
            source,
        );
        self.enforce_cache_cap()?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn resolve_repo_ref(&self, target: &str) -> Result<ResolvedArtifact, DistError> {
        let mapped = map_registry_target(target, self.opts.repo_registry_base.as_deref())
            .ok_or_else(|| DistError::Unauthorized {
                target: format!("repo://{target}"),
            })?;
        self.pull_oci_with_source(
            &mapped,
            ArtifactSource::Repo(format!("repo://{target}")),
            target.to_string(),
        )
        .await
    }

    async fn resolve_store_ref(&self, target: &str) -> Result<ResolvedArtifact, DistError> {
        let mapped = map_registry_target(target, self.opts.store_registry_base.as_deref())
            .ok_or_else(|| DistError::Unauthorized {
                target: format!("store://{target}"),
            })?;
        self.pull_oci_with_source(
            &mapped,
            ArtifactSource::Store(format!("store://{target}")),
            target.to_string(),
        )
        .await
    }

    #[cfg(feature = "fixture-resolver")]
    async fn resolve_fixture_ref(&self, target: &str) -> Result<ResolvedArtifact, DistError> {
        let fixture_dir = self
            .opts
            .fixture_dir
            .as_ref()
            .ok_or_else(|| DistError::InvalidInput("fixture:// requires fixture_dir".into()))?;
        let raw = target.trim_start_matches('/');
        let candidate = if raw.ends_with(".wasm") {
            fixture_dir.join(raw)
        } else {
            fixture_dir.join(format!("{raw}.wasm"))
        };
        if !candidate.exists() {
            return Err(DistError::NotFound {
                reference: format!("fixture://{target}"),
            });
        }
        self.ingest_file(&candidate).await
    }

    fn materialize_injected(
        &self,
        resolved: InjectedResolution,
    ) -> Result<ResolvedArtifact, DistError> {
        let artifact = match resolved {
            InjectedResolution::Redirect(_) => {
                return Err(DistError::InvalidInput(
                    "unexpected redirect during materialization".into(),
                ));
            }
            InjectedResolution::WasmBytes {
                resolved_digest,
                wasm_bytes,
                component_id,
                abi_version,
                source,
            } => {
                let path = self
                    .cache
                    .write_component(&resolved_digest, &wasm_bytes)
                    .map_err(|source| DistError::CacheError {
                        path: self
                            .cache
                            .component_path(&resolved_digest)
                            .display()
                            .to_string(),
                        source,
                    })?;
                ResolvedArtifact::from_path(
                    resolved_digest,
                    path,
                    component_id,
                    abi_version,
                    None,
                    Some(wasm_bytes.len() as u64),
                    Some(WASM_CONTENT_TYPE.to_string()),
                    true,
                    source,
                )
            }
            InjectedResolution::WasmPath {
                resolved_digest,
                wasm_path,
                component_id,
                abi_version,
                source,
            } => ResolvedArtifact::from_path(
                resolved_digest,
                wasm_path.clone(),
                component_id,
                abi_version,
                resolve_describe_artifact_ref_from_cache(&wasm_path),
                file_size_if_exists(&wasm_path),
                Some(WASM_CONTENT_TYPE.to_string()),
                false,
                source,
            ),
        };
        self.enforce_cache_cap()?;
        artifact.validate_payload()?;
        Ok(artifact)
    }

    fn enforce_cache_cap(&self) -> Result<(), DistError> {
        self.cache
            .enforce_size_cap(self.opts.cache_max_bytes)
            .map_err(|source| DistError::CacheError {
                path: self.opts.cache_dir.display().to_string(),
                source,
            })
    }

    pub async fn pull_oci_with_details(
        &self,
        reference: &str,
    ) -> Result<OciCacheInspection, DistError> {
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: reference.to_string(),
            });
        }
        let result = self
            .oci
            .resolve_refs(&crate::oci_components::ComponentsExtension {
                refs: vec![reference.to_string()],
                mode: crate::oci_components::ComponentsMode::Eager,
            })
            .await
            .map_err(DistError::Oci)?;
        let resolved = result
            .into_iter()
            .next()
            .ok_or_else(|| DistError::InvalidRef {
                reference: reference.to_string(),
            })?;
        let cache_dir = resolved
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .ok_or_else(|| DistError::InvalidInput("cache path missing parent".into()))?;
        Ok(OciCacheInspection {
            digest: resolved.resolved_digest,
            cache_dir,
            selected_media_type: resolved.media_type,
            fetched: resolved.fetched_from_network,
        })
    }
}

fn default_distribution_cache_root() -> PathBuf {
    if let Ok(root) = std::env::var("GREENTIC_HOME") {
        return PathBuf::from(root).join("cache").join("distribution");
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".greentic")
            .join("cache")
            .join("distribution");
    }
    PathBuf::from(".greentic")
        .join("cache")
        .join("distribution")
}

#[derive(Debug, Error)]
pub enum DistError {
    #[error("invalid reference `{reference}`")]
    InvalidRef { reference: String },
    #[error("reference `{reference}` not found")]
    NotFound { reference: String },
    #[error("unauthorized for `{target}`")]
    Unauthorized { target: String },
    #[error("network error: {0}")]
    Network(String),
    #[error("corrupt artifact `{reference}`: {reason}")]
    CorruptArtifact { reference: String, reason: String },
    #[error("unsupported abi `{abi}`")]
    UnsupportedAbi { abi: String },
    #[error("cache error at `{path}`: {source}")]
    CacheError {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("insecure url `{url}`: only https is allowed")]
    InsecureUrl { url: String },
    #[error("offline mode forbids fetching `{reference}`")]
    Offline { reference: String },
    #[error("oci error: {0}")]
    Oci(#[from] crate::oci_components::OciComponentError),
    #[error("invalid lockfile: {0}")]
    Serde(#[from] serde_json::Error),
}

impl DistError {
    pub fn exit_code(&self) -> i32 {
        match self {
            DistError::InvalidRef { .. }
            | DistError::InvalidInput(_)
            | DistError::InsecureUrl { .. }
            | DistError::Serde(_) => 2,
            DistError::NotFound { .. } => 3,
            DistError::Offline { .. } => 4,
            DistError::Unauthorized { .. } => 5,
            _ => 10,
        }
    }
}

pub type ResolveError = DistError;

#[derive(Clone, Debug)]
struct ComponentCache {
    base: PathBuf,
}

impl ComponentCache {
    fn new(base: PathBuf) -> Self {
        Self { base }
    }

    fn component_dir(&self, digest: &str) -> PathBuf {
        self.base
            .join(trim_digest_prefix(&normalize_digest(digest)))
    }

    fn component_path(&self, digest: &str) -> PathBuf {
        self.component_dir(digest).join("component.wasm")
    }

    fn existing_component(&self, digest: &str) -> Option<PathBuf> {
        let path = self.component_path(digest);
        if path.exists() {
            let _ = self.touch_last_used(digest);
            Some(path)
        } else {
            None
        }
    }

    fn write_component(&self, digest: &str, data: &[u8]) -> Result<PathBuf, std::io::Error> {
        let dir = self.component_dir(digest);
        fs::create_dir_all(&dir)?;
        let path = dir.join("component.wasm");
        fs::write(&path, data)?;
        self.touch_last_used(digest)?;
        Ok(path)
    }

    fn list_digests(&self) -> Vec<String> {
        let mut digests = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.base) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata()
                    && meta.is_dir()
                    && let Some(name) = entry.file_name().to_str()
                {
                    digests.push(format!("sha256:{name}"));
                }
            }
        }
        digests
    }

    fn touch_last_used(&self, digest: &str) -> Result<(), std::io::Error> {
        let marker = self.component_dir(digest).join("last_used");
        fs::write(marker, b"1")
    }

    fn enforce_size_cap(&self, max_bytes: u64) -> Result<(), std::io::Error> {
        if max_bytes == 0 {
            return Ok(());
        }
        let mut entries = self.cache_entries()?;
        let mut total: u64 = entries.iter().map(|entry| entry.size).sum();
        if total <= max_bytes {
            return Ok(());
        }
        entries.sort_by_key(|entry| entry.last_used);
        for entry in entries {
            if total <= max_bytes {
                break;
            }
            if entry.dir.exists() {
                fs::remove_dir_all(&entry.dir)?;
            }
            total = total.saturating_sub(entry.size);
        }
        Ok(())
    }

    fn cache_entries(&self) -> Result<Vec<CacheEntry>, std::io::Error> {
        let mut out = Vec::new();
        if !self.base.exists() {
            return Ok(out);
        }
        for entry in fs::read_dir(&self.base)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if !meta.is_dir() {
                continue;
            }
            let dir = entry.path();
            let wasm = dir.join("component.wasm");
            if !wasm.exists() {
                continue;
            }
            let size = wasm.metadata()?.len();
            let last_used = dir
                .join("last_used")
                .metadata()
                .and_then(|m| m.modified())
                .or_else(|_| wasm.metadata().and_then(|m| m.modified()))
                .unwrap_or(UNIX_EPOCH);
            out.push(CacheEntry {
                dir,
                size,
                last_used,
            });
        }
        Ok(out)
    }
}

#[derive(Debug)]
struct CacheEntry {
    dir: PathBuf,
    size: u64,
    last_used: SystemTime,
}

fn digest_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn trim_digest_prefix(digest: &str) -> &str {
    digest
        .strip_prefix("sha256:")
        .unwrap_or_else(|| digest.trim_start_matches('@'))
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

fn component_id_from_ref(kind: &RefKind) -> String {
    match kind {
        RefKind::Digest(digest) => digest.clone(),
        RefKind::Http(url) => Url::parse(url)
            .ok()
            .and_then(|u| {
                Path::new(u.path())
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(strip_file_component_suffix)
            })
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "http-component".to_string()),
        RefKind::File(path) => path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(strip_file_component_suffix)
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "file-component".to_string()),
        RefKind::Oci(reference) => canonical_oci_component_id(reference),
        RefKind::Repo(reference) => reference.trim_start_matches("repo://").to_string(),
        RefKind::Store(reference) => reference.trim_start_matches("store://").to_string(),
        #[cfg(feature = "fixture-resolver")]
        RefKind::Fixture(reference) => reference.trim_start_matches('/').to_string(),
    }
}

fn canonical_oci_component_id(reference: &str) -> String {
    let raw = reference.trim_start_matches("oci://");
    if let Ok(parsed) = Reference::try_from(raw) {
        return parsed.repository().to_string();
    }
    let without_digest = raw.split('@').next().unwrap_or(raw);
    let last_colon = without_digest.rfind(':');
    let last_slash = without_digest.rfind('/');
    if let (Some(colon), Some(slash)) = (last_colon, last_slash)
        && colon > slash
    {
        return without_digest[..colon].to_string();
    }
    without_digest.to_string()
}

fn strip_file_component_suffix(input: &str) -> String {
    if let Some((prefix, suffix)) = input.rsplit_once("__")
        && !prefix.is_empty()
        && suffix.chars().all(|ch| ch.is_ascii_digit() || ch == '_')
        && suffix.contains('_')
    {
        return prefix.to_string();
    }
    input.to_string()
}

fn normalize_content_type(current: Option<&str>, fallback: &str) -> String {
    let value = current.unwrap_or("").trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn file_size_if_exists(path: &Path) -> Option<u64> {
    path.metadata().ok().map(|m| m.len())
}

fn source_sidecar_describe_ref(source_wasm_path: &Path) -> Option<String> {
    let parent = source_wasm_path.parent()?;
    let describe = parent.join("describe.cbor");
    if !describe.exists() {
        return None;
    }
    Some(describe.display().to_string())
}

fn resolve_component_id_from_cache(wasm_path: &Path, fallback: &str) -> String {
    let cache_dir = match wasm_path.parent() {
        Some(dir) => dir,
        None => return fallback.to_string(),
    };
    read_component_id_from_json(cache_dir.join("metadata.json"))
        .or_else(|| read_component_id_from_json(cache_dir.join("component.manifest.json")))
        .unwrap_or_else(|| fallback.to_string())
}

fn read_component_id_from_json(path: PathBuf) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    extract_string_anywhere(
        &value,
        &["component_id", "componentId", "canonical_component_id"],
    )
    .filter(|id| !id.trim().is_empty())
}

fn resolve_abi_version_from_cache(wasm_path: &Path) -> Option<String> {
    let cache_dir = wasm_path.parent()?;
    read_abi_version_from_json(cache_dir.join("metadata.json"))
        .or_else(|| read_abi_version_from_json(cache_dir.join("component.manifest.json")))
}

fn resolve_describe_artifact_ref_from_cache(wasm_path: &Path) -> Option<String> {
    let cache_dir = wasm_path.parent()?;
    read_describe_artifact_ref_from_json(cache_dir.join("metadata.json"))
        .or_else(|| read_describe_artifact_ref_from_json(cache_dir.join("component.manifest.json")))
        .or_else(|| {
            let describe = cache_dir.join("describe.cbor");
            if describe.exists() {
                Some(describe.display().to_string())
            } else {
                None
            }
        })
}

fn read_describe_artifact_ref_from_json(path: PathBuf) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let found = extract_string_anywhere(
        &value,
        &[
            "describe_artifact_ref",
            "describeArtifactRef",
            "describe_ref",
            "describeRef",
        ],
    )?;
    let trimmed = found.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn read_abi_version_from_json(path: PathBuf) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let found = extract_string_anywhere(&value, &["abi_version", "abiVersion", "abi"])?;
    normalize_abi_version(&found)
}

fn normalize_abi_version(input: &str) -> Option<String> {
    let candidate = input.trim();
    if candidate.is_empty() {
        return None;
    }
    match semver::Version::parse(candidate) {
        Ok(version) => Some(version.to_string()),
        Err(_) => None,
    }
}

fn extract_string_anywhere(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in keys {
                if let Some(serde_json::Value::String(found)) = map.get(*key) {
                    return Some(found.clone());
                }
            }
            for nested in map.values() {
                if let Some(found) = extract_string_anywhere(nested, keys) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(found) = extract_string_anywhere(item, keys) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

fn map_registry_target(target: &str, base: Option<&str>) -> Option<String> {
    if Reference::try_from(target).is_ok() {
        return Some(target.to_string());
    }
    let base = base?;
    let normalized_base = base.trim_end_matches('/');
    let normalized_target = target.trim_start_matches('/');
    Some(format!("{normalized_base}/{normalized_target}"))
}

enum RefKind {
    Digest(String),
    Http(String),
    File(PathBuf),
    Oci(String),
    Repo(String),
    Store(String),
    #[cfg(feature = "fixture-resolver")]
    Fixture(String),
}

fn classify_reference(input: &str) -> Result<RefKind, DistError> {
    if is_digest(input) {
        return Ok(RefKind::Digest(normalize_digest(input)));
    }
    if let Ok(url) = Url::parse(input) {
        match url.scheme() {
            "http" | "https" => return Ok(RefKind::Http(input.to_string())),
            "file" => {
                if let Ok(path) = url.to_file_path() {
                    return Ok(RefKind::File(path));
                }
            }
            "oci" => {
                let trimmed = input.trim_start_matches("oci://");
                return Ok(RefKind::Oci(trimmed.to_string()));
            }
            "repo" => {
                let trimmed = input.trim_start_matches("repo://");
                return Ok(RefKind::Repo(trimmed.to_string()));
            }
            "store" => {
                let trimmed = input.trim_start_matches("store://");
                return Ok(RefKind::Store(trimmed.to_string()));
            }
            #[cfg(feature = "fixture-resolver")]
            "fixture" => {
                let trimmed = input.trim_start_matches("fixture://");
                return Ok(RefKind::Fixture(trimmed.to_string()));
            }
            _ => {}
        }
    }
    let path = Path::new(input);
    if path.exists() {
        return Ok(RefKind::File(path.to_path_buf()));
    }
    if Reference::try_from(input).is_ok() {
        Ok(RefKind::Oci(input.to_string()))
    } else {
        Err(DistError::InvalidRef {
            reference: input.to_string(),
        })
    }
}

fn is_digest(s: &str) -> bool {
    s.starts_with("sha256:") && s.len() == "sha256:".len() + 64
}

fn is_loopback_http(url: &Url) -> bool {
    url.scheme() == "http" && matches!(url.host_str(), Some("localhost") | Some("127.0.0.1"))
}

fn ensure_secure_http_url(url: &str, allow_loopback_local: bool) -> Result<Url, DistError> {
    let parsed = Url::parse(url).map_err(|_| DistError::InvalidRef {
        reference: url.to_string(),
    })?;
    if parsed.scheme() == "https" || (allow_loopback_local && is_loopback_http(&parsed)) {
        Ok(parsed)
    } else {
        Err(DistError::InsecureUrl {
            url: url.to_string(),
        })
    }
}

#[derive(Debug, serde::Deserialize)]
struct LockFile {
    #[serde(default)]
    schema_version: Option<u64>,
    #[serde(default)]
    components: Vec<LockEntry>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum LockEntry {
    String(String),
    Object(LockComponent),
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct LockComponent {
    reference: Option<String>,
    #[serde(rename = "ref")]
    ref_field: Option<String>,
    digest: Option<String>,
    name: Option<String>,
}

impl LockEntry {
    fn to_resolved(&self) -> LockResolvedEntry {
        match self {
            LockEntry::String(s) => LockResolvedEntry {
                reference: Some(s.clone()),
                digest: None,
            },
            LockEntry::Object(obj) => LockResolvedEntry {
                reference: obj
                    .reference
                    .clone()
                    .or_else(|| obj.ref_field.clone())
                    .or_else(|| obj.digest.clone()),
                digest: obj.digest.clone(),
            },
        }
    }
}

#[derive(Clone, Debug)]
struct LockResolvedEntry {
    reference: Option<String>,
    digest: Option<String>,
}

fn parse_lockfile(data: &str) -> Result<Vec<LockResolvedEntry>, serde_json::Error> {
    if let Ok(entries) = serde_json::from_str::<Vec<LockEntry>>(data) {
        return Ok(entries.into_iter().map(|e| e.to_resolved()).collect());
    }
    let parsed: LockFile = serde_json::from_str(data)?;
    let _ = parsed.schema_version;
    Ok(parsed
        .components
        .into_iter()
        .map(|c| c.to_resolved())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_id_prefers_metadata_then_manifest_then_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let cache_dir = temp.path().join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        let wasm = cache_dir.join("component.wasm");
        fs::write(&wasm, b"wasm").unwrap();

        let fallback = "repo/name";
        assert_eq!(resolve_component_id_from_cache(&wasm, fallback), fallback);

        fs::write(
            cache_dir.join("component.manifest.json"),
            r#"{"component_id":"from-manifest"}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_component_id_from_cache(&wasm, fallback),
            "from-manifest"
        );

        fs::write(
            cache_dir.join("metadata.json"),
            r#"{"component_id":"from-metadata"}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_component_id_from_cache(&wasm, fallback),
            "from-metadata"
        );
    }

    #[test]
    fn abi_version_is_best_effort_from_metadata_or_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let cache_dir = temp.path().join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        let wasm = cache_dir.join("component.wasm");
        fs::write(&wasm, b"wasm").unwrap();

        assert_eq!(resolve_abi_version_from_cache(&wasm), None);

        fs::write(
            cache_dir.join("component.manifest.json"),
            r#"{"abi_version":"0.6.0"}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_abi_version_from_cache(&wasm),
            Some("0.6.0".to_string())
        );

        fs::write(cache_dir.join("metadata.json"), r#"{"abi":"not-semver"}"#).unwrap();
        assert_eq!(
            resolve_abi_version_from_cache(&wasm),
            Some("0.6.0".to_string())
        );

        fs::write(cache_dir.join("metadata.json"), r#"{"abiVersion":"1.2.3"}"#).unwrap();
        assert_eq!(
            resolve_abi_version_from_cache(&wasm),
            Some("1.2.3".to_string())
        );
    }
}
