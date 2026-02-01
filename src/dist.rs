use crate::oci_components::{
    ComponentResolveOptions, DefaultRegistryClient, OciComponentResolver, default_cache_root,
};
use oci_distribution::Reference;
use reqwest::Url;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct DistOptions {
    pub cache_dir: PathBuf,
    pub allow_tags: bool,
    pub offline: bool,
    pub allow_insecure_local_http: bool,
}

impl Default for DistOptions {
    fn default() -> Self {
        let offline = std::env::var("GREENTIC_DIST_OFFLINE").is_ok_and(|v| v == "1");
        let allow_insecure_local_http =
            std::env::var("GREENTIC_DIST_ALLOW_INSECURE_LOCAL_HTTP").is_ok_and(|v| v == "1");
        Self {
            cache_dir: default_cache_root(),
            allow_tags: true,
            offline,
            allow_insecure_local_http,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedArtifact {
    pub digest: String,
    pub cache_path: Option<PathBuf>,
    pub fetched: bool,
    pub source: ArtifactSource,
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
}

#[derive(Clone, Debug)]
pub struct OciCacheInspection {
    pub digest: String,
    pub cache_dir: PathBuf,
    pub selected_media_type: String,
    pub fetched: bool,
}

impl DistClient {
    pub fn new(opts: DistOptions) -> Self {
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
        }
    }

    pub async fn resolve_ref(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        match classify_reference(reference)? {
            RefKind::Digest(digest) => Ok(ResolvedArtifact {
                cache_path: self.cache.existing_component(&digest),
                digest,
                fetched: false,
                source: ArtifactSource::Digest,
            }),
            RefKind::Http(url) => self.fetch_http(&url).await,
            RefKind::File(path) => self.ingest_file(&path).await,
            RefKind::Oci(reference) => self.pull_oci(&reference).await,
            RefKind::Repo(target) => Err(DistError::AuthRequired { target }),
            RefKind::Store(target) => Err(DistError::AuthRequired { target }),
        }
    }

    pub async fn ensure_cached(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let resolved = self.resolve_ref(reference).await?;
        if let Some(path) = &resolved.cache_path
            && path.exists()
        {
            return Ok(resolved);
        }
        Err(DistError::CacheMiss {
            reference: reference.to_string(),
        })
    }

    pub async fn fetch_digest(&self, digest: &str) -> Result<PathBuf, DistError> {
        let normalized = normalize_digest(digest);
        self.cache
            .existing_component(&normalized)
            .ok_or(DistError::CacheMiss {
                reference: normalized,
            })
    }

    pub async fn pull_lock(&self, lock_path: &Path) -> Result<Vec<ResolvedArtifact>, DistError> {
        let contents = fs::read_to_string(lock_path)?;
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
            resolved.push(ResolvedArtifact {
                digest,
                ..resolved_item
            });
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
                fs::remove_dir_all(&dir)?;
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
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let digest = digest_for_bytes(&bytes);
        let path = self.cache.write_component(&digest, &bytes)?;
        Ok(ResolvedArtifact {
            cache_path: Some(path),
            digest,
            fetched: true,
            source: ArtifactSource::Http(request_url.to_string()),
        })
    }

    async fn ingest_file(&self, path: &Path) -> Result<ResolvedArtifact, DistError> {
        let bytes = fs::read(path)?;
        let digest = digest_for_bytes(&bytes);
        let cached = self.cache.write_component(&digest, &bytes)?;
        Ok(ResolvedArtifact {
            cache_path: Some(cached),
            digest,
            fetched: true,
            source: ArtifactSource::File(path.to_path_buf()),
        })
    }

    async fn pull_oci(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
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
            .ok_or_else(|| DistError::InvalidReference {
                reference: reference.to_string(),
            })?;
        Ok(ResolvedArtifact {
            cache_path: Some(resolved.path.clone()),
            digest: resolved.resolved_digest,
            fetched: resolved.fetched_from_network,
            source: ArtifactSource::Oci(reference.to_string()),
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
            .ok_or_else(|| DistError::InvalidReference {
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

#[derive(Debug, Error)]
pub enum DistError {
    #[error("invalid reference `{reference}`")]
    InvalidReference { reference: String },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("insecure url `{url}`: only https is allowed")]
    InsecureUrl { url: String },
    #[error("offline mode forbids fetching `{reference}`")]
    Offline { reference: String },
    #[error("reference `{reference}` is not cached")]
    CacheMiss { reference: String },
    #[error("auth not implemented for `{target}`")]
    AuthRequired { target: String },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("oci error: {0}")]
    Oci(#[from] crate::oci_components::OciComponentError),
    #[error("invalid lockfile: {0}")]
    Serde(#[from] serde_json::Error),
}

impl DistError {
    pub fn exit_code(&self) -> i32 {
        match self {
            DistError::InvalidReference { .. }
            | DistError::InvalidInput(_)
            | DistError::InsecureUrl { .. }
            | DistError::Serde(_) => 2,
            DistError::CacheMiss { .. } => 3,
            DistError::Offline { .. } => 4,
            DistError::AuthRequired { .. } => 5,
            _ => 10,
        }
    }
}

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
        if path.exists() { Some(path) } else { None }
    }

    fn write_component(&self, digest: &str, data: &[u8]) -> Result<PathBuf, std::io::Error> {
        let dir = self.component_dir(digest);
        fs::create_dir_all(&dir)?;
        let path = dir.join("component.wasm");
        fs::write(&path, data)?;
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

enum RefKind {
    Digest(String),
    Http(String),
    File(PathBuf),
    Oci(String),
    Repo(String),
    Store(String),
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
            "repo" => return Ok(RefKind::Repo(input.to_string())),
            "store" => return Ok(RefKind::Store(input.to_string())),
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
        Err(DistError::InvalidReference {
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
    let parsed = Url::parse(url).map_err(|_| DistError::InvalidReference {
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
