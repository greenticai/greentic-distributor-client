use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use oci_distribution::Reference;
use oci_distribution::client::{Client, ClientConfig, ClientProtocol, ImageData};
use oci_distribution::errors::OciDistributionError;
use oci_distribution::manifest::{
    IMAGE_MANIFEST_LIST_MEDIA_TYPE, IMAGE_MANIFEST_MEDIA_TYPE, OCI_IMAGE_INDEX_MEDIA_TYPE,
    OCI_IMAGE_MEDIA_TYPE,
};
use oci_distribution::secrets::RegistryAuth;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const OCI_ARTIFACT_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.artifact.manifest.v1+json";
const DOCKER_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
const DOCKER_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";
const DEFAULT_BLOB_FILENAME: &str = "blob.bin";

/// Accepted manifest media types when pulling digests.
static DEFAULT_ACCEPTED_MANIFEST_TYPES: &[&str] = &[
    OCI_ARTIFACT_MANIFEST_MEDIA_TYPE,
    OCI_IMAGE_MEDIA_TYPE,
    OCI_IMAGE_INDEX_MEDIA_TYPE,
    IMAGE_MANIFEST_MEDIA_TYPE,
    IMAGE_MANIFEST_LIST_MEDIA_TYPE,
    DOCKER_MANIFEST_MEDIA_TYPE,
    DOCKER_MANIFEST_LIST_MEDIA_TYPE,
];

/// Options for fetching digest-addressed blobs.
#[derive(Clone, Debug)]
pub struct DigestFetchOptions {
    pub cache_dir: PathBuf,
    pub accepted_manifest_types: Vec<String>,
}

impl Default for DigestFetchOptions {
    fn default() -> Self {
        Self {
            cache_dir: default_cache_root(),
            accepted_manifest_types: DEFAULT_ACCEPTED_MANIFEST_TYPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

/// A digest reference guaranteed to be pinned by digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestRef {
    reference: Reference,
    digest: String,
}

impl DigestRef {
    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn reference(&self) -> &Reference {
        &self.reference
    }
}

impl TryFrom<&str> for DigestRef {
    type Error = RunnerApiError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        parse_digest_ref(value)
    }
}

impl TryFrom<String> for DigestRef {
    type Error = RunnerApiError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        parse_digest_ref(&value)
    }
}

pub trait DigestRefInput {
    fn to_digest_ref(&self) -> Result<DigestRef, RunnerApiError>;
}

impl DigestRefInput for DigestRef {
    fn to_digest_ref(&self) -> Result<DigestRef, RunnerApiError> {
        Ok(self.clone())
    }
}

impl DigestRefInput for &DigestRef {
    fn to_digest_ref(&self) -> Result<DigestRef, RunnerApiError> {
        Ok((*self).clone())
    }
}

impl DigestRefInput for &str {
    fn to_digest_ref(&self) -> Result<DigestRef, RunnerApiError> {
        parse_digest_ref(self)
    }
}

impl DigestRefInput for String {
    fn to_digest_ref(&self) -> Result<DigestRef, RunnerApiError> {
        parse_digest_ref(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CacheInfo {
    pub hit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FetchMetadata {
    pub digest: String,
    pub cache: CacheInfo,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedDigest {
    pub path: PathBuf,
    pub metadata: FetchMetadata,
}

/// Fetch digest-addressed blobs with caching.
pub struct DigestFetcher<C: RegistryClient = DefaultRegistryClient> {
    client: C,
    opts: DigestFetchOptions,
    cache: DigestCache,
}

impl Default for DigestFetcher<DefaultRegistryClient> {
    fn default() -> Self {
        Self::new(DigestFetchOptions::default())
    }
}

impl<C: RegistryClient> DigestFetcher<C> {
    pub fn new(opts: DigestFetchOptions) -> Self {
        let cache = DigestCache::new(opts.cache_dir.clone());
        Self {
            client: C::default_client(),
            opts,
            cache,
        }
    }

    pub fn with_client(client: C, opts: DigestFetchOptions) -> Self {
        let cache = DigestCache::new(opts.cache_dir.clone());
        Self {
            client,
            opts,
            cache,
        }
    }

    pub async fn fetch_by_digest<R: DigestRefInput>(
        &self,
        digest_ref: R,
    ) -> Result<(Vec<u8>, FetchMetadata), RunnerApiError> {
        let cached = self.ensure_cached(digest_ref).await?;
        let bytes = fs::read(&cached.path).map_err(|source| RunnerApiError::Io {
            reference: cached.metadata.digest.clone(),
            source,
        })?;
        Ok((bytes, cached.metadata))
    }

    pub async fn ensure_cached<R: DigestRefInput>(
        &self,
        digest_ref: R,
    ) -> Result<CachedDigest, RunnerApiError> {
        let digest_ref = digest_ref.to_digest_ref()?;
        if let Some(hit) = self.cache.try_hit(&digest_ref) {
            verify_file_digest(&hit.path, digest_ref.digest())?;
            return Ok(hit);
        }

        let accepted_manifest_types = self
            .opts
            .accepted_manifest_types
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        let image = self
            .client
            .pull(digest_ref.reference(), &accepted_manifest_types)
            .await
            .map_err(|source| RunnerApiError::PullFailed {
                reference: digest_ref.reference.to_string(),
                source,
            })?;

        let layer = select_layer_by_digest(&image.layers, digest_ref.digest())?;
        verify_bytes_digest(&layer.data, digest_ref.digest())?;

        let path = self.cache.write(
            digest_ref.digest(),
            layer.media_type.as_deref(),
            &layer.data,
        )?;
        let metadata = FetchMetadata {
            digest: digest_ref.digest().to_string(),
            cache: CacheInfo { hit: false },
            size_bytes: layer.data.len() as u64,
            media_type: layer.media_type.clone(),
        };
        self.cache.write_metadata(&metadata)?;
        Ok(CachedDigest { path, metadata })
    }
}

fn parse_digest_ref(value: &str) -> Result<DigestRef, RunnerApiError> {
    if is_digest(value) {
        return Err(RunnerApiError::MissingRepositoryContext {
            digest: normalize_digest(value),
        });
    }
    let trimmed = value.trim_start_matches("oci://");
    let reference = Reference::try_from(trimmed).map_err(|e| RunnerApiError::InvalidReference {
        reference: value.to_string(),
        reason: e.to_string(),
    })?;
    let digest = reference
        .digest()
        .ok_or_else(|| RunnerApiError::DigestRequired {
            reference: value.to_string(),
        })?
        .to_string();
    let digest = normalize_digest(&digest);
    Ok(DigestRef { reference, digest })
}

fn is_digest(value: &str) -> bool {
    value.starts_with("sha256:") && value.len() == "sha256:".len() + 64
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

fn verify_bytes_digest(bytes: &[u8], expected_digest: &str) -> Result<(), RunnerApiError> {
    let computed = compute_digest(bytes);
    if computed != expected_digest {
        return Err(RunnerApiError::DigestMismatch {
            expected: expected_digest.to_string(),
            actual: computed,
        });
    }
    Ok(())
}

fn verify_file_digest(path: &PathBuf, expected_digest: &str) -> Result<(), RunnerApiError> {
    let mut file = fs::File::open(path).map_err(|source| RunnerApiError::Io {
        reference: expected_digest.to_string(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = file.read(&mut buf).map_err(|source| RunnerApiError::Io {
            reference: expected_digest.to_string(),
            source,
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    let digest = hasher.finalize();
    let mut computed = String::with_capacity("sha256:".len() + digest.len() * 2);
    computed.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut computed, "{byte:02x}");
    }
    if computed != expected_digest {
        return Err(RunnerApiError::CacheDigestMismatch {
            expected: expected_digest.to_string(),
            actual: computed,
        });
    }
    Ok(())
}

fn compute_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut rendered = String::with_capacity("sha256:".len() + digest.len() * 2);
    rendered.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut rendered, "{byte:02x}");
    }
    rendered
}

fn select_layer_by_digest<'a>(
    layers: &'a [PulledLayer],
    digest: &str,
) -> Result<&'a PulledLayer, RunnerApiError> {
    layers
        .iter()
        .find(|layer| layer.digest.as_deref() == Some(digest))
        .ok_or_else(|| RunnerApiError::DigestLayerMissing {
            digest: digest.to_string(),
        })
}

fn default_cache_root() -> PathBuf {
    if let Ok(root) = std::env::var("GREENTIC_DIST_CACHE_DIR") {
        return PathBuf::from(root).join("digests");
    }
    if let Some(cache) = dirs_next::cache_dir() {
        return cache.join("greentic").join("digests");
    }
    if let Ok(root) = std::env::var("GREENTIC_HOME") {
        return PathBuf::from(root).join("cache").join("digests");
    }
    PathBuf::from(".greentic").join("cache").join("digests")
}

#[derive(Clone, Debug)]
struct DigestCache {
    root: PathBuf,
}

impl DigestCache {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn write(
        &self,
        digest: &str,
        _media_type: Option<&str>,
        data: &[u8],
    ) -> Result<PathBuf, RunnerApiError> {
        let dir = self.artifact_dir(digest);
        fs::create_dir_all(&dir).map_err(|source| RunnerApiError::Io {
            reference: digest.to_string(),
            source,
        })?;
        let path = dir.join(DEFAULT_BLOB_FILENAME);
        let mut file = fs::File::create(&path).map_err(|source| RunnerApiError::Io {
            reference: digest.to_string(),
            source,
        })?;
        file.write_all(data).map_err(|source| RunnerApiError::Io {
            reference: digest.to_string(),
            source,
        })?;
        Ok(path)
    }

    fn write_metadata(&self, metadata: &FetchMetadata) -> Result<(), RunnerApiError> {
        let dir = self.artifact_dir(&metadata.digest);
        fs::create_dir_all(&dir).map_err(|source| RunnerApiError::Io {
            reference: metadata.digest.clone(),
            source,
        })?;
        let stored = CacheMetadata {
            digest: metadata.digest.clone(),
            size_bytes: metadata.size_bytes,
            media_type: metadata.media_type.clone(),
            fetched_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        let bytes = serde_json::to_vec_pretty(&stored).map_err(|source| RunnerApiError::Serde {
            reference: metadata.digest.clone(),
            source,
        })?;
        let path = dir.join("metadata.json");
        fs::write(path, bytes).map_err(|source| RunnerApiError::Io {
            reference: metadata.digest.clone(),
            source,
        })?;
        Ok(())
    }

    fn try_hit(&self, digest_ref: &DigestRef) -> Option<CachedDigest> {
        let path = self
            .artifact_dir(digest_ref.digest())
            .join(DEFAULT_BLOB_FILENAME);
        if !path.exists() {
            return None;
        }
        let metadata = self.read_metadata(digest_ref.digest()).ok();
        let size_bytes = metadata
            .as_ref()
            .map(|m| m.size_bytes)
            .or_else(|| fs::metadata(&path).ok().map(|m| m.len()))
            .unwrap_or_default();
        let media_type = metadata.and_then(|m| m.media_type);
        Some(CachedDigest {
            path,
            metadata: FetchMetadata {
                digest: digest_ref.digest().to_string(),
                cache: CacheInfo { hit: true },
                size_bytes,
                media_type,
            },
        })
    }

    fn read_metadata(&self, digest: &str) -> anyhow::Result<CacheMetadata> {
        let path = self.artifact_dir(digest).join("metadata.json");
        let bytes = fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn artifact_dir(&self, digest: &str) -> PathBuf {
        self.root.join(trim_digest_prefix(digest))
    }
}

fn trim_digest_prefix(digest: &str) -> &str {
    digest
        .strip_prefix("sha256:")
        .unwrap_or_else(|| digest.trim_start_matches('@'))
}

#[derive(Clone, Debug)]
pub struct PulledImage {
    pub layers: Vec<PulledLayer>,
}

#[derive(Clone, Debug)]
pub struct PulledLayer {
    pub media_type: Option<String>,
    pub data: Vec<u8>,
    pub digest: Option<String>,
}

#[async_trait]
pub trait RegistryClient: Send + Sync {
    fn default_client() -> Self
    where
        Self: Sized;

    async fn pull(
        &self,
        reference: &Reference,
        accepted_manifest_types: &[&str],
    ) -> Result<PulledImage, OciDistributionError>;
}

/// Registry client backed by `oci-distribution` with HTTPS enforced and anonymous pulls.
#[derive(Clone)]
pub struct DefaultRegistryClient {
    inner: Client,
}

impl Default for DefaultRegistryClient {
    fn default() -> Self {
        Self::default_client()
    }
}

#[async_trait]
impl RegistryClient for DefaultRegistryClient {
    fn default_client() -> Self {
        let config = ClientConfig {
            protocol: ClientProtocol::Https,
            ..Default::default()
        };
        Self {
            inner: Client::new(config),
        }
    }

    async fn pull(
        &self,
        reference: &Reference,
        accepted_manifest_types: &[&str],
    ) -> Result<PulledImage, OciDistributionError> {
        let image = self
            .inner
            .pull(
                reference,
                &RegistryAuth::Anonymous,
                accepted_manifest_types.to_vec(),
            )
            .await?;
        Ok(convert_image(image))
    }
}

fn convert_image(image: ImageData) -> PulledImage {
    let layers = image
        .layers
        .into_iter()
        .map(|layer| {
            let digest = format!("sha256:{}", layer.sha256_digest());
            PulledLayer {
                media_type: Some(layer.media_type),
                data: layer.data,
                digest: Some(digest),
            }
        })
        .collect();
    PulledImage { layers }
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheMetadata {
    digest: String,
    size_bytes: u64,
    #[serde(default)]
    media_type: Option<String>,
    fetched_at_unix_seconds: u64,
}

#[derive(Debug, Error)]
pub enum RunnerApiError {
    #[error("invalid OCI reference `{reference}`: {reason}")]
    InvalidReference { reference: String, reason: String },
    #[error("digest pin required for `{reference}`")]
    DigestRequired { reference: String },
    #[error("digest `{digest}` lacks repository context; pass an OCI digest ref")]
    MissingRepositoryContext { digest: String },
    #[error("no layer found matching digest `{digest}`")]
    DigestLayerMissing { digest: String },
    #[error("digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("cached digest mismatch: expected {expected}, got {actual}")]
    CacheDigestMismatch { expected: String, actual: String },
    #[error("failed to pull `{reference}`: {source}")]
    PullFailed {
        reference: String,
        #[source]
        source: oci_distribution::errors::OciDistributionError,
    },
    #[error("io error while handling `{reference}`: {source}")]
    Io {
        reference: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize metadata for `{reference}`: {source}")]
    Serde {
        reference: String,
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DIGEST: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn parse_digest_ref_rejects_bare_digest() {
        let err = parse_digest_ref(TEST_DIGEST).unwrap_err();
        assert!(matches!(
            err,
            RunnerApiError::MissingRepositoryContext { .. }
        ));
    }

    #[test]
    fn verify_bytes_digest_rejects_mismatch() {
        let err = verify_bytes_digest(b"hello", TEST_DIGEST).unwrap_err();
        assert!(matches!(err, RunnerApiError::DigestMismatch { .. }));
    }

    #[test]
    fn select_layer_by_digest_requires_exact_match() {
        let layers = vec![PulledLayer {
            media_type: Some("application/wasm".to_string()),
            data: b"blob".to_vec(),
            digest: Some(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            ),
        }];
        let err = select_layer_by_digest(&layers, TEST_DIGEST).unwrap_err();
        assert!(matches!(err, RunnerApiError::DigestLayerMissing { .. }));
    }
}
