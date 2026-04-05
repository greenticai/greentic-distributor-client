use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
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

/// Accepted manifest media types when pulling components.
static DEFAULT_ACCEPTED_MANIFEST_TYPES: &[&str] = &[
    OCI_ARTIFACT_MANIFEST_MEDIA_TYPE,
    OCI_IMAGE_MEDIA_TYPE,
    OCI_IMAGE_INDEX_MEDIA_TYPE,
    IMAGE_MANIFEST_MEDIA_TYPE,
    IMAGE_MANIFEST_LIST_MEDIA_TYPE,
    DOCKER_MANIFEST_MEDIA_TYPE,
    DOCKER_MANIFEST_LIST_MEDIA_TYPE,
];

const COMPONENT_MANIFEST_MEDIA_TYPE: &str = "application/vnd.greentic.component.manifest+json";
const COMPONENT_MANIFEST_CBOR_MEDIA_TYPE: &str = "application/vnd.greentic.component.manifest+cbor";
const DEFAULT_WASM_FILENAME: &str = "component.wasm";

/// Preferred component layer media types.
static DEFAULT_LAYER_MEDIA_TYPES: &[&str] = &[
    "application/vnd.wasm.component.v1+wasm",
    "application/vnd.module.wasm.content.layer.v1+wasm",
    "application/wasm",
    COMPONENT_MANIFEST_CBOR_MEDIA_TYPE,
    COMPONENT_MANIFEST_MEDIA_TYPE,
    "application/octet-stream",
];

/// Greentic pack extension for components.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ComponentsExtension {
    pub refs: Vec<String>,
    #[serde(default)]
    pub mode: ComponentsMode,
}

/// Pull mode for components.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ComponentsMode {
    #[default]
    Eager,
    Lazy,
}

/// Configuration for resolving OCI component references.
#[derive(Clone, Debug)]
pub struct ComponentResolveOptions {
    pub allow_tags: bool,
    pub offline: bool,
    pub cache_dir: PathBuf,
    pub accepted_manifest_types: Vec<String>,
    pub preferred_layer_media_types: Vec<String>,
}

impl Default for ComponentResolveOptions {
    fn default() -> Self {
        Self {
            allow_tags: false,
            offline: false,
            cache_dir: default_cache_root(),
            accepted_manifest_types: DEFAULT_ACCEPTED_MANIFEST_TYPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            preferred_layer_media_types: DEFAULT_LAYER_MEDIA_TYPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

/// Result of resolving a single component reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedComponent {
    pub original_reference: String,
    pub resolved_digest: String,
    pub media_type: String,
    pub path: PathBuf,
    pub fetched_from_network: bool,
    pub manifest_digest: Option<String>,
}

/// Descriptor-time resolution result for a single OCI component reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedComponentDescriptor {
    pub original_reference: String,
    pub resolved_digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub fetched_from_network: bool,
    pub manifest_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComponentManifest {
    #[serde(default)]
    artifacts: Option<ComponentManifestArtifacts>,
}

#[derive(Debug, Deserialize)]
struct ComponentManifestArtifacts {
    #[serde(default)]
    component_wasm: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CacheMetadata {
    original_reference: String,
    resolved_digest: String,
    media_type: String,
    fetched_at_unix_seconds: u64,
    size_bytes: u64,
    #[serde(default)]
    manifest_digest: Option<String>,
    #[serde(default)]
    manifest_wasm_name: Option<String>,
}

/// Resolve OCI component references with caching and offline support.
pub struct OciComponentResolver<C: RegistryClient = DefaultRegistryClient> {
    client: C,
    opts: ComponentResolveOptions,
    cache: OciCache,
}

impl Default for OciComponentResolver<DefaultRegistryClient> {
    fn default() -> Self {
        Self::new(ComponentResolveOptions::default())
    }
}

impl<C: RegistryClient> OciComponentResolver<C> {
    pub fn new(opts: ComponentResolveOptions) -> Self {
        let cache = OciCache::new(opts.cache_dir.clone());
        Self {
            client: C::default_client(),
            opts,
            cache,
        }
    }

    pub fn with_client(client: C, opts: ComponentResolveOptions) -> Self {
        let cache = OciCache::new(opts.cache_dir.clone());
        Self {
            client,
            opts,
            cache,
        }
    }

    pub async fn resolve_refs(
        &self,
        extension: &ComponentsExtension,
    ) -> Result<Vec<ResolvedComponent>, OciComponentError> {
        let mut results = Vec::with_capacity(extension.refs.len());
        for reference in &extension.refs {
            results.push(self.resolve_single(reference).await?);
        }
        Ok(results)
    }

    pub async fn resolve_descriptors(
        &self,
        extension: &ComponentsExtension,
    ) -> Result<Vec<ResolvedComponentDescriptor>, OciComponentError> {
        let mut results = Vec::with_capacity(extension.refs.len());
        for reference in &extension.refs {
            results.push(self.resolve_descriptor(reference).await?);
        }
        Ok(results)
    }

    pub async fn resolve_descriptor(
        &self,
        reference: &str,
    ) -> Result<ResolvedComponentDescriptor, OciComponentError> {
        let parsed =
            Reference::try_from(reference).map_err(|e| OciComponentError::InvalidReference {
                reference: reference.to_string(),
                reason: e.to_string(),
            })?;

        if parsed.digest().is_none() && !self.opts.allow_tags {
            return Err(OciComponentError::DigestRequired {
                reference: reference.to_string(),
            });
        }

        let expected_digest = parsed.digest().map(normalize_digest);
        if let Some(expected_digest) = expected_digest.as_ref() {
            if let Some(hit) = self.cache.try_descriptor_hit(expected_digest, reference) {
                return Ok(hit);
            }
            if self.opts.offline {
                return Err(OciComponentError::OfflineMissing {
                    reference: reference.to_string(),
                    digest: expected_digest.clone(),
                });
            }
        } else if self.opts.offline {
            return Err(OciComponentError::OfflineTaggedReference {
                reference: reference.to_string(),
            });
        }

        let accepted_layer_types = self
            .opts
            .preferred_layer_media_types
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        let image = self
            .client
            .pull(&parsed, &accepted_layer_types)
            .await
            .map_err(|source| OciComponentError::PullFailed {
                reference: reference.to_string(),
                source,
            })?;

        let chosen_layer = select_layer(
            &image.layers,
            &self.opts.preferred_layer_media_types,
            reference,
        )?;
        let resolved_digest = image
            .digest
            .clone()
            .or_else(|| chosen_layer.digest.clone())
            .unwrap_or_else(|| compute_digest(&chosen_layer.data));
        let manifest_digest = image.digest.clone();

        if let Some(expected) = expected_digest.as_ref()
            && expected != &resolved_digest
        {
            return Err(OciComponentError::DigestMismatch {
                reference: reference.to_string(),
                expected: expected.clone(),
                actual: resolved_digest.clone(),
            });
        }

        Ok(ResolvedComponentDescriptor {
            original_reference: reference.to_string(),
            resolved_digest,
            media_type: chosen_layer.media_type.clone(),
            size_bytes: chosen_layer.data.len() as u64,
            fetched_from_network: true,
            manifest_digest,
        })
    }

    async fn resolve_single(
        &self,
        reference: &str,
    ) -> Result<ResolvedComponent, OciComponentError> {
        let parsed =
            Reference::try_from(reference).map_err(|e| OciComponentError::InvalidReference {
                reference: reference.to_string(),
                reason: e.to_string(),
            })?;

        if parsed.digest().is_none() && !self.opts.allow_tags {
            return Err(OciComponentError::DigestRequired {
                reference: reference.to_string(),
            });
        }

        let expected_digest = parsed.digest().map(normalize_digest);
        if let Some(expected_digest) = expected_digest.as_ref() {
            if let Some(hit) = self.cache.try_hit(expected_digest, reference) {
                return Ok(hit);
            }
            if self.opts.offline {
                return Err(OciComponentError::OfflineMissing {
                    reference: reference.to_string(),
                    digest: expected_digest.clone(),
                });
            }
        } else if self.opts.offline {
            return Err(OciComponentError::OfflineTaggedReference {
                reference: reference.to_string(),
            });
        }

        let accepted_layer_types = self
            .opts
            .preferred_layer_media_types
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        let image = self
            .client
            .pull(&parsed, &accepted_layer_types)
            .await
            .map_err(|source| OciComponentError::PullFailed {
                reference: reference.to_string(),
                source,
            })?;

        let chosen_layer = select_layer(
            &image.layers,
            &self.opts.preferred_layer_media_types,
            reference,
        )?;
        // Prefer CBOR manifest layer (auto-generated from describe()) over JSON.
        let manifest_layer = image
            .layers
            .iter()
            .find(|layer| layer.media_type == COMPONENT_MANIFEST_CBOR_MEDIA_TYPE)
            .or_else(|| {
                image
                    .layers
                    .iter()
                    .find(|layer| layer.media_type == COMPONENT_MANIFEST_MEDIA_TYPE)
            });
        let manifest_wasm_name = if let Some(layer) = manifest_layer {
            if layer.media_type == COMPONENT_MANIFEST_CBOR_MEDIA_TYPE {
                manifest_component_wasm_name_cbor(&layer.data, reference)?
            } else {
                manifest_component_wasm_name(&layer.data, reference)?
            }
        } else {
            None
        };
        let resolved_digest = image
            .digest
            .clone()
            .or_else(|| chosen_layer.digest.clone())
            .unwrap_or_else(|| compute_digest(&chosen_layer.data));
        let manifest_digest = image.digest.clone();

        if let Some(expected) = expected_digest.as_ref()
            && expected != &resolved_digest
        {
            return Err(OciComponentError::DigestMismatch {
                reference: reference.to_string(),
                expected: expected.clone(),
                actual: resolved_digest.clone(),
            });
        }

        let path = self.cache.write(
            &resolved_digest,
            &chosen_layer.media_type,
            &chosen_layer.data,
            reference,
            manifest_digest.clone(),
            manifest_wasm_name.as_deref(),
        )?;
        if let Some(layer) = manifest_layer
            && layer.media_type != chosen_layer.media_type
        {
            self.cache.write_manifest_layer(
                &resolved_digest,
                &layer.media_type,
                &layer.data,
                reference,
            )?;
        }

        Ok(ResolvedComponent {
            original_reference: reference.to_string(),
            resolved_digest,
            media_type: chosen_layer.media_type.clone(),
            path,
            fetched_from_network: true,
            manifest_digest,
        })
    }
}

fn select_layer<'a>(
    layers: &'a [PulledLayer],
    preferred_types: &[String],
    reference: &str,
) -> Result<&'a PulledLayer, OciComponentError> {
    if layers.is_empty() {
        return Err(OciComponentError::MissingLayers {
            reference: reference.to_string(),
        });
    }
    let mut best_idx = 0usize;
    let mut best_rank = usize::MAX;
    let mut last_media_type = "";
    let mut last_rank = None;
    for (idx, layer) in layers.iter().enumerate() {
        let media_type = layer.media_type.as_str();
        let rank = if media_type == last_media_type {
            last_rank
        } else {
            let looked_up = preferred_types
                .iter()
                .position(|preferred| preferred == media_type);
            last_media_type = media_type;
            last_rank = looked_up;
            looked_up
        };
        if let Some(rank) = rank
            && rank < best_rank
        {
            best_idx = idx;
            best_rank = rank;
            if rank == 0 {
                break;
            }
        }
    }
    Ok(&layers[best_idx])
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

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

pub(crate) fn default_cache_root() -> PathBuf {
    if let Ok(root) = std::env::var("GREENTIC_DIST_CACHE_DIR") {
        return PathBuf::from(root);
    }
    if let Some(cache) = dirs_next::cache_dir() {
        return cache.join("greentic").join("components");
    }
    if let Ok(root) = std::env::var("GREENTIC_HOME") {
        return PathBuf::from(root).join("cache").join("components");
    }
    PathBuf::from(".greentic").join("cache").join("components")
}

fn manifest_component_wasm_name(
    data: &[u8],
    reference: &str,
) -> Result<Option<String>, OciComponentError> {
    let manifest: ComponentManifest =
        serde_json::from_slice(data).map_err(|source| OciComponentError::ManifestParse {
            reference: reference.to_string(),
            source,
        })?;
    validate_manifest_wasm_name(manifest, reference)
}

/// Extract the component WASM filename from CBOR-encoded manifest data.
fn manifest_component_wasm_name_cbor(
    data: &[u8],
    reference: &str,
) -> Result<Option<String>, OciComponentError> {
    const MAX_MANIFEST_SIZE: usize = 64 * 1024; // 64KB
    if data.len() > MAX_MANIFEST_SIZE {
        return Err(OciComponentError::ManifestParseCbor {
            reference: reference.to_string(),
            reason: format!(
                "manifest too large: {} bytes (max {})",
                data.len(),
                MAX_MANIFEST_SIZE
            ),
        });
    }
    let manifest: ComponentManifest =
        ciborium::from_reader(data).map_err(|e| OciComponentError::ManifestParseCbor {
            reference: reference.to_string(),
            reason: e.to_string(),
        })?;
    validate_manifest_wasm_name(manifest, reference)
}

fn validate_manifest_wasm_name(
    manifest: ComponentManifest,
    reference: &str,
) -> Result<Option<String>, OciComponentError> {
    let name = manifest
        .artifacts
        .and_then(|artifacts| artifacts.component_wasm)
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty());
    if let Some(name) = name.as_deref() {
        let path = std::path::Path::new(name);
        if path.components().count() != 1 {
            return Err(OciComponentError::InvalidManifestWasmName {
                reference: reference.to_string(),
                name: name.to_string(),
            });
        }
    }
    Ok(name)
}

#[derive(Debug)]
struct OciCache {
    root: PathBuf,
    metadata_cache: RwLock<HashMap<String, CacheMetadata>>,
}

impl OciCache {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            metadata_cache: RwLock::new(HashMap::new()),
        }
    }

    fn write_layer_data(
        &self,
        digest: &str,
        media_type: &str,
        data: &[u8],
        reference: &str,
    ) -> Result<PathBuf, OciComponentError> {
        let dir = self.artifact_dir(digest);
        fs::create_dir_all(&dir).map_err(|source| OciComponentError::Io {
            reference: reference.to_string(),
            source,
        })?;

        let artifact_path = self.artifact_path_for_media_type(digest, media_type, None);
        fs::write(&artifact_path, data).map_err(|source| OciComponentError::Io {
            reference: reference.to_string(),
            source,
        })?;
        Ok(artifact_path)
    }

    fn write(
        &self,
        digest: &str,
        media_type: &str,
        data: &[u8],
        reference: &str,
        manifest_digest: Option<String>,
        manifest_wasm_name: Option<&str>,
    ) -> Result<PathBuf, OciComponentError> {
        let artifact_path = if media_type == COMPONENT_MANIFEST_MEDIA_TYPE
            || media_type == COMPONENT_MANIFEST_CBOR_MEDIA_TYPE
        {
            self.write_layer_data(digest, media_type, data, reference)?
        } else if let Some(name) = manifest_wasm_name {
            let path = self.write_named_file(digest, name, data, reference)?;
            if name != DEFAULT_WASM_FILENAME {
                self.write_legacy_symlink(self.artifact_dir(digest).as_path(), name);
            }
            path
        } else {
            self.write_layer_data(digest, media_type, data, reference)?
        };
        let dir = self.artifact_dir(digest);

        let metadata = CacheMetadata {
            original_reference: reference.to_string(),
            resolved_digest: digest.to_string(),
            media_type: media_type.to_string(),
            fetched_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            size_bytes: data.len() as u64,
            manifest_digest,
            manifest_wasm_name: manifest_wasm_name.map(|name| name.to_string()),
        };
        let metadata_path = dir.join("metadata.json");
        let buf = serde_json::to_vec(&metadata).map_err(|source| OciComponentError::Serde {
            reference: reference.to_string(),
            source,
        })?;
        fs::write(&metadata_path, buf).map_err(|source| OciComponentError::Io {
            reference: reference.to_string(),
            source,
        })?;
        self.store_metadata(digest, &metadata);

        Ok(artifact_path)
    }

    fn write_named_file(
        &self,
        digest: &str,
        filename: &str,
        data: &[u8],
        reference: &str,
    ) -> Result<PathBuf, OciComponentError> {
        let dir = self.artifact_dir(digest);
        fs::create_dir_all(&dir).map_err(|source| OciComponentError::Io {
            reference: reference.to_string(),
            source,
        })?;
        let path = dir.join(filename);
        fs::write(&path, data).map_err(|source| OciComponentError::Io {
            reference: reference.to_string(),
            source,
        })?;
        Ok(path)
    }

    fn write_manifest_layer(
        &self,
        digest: &str,
        media_type: &str,
        data: &[u8],
        reference: &str,
    ) -> Result<PathBuf, OciComponentError> {
        self.write_layer_data(digest, media_type, data, reference)
    }

    fn try_hit(&self, digest: &str, reference: &str) -> Option<ResolvedComponent> {
        let metadata = self.read_metadata(digest).ok();
        let media_type = metadata
            .as_ref()
            .map(|m| m.media_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let manifest_wasm_name = metadata
            .as_ref()
            .and_then(|m| m.manifest_wasm_name.clone())
            .or_else(|| self.manifest_wasm_name_from_cache(digest, reference));
        let path =
            self.artifact_path_for_media_type(digest, &media_type, manifest_wasm_name.as_deref());
        if !path.exists() {
            return None;
        }
        Some(ResolvedComponent {
            original_reference: reference.to_string(),
            resolved_digest: digest.to_string(),
            media_type,
            path,
            fetched_from_network: false,
            manifest_digest: metadata.and_then(|m| m.manifest_digest),
        })
    }

    fn try_descriptor_hit(
        &self,
        digest: &str,
        reference: &str,
    ) -> Option<ResolvedComponentDescriptor> {
        let metadata = self.read_metadata(digest).ok();
        let media_type = metadata
            .as_ref()
            .map(|m| m.media_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let manifest_wasm_name = metadata
            .as_ref()
            .and_then(|m| m.manifest_wasm_name.clone())
            .or_else(|| self.manifest_wasm_name_from_cache(digest, reference));
        let path =
            self.artifact_path_for_media_type(digest, &media_type, manifest_wasm_name.as_deref());
        if !path.exists() {
            return None;
        }
        let size_bytes = metadata
            .as_ref()
            .map(|m| m.size_bytes)
            .or_else(|| fs::metadata(&path).ok().map(|m| m.len()))
            .unwrap_or_default();
        Some(ResolvedComponentDescriptor {
            original_reference: reference.to_string(),
            resolved_digest: digest.to_string(),
            media_type,
            size_bytes,
            fetched_from_network: false,
            manifest_digest: metadata.and_then(|m| m.manifest_digest),
        })
    }

    fn read_metadata(&self, digest: &str) -> anyhow::Result<CacheMetadata> {
        if let Some(metadata) = self.cached_metadata(digest) {
            return Ok(metadata);
        }
        let metadata_path = self.metadata_path(digest);
        let bytes = fs::read(metadata_path)?;
        let metadata = serde_json::from_slice(&bytes)?;
        self.store_metadata(digest, &metadata);
        Ok(metadata)
    }

    fn artifact_dir(&self, digest: &str) -> PathBuf {
        self.root.join(trim_digest_prefix(digest))
    }

    fn artifact_path_for_media_type(
        &self,
        digest: &str,
        media_type: &str,
        manifest_wasm_name: Option<&str>,
    ) -> PathBuf {
        let dir = self.artifact_dir(digest);
        let filename = if media_type == COMPONENT_MANIFEST_MEDIA_TYPE {
            "component.manifest.json"
        } else if media_type == COMPONENT_MANIFEST_CBOR_MEDIA_TYPE {
            "component.manifest.cbor"
        } else if let Some(name) = manifest_wasm_name {
            name
        } else {
            DEFAULT_WASM_FILENAME
        };
        dir.join(filename)
    }

    fn manifest_wasm_name_from_cache(&self, digest: &str, reference: &str) -> Option<String> {
        let dir = self.artifact_dir(digest);
        // Prefer CBOR manifest (auto-generated from describe()) over JSON.
        let cbor_path = dir.join("component.manifest.cbor");
        if cbor_path.exists() {
            let data = fs::read(cbor_path).ok()?;
            return manifest_component_wasm_name_cbor(&data, reference)
                .ok()
                .flatten();
        }
        let json_path = dir.join("component.manifest.json");
        if json_path.exists() {
            let data = fs::read(json_path).ok()?;
            return manifest_component_wasm_name(&data, reference)
                .ok()
                .flatten();
        }
        None
    }

    fn write_legacy_symlink(&self, dir: &Path, target: &str) {
        let legacy_path = dir.join(DEFAULT_WASM_FILENAME);
        if legacy_path.exists() {
            return;
        }
        let target_path = dir.join(target);
        #[cfg(unix)]
        {
            let _ = std::os::unix::fs::symlink(&target_path, &legacy_path);
        }
        #[cfg(windows)]
        {
            let _ = std::os::windows::fs::symlink_file(&target_path, &legacy_path);
        }
    }

    fn metadata_path(&self, digest: &str) -> PathBuf {
        self.artifact_dir(digest).join("metadata.json")
    }

    fn cached_metadata(&self, digest: &str) -> Option<CacheMetadata> {
        self.metadata_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(trim_digest_prefix(digest)).cloned())
    }

    fn store_metadata(&self, digest: &str, metadata: &CacheMetadata) {
        if let Ok(mut cache) = self.metadata_cache.write() {
            cache.insert(trim_digest_prefix(digest).to_string(), metadata.clone());
        }
    }
}

fn trim_digest_prefix(digest: &str) -> &str {
    digest
        .strip_prefix("sha256:")
        .unwrap_or_else(|| digest.trim_start_matches('@'))
}

#[derive(Clone, Debug)]
pub struct PulledImage {
    pub digest: Option<String>,
    pub layers: Vec<PulledLayer>,
}

#[derive(Clone, Debug)]
pub struct PulledLayer {
    pub media_type: String,
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
    auth: RegistryClientAuth,
}

#[derive(Clone, Debug)]
enum RegistryClientAuth {
    Anonymous,
    Basic { username: String, password: String },
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
            auth: RegistryClientAuth::Anonymous,
        }
    }

    async fn pull(
        &self,
        reference: &Reference,
        accepted_manifest_types: &[&str],
    ) -> Result<PulledImage, OciDistributionError> {
        let auth = match &self.auth {
            RegistryClientAuth::Anonymous => RegistryAuth::Anonymous,
            RegistryClientAuth::Basic { username, password } => {
                RegistryAuth::Basic(username.clone(), password.clone())
            }
        };
        let image = self
            .inner
            .pull(reference, &auth, accepted_manifest_types.to_vec())
            .await?;
        Ok(convert_image(image))
    }
}

impl DefaultRegistryClient {
    pub fn with_basic_auth(username: impl Into<String>, password: impl Into<String>) -> Self {
        let mut client = Self::default_client();
        client.auth = RegistryClientAuth::Basic {
            username: username.into(),
            password: password.into(),
        };
        client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DIGEST: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn select_layer_prefers_wasm_over_manifest() {
        let layers = vec![
            PulledLayer {
                media_type: COMPONENT_MANIFEST_MEDIA_TYPE.to_string(),
                data: br#"{"name":"demo"}"#.to_vec(),
                digest: None,
            },
            PulledLayer {
                media_type: "application/wasm".to_string(),
                data: b"wasm-bytes".to_vec(),
                digest: None,
            },
        ];
        let opts = ComponentResolveOptions::default();

        let chosen = select_layer(&layers, &opts.preferred_layer_media_types, "ref").unwrap();

        assert_eq!(chosen.media_type, "application/wasm");
    }

    #[test]
    fn cache_writes_manifest_and_wasm_paths() {
        let temp = tempfile::tempdir().unwrap();
        let cache = OciCache::new(temp.path().to_path_buf());
        let digest = TEST_DIGEST;
        let reference = "ghcr.io/greentic/components@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let manifest_path = cache
            .write(
                digest,
                COMPONENT_MANIFEST_MEDIA_TYPE,
                br#"{"name":"demo"}"#,
                reference,
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            manifest_path.file_name().and_then(|s| s.to_str()),
            Some("component.manifest.json")
        );
        assert!(manifest_path.exists());

        let wasm_path = cache
            .write(
                digest,
                "application/wasm",
                b"wasm-bytes",
                reference,
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            wasm_path.file_name().and_then(|s| s.to_str()),
            Some("component.wasm")
        );
        assert!(wasm_path.exists());
    }

    #[test]
    fn cache_writes_manifest_named_wasm_file() {
        let temp = tempfile::tempdir().unwrap();
        let cache = OciCache::new(temp.path().to_path_buf());
        let digest = TEST_DIGEST;
        let reference = "ghcr.io/greentic/components@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let manifest_bytes = br#"{"artifacts":{"component_wasm":"component_templates.wasm"}}"#;

        let manifest_path = cache
            .write(
                digest,
                COMPONENT_MANIFEST_MEDIA_TYPE,
                manifest_bytes,
                reference,
                None,
                None,
            )
            .unwrap();
        assert!(manifest_path.exists());

        let manifest_name = manifest_component_wasm_name(manifest_bytes, reference)
            .unwrap()
            .unwrap();
        let wasm_path = cache
            .write(
                digest,
                "application/wasm",
                b"wasm-bytes",
                reference,
                None,
                Some(&manifest_name),
            )
            .unwrap();
        assert!(wasm_path.exists());
        assert!(cache.artifact_dir(digest).join(&manifest_name).exists());
        let legacy_path = cache.artifact_dir(digest).join(DEFAULT_WASM_FILENAME);
        if legacy_path.exists() {
            let metadata = fs::symlink_metadata(&legacy_path).unwrap();
            assert!(metadata.file_type().is_symlink());
        }
    }

    #[derive(Clone)]
    struct FakeClient {
        image: PulledImage,
    }

    #[async_trait]
    impl RegistryClient for FakeClient {
        fn default_client() -> Self {
            Self {
                image: PulledImage {
                    digest: None,
                    layers: Vec::new(),
                },
            }
        }

        async fn pull(
            &self,
            _reference: &Reference,
            _accepted_manifest_types: &[&str],
        ) -> Result<PulledImage, OciDistributionError> {
            Ok(self.image.clone())
        }
    }

    #[tokio::test]
    async fn resolve_returns_manifest_named_wasm_path() {
        let temp = tempfile::tempdir().unwrap();
        let manifest_bytes =
            br#"{"artifacts":{"component_wasm":"component_templates.wasm"}}"#.to_vec();
        let image = PulledImage {
            digest: Some(TEST_DIGEST.to_string()),
            layers: vec![
                PulledLayer {
                    media_type: COMPONENT_MANIFEST_MEDIA_TYPE.to_string(),
                    data: manifest_bytes.clone(),
                    digest: None,
                },
                PulledLayer {
                    media_type: "application/wasm".to_string(),
                    data: b"wasm-bytes".to_vec(),
                    digest: None,
                },
            ],
        };
        let client = FakeClient { image };
        let opts = ComponentResolveOptions {
            cache_dir: temp.path().to_path_buf(),
            ..Default::default()
        };
        let resolver = OciComponentResolver::with_client(client, opts);
        let reference = "ghcr.io/greentic/components@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let resolved = resolver
            .resolve_refs(&ComponentsExtension {
                refs: vec![reference.to_string()],
                mode: ComponentsMode::Eager,
            })
            .await
            .unwrap();
        let resolved = &resolved[0];
        assert_eq!(
            resolved.path.file_name().and_then(|s| s.to_str()),
            Some("component_templates.wasm")
        );
        let cache_dir = resolved.path.parent().unwrap();
        assert!(cache_dir.join("component.manifest.json").exists());
        assert!(cache_dir.join("component_templates.wasm").exists());
        let legacy_path = cache_dir.join(DEFAULT_WASM_FILENAME);
        if legacy_path.exists() {
            let metadata = fs::symlink_metadata(&legacy_path).unwrap();
            assert!(metadata.file_type().is_symlink());
        }

        let client_offline = FakeClient {
            image: PulledImage {
                digest: None,
                layers: Vec::new(),
            },
        };
        let opts_offline = ComponentResolveOptions {
            cache_dir: temp.path().to_path_buf(),
            offline: true,
            ..Default::default()
        };
        let resolver_offline = OciComponentResolver::with_client(client_offline, opts_offline);
        let resolved_offline = resolver_offline
            .resolve_refs(&ComponentsExtension {
                refs: vec![reference.to_string()],
                mode: ComponentsMode::Eager,
            })
            .await
            .unwrap();
        assert_eq!(
            resolved_offline[0]
                .path
                .file_name()
                .and_then(|s| s.to_str()),
            Some("component_templates.wasm")
        );
    }

    #[test]
    fn cbor_manifest_extracts_wasm_name() {
        let manifest = serde_json::json!({
            "artifacts": {"component_wasm": "my-component.wasm"},
            "id": "test"
        });
        let mut cbor_bytes = Vec::new();
        ciborium::into_writer(&manifest, &mut cbor_bytes).unwrap();

        let result = manifest_component_wasm_name_cbor(&cbor_bytes, "test-ref").unwrap();

        assert_eq!(result.as_deref(), Some("my-component.wasm"));
    }

    #[test]
    fn corrupt_cbor_returns_parse_error() {
        let garbage = b"\xff\xfe\xfd\x00\x01\x02";

        let result = manifest_component_wasm_name_cbor(garbage, "bad-ref");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, OciComponentError::ManifestParseCbor { .. }),
            "expected ManifestParseCbor, got: {err:?}"
        );
    }

    #[test]
    fn oversized_cbor_rejected() {
        let oversized = vec![0u8; 64 * 1024 + 1];

        let result = manifest_component_wasm_name_cbor(&oversized, "big-ref");

        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            OciComponentError::ManifestParseCbor { reason, .. } => {
                assert!(
                    reason.contains("manifest too large"),
                    "expected 'manifest too large' in reason, got: {reason}"
                );
            }
            other => panic!("expected ManifestParseCbor, got: {other:?}"),
        }
    }

    #[test]
    fn artifact_path_maps_cbor_media_type() {
        let temp = tempfile::tempdir().unwrap();
        let cache = OciCache::new(temp.path().to_path_buf());

        let path = cache.artifact_path_for_media_type(
            TEST_DIGEST,
            COMPONENT_MANIFEST_CBOR_MEDIA_TYPE,
            None,
        );

        assert_eq!(
            path.file_name().and_then(|s| s.to_str()),
            Some("component.manifest.cbor")
        );
    }
}

fn convert_image(image: ImageData) -> PulledImage {
    let layers = image
        .layers
        .into_iter()
        .map(|layer| {
            let digest = format!("sha256:{}", layer.sha256_digest());
            PulledLayer {
                media_type: layer.media_type,
                data: layer.data,
                digest: Some(digest),
            }
        })
        .collect();
    PulledImage {
        digest: image.digest,
        layers,
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OciComponentError {
    #[error("invalid OCI reference `{reference}`: {reason}")]
    InvalidReference { reference: String, reason: String },
    #[error("digest pin required for `{reference}` (rerun with --allow-tags to permit tag refs)")]
    DigestRequired { reference: String },
    #[error("offline mode prohibits tagged reference `{reference}`; pin by digest first")]
    OfflineTaggedReference { reference: String },
    #[error("offline mode could not find cached component for `{reference}` (digest `{digest}`)")]
    OfflineMissing { reference: String, digest: String },
    #[error("no layers returned for `{reference}`")]
    MissingLayers { reference: String },
    #[error("component layer missing for `{reference}`; tried media types {media_types}")]
    MissingComponent {
        reference: String,
        media_types: String,
    },
    #[error("digest mismatch for `{reference}`: expected {expected}, got {actual}")]
    DigestMismatch {
        reference: String,
        expected: String,
        actual: String,
    },
    #[error("failed to pull `{reference}`: {source}")]
    PullFailed {
        reference: String,
        #[source]
        source: oci_distribution::errors::OciDistributionError,
    },
    #[error("io error while caching `{reference}`: {source}")]
    Io {
        reference: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize cache metadata for `{reference}`: {source}")]
    Serde {
        reference: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse component manifest for `{reference}`: {source}")]
    ManifestParse {
        reference: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse CBOR component manifest for `{reference}`: {reason}")]
    ManifestParseCbor { reference: String, reason: String },
    #[error("invalid component_wasm filename `{name}` in manifest for `{reference}`")]
    InvalidManifestWasmName { reference: String, name: String },
}
