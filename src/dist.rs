use crate::oci_components::{
    ComponentResolveOptions, DefaultRegistryClient as ComponentRegistryClient, OciComponentError,
    OciComponentResolver, PulledImage, PulledLayer,
};
use crate::oci_packs::{
    DefaultRegistryClient as PackRegistryClient, OciPackFetcher, PackFetchOptions,
};
use crate::store_auth::{
    StoreCredentials, default_store_auth_path, default_store_state_path, load_login,
};
use async_trait::async_trait;
use oci_distribution::Reference;
use oci_distribution::client::{Client, ClientConfig, ClientProtocol};
use oci_distribution::errors::OciDistributionError;
use oci_distribution::manifest::OciManifest;
use oci_distribution::secrets::RegistryAuth;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::OnceCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const WASM_CONTENT_TYPE: &str = "application/wasm";
static LAST_USED_COUNTER: AtomicU64 = AtomicU64::new(1);

const CACHE_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactSourceKind {
    Oci,
    Https,
    File,
    Fixture,
    Repo,
    Store,
    CacheDigest,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportHints {
    pub offline: bool,
    pub allow_insecure_local_http: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSource {
    pub raw_ref: String,
    pub kind: ArtifactSourceKind,
    pub transport_hints: TransportHints,
    pub dev_mode: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactType {
    Bundle,
    Pack,
    Component,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedVia {
    Direct,
    TagResolution,
    RepoMapping,
    StoreMapping,
    Fixture,
    File,
    Https,
    CacheDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    pub artifact_type: ArtifactType,
    pub source_kind: ArtifactSourceKind,
    pub raw_ref: String,
    pub canonical_ref: String,
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub created_at: Option<u64>,
    pub annotations: serde_json::Map<String, serde_json::Value>,
    pub manifest_digest: Option<String>,
    pub resolved_via: ResolvedVia,
    pub signature_refs: Vec<String>,
    pub sbom_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadedStoreArtifact {
    pub source_ref: String,
    pub mapped_reference: String,
    pub canonical_ref: String,
    pub digest: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
    pub size_bytes: u64,
    pub manifest_digest: Option<String>,
}

impl ArtifactDescriptor {
    pub fn cache_key(&self) -> String {
        self.digest.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityState {
    Partial,
    Ready,
    Corrupt,
    Evicted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub raw_ref: String,
    pub canonical_ref: String,
    pub source_kind: ArtifactSourceKind,
    pub authoritative: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheEntryState {
    Partial,
    Ready,
    Corrupt,
    Evicted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub format_version: u32,
    pub cache_key: String,
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub artifact_type: ArtifactType,
    pub source_kind: ArtifactSourceKind,
    pub raw_ref: String,
    pub canonical_ref: String,
    pub fetched_at: u64,
    pub last_accessed_at: u64,
    pub last_verified_at: Option<u64>,
    pub state: CacheEntryState,
    pub advisory_epoch: Option<u64>,
    pub signature_summary: Option<serde_json::Value>,
    pub local_path: PathBuf,
    pub source_snapshot: SourceSnapshot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResolvePolicy;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachePolicy;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionReport {
    pub scanned_entries: usize,
    pub kept: usize,
    pub evicted: usize,
    pub protected: usize,
    pub bytes_reclaimed: u64,
    pub refusals: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionEnvironment {
    Dev,
    Staging,
    Prod,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInput {
    pub entries: Vec<CacheEntry>,
    pub active_bundle_ids: Vec<String>,
    pub staged_bundle_ids: Vec<String>,
    pub warming_bundle_ids: Vec<String>,
    pub ready_bundle_ids: Vec<String>,
    pub draining_bundle_ids: Vec<String>,
    pub session_referenced_bundle_ids: Vec<String>,
    pub max_cache_bytes: u64,
    pub max_entry_age: Option<u64>,
    pub minimum_rollback_depth: usize,
    pub environment: RetentionEnvironment,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionDisposition {
    Keep,
    Evict,
    Protect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionDecision {
    pub cache_key: String,
    pub bundle_id: String,
    pub decision: RetentionDisposition,
    pub reason_code: String,
    pub reason_detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionOutcome {
    pub decisions: Vec<RetentionDecision>,
    pub report: RetentionReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationEnvironment {
    Dev,
    Staging,
    Prod,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationPolicy {
    pub require_signature: bool,
    pub trusted_issuers: Vec<String>,
    pub deny_issuers: Vec<String>,
    pub deny_digests: Vec<String>,
    pub allowed_media_types: Vec<String>,
    pub require_sbom: bool,
    pub minimum_operator_version: Option<String>,
    pub environment: VerificationEnvironment,
}

impl Default for VerificationPolicy {
    fn default() -> Self {
        Self {
            require_signature: false,
            trusted_issuers: Vec::new(),
            deny_issuers: Vec::new(),
            deny_digests: Vec::new(),
            allowed_media_types: Vec::new(),
            require_sbom: false,
            minimum_operator_version: None,
            environment: VerificationEnvironment::Dev,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisorySet {
    pub version: String,
    pub issued_at: u64,
    pub source: String,
    pub deny_digests: Vec<String>,
    pub deny_issuers: Vec<String>,
    pub minimum_operator_version: Option<String>,
    pub release_train: Option<ReleaseTrainDescriptor>,
    pub expires_at: Option<u64>,
    pub next_refresh_hint: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseTrainDescriptor {
    pub train_id: String,
    pub operator_digest: Option<String>,
    pub bundle_digests: Vec<String>,
    pub required_extension_digests: Vec<String>,
    pub baseline_observer_digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationOutcome {
    Passed,
    Failed,
    Warning,
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationCheck {
    pub name: String,
    pub outcome: VerificationOutcome,
    pub detail: String,
    pub payload: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub artifact_digest: String,
    pub canonical_ref: String,
    pub checks: Vec<VerificationCheck>,
    pub passed: bool,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub policy_fingerprint: String,
    pub advisory_version: Option<String>,
    pub cache_entry_fingerprint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreliminaryDecision {
    pub passed: bool,
    pub checks: Vec<VerificationCheck>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode {
    Userspace,
    Mount,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BundleOpenMode {
    CacheReuse,
    Userspace,
    Mount,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageBundleInput {
    pub bundle_ref: String,
    pub requested_access_mode: AccessMode,
    pub verification_policy_ref: String,
    pub cache_policy_ref: String,
    pub tenant: Option<String>,
    pub team: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmBundleInput {
    pub bundle_id: String,
    pub cache_key: String,
    pub smoke_test: bool,
    pub dry_run: bool,
    pub expected_operator_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackBundleInput {
    pub target_bundle_id: String,
    pub expected_cache_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifestSummary {
    pub component_id: String,
    pub abi_version: Option<String>,
    pub describe_artifact_ref: Option<String>,
    pub artifact_type: ArtifactType,
    pub media_type: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageAuditFields {
    pub staged_at: u64,
    pub requested_access_mode: AccessMode,
    pub verification_policy_ref: String,
    pub cache_policy_ref: String,
    pub tenant: Option<String>,
    pub team: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmAuditFields {
    pub warmed_at: u64,
    pub smoke_test: bool,
    pub dry_run: bool,
    pub reopened_from_cache: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackAuditFields {
    pub rolled_back_at: u64,
    pub reopened_from_cache: bool,
    pub expected_cache_key: Option<String>,
}

#[derive(Debug)]
pub struct StageBundleResult {
    pub bundle_id: String,
    pub canonical_ref: String,
    pub descriptor: ArtifactDescriptor,
    pub resolved_artifact: ResolvedArtifact,
    pub verification_report: VerificationReport,
    pub cache_entry: CacheEntry,
    pub stage_audit_fields: StageAuditFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmBundleResult {
    pub bundle_id: String,
    pub verification_report: VerificationReport,
    pub bundle_manifest_summary: BundleManifestSummary,
    pub bundle_open_mode: BundleOpenMode,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub warm_audit_fields: WarmAuditFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackBundleResult {
    pub bundle_id: String,
    pub reopened_from_cache: bool,
    pub cache_entry: CacheEntry,
    pub verification_report: VerificationReport,
    pub rollback_audit_fields: RollbackAuditFields,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleRecord {
    pub bundle_id: String,
    pub cache_key: String,
    pub canonical_ref: String,
    pub source_kind: ArtifactSourceKind,
    pub fetched_at: u64,
    pub lifecycle_state: BundleLifecycleState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BundleLifecycleState {
    Inactive,
    Staged,
    Warming,
    Ready,
    Draining,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOpenRequest {
    pub bundle_id: String,
    pub dry_run: bool,
    pub smoke_test: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOpenOutput {
    pub bundle_manifest_summary: BundleManifestSummary,
    pub bundle_open_mode: BundleOpenMode,
    pub warnings: Vec<String>,
}

#[deprecated(note = "use ArtifactOpenRequest instead")]
pub type BundleOpenRequest = ArtifactOpenRequest;

#[deprecated(note = "use ArtifactOpenOutput instead")]
pub type BundleOpenOutput = ArtifactOpenOutput;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrationErrorCode {
    InvalidReference,
    UnsupportedSource,
    ResolutionFailed,
    DownloadFailed,
    ResolutionUnavailable,
    DigestMismatch,
    MediaTypeRejected,
    IssuerRejected,
    DigestDenied,
    SignatureRequired,
    CacheCorrupt,
    CacheMiss,
    OfflineRequiredButUnavailable,
    UnsupportedArtifactType,
    DescriptorCorrupt,
    PolicyInputInvalid,
    AdvisoryRejected,
    VerificationFailed,
    BundleOpenFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationError {
    pub code: IntegrationErrorCode,
    pub summary: String,
    pub retryable: bool,
    pub details: Option<serde_json::Value>,
}

pub trait ArtifactOpener: Send + Sync {
    fn open(
        &self,
        artifact: &ResolvedArtifact,
        request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError>;
}

#[deprecated(
    note = "use ArtifactOpener instead; format ownership belongs outside distributor-client"
)]
pub trait BundleOpener: Send + Sync {
    fn open(
        &self,
        artifact: &ResolvedArtifact,
        request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError>;
}

#[allow(deprecated)]
impl<T: BundleOpener + ?Sized> ArtifactOpener for T {
    fn open(
        &self,
        artifact: &ResolvedArtifact,
        request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError> {
        BundleOpener::open(self, artifact, request)
    }
}

#[derive(Clone, Debug)]
pub struct DistOptions {
    pub cache_dir: PathBuf,
    pub allow_tags: bool,
    pub offline: bool,
    pub allow_insecure_local_http: bool,
    pub cache_max_bytes: u64,
    pub repo_registry_base: Option<String>,
    pub store_registry_base: Option<String>,
    pub store_auth_path: PathBuf,
    pub store_state_path: PathBuf,
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
            store_auth_path: default_store_auth_path(),
            store_state_path: default_store_state_path(),
            #[cfg(feature = "fixture-resolver")]
            fixture_dir: std::env::var("GREENTIC_FIXTURE_DIR")
                .ok()
                .map(PathBuf::from),
        }
    }
}

#[derive(Debug)]
pub struct ResolvedArtifact {
    pub descriptor: ArtifactDescriptor,
    pub cache_key: String,
    pub local_path: PathBuf,
    pub fetched_at: u64,
    pub integrity_state: IntegrityState,
    pub source_snapshot: SourceSnapshot,
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
        source: LegacyArtifactSource,
    ) -> Self {
        let source_kind = source.kind();
        let canonical_ref = source.canonical_ref(&resolved_digest);
        let fetched_at = unix_now();
        let public_source = artifact_source_from_legacy(&source);
        let descriptor = ArtifactDescriptor {
            artifact_type: ArtifactType::Component,
            source_kind: source_kind.clone(),
            raw_ref: public_source.raw_ref.clone(),
            canonical_ref: canonical_ref.clone(),
            digest: resolved_digest.clone(),
            media_type: content_type
                .clone()
                .unwrap_or_else(|| WASM_CONTENT_TYPE.to_string()),
            size_bytes: content_length.unwrap_or_default(),
            created_at: None,
            annotations: serde_json::Map::new(),
            manifest_digest: None,
            resolved_via: source.resolved_via(),
            signature_refs: Vec::new(),
            sbom_refs: Vec::new(),
        };
        let source_snapshot = SourceSnapshot {
            raw_ref: descriptor.raw_ref.clone(),
            canonical_ref: descriptor.canonical_ref.clone(),
            source_kind: descriptor.source_kind.clone(),
            authoritative: descriptor.raw_ref == descriptor.canonical_ref,
        };
        Self {
            descriptor,
            cache_key: resolved_digest.clone(),
            local_path: wasm_path.clone(),
            fetched_at,
            integrity_state: IntegrityState::Ready,
            source_snapshot,
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
            source: public_source,
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
enum LegacyArtifactSource {
    Digest,
    Http(String),
    File(PathBuf),
    Oci(String),
    Repo(String),
    Store(String),
}

impl LegacyArtifactSource {
    fn kind(&self) -> ArtifactSourceKind {
        match self {
            Self::Digest => ArtifactSourceKind::CacheDigest,
            Self::Http(_) => ArtifactSourceKind::Https,
            Self::File(_) => ArtifactSourceKind::File,
            Self::Oci(_) => ArtifactSourceKind::Oci,
            Self::Repo(_) => ArtifactSourceKind::Repo,
            Self::Store(_) => ArtifactSourceKind::Store,
        }
    }

    fn raw_ref(&self) -> String {
        match self {
            Self::Digest => String::new(),
            Self::Http(url) => url.clone(),
            Self::File(path) => path.display().to_string(),
            Self::Oci(reference) => format!("oci://{reference}"),
            Self::Repo(reference) => reference.clone(),
            Self::Store(reference) => reference.clone(),
        }
    }

    fn canonical_ref(&self, digest: &str) -> String {
        match self {
            Self::Digest => digest.to_string(),
            Self::Http(url) => format!("{url}@{digest}"),
            Self::File(path) => format!("file://{}@{digest}", path.display()),
            Self::Oci(reference) => canonical_oci_ref(reference, digest),
            Self::Repo(reference) | Self::Store(reference) => {
                let raw = reference
                    .trim_start_matches("repo://")
                    .trim_start_matches("store://");
                canonical_oci_ref(raw, digest)
            }
        }
    }

    fn resolved_via(&self) -> ResolvedVia {
        match self {
            Self::Digest => ResolvedVia::CacheDigest,
            Self::Http(_) => ResolvedVia::Https,
            Self::File(_) => ResolvedVia::File,
            Self::Oci(reference) => {
                if reference.contains(':') && !reference.contains("@sha256:") {
                    ResolvedVia::TagResolution
                } else {
                    ResolvedVia::Direct
                }
            }
            Self::Repo(_) => ResolvedVia::RepoMapping,
            Self::Store(_) => ResolvedVia::StoreMapping,
        }
    }
}

fn artifact_source_from_legacy(source: &LegacyArtifactSource) -> ArtifactSource {
    ArtifactSource {
        raw_ref: source.raw_ref(),
        kind: source.kind(),
        transport_hints: TransportHints::default(),
        dev_mode: matches!(
            source.kind(),
            ArtifactSourceKind::Fixture | ArtifactSourceKind::File
        ),
    }
}

fn legacy_source_from_public(source: &ArtifactSource) -> LegacyArtifactSource {
    match source.kind {
        ArtifactSourceKind::CacheDigest => LegacyArtifactSource::Digest,
        ArtifactSourceKind::Https => LegacyArtifactSource::Http(source.raw_ref.clone()),
        ArtifactSourceKind::File | ArtifactSourceKind::Fixture => {
            LegacyArtifactSource::File(PathBuf::from(source.raw_ref.clone()))
        }
        ArtifactSourceKind::Oci => {
            LegacyArtifactSource::Oci(source.raw_ref.trim_start_matches("oci://").to_string())
        }
        ArtifactSourceKind::Repo => LegacyArtifactSource::Repo(source.raw_ref.clone()),
        ArtifactSourceKind::Store => LegacyArtifactSource::Store(source.raw_ref.clone()),
    }
}

pub struct DistClient {
    cache: ComponentCache,
    oci: OciComponentResolver<ComponentRegistryClient>,
    http: reqwest::Client,
    opts: DistOptions,
    injected: Option<Arc<dyn ResolveRefInjector>>,
    artifact_opener: Arc<dyn ArtifactOpener>,
}

#[derive(Clone, Debug, Default)]
struct DefaultArtifactOpener;

#[derive(Clone, Debug)]
enum StoreRegistryAuth {
    Anonymous,
    Basic { username: String, password: String },
}

#[async_trait]
trait StoreDownloadRegistryClient: Send + Sync {
    async fn pull_manifest(
        &self,
        reference: &Reference,
        auth: &StoreRegistryAuth,
    ) -> Result<OciManifest, OciDistributionError>;

    async fn pull(
        &self,
        reference: &Reference,
        auth: &StoreRegistryAuth,
        accepted_media_types: &[String],
    ) -> Result<PulledImage, OciDistributionError>;
}

#[derive(Clone)]
struct DefaultStoreDownloadRegistryClient {
    inner: Client,
}

impl Default for DefaultStoreDownloadRegistryClient {
    fn default() -> Self {
        let config = ClientConfig {
            protocol: ClientProtocol::Https,
            ..Default::default()
        };
        Self {
            inner: Client::new(config),
        }
    }
}

#[async_trait]
impl StoreDownloadRegistryClient for DefaultStoreDownloadRegistryClient {
    async fn pull_manifest(
        &self,
        reference: &Reference,
        auth: &StoreRegistryAuth,
    ) -> Result<OciManifest, OciDistributionError> {
        let (manifest, _) = self
            .inner
            .pull_manifest(reference, &store_registry_auth(auth))
            .await?;
        Ok(manifest)
    }

    async fn pull(
        &self,
        reference: &Reference,
        auth: &StoreRegistryAuth,
        accepted_media_types: &[String],
    ) -> Result<PulledImage, OciDistributionError> {
        let accepted = accepted_media_types
            .iter()
            .map(|media_type| media_type.as_str())
            .collect::<Vec<_>>();
        let image = self
            .inner
            .pull(reference, &store_registry_auth(auth), accepted)
            .await?;
        Ok(PulledImage {
            digest: image.digest,
            layers: image
                .layers
                .into_iter()
                .map(|layer| PulledLayer {
                    media_type: layer.media_type,
                    data: layer.data,
                    digest: None,
                })
                .collect(),
        })
    }
}

fn store_registry_auth(auth: &StoreRegistryAuth) -> RegistryAuth {
    match auth {
        StoreRegistryAuth::Anonymous => RegistryAuth::Anonymous,
        StoreRegistryAuth::Basic { username, password } => {
            RegistryAuth::Basic(username.clone(), password.clone())
        }
    }
}

#[derive(Clone, Debug)]
pub struct OciCacheInspection {
    pub digest: String,
    pub cache_dir: PathBuf,
    pub artifact_path: PathBuf,
    pub artifact_type: ArtifactType,
    pub selected_media_type: String,
    pub fetched: bool,
}

#[derive(Clone, Debug)]
#[deprecated(note = "use ArtifactSource with DistClient::resolve/fetch instead")]
pub struct ResolveRefRequest {
    pub reference: String,
}

#[derive(Clone, Debug, Default)]
#[deprecated(note = "use ArtifactSource with DistClient::resolve/fetch instead")]
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
        Self::with_parts(opts, None, Arc::new(DefaultArtifactOpener))
    }

    pub fn with_ref_injector(opts: DistOptions, injector: Arc<dyn ResolveRefInjector>) -> Self {
        Self::with_parts(opts, Some(injector), Arc::new(DefaultArtifactOpener))
    }

    pub fn with_artifact_opener(
        opts: DistOptions,
        artifact_opener: Arc<dyn ArtifactOpener>,
    ) -> Self {
        Self::with_parts(opts, None, artifact_opener)
    }

    #[allow(deprecated)]
    #[deprecated(note = "use with_artifact_opener instead")]
    pub fn with_bundle_opener<T: BundleOpener + 'static>(
        opts: DistOptions,
        bundle_opener: Arc<T>,
    ) -> Self {
        Self::with_artifact_opener(opts, bundle_opener)
    }

    fn with_parts(
        opts: DistOptions,
        injected: Option<Arc<dyn ResolveRefInjector>>,
        artifact_opener: Arc<dyn ArtifactOpener>,
    ) -> Self {
        let oci_opts = ComponentResolveOptions {
            allow_tags: opts.allow_tags,
            offline: opts.offline,
            cache_dir: opts.cache_dir.join("legacy-components"),
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
            artifact_opener,
        }
    }

    async fn resolve_descriptor_from_reference(
        &self,
        reference: &str,
    ) -> Result<ArtifactDescriptor, DistError> {
        match classify_reference(reference)? {
            RefKind::Digest(digest) => {
                let entry = self.stat_cache(&digest)?;
                Ok(descriptor_from_entry(&entry))
            }
            RefKind::Http(url) => {
                if self.opts.offline {
                    return Err(DistError::Offline { reference: url });
                }
                let normalized = ensure_secure_http_url(&url, self.opts.allow_insecure_local_http)?;
                Ok(ArtifactDescriptor {
                    artifact_type: ArtifactType::Component,
                    source_kind: ArtifactSourceKind::Https,
                    raw_ref: normalized.to_string(),
                    canonical_ref: normalized.to_string(),
                    digest: String::new(),
                    media_type: WASM_CONTENT_TYPE.to_string(),
                    size_bytes: 0,
                    created_at: None,
                    annotations: serde_json::Map::new(),
                    manifest_digest: None,
                    resolved_via: ResolvedVia::Https,
                    signature_refs: Vec::new(),
                    sbom_refs: Vec::new(),
                })
            }
            RefKind::File(path) => {
                let bytes = fs::read(&path).map_err(|source| DistError::CacheError {
                    path: path.display().to_string(),
                    source,
                })?;
                let digest = digest_for_bytes(&bytes);
                Ok(ArtifactDescriptor {
                    artifact_type: ArtifactType::Component,
                    source_kind: ArtifactSourceKind::File,
                    raw_ref: path.display().to_string(),
                    canonical_ref: format!("file://{}@{}", path.display(), digest),
                    digest,
                    media_type: WASM_CONTENT_TYPE.to_string(),
                    size_bytes: bytes.len() as u64,
                    created_at: None,
                    annotations: serde_json::Map::new(),
                    manifest_digest: None,
                    resolved_via: ResolvedVia::File,
                    signature_refs: Vec::new(),
                    sbom_refs: Vec::new(),
                })
            }
            RefKind::Oci(reference) => self.resolve_oci_descriptor(&reference).await,
            RefKind::Repo(target) => {
                if self.opts.repo_registry_base.is_none() {
                    return Err(DistError::ResolutionUnavailable {
                        reference: format!("repo://{target}"),
                    });
                }
                let mapped = map_registry_target(&target, self.opts.repo_registry_base.as_deref())
                    .ok_or_else(|| DistError::ResolutionUnavailable {
                        reference: format!("repo://{target}"),
                    })?;
                let mut descriptor = self.resolve_oci_descriptor(&mapped).await?;
                descriptor.source_kind = ArtifactSourceKind::Repo;
                descriptor.raw_ref = format!("repo://{target}");
                descriptor.resolved_via = ResolvedVia::RepoMapping;
                Ok(descriptor)
            }
            RefKind::Store(target) => {
                if is_greentic_biz_store_target(&target) {
                    return self.resolve_greentic_biz_store_descriptor(&target).await;
                }
                if self.opts.store_registry_base.is_none() {
                    return Err(DistError::ResolutionUnavailable {
                        reference: format!("store://{target}"),
                    });
                }
                let mapped = map_registry_target(&target, self.opts.store_registry_base.as_deref())
                    .ok_or_else(|| DistError::ResolutionUnavailable {
                        reference: format!("store://{target}"),
                    })?;
                let mut descriptor = self.resolve_oci_descriptor(&mapped).await?;
                descriptor.source_kind = ArtifactSourceKind::Store;
                descriptor.raw_ref = format!("store://{target}");
                descriptor.resolved_via = ResolvedVia::StoreMapping;
                Ok(descriptor)
            }
            #[cfg(feature = "fixture-resolver")]
            RefKind::Fixture(target) => {
                let fixture_dir = self.opts.fixture_dir.as_ref().ok_or_else(|| {
                    DistError::InvalidInput("fixture:// requires fixture_dir".into())
                })?;
                let raw = target.trim_start_matches('/');
                let candidate = if raw.ends_with(".wasm") {
                    fixture_dir.join(raw)
                } else {
                    fixture_dir.join(format!("{raw}.wasm"))
                };
                let bytes = fs::read(&candidate).map_err(|source| DistError::CacheError {
                    path: candidate.display().to_string(),
                    source,
                })?;
                let digest = digest_for_bytes(&bytes);
                Ok(ArtifactDescriptor {
                    artifact_type: ArtifactType::Component,
                    source_kind: ArtifactSourceKind::Fixture,
                    raw_ref: format!("fixture://{target}"),
                    canonical_ref: format!("file://{}@{}", candidate.display(), digest),
                    digest,
                    media_type: WASM_CONTENT_TYPE.to_string(),
                    size_bytes: bytes.len() as u64,
                    created_at: None,
                    annotations: serde_json::Map::new(),
                    manifest_digest: None,
                    resolved_via: ResolvedVia::Fixture,
                    signature_refs: Vec::new(),
                    sbom_refs: Vec::new(),
                })
            }
        }
    }

    async fn resolve_oci_descriptor(
        &self,
        reference: &str,
    ) -> Result<ArtifactDescriptor, DistError> {
        self.resolve_oci_descriptor_with_client(reference, ComponentRegistryClient::default())
            .await
    }

    async fn resolve_oci_descriptor_with_client(
        &self,
        reference: &str,
        client: ComponentRegistryClient,
    ) -> Result<ArtifactDescriptor, DistError> {
        let resolver = OciComponentResolver::with_client(client, self.oci_resolve_options());
        let resolved = resolver
            .resolve_descriptor(reference)
            .await
            .map_err(DistError::Oci)?;
        Ok(ArtifactDescriptor {
            artifact_type: ArtifactType::Component,
            source_kind: ArtifactSourceKind::Oci,
            raw_ref: format!("oci://{reference}"),
            canonical_ref: canonical_oci_ref(reference, &resolved.resolved_digest),
            digest: resolved.resolved_digest,
            media_type: normalize_content_type(Some(&resolved.media_type), WASM_CONTENT_TYPE),
            size_bytes: resolved.size_bytes,
            created_at: None,
            annotations: serde_json::Map::new(),
            manifest_digest: resolved.manifest_digest,
            resolved_via: if reference.contains(':') && !reference.contains("@sha256:") {
                ResolvedVia::TagResolution
            } else {
                ResolvedVia::Direct
            },
            signature_refs: Vec::new(),
            sbom_refs: Vec::new(),
        })
    }

    fn oci_resolve_options(&self) -> ComponentResolveOptions {
        ComponentResolveOptions {
            allow_tags: self.opts.allow_tags,
            offline: self.opts.offline,
            cache_dir: self.opts.cache_dir.join("legacy-components"),
            ..Default::default()
        }
    }

    fn oci_pack_options(&self) -> PackFetchOptions {
        PackFetchOptions {
            allow_tags: self.opts.allow_tags,
            offline: self.opts.offline,
            cache_dir: self.opts.cache_dir.join("legacy-packs"),
            ..Default::default()
        }
    }

    async fn load_store_credentials(&self, tenant: &str) -> Result<StoreCredentials, DistError> {
        load_login(
            &self.opts.store_auth_path,
            &self.opts.store_state_path,
            tenant,
        )
        .await
        .map_err(|err| DistError::StoreAuth(err.to_string()))
    }

    async fn greentic_biz_store_component_client(
        &self,
        tenant: &str,
    ) -> Result<ComponentRegistryClient, DistError> {
        let credentials = self.load_store_credentials(tenant).await?;
        Ok(ComponentRegistryClient::with_basic_auth(
            credentials.username,
            credentials.token,
        ))
    }

    async fn greentic_biz_store_pack_client(
        &self,
        tenant: &str,
    ) -> Result<PackRegistryClient, DistError> {
        let credentials = self.load_store_credentials(tenant).await?;
        Ok(PackRegistryClient::with_basic_auth(
            credentials.username,
            credentials.token,
        ))
    }

    pub async fn download_store_artifact(
        &self,
        reference: &str,
    ) -> Result<DownloadedStoreArtifact, DistError> {
        let target = match classify_reference(reference)? {
            RefKind::Store(target) => target,
            _ => {
                return Err(DistError::InvalidInput(
                    "store download requires a store:// reference".into(),
                ));
            }
        };
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: reference.to_string(),
            });
        }

        let (mapped_reference, auth) = if is_greentic_biz_store_target(&target) {
            let parsed = parse_greentic_biz_store_target(&target)?;
            let credentials = self.load_store_credentials(&parsed.tenant).await?;
            (
                parsed.mapped_reference,
                StoreRegistryAuth::Basic {
                    username: credentials.username,
                    password: credentials.token,
                },
            )
        } else {
            if self.opts.store_registry_base.is_none() {
                return Err(DistError::ResolutionUnavailable {
                    reference: format!("store://{target}"),
                });
            }
            let mapped = map_registry_target(&target, self.opts.store_registry_base.as_deref())
                .ok_or_else(|| DistError::Unauthorized {
                    target: format!("store://{target}"),
                })?;
            (mapped, StoreRegistryAuth::Anonymous)
        };

        download_store_artifact_with_client(
            &DefaultStoreDownloadRegistryClient::default(),
            reference,
            &mapped_reference,
            &auth,
        )
        .await
    }

    async fn resolve_oci_pack_descriptor_with_client(
        &self,
        reference: &str,
        client: PackRegistryClient,
    ) -> Result<ArtifactDescriptor, DistError> {
        let fetcher = OciPackFetcher::with_client(client, self.oci_pack_options());
        let resolved = fetcher
            .fetch_pack_to_cache(reference)
            .await
            .map_err(|err| DistError::Pack(err.to_string()))?;
        Ok(ArtifactDescriptor {
            artifact_type: ArtifactType::Bundle,
            source_kind: ArtifactSourceKind::Oci,
            raw_ref: format!("oci://{reference}"),
            canonical_ref: canonical_oci_ref(reference, &resolved.resolved_digest),
            digest: resolved.resolved_digest,
            media_type: resolved.media_type,
            size_bytes: file_size_if_exists(&resolved.path).unwrap_or_default(),
            created_at: None,
            annotations: serde_json::Map::new(),
            manifest_digest: resolved.manifest_digest,
            resolved_via: if reference.contains(':') && !reference.contains("@sha256:") {
                ResolvedVia::TagResolution
            } else {
                ResolvedVia::Direct
            },
            signature_refs: Vec::new(),
            sbom_refs: Vec::new(),
        })
    }

    async fn resolve_greentic_biz_store_descriptor(
        &self,
        target: &str,
    ) -> Result<ArtifactDescriptor, DistError> {
        let parsed = parse_greentic_biz_store_target(target)?;
        let mut descriptor = match self
            .resolve_oci_descriptor_with_client(
                &parsed.mapped_reference,
                self.greentic_biz_store_component_client(&parsed.tenant)
                    .await?,
            )
            .await
        {
            Ok(descriptor) => descriptor,
            Err(DistError::Oci(err)) if should_retry_store_as_pack(&err) => {
                self.resolve_oci_pack_descriptor_with_client(
                    &parsed.mapped_reference,
                    self.greentic_biz_store_pack_client(&parsed.tenant).await?,
                )
                .await?
            }
            Err(err) => return Err(err),
        };
        descriptor.source_kind = ArtifactSourceKind::Store;
        descriptor.raw_ref = format!("store://{target}");
        descriptor.resolved_via = ResolvedVia::StoreMapping;
        Ok(descriptor)
    }

    async fn pull_oci_with_source_and_client(
        &self,
        reference: &str,
        source: LegacyArtifactSource,
        component_id: String,
        client: ComponentRegistryClient,
    ) -> Result<ResolvedArtifact, DistError> {
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: reference.to_string(),
            });
        }
        let resolver = OciComponentResolver::with_client(client, self.oci_resolve_options());
        let result = resolver
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
        let resolved_digest = resolved.resolved_digest.clone();
        let resolved_bytes = fs::read(&resolved.path).map_err(|source| DistError::CacheError {
            path: resolved.path.display().to_string(),
            source,
        })?;
        let resolved = ResolvedArtifact::from_path(
            resolved_digest.clone(),
            self.cache
                .write_component(&resolved_digest, &resolved_bytes)
                .map_err(|source| DistError::CacheError {
                    path: self
                        .cache
                        .component_path(&resolved_digest)
                        .display()
                        .to_string(),
                    source,
                })?,
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
        self.persist_cache_entry(&resolved)?;
        self.enforce_cache_cap(Some(&resolved.descriptor.digest))?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn pull_oci_pack_with_source_and_client(
        &self,
        reference: &str,
        source: LegacyArtifactSource,
        component_id: String,
        client: PackRegistryClient,
    ) -> Result<ResolvedArtifact, DistError> {
        if self.opts.offline {
            return Err(DistError::Offline {
                reference: reference.to_string(),
            });
        }
        let fetcher = OciPackFetcher::with_client(client, self.oci_pack_options());
        let resolved = fetcher
            .fetch_pack_to_cache(reference)
            .await
            .map_err(|err| DistError::Pack(err.to_string()))?;
        let resolved_bytes = fs::read(&resolved.path).map_err(|source| DistError::CacheError {
            path: resolved.path.display().to_string(),
            source,
        })?;
        let resolved_digest = resolved.resolved_digest.clone();
        let mut artifact = ResolvedArtifact::from_path(
            resolved_digest.clone(),
            self.cache
                .write_component(&resolved_digest, &resolved_bytes)
                .map_err(|source| DistError::CacheError {
                    path: self
                        .cache
                        .component_path(&resolved_digest)
                        .display()
                        .to_string(),
                    source,
                })?,
            component_id,
            None,
            None,
            Some(resolved_bytes.len() as u64),
            Some(resolved.media_type.clone()),
            resolved.fetched_from_network,
            source,
        );
        artifact.descriptor.artifact_type = ArtifactType::Bundle;
        artifact.descriptor.media_type = resolved.media_type;
        artifact.descriptor.manifest_digest = resolved.manifest_digest;
        self.persist_cache_entry(&artifact)?;
        self.enforce_cache_cap(Some(&artifact.descriptor.digest))?;
        artifact.validate_payload()?;
        Ok(artifact)
    }

    fn persist_cache_entry(&self, artifact: &ResolvedArtifact) -> Result<(), DistError> {
        let local_path = artifact
            .cache_path
            .clone()
            .or_else(|| artifact.wasm_path.clone())
            .unwrap_or_else(|| artifact.local_path.clone());
        let entry = CacheEntry {
            format_version: CACHE_FORMAT_VERSION,
            cache_key: artifact.cache_key.clone(),
            digest: artifact.descriptor.digest.clone(),
            media_type: artifact.descriptor.media_type.clone(),
            size_bytes: artifact.descriptor.size_bytes,
            artifact_type: artifact.descriptor.artifact_type.clone(),
            source_kind: artifact.descriptor.source_kind.clone(),
            raw_ref: artifact.descriptor.raw_ref.clone(),
            canonical_ref: artifact.descriptor.canonical_ref.clone(),
            fetched_at: artifact.fetched_at,
            last_accessed_at: unix_now(),
            last_verified_at: None,
            state: cache_entry_state_from_integrity(&artifact.integrity_state),
            advisory_epoch: None,
            signature_summary: None,
            local_path,
            source_snapshot: artifact.source_snapshot.clone(),
        };
        self.cache
            .write_entry(&entry)
            .map_err(|source| DistError::CacheError {
                path: self.cache.entry_path(&entry.digest).display().to_string(),
                source,
            })
    }

    fn persist_verification_report(
        &self,
        digest: &str,
        report: &VerificationReport,
    ) -> Result<(), DistError> {
        let mut entry = self.stat_cache(digest)?;
        entry.last_verified_at = Some(unix_now());
        entry.advisory_epoch = report
            .advisory_version
            .as_ref()
            .and_then(|raw| raw.parse::<u64>().ok());
        entry.signature_summary = report
            .checks
            .iter()
            .find(|check| check.name == "signature_verified")
            .map(|check| {
                serde_json::json!({
                    "outcome": verification_outcome_name(&check.outcome),
                    "detail": check.detail,
                })
            });
        self.cache
            .write_entry(&entry)
            .map_err(|source| DistError::CacheError {
                path: self.cache.entry_path(&entry.digest).display().to_string(),
                source,
            })
    }

    pub async fn resolve(
        &self,
        source: ArtifactSource,
        _policy: ResolvePolicy,
    ) -> Result<ArtifactDescriptor, DistError> {
        let mut current = source.raw_ref.clone();
        for _ in 0..8 {
            if let Some(injected) = &self.injected
                && let Some(result) = injected.resolve(&current).await?
            {
                if let InjectedResolution::Redirect(next) = result {
                    current = next;
                    continue;
                }
                let artifact = self.materialize_injected(result)?;
                return Ok(artifact.descriptor);
            }

            return self.resolve_descriptor_from_reference(&current).await;
        }
        Err(DistError::InvalidInput(
            "too many injected redirect hops".to_string(),
        ))
    }

    pub fn parse_source(&self, reference: &str) -> Result<ArtifactSource, DistError> {
        artifact_source_from_reference(reference, &self.opts)
    }

    pub fn load_advisory_set(
        &self,
        bytes: &[u8],
        source: impl Into<String>,
    ) -> Result<AdvisorySet, DistError> {
        let mut advisory: AdvisorySet = serde_json::from_slice(bytes)?;
        advisory.source = source.into();
        Ok(advisory)
    }

    pub fn apply_policy(
        &self,
        descriptor: &ArtifactDescriptor,
        advisory_set: Option<&AdvisorySet>,
        verification_policy: &VerificationPolicy,
    ) -> PreliminaryDecision {
        let checks = vec![
            check_digest_allowed(&descriptor.digest, advisory_set, verification_policy),
            check_media_type_allowed(&descriptor.media_type, verification_policy),
            check_issuer_allowed(
                issuer_from_descriptor(descriptor),
                advisory_set,
                verification_policy,
            ),
            check_operator_version_compatible(descriptor, advisory_set, verification_policy),
        ];

        preliminary_decision_from_checks(checks)
    }

    pub fn verify_artifact(
        &self,
        resolved_artifact: &ResolvedArtifact,
        advisory_set: Option<&AdvisorySet>,
        verification_policy: &VerificationPolicy,
    ) -> Result<VerificationReport, DistError> {
        let mut checks = self
            .apply_policy(
                &resolved_artifact.descriptor,
                advisory_set,
                verification_policy,
            )
            .checks;

        checks.push(check_content_digest_match(resolved_artifact)?);
        checks.push(check_signature_present(
            &resolved_artifact.descriptor,
            verification_policy,
        ));
        checks.push(check_signature_verified(
            &resolved_artifact.descriptor,
            verification_policy,
        ));
        checks.push(check_sbom_present(
            &resolved_artifact.descriptor,
            verification_policy,
        ));

        let report = verification_report_from_checks(
            &resolved_artifact.descriptor,
            advisory_set,
            verification_policy,
            self.stat_cache(&resolved_artifact.descriptor.digest)
                .ok()
                .as_ref(),
            checks,
        );

        self.persist_verification_report(&resolved_artifact.descriptor.digest, &report)?;
        Ok(report)
    }

    pub async fn stage_bundle(
        &self,
        input: &StageBundleInput,
        advisory_set: Option<&AdvisorySet>,
        verification_policy: &VerificationPolicy,
        cache_policy: CachePolicy,
    ) -> Result<StageBundleResult, IntegrationError> {
        let source = self
            .parse_source(&input.bundle_ref)
            .map_err(IntegrationError::from_dist_error)?;
        let descriptor = self
            .resolve(source, ResolvePolicy)
            .await
            .map_err(IntegrationError::from_dist_error)?;
        let resolved_artifact = self
            .fetch(&descriptor, cache_policy)
            .await
            .map_err(IntegrationError::from_dist_error)?;
        let verification_report = self
            .verify_artifact(&resolved_artifact, advisory_set, verification_policy)
            .map_err(IntegrationError::from_dist_error)?;
        if !verification_report.passed {
            return Err(IntegrationError::from_verification_report(
                verification_report,
            ));
        }
        let cache_entry = self
            .stat_cache(&descriptor.digest)
            .map_err(IntegrationError::from_dist_error)?;
        let bundle_id = bundle_id_for_digest(&descriptor.digest);
        self.persist_bundle_record(&BundleRecord {
            bundle_id: bundle_id.clone(),
            cache_key: cache_entry.cache_key.clone(),
            canonical_ref: descriptor.canonical_ref.clone(),
            source_kind: descriptor.source_kind.clone(),
            fetched_at: cache_entry.fetched_at,
            lifecycle_state: BundleLifecycleState::Staged,
        })
        .map_err(IntegrationError::from_dist_error)?;

        Ok(StageBundleResult {
            bundle_id,
            canonical_ref: descriptor.canonical_ref.clone(),
            descriptor,
            resolved_artifact,
            verification_report,
            cache_entry,
            stage_audit_fields: StageAuditFields {
                staged_at: unix_now(),
                requested_access_mode: input.requested_access_mode.clone(),
                verification_policy_ref: input.verification_policy_ref.clone(),
                cache_policy_ref: input.cache_policy_ref.clone(),
                tenant: input.tenant.clone(),
                team: input.team.clone(),
            },
        })
    }

    pub fn warm_bundle(
        &self,
        input: &WarmBundleInput,
        advisory_set: Option<&AdvisorySet>,
        verification_policy: &VerificationPolicy,
    ) -> Result<WarmBundleResult, IntegrationError> {
        let expected_bundle_id = bundle_id_for_digest(&input.cache_key);
        if input.bundle_id != expected_bundle_id {
            return Err(IntegrationError {
                code: IntegrationErrorCode::InvalidReference,
                summary: format!(
                    "bundle id {} does not match cache key {}",
                    input.bundle_id, input.cache_key
                ),
                retryable: false,
                details: Some(serde_json::json!({
                    "bundle_id": input.bundle_id,
                    "expected_bundle_id": expected_bundle_id,
                    "cache_key": input.cache_key,
                })),
            });
        }

        let mut resolved_artifact = self
            .open_cached(&input.cache_key)
            .map_err(IntegrationError::from_dist_error)?;
        if let Some(expected_operator_version) = &input.expected_operator_version {
            resolved_artifact.descriptor.annotations.insert(
                "operator_version".to_string(),
                serde_json::Value::String(expected_operator_version.clone()),
            );
        }
        let verification_report = self
            .verify_artifact(&resolved_artifact, advisory_set, verification_policy)
            .map_err(IntegrationError::from_dist_error)?;
        if !verification_report.passed {
            return Err(IntegrationError::from_verification_report(
                verification_report,
            ));
        }
        let opened = self.artifact_opener.open(
            &resolved_artifact,
            &ArtifactOpenRequest {
                bundle_id: input.bundle_id.clone(),
                dry_run: input.dry_run,
                smoke_test: input.smoke_test,
            },
        )?;

        Ok(WarmBundleResult {
            bundle_id: input.bundle_id.clone(),
            bundle_manifest_summary: opened.bundle_manifest_summary,
            bundle_open_mode: opened.bundle_open_mode,
            warnings: verification_report
                .warnings
                .iter()
                .cloned()
                .chain(opened.warnings)
                .collect(),
            errors: verification_report.errors.clone(),
            verification_report,
            warm_audit_fields: WarmAuditFields {
                warmed_at: unix_now(),
                smoke_test: input.smoke_test,
                dry_run: input.dry_run,
                reopened_from_cache: true,
            },
        })
    }

    pub fn rollback_bundle(
        &self,
        input: &RollbackBundleInput,
        advisory_set: Option<&AdvisorySet>,
        verification_policy: &VerificationPolicy,
    ) -> Result<RollbackBundleResult, IntegrationError> {
        let bundle_record = self.stat_bundle(&input.target_bundle_id).ok();
        let digest = if let Some(record) = &bundle_record {
            normalize_digest(&record.cache_key)
        } else {
            digest_from_bundle_id(&input.target_bundle_id).ok_or_else(|| IntegrationError {
                code: IntegrationErrorCode::InvalidReference,
                summary: format!("invalid bundle id {}", input.target_bundle_id),
                retryable: false,
                details: Some(serde_json::json!({
                    "bundle_id": input.target_bundle_id,
                })),
            })?
        };

        if let Some(expected_cache_key) = &input.expected_cache_key
            && expected_cache_key != &digest
        {
            return Err(IntegrationError {
                code: IntegrationErrorCode::InvalidReference,
                summary: format!(
                    "expected cache key {} does not match rollback bundle digest {}",
                    expected_cache_key, digest
                ),
                retryable: false,
                details: Some(serde_json::json!({
                    "bundle_id": input.target_bundle_id,
                    "expected_cache_key": expected_cache_key,
                    "actual_cache_key": digest,
                })),
            });
        }

        let resolved_artifact = self
            .open_cached(&digest)
            .map_err(IntegrationError::from_dist_error)?;
        let verification_report = self
            .verify_artifact(&resolved_artifact, advisory_set, verification_policy)
            .map_err(IntegrationError::from_dist_error)?;
        if !verification_report.passed {
            return Err(IntegrationError::from_verification_report(
                verification_report,
            ));
        }
        let cache_entry = self
            .stat_cache(&digest)
            .map_err(IntegrationError::from_dist_error)?;

        Ok(RollbackBundleResult {
            bundle_id: input.target_bundle_id.clone(),
            reopened_from_cache: true,
            cache_entry,
            verification_report,
            rollback_audit_fields: RollbackAuditFields {
                rolled_back_at: unix_now(),
                reopened_from_cache: true,
                expected_cache_key: input.expected_cache_key.clone(),
            },
        })
    }

    pub fn stat_bundle(&self, bundle_id: &str) -> Result<BundleRecord, DistError> {
        self.cache.read_bundle_record(bundle_id).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                DistError::NotFound {
                    reference: bundle_id.to_string(),
                }
            } else {
                DistError::CacheError {
                    path: self
                        .cache
                        .bundle_record_path(bundle_id)
                        .display()
                        .to_string(),
                    source,
                }
            }
        })
    }

    pub fn list_bundles(&self) -> Result<Vec<BundleRecord>, DistError> {
        self.cache
            .list_bundle_records()
            .map_err(|source| DistError::CacheError {
                path: self.cache.bundle_records_root().display().to_string(),
                source,
            })
    }

    pub fn set_bundle_state(
        &self,
        bundle_id: &str,
        lifecycle_state: BundleLifecycleState,
    ) -> Result<BundleRecord, DistError> {
        let mut record = self.stat_bundle(bundle_id)?;
        record.lifecycle_state = lifecycle_state;
        self.persist_bundle_record(&record)?;
        Ok(record)
    }

    pub async fn fetch(
        &self,
        descriptor: &ArtifactDescriptor,
        _cache_policy: CachePolicy,
    ) -> Result<ResolvedArtifact, DistError> {
        if let Ok(existing) = self.open_cached(&descriptor.cache_key()) {
            return Ok(existing);
        }

        let raw = descriptor.raw_ref.as_str();
        let artifact = match descriptor.source_kind {
            ArtifactSourceKind::CacheDigest => self.open_cached(&descriptor.digest)?,
            ArtifactSourceKind::Https => self.fetch_http(raw).await?,
            ArtifactSourceKind::File => self.ingest_file(Path::new(raw)).await?,
            ArtifactSourceKind::Oci => {
                let reference = descriptor
                    .canonical_ref
                    .trim_start_matches("oci://")
                    .to_string();
                self.pull_oci(&reference).await?
            }
            ArtifactSourceKind::Repo => {
                self.resolve_repo_ref(raw.trim_start_matches("repo://"))
                    .await?
            }
            ArtifactSourceKind::Store => {
                self.resolve_store_ref(raw.trim_start_matches("store://"))
                    .await?
            }
            #[cfg(feature = "fixture-resolver")]
            ArtifactSourceKind::Fixture => {
                self.resolve_fixture_ref(raw.trim_start_matches("fixture://"))
                    .await?
            }
            #[cfg(not(feature = "fixture-resolver"))]
            ArtifactSourceKind::Fixture => {
                return Err(DistError::InvalidInput(
                    "fixture resolver feature is disabled".to_string(),
                ));
            }
        };

        if !descriptor.digest.is_empty() && artifact.descriptor.digest != descriptor.digest {
            return Err(DistError::CorruptArtifact {
                reference: descriptor.digest.clone(),
                reason: format!(
                    "fetched digest {} did not match resolved descriptor {}",
                    artifact.descriptor.digest, descriptor.digest
                ),
            });
        }
        Ok(artifact)
    }

    pub fn open_cached(&self, digest_or_cache_key: &str) -> Result<ResolvedArtifact, DistError> {
        let digest = normalize_digest(digest_or_cache_key);
        let entry = self.stat_cache(&digest)?;
        let path = self
            .cache
            .existing_component(&entry.digest)
            .ok_or_else(|| DistError::CorruptArtifact {
                reference: digest_or_cache_key.to_string(),
                reason: "cache metadata exists but cached blob is missing".to_string(),
            })?;
        let mut artifact = ResolvedArtifact::from_path(
            entry.digest.clone(),
            path.clone(),
            component_id_from_descriptor(&entry),
            None,
            None,
            Some(entry.size_bytes),
            Some(entry.media_type.clone()),
            false,
            legacy_source_from_entry(&entry),
        );
        artifact.descriptor = descriptor_from_entry(&entry);
        artifact.cache_key = entry.cache_key.clone();
        artifact.local_path = path.clone();
        artifact.fetched_at = entry.fetched_at;
        artifact.integrity_state = integrity_state_from_entry(&entry.state);
        artifact.source_snapshot = entry.source_snapshot.clone();
        artifact.cache_path = Some(path.clone());
        artifact.wasm_path = Some(path);
        Ok(artifact)
    }

    pub fn stat_cache(&self, digest_or_cache_key: &str) -> Result<CacheEntry, DistError> {
        let digest = normalize_digest(digest_or_cache_key);
        let entry = self.cache.read_entry(&digest).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                DistError::NotFound {
                    reference: digest_or_cache_key.to_string(),
                }
            } else {
                DistError::CacheError {
                    path: self.cache.entry_path(&digest).display().to_string(),
                    source,
                }
            }
        })?;
        let _ = self.cache.touch_last_used(&entry.digest);
        Ok(entry)
    }

    #[deprecated(note = "use evaluate_retention/apply_retention for explicit retention decisions")]
    pub fn evict_cache(&self, digests: &[String]) -> Result<RetentionReport, DistError> {
        let mut report = RetentionReport {
            scanned_entries: digests.len(),
            ..RetentionReport::default()
        };
        let entries = digests
            .iter()
            .filter_map(|digest| self.stat_cache(digest).ok())
            .collect::<Vec<_>>();
        let input = RetentionInput {
            entries,
            active_bundle_ids: Vec::new(),
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: 0,
            max_entry_age: Some(0),
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Dev,
        };
        let outcome = self.apply_retention(&input)?;
        report.evicted = outcome.report.evicted;
        report.bytes_reclaimed = outcome.report.bytes_reclaimed;
        report.refusals = digests
            .iter()
            .filter(|digest| self.stat_cache(digest).is_err())
            .cloned()
            .chain(outcome.report.refusals)
            .collect();
        report.kept = report
            .scanned_entries
            .saturating_sub(report.evicted + report.refusals.len());
        Ok(report)
    }

    #[deprecated(note = "use parse_source + resolve + fetch instead")]
    pub async fn resolve_ref(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let source = self.parse_source(reference)?;
        let descriptor = self.resolve(source, ResolvePolicy).await?;
        self.fetch(&descriptor, CachePolicy).await
    }

    #[allow(deprecated)]
    #[deprecated(note = "use parse_source + resolve + fetch instead")]
    pub async fn resolve_ref_request(
        &self,
        req: ResolveRefRequest,
    ) -> Result<ResolvedArtifact, DistError> {
        self.resolve_ref(&req.reference).await
    }

    #[allow(deprecated)]
    #[deprecated(note = "use parse_source + resolve + fetch instead")]
    pub async fn resolve_component(
        &self,
        req: ResolveComponentRequest,
    ) -> Result<ResolvedArtifact, DistError> {
        self.resolve_ref(&req.reference).await
    }

    #[allow(deprecated)]
    #[deprecated(note = "use parse_source + resolve + fetch or open_cached instead")]
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
            let resolved_item = if let Some(digest) = entry.digest.as_ref() {
                if let Ok(item) = self.open_cached(digest) {
                    item
                } else {
                    let reference = entry
                        .reference
                        .clone()
                        .ok_or_else(|| DistError::InvalidInput("lock entry missing ref".into()))?;
                    let source = self.parse_source(&reference)?;
                    let mut descriptor = self.resolve(source, ResolvePolicy).await?;
                    if !descriptor.digest.is_empty() && descriptor.digest != *digest {
                        return Err(DistError::CorruptArtifact {
                            reference: reference.clone(),
                            reason: format!(
                                "lock digest {} did not match resolved descriptor {}",
                                digest, descriptor.digest
                            ),
                        });
                    }
                    descriptor.digest = digest.clone();
                    self.fetch(&descriptor, CachePolicy).await?
                }
            } else {
                let reference = entry
                    .reference
                    .clone()
                    .ok_or_else(|| DistError::InvalidInput("lock entry missing ref".into()))?;
                let source = self.parse_source(&reference)?;
                let descriptor = self.resolve(source, ResolvePolicy).await?;
                self.fetch(&descriptor, CachePolicy).await?
            };
            resolved.push(resolved_item);
        }
        Ok(resolved)
    }

    pub fn list_cache(&self) -> Vec<String> {
        self.cache.list_digests()
    }

    pub fn list_cache_entries(&self) -> Vec<CacheEntry> {
        self.cache
            .list_digests()
            .into_iter()
            .filter_map(|digest| self.stat_cache(&digest).ok())
            .collect()
    }

    pub fn evaluate_retention(
        &self,
        input: &RetentionInput,
    ) -> Result<RetentionOutcome, DistError> {
        let decisions = retention_decisions(input);
        let mut report = RetentionReport {
            scanned_entries: decisions.len(),
            ..RetentionReport::default()
        };
        for decision in &decisions {
            match decision.decision {
                RetentionDisposition::Keep => report.kept += 1,
                RetentionDisposition::Evict => report.evicted += 1,
                RetentionDisposition::Protect => report.protected += 1,
            }
        }
        Ok(RetentionOutcome { decisions, report })
    }

    pub fn apply_retention(&self, input: &RetentionInput) -> Result<RetentionOutcome, DistError> {
        let mut outcome = self.evaluate_retention(input)?;
        let decision_map = input
            .entries
            .iter()
            .map(|entry| (entry.cache_key.clone(), entry.size_bytes))
            .collect::<std::collections::BTreeMap<_, _>>();
        let evicted = outcome
            .decisions
            .iter()
            .filter(|decision| matches!(decision.decision, RetentionDisposition::Evict))
            .map(|decision| decision.cache_key.clone())
            .collect::<Vec<_>>();
        for cache_key in evicted {
            let digest = normalize_digest(&cache_key);
            let dir = self.cache.component_dir(&digest);
            if dir.exists() {
                fs::remove_dir_all(&dir).map_err(|source| DistError::CacheError {
                    path: dir.display().to_string(),
                    source,
                })?;
                self.cache
                    .remove_bundle_record(&bundle_id_for_digest(&digest))
                    .ok();
                outcome.report.bytes_reclaimed +=
                    decision_map.get(&cache_key).copied().unwrap_or(0);
            } else {
                outcome.report.refusals.push(cache_key);
            }
        }
        outcome.report.kept = outcome
            .decisions
            .iter()
            .filter(|decision| matches!(decision.decision, RetentionDisposition::Keep))
            .count();
        outcome.report.protected = outcome
            .decisions
            .iter()
            .filter(|decision| matches!(decision.decision, RetentionDisposition::Protect))
            .count();
        outcome.report.evicted = outcome
            .decisions
            .iter()
            .filter(|decision| matches!(decision.decision, RetentionDisposition::Evict))
            .count()
            .saturating_sub(outcome.report.refusals.len());
        Ok(outcome)
    }

    fn persist_bundle_record(&self, record: &BundleRecord) -> Result<(), DistError> {
        self.cache
            .write_bundle_record(record)
            .map_err(|source| DistError::CacheError {
                path: self
                    .cache
                    .bundle_record_path(&record.bundle_id)
                    .display()
                    .to_string(),
                source,
            })
    }

    #[deprecated(note = "use evict_cache or apply_retention instead")]
    pub fn remove_cached(&self, digests: &[String]) -> Result<(), DistError> {
        for digest in digests {
            for dir in [
                self.cache.component_dir(digest),
                self.cache.legacy_component_dir(digest),
            ] {
                if dir.exists() {
                    fs::remove_dir_all(&dir).map_err(|source| DistError::CacheError {
                        path: dir.display().to_string(),
                        source,
                    })?;
                }
            }
            self.cache
                .remove_bundle_record(&bundle_id_for_digest(digest))
                .ok();
        }
        Ok(())
    }

    #[deprecated(note = "use evaluate_retention/apply_retention for cache lifecycle management")]
    pub fn gc(&self) -> Result<Vec<String>, DistError> {
        let mut removed = Vec::new();
        for digest in self.cache.list_digests() {
            let primary = self.cache.component_path(&digest);
            let legacy = self.cache.legacy_component_path(&digest);
            if !primary.exists() && !legacy.exists() {
                let dir = self.cache.component_dir(&digest);
                let legacy_dir = self.cache.legacy_component_dir(&digest);
                fs::remove_dir_all(&dir).ok();
                fs::remove_dir_all(&legacy_dir).ok();
                self.cache
                    .remove_bundle_record(&bundle_id_for_digest(&digest))
                    .ok();
                removed.push(digest);
            }
        }
        for record in self.list_bundles()? {
            if self.stat_cache(&record.cache_key).is_err() {
                self.cache.remove_bundle_record(&record.bundle_id).ok();
                removed.push(record.cache_key);
            }
        }
        removed.sort();
        removed.dedup();
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
            LegacyArtifactSource::Http(request_url.to_string()),
        );
        self.persist_cache_entry(&resolved)?;
        self.enforce_cache_cap(Some(&resolved.descriptor.digest))?;
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
            LegacyArtifactSource::File(path.to_path_buf()),
        );
        self.persist_cache_entry(&resolved)?;
        self.enforce_cache_cap(Some(&resolved.descriptor.digest))?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn pull_oci(&self, reference: &str) -> Result<ResolvedArtifact, DistError> {
        let component_id = component_id_from_ref(&RefKind::Oci(reference.to_string()));
        self.pull_oci_with_source(
            reference,
            LegacyArtifactSource::Oci(reference.to_string()),
            component_id,
        )
        .await
    }

    async fn pull_oci_with_source(
        &self,
        reference: &str,
        source: LegacyArtifactSource,
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
        let resolved_digest = resolved.resolved_digest.clone();
        let resolved_bytes = fs::read(&resolved.path).map_err(|source| DistError::CacheError {
            path: resolved.path.display().to_string(),
            source,
        })?;
        let resolved = ResolvedArtifact::from_path(
            resolved_digest.clone(),
            self.cache
                .write_component(&resolved_digest, &resolved_bytes)
                .map_err(|source| DistError::CacheError {
                    path: self
                        .cache
                        .component_path(&resolved_digest)
                        .display()
                        .to_string(),
                    source,
                })?,
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
        self.persist_cache_entry(&resolved)?;
        self.enforce_cache_cap(Some(&resolved.descriptor.digest))?;
        resolved.validate_payload()?;
        Ok(resolved)
    }

    async fn resolve_repo_ref(&self, target: &str) -> Result<ResolvedArtifact, DistError> {
        if self.opts.repo_registry_base.is_none() {
            return Err(DistError::ResolutionUnavailable {
                reference: format!("repo://{target}"),
            });
        }
        let mapped = map_registry_target(target, self.opts.repo_registry_base.as_deref())
            .ok_or_else(|| DistError::Unauthorized {
                target: format!("repo://{target}"),
            })?;
        self.pull_oci_with_source(
            &mapped,
            LegacyArtifactSource::Repo(format!("repo://{target}")),
            target.to_string(),
        )
        .await
    }

    async fn resolve_store_ref(&self, target: &str) -> Result<ResolvedArtifact, DistError> {
        if is_greentic_biz_store_target(target) {
            let parsed = parse_greentic_biz_store_target(target)?;
            return match self
                .pull_oci_with_source_and_client(
                    &parsed.mapped_reference,
                    LegacyArtifactSource::Store(format!("store://{target}")),
                    target.to_string(),
                    self.greentic_biz_store_component_client(&parsed.tenant)
                        .await?,
                )
                .await
            {
                Ok(artifact) => Ok(artifact),
                Err(DistError::Oci(err)) if should_retry_store_as_pack(&err) => {
                    self.pull_oci_pack_with_source_and_client(
                        &parsed.mapped_reference,
                        LegacyArtifactSource::Store(format!("store://{target}")),
                        target.to_string(),
                        self.greentic_biz_store_pack_client(&parsed.tenant).await?,
                    )
                    .await
                }
                Err(err) => Err(err),
            };
        }
        if self.opts.store_registry_base.is_none() {
            return Err(DistError::ResolutionUnavailable {
                reference: format!("store://{target}"),
            });
        }
        let mapped = map_registry_target(target, self.opts.store_registry_base.as_deref())
            .ok_or_else(|| DistError::Unauthorized {
                target: format!("store://{target}"),
            })?;
        self.pull_oci_with_source(
            &mapped,
            LegacyArtifactSource::Store(format!("store://{target}")),
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
                let legacy_source = legacy_source_from_public(&source);
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
                    legacy_source,
                )
            }
            InjectedResolution::WasmPath {
                resolved_digest,
                wasm_path,
                component_id,
                abi_version,
                source,
            } => {
                let legacy_source = legacy_source_from_public(&source);
                ResolvedArtifact::from_path(
                    resolved_digest,
                    wasm_path.clone(),
                    component_id,
                    abi_version,
                    resolve_describe_artifact_ref_from_cache(&wasm_path),
                    file_size_if_exists(&wasm_path),
                    Some(WASM_CONTENT_TYPE.to_string()),
                    false,
                    legacy_source,
                )
            }
        };
        self.persist_cache_entry(&artifact)?;
        self.enforce_cache_cap(Some(&artifact.descriptor.digest))?;
        artifact.validate_payload()?;
        Ok(artifact)
    }

    fn enforce_cache_cap(&self, current_digest: Option<&str>) -> Result<(), DistError> {
        let bundle_records = self.list_bundles()?;
        let mut staged_bundle_ids = bundle_records
            .iter()
            .filter(|record| matches!(record.lifecycle_state, BundleLifecycleState::Staged))
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();
        let warming_bundle_ids = bundle_records
            .iter()
            .filter(|record| matches!(record.lifecycle_state, BundleLifecycleState::Warming))
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();
        let ready_bundle_ids = bundle_records
            .iter()
            .filter(|record| matches!(record.lifecycle_state, BundleLifecycleState::Ready))
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();
        let draining_bundle_ids = bundle_records
            .iter()
            .filter(|record| matches!(record.lifecycle_state, BundleLifecycleState::Draining))
            .map(|record| record.bundle_id.clone())
            .collect::<Vec<_>>();
        if let Some(current_digest) = current_digest {
            staged_bundle_ids.push(bundle_id_for_digest(current_digest));
        }
        self.apply_retention(&RetentionInput {
            entries: self.list_cache_entries(),
            active_bundle_ids: Vec::new(),
            staged_bundle_ids,
            warming_bundle_ids,
            ready_bundle_ids,
            draining_bundle_ids,
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: self.opts.cache_max_bytes,
            max_entry_age: None,
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Dev,
        })
        .map(|_| ())
    }

    pub async fn pull_oci_with_details(
        &self,
        reference: &str,
    ) -> Result<OciCacheInspection, DistError> {
        if let RefKind::Store(target) = classify_reference(reference)?
            && is_greentic_biz_store_target(&target)
        {
            let resolved = self.resolve_store_ref(&target).await?;
            let artifact_path =
                resolved
                    .cache_path
                    .clone()
                    .ok_or_else(|| DistError::CorruptArtifact {
                        reference: reference.to_string(),
                        reason: "resolved store artifact missing cache path".into(),
                    })?;
            let cache_dir = artifact_path
                .parent()
                .map(|p| p.to_path_buf())
                .ok_or_else(|| DistError::InvalidInput("cache path missing parent".into()))?;
            return Ok(OciCacheInspection {
                digest: resolved.descriptor.digest.clone(),
                cache_dir,
                artifact_path,
                artifact_type: resolved.descriptor.artifact_type.clone(),
                selected_media_type: resolved.descriptor.media_type.clone(),
                fetched: resolved.fetched,
            });
        }
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
            artifact_path: resolved.path,
            artifact_type: ArtifactType::Component,
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
    #[error("resolution unavailable for `{reference}`")]
    ResolutionUnavailable { reference: String },
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
    #[error("store auth error: {0}")]
    StoreAuth(String),
    #[error("oci error: {0}")]
    Oci(#[from] crate::oci_components::OciComponentError),
    #[error("oci pack error: {0}")]
    Pack(String),
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
            DistError::Unauthorized { .. }
            | DistError::ResolutionUnavailable { .. }
            | DistError::StoreAuth(_) => 5,
            _ => 10,
        }
    }
}

impl IntegrationError {
    fn from_dist_error(error: DistError) -> Self {
        match error {
            DistError::InvalidRef { reference } => Self {
                code: IntegrationErrorCode::InvalidReference,
                summary: format!("invalid reference `{reference}`"),
                retryable: false,
                details: Some(serde_json::json!({ "reference": reference })),
            },
            DistError::NotFound { reference } => Self {
                code: IntegrationErrorCode::CacheMiss,
                summary: format!("artifact `{reference}` was not found"),
                retryable: true,
                details: Some(serde_json::json!({ "reference": reference })),
            },
            DistError::Unauthorized { target } => Self {
                code: IntegrationErrorCode::ResolutionFailed,
                summary: format!("unauthorized for `{target}`"),
                retryable: false,
                details: Some(serde_json::json!({ "target": target })),
            },
            DistError::ResolutionUnavailable { reference } => Self {
                code: IntegrationErrorCode::ResolutionUnavailable,
                summary: format!("resolution unavailable for `{reference}`"),
                retryable: false,
                details: Some(serde_json::json!({ "reference": reference })),
            },
            DistError::Network(summary) => Self {
                code: IntegrationErrorCode::DownloadFailed,
                summary,
                retryable: true,
                details: None,
            },
            DistError::CorruptArtifact { reference, reason } => Self {
                code: IntegrationErrorCode::CacheCorrupt,
                summary: format!("corrupt artifact `{reference}`: {reason}"),
                retryable: false,
                details: Some(serde_json::json!({ "reference": reference, "reason": reason })),
            },
            DistError::UnsupportedAbi { abi } => Self {
                code: IntegrationErrorCode::UnsupportedArtifactType,
                summary: format!("unsupported abi `{abi}`"),
                retryable: false,
                details: Some(serde_json::json!({ "abi": abi })),
            },
            DistError::CacheError { path, source } => Self {
                code: IntegrationErrorCode::BundleOpenFailed,
                summary: format!("cache error at `{path}`: {source}"),
                retryable: true,
                details: Some(serde_json::json!({ "path": path })),
            },
            DistError::InvalidInput(summary) => Self {
                code: IntegrationErrorCode::PolicyInputInvalid,
                summary,
                retryable: false,
                details: None,
            },
            DistError::InsecureUrl { url } => Self {
                code: IntegrationErrorCode::UnsupportedSource,
                summary: format!("insecure url `{url}`: only https is allowed"),
                retryable: false,
                details: Some(serde_json::json!({ "url": url })),
            },
            DistError::Offline { reference } => Self {
                code: IntegrationErrorCode::OfflineRequiredButUnavailable,
                summary: format!("offline mode forbids fetching `{reference}`"),
                retryable: true,
                details: Some(serde_json::json!({ "reference": reference })),
            },
            DistError::StoreAuth(summary) => Self {
                code: IntegrationErrorCode::ResolutionFailed,
                summary,
                retryable: false,
                details: None,
            },
            DistError::Oci(source) => Self {
                code: IntegrationErrorCode::ResolutionFailed,
                summary: source.to_string(),
                retryable: true,
                details: None,
            },
            DistError::Pack(summary) => Self {
                code: IntegrationErrorCode::ResolutionFailed,
                summary,
                retryable: true,
                details: None,
            },
            DistError::Serde(source) => Self {
                code: IntegrationErrorCode::DescriptorCorrupt,
                summary: format!("invalid serialized input: {source}"),
                retryable: false,
                details: None,
            },
        }
    }

    fn from_verification_report(report: VerificationReport) -> Self {
        let code = verification_failure_code(&report);
        let summary = report
            .errors
            .first()
            .cloned()
            .or_else(|| report.warnings.first().cloned())
            .unwrap_or_else(|| "verification failed".to_string());
        Self {
            code,
            summary,
            retryable: false,
            details: Some(serde_json::json!({
                "artifact_digest": report.artifact_digest,
                "canonical_ref": report.canonical_ref,
                "errors": report.errors,
                "warnings": report.warnings,
                "failed_checks": report
                    .checks
                    .iter()
                    .filter(|check| matches!(check.outcome, VerificationOutcome::Failed))
                    .map(|check| serde_json::json!({
                        "name": check.name,
                        "detail": check.detail,
                        "payload": check.payload,
                    }))
                    .collect::<Vec<_>>(),
            })),
        }
    }
}

impl ArtifactOpener for DefaultArtifactOpener {
    fn open(
        &self,
        artifact: &ResolvedArtifact,
        request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError> {
        Ok(ArtifactOpenOutput {
            bundle_manifest_summary: manifest_summary_from_artifact(artifact),
            bundle_open_mode: if request.dry_run {
                BundleOpenMode::CacheReuse
            } else {
                BundleOpenMode::Userspace
            },
            warnings: if request.smoke_test {
                vec!["smoke test requested but no bundle runtime opener is configured".to_string()]
            } else {
                Vec::new()
            },
        })
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

    fn artifacts_root(&self) -> PathBuf {
        self.base.join("artifacts").join("sha256")
    }

    fn bundle_records_root(&self) -> PathBuf {
        self.base.join("bundles")
    }

    fn bundle_record_path(&self, bundle_id: &str) -> PathBuf {
        let safe = bundle_id.replace(':', "__");
        self.bundle_records_root().join(format!("{safe}.json"))
    }

    fn component_dir(&self, digest: &str) -> PathBuf {
        let normalized = trim_digest_prefix(&normalize_digest(digest)).to_string();
        let (prefix, rest) = normalized.split_at(normalized.len().min(2));
        self.artifacts_root().join(prefix).join(rest)
    }

    fn legacy_component_dir(&self, digest: &str) -> PathBuf {
        self.base
            .join(trim_digest_prefix(&normalize_digest(digest)))
    }

    fn component_path(&self, digest: &str) -> PathBuf {
        self.component_dir(digest).join("blob")
    }

    fn legacy_component_path(&self, digest: &str) -> PathBuf {
        self.legacy_component_dir(digest).join("component.wasm")
    }

    fn entry_path(&self, digest: &str) -> PathBuf {
        self.component_dir(digest).join("entry.json")
    }

    fn existing_component(&self, digest: &str) -> Option<PathBuf> {
        let path = self.component_path(digest);
        if path.exists() {
            let _ = self.touch_last_used(digest);
            Some(path)
        } else {
            let legacy = self.legacy_component_path(digest);
            if legacy.exists() {
                let _ = self.touch_last_used(digest);
                Some(legacy)
            } else {
                None
            }
        }
    }

    fn write_component(&self, digest: &str, data: &[u8]) -> Result<PathBuf, std::io::Error> {
        let dir = self.component_dir(digest);
        fs::create_dir_all(&dir)?;
        let path = dir.join("blob");
        fs::write(&path, data)?;
        self.touch_last_used(digest)?;
        Ok(path)
    }

    fn write_entry(&self, entry: &CacheEntry) -> Result<(), std::io::Error> {
        let path = self.entry_path(&entry.digest);
        let bytes = serde_json::to_vec_pretty(entry)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        fs::write(path, bytes)
    }

    fn read_entry(&self, digest: &str) -> Result<CacheEntry, std::io::Error> {
        let path = self.entry_path(digest);
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|err| std::io::Error::other(err.to_string()))
    }

    fn write_bundle_record(&self, record: &BundleRecord) -> Result<(), std::io::Error> {
        let path = self.bundle_record_path(&record.bundle_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        fs::write(path, bytes)
    }

    fn read_bundle_record(&self, bundle_id: &str) -> Result<BundleRecord, std::io::Error> {
        let path = self.bundle_record_path(bundle_id);
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|err| std::io::Error::other(err.to_string()))
    }

    fn remove_bundle_record(&self, bundle_id: &str) -> Result<(), std::io::Error> {
        let path = self.bundle_record_path(bundle_id);
        if path.exists() {
            fs::remove_file(path)
        } else {
            Ok(())
        }
    }

    fn list_bundle_records(&self) -> Result<Vec<BundleRecord>, std::io::Error> {
        let root = self.bundle_records_root();
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(root) else {
            return Ok(out);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let bytes = fs::read(&path)?;
            let record = serde_json::from_slice::<BundleRecord>(&bytes)
                .map_err(|err| std::io::Error::other(format!("{}: {err}", path.display())))?;
            out.push(record);
        }
        out.sort_by(|a, b| a.bundle_id.cmp(&b.bundle_id));
        Ok(out)
    }

    fn list_digests(&self) -> Vec<String> {
        let mut digests = Vec::new();
        self.collect_digests(&self.base, &mut digests);
        let root = self.artifacts_root();
        if root != self.base {
            self.collect_digests(&root, &mut digests);
        }
        digests.sort();
        digests.dedup();
        digests
    }

    fn collect_digests(&self, dir: &Path, digests: &mut Vec<String>) {
        let _ = &self.base;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let blob = path.join("blob");
                    let legacy_blob = path.join("component.wasm");
                    if blob.exists() || legacy_blob.exists() {
                        if let Some(digest) = digest_from_component_dir(&path) {
                            digests.push(digest);
                        }
                    } else {
                        self.collect_digests(&path, digests);
                    }
                }
            }
        }
    }

    fn touch_last_used(&self, digest: &str) -> Result<(), std::io::Error> {
        let marker = self.component_dir(digest).join("last_used");
        let seq = LAST_USED_COUNTER.fetch_add(1, Ordering::Relaxed);
        fs::write(marker, seq.to_string())
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn artifact_source_from_reference(
    reference: &str,
    opts: &DistOptions,
) -> Result<ArtifactSource, DistError> {
    let kind = match classify_reference(reference)? {
        RefKind::Digest(_) => ArtifactSourceKind::CacheDigest,
        RefKind::Http(_) => ArtifactSourceKind::Https,
        RefKind::File(_) => ArtifactSourceKind::File,
        RefKind::Oci(_) => ArtifactSourceKind::Oci,
        RefKind::Repo(_) => ArtifactSourceKind::Repo,
        RefKind::Store(_) => ArtifactSourceKind::Store,
        #[cfg(feature = "fixture-resolver")]
        RefKind::Fixture(_) => ArtifactSourceKind::Fixture,
    };
    Ok(ArtifactSource {
        raw_ref: reference.to_string(),
        kind: kind.clone(),
        transport_hints: TransportHints {
            offline: opts.offline,
            allow_insecure_local_http: opts.allow_insecure_local_http,
        },
        dev_mode: matches!(kind, ArtifactSourceKind::Fixture | ArtifactSourceKind::File),
    })
}

fn canonical_oci_ref(reference: &str, digest: &str) -> String {
    let repo = canonical_oci_component_id(reference);
    format!("oci://{repo}@{digest}")
}

fn digest_from_component_dir(dir: &Path) -> Option<String> {
    let leaf = dir.file_name()?.to_str()?;
    if leaf.len() == 64 && leaf.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Some(format!("sha256:{leaf}"));
    }
    let parent = dir.parent()?.file_name()?.to_str()?;
    if parent.len() == 2 && leaf.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(format!("sha256:{parent}{leaf}"))
    } else {
        None
    }
}

fn cache_entry_state_from_integrity(state: &IntegrityState) -> CacheEntryState {
    match state {
        IntegrityState::Partial => CacheEntryState::Partial,
        IntegrityState::Ready => CacheEntryState::Ready,
        IntegrityState::Corrupt => CacheEntryState::Corrupt,
        IntegrityState::Evicted => CacheEntryState::Evicted,
    }
}

fn integrity_state_from_entry(state: &CacheEntryState) -> IntegrityState {
    match state {
        CacheEntryState::Partial => IntegrityState::Partial,
        CacheEntryState::Ready => IntegrityState::Ready,
        CacheEntryState::Corrupt => IntegrityState::Corrupt,
        CacheEntryState::Evicted => IntegrityState::Evicted,
    }
}

fn descriptor_from_entry(entry: &CacheEntry) -> ArtifactDescriptor {
    ArtifactDescriptor {
        artifact_type: entry.artifact_type.clone(),
        source_kind: entry.source_kind.clone(),
        raw_ref: entry.raw_ref.clone(),
        canonical_ref: entry.canonical_ref.clone(),
        digest: entry.digest.clone(),
        media_type: entry.media_type.clone(),
        size_bytes: entry.size_bytes,
        created_at: None,
        annotations: serde_json::Map::new(),
        manifest_digest: None,
        resolved_via: match entry.source_kind {
            ArtifactSourceKind::Repo => ResolvedVia::RepoMapping,
            ArtifactSourceKind::Store => ResolvedVia::StoreMapping,
            ArtifactSourceKind::Fixture => ResolvedVia::Fixture,
            ArtifactSourceKind::File => ResolvedVia::File,
            ArtifactSourceKind::Https => ResolvedVia::Https,
            ArtifactSourceKind::CacheDigest => ResolvedVia::CacheDigest,
            ArtifactSourceKind::Oci => ResolvedVia::Direct,
        },
        signature_refs: Vec::new(),
        sbom_refs: Vec::new(),
    }
}

fn legacy_source_from_entry(entry: &CacheEntry) -> LegacyArtifactSource {
    match entry.source_kind {
        ArtifactSourceKind::CacheDigest => LegacyArtifactSource::Digest,
        ArtifactSourceKind::Https => LegacyArtifactSource::Http(entry.raw_ref.clone()),
        ArtifactSourceKind::File | ArtifactSourceKind::Fixture => {
            LegacyArtifactSource::File(PathBuf::from(entry.raw_ref.clone()))
        }
        ArtifactSourceKind::Oci => {
            LegacyArtifactSource::Oci(entry.canonical_ref.trim_start_matches("oci://").to_string())
        }
        ArtifactSourceKind::Repo => LegacyArtifactSource::Repo(entry.raw_ref.clone()),
        ArtifactSourceKind::Store => LegacyArtifactSource::Store(entry.raw_ref.clone()),
    }
}

fn component_id_from_descriptor(entry: &CacheEntry) -> String {
    component_id_from_ref(&match entry.source_kind {
        ArtifactSourceKind::CacheDigest => RefKind::Digest(entry.digest.clone()),
        ArtifactSourceKind::Https => RefKind::Http(entry.raw_ref.clone()),
        ArtifactSourceKind::File | ArtifactSourceKind::Fixture => {
            RefKind::File(PathBuf::from(entry.raw_ref.clone()))
        }
        ArtifactSourceKind::Oci => {
            RefKind::Oci(entry.canonical_ref.trim_start_matches("oci://").to_string())
        }
        ArtifactSourceKind::Repo => RefKind::Repo(entry.raw_ref.clone()),
        ArtifactSourceKind::Store => RefKind::Store(entry.raw_ref.clone()),
    })
}

fn verification_outcome_name(outcome: &VerificationOutcome) -> &'static str {
    match outcome {
        VerificationOutcome::Passed => "passed",
        VerificationOutcome::Failed => "failed",
        VerificationOutcome::Warning => "warning",
        VerificationOutcome::Skipped => "skipped",
    }
}

fn make_check(
    name: &str,
    outcome: VerificationOutcome,
    detail: impl Into<String>,
    payload: Option<serde_json::Value>,
) -> VerificationCheck {
    VerificationCheck {
        name: name.to_string(),
        outcome,
        detail: detail.into(),
        payload,
    }
}

fn preliminary_decision_from_checks(checks: Vec<VerificationCheck>) -> PreliminaryDecision {
    let warnings = checks
        .iter()
        .filter(|check| matches!(check.outcome, VerificationOutcome::Warning))
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();
    let errors = checks
        .iter()
        .filter(|check| matches!(check.outcome, VerificationOutcome::Failed))
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();
    PreliminaryDecision {
        passed: errors.is_empty(),
        checks,
        warnings,
        errors,
    }
}

fn verification_report_from_checks(
    descriptor: &ArtifactDescriptor,
    advisory_set: Option<&AdvisorySet>,
    verification_policy: &VerificationPolicy,
    cache_entry: Option<&CacheEntry>,
    checks: Vec<VerificationCheck>,
) -> VerificationReport {
    let warnings = checks
        .iter()
        .filter(|check| matches!(check.outcome, VerificationOutcome::Warning))
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();
    let errors = checks
        .iter()
        .filter(|check| matches!(check.outcome, VerificationOutcome::Failed))
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();

    VerificationReport {
        artifact_digest: descriptor.digest.clone(),
        canonical_ref: descriptor.canonical_ref.clone(),
        passed: errors.is_empty(),
        warnings,
        errors,
        policy_fingerprint: policy_fingerprint(verification_policy),
        advisory_version: advisory_set.map(|advisory| advisory.version.clone()),
        cache_entry_fingerprint: cache_entry.map(cache_entry_fingerprint),
        checks,
    }
}

fn policy_fingerprint(policy: &VerificationPolicy) -> String {
    let bytes = serde_json::to_vec(policy).unwrap_or_default();
    digest_for_bytes(&bytes)
}

fn cache_entry_fingerprint(entry: &CacheEntry) -> String {
    let bytes = serde_json::to_vec(entry).unwrap_or_default();
    digest_for_bytes(&bytes)
}

fn issuer_from_descriptor(descriptor: &ArtifactDescriptor) -> Option<String> {
    descriptor
        .annotations
        .get("issuer")
        .and_then(|value| value.as_str())
        .map(|raw| raw.to_string())
}

fn minimum_operator_version_from_descriptor(descriptor: &ArtifactDescriptor) -> Option<String> {
    descriptor
        .annotations
        .get("minimum_operator_version")
        .and_then(|value| value.as_str())
        .map(|raw| raw.to_string())
}

fn check_digest_allowed(
    digest: &str,
    advisory_set: Option<&AdvisorySet>,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    let denied = verification_policy
        .deny_digests
        .iter()
        .chain(
            advisory_set
                .into_iter()
                .flat_map(|advisory| advisory.deny_digests.iter()),
        )
        .any(|candidate| candidate == digest);
    if denied {
        make_check(
            "digest_allowed",
            VerificationOutcome::Failed,
            format!("digest {digest} is denied by policy or advisory"),
            Some(serde_json::json!({
                "digest": digest,
                "advisory_version": advisory_set.map(|advisory| advisory.version.clone()),
            })),
        )
    } else {
        make_check(
            "digest_allowed",
            VerificationOutcome::Passed,
            format!("digest {digest} is allowed"),
            Some(serde_json::json!({
                "digest": digest,
            })),
        )
    }
}

fn check_media_type_allowed(
    media_type: &str,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    if verification_policy.allowed_media_types.is_empty() {
        return make_check(
            "media_type_allowed",
            VerificationOutcome::Skipped,
            "no media type allowlist configured",
            Some(serde_json::json!({
                "media_type": media_type,
            })),
        );
    }
    if verification_policy
        .allowed_media_types
        .iter()
        .any(|candidate| candidate == media_type)
    {
        make_check(
            "media_type_allowed",
            VerificationOutcome::Passed,
            format!("media type {media_type} is allowed"),
            Some(serde_json::json!({
                "media_type": media_type,
            })),
        )
    } else {
        make_check(
            "media_type_allowed",
            VerificationOutcome::Failed,
            format!("media type {media_type} is not allowed"),
            Some(serde_json::json!({
                "media_type": media_type,
                "allowed_media_types": verification_policy.allowed_media_types,
            })),
        )
    }
}

fn check_issuer_allowed(
    issuer: Option<String>,
    advisory_set: Option<&AdvisorySet>,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    let Some(issuer) = issuer else {
        return make_check(
            "issuer_allowed",
            VerificationOutcome::Warning,
            "issuer metadata is missing",
            Some(serde_json::json!({
                "issuer": null,
                "advisory_version": advisory_set.map(|advisory| advisory.version.clone()),
            })),
        );
    };
    if verification_policy
        .deny_issuers
        .iter()
        .chain(
            advisory_set
                .into_iter()
                .flat_map(|advisory| advisory.deny_issuers.iter()),
        )
        .any(|candidate| candidate == &issuer)
    {
        return make_check(
            "issuer_allowed",
            VerificationOutcome::Failed,
            format!("issuer {issuer} is denied"),
            Some(serde_json::json!({
                "issuer": issuer,
                "advisory_version": advisory_set.map(|advisory| advisory.version.clone()),
            })),
        );
    }
    if !verification_policy.trusted_issuers.is_empty()
        && !verification_policy
            .trusted_issuers
            .iter()
            .any(|candidate| candidate == &issuer)
    {
        return make_check(
            "issuer_allowed",
            VerificationOutcome::Warning,
            format!("issuer {issuer} is not on the trusted issuer allowlist"),
            Some(serde_json::json!({
                "issuer": issuer,
                "trusted_issuers": verification_policy.trusted_issuers,
            })),
        );
    }
    make_check(
        "issuer_allowed",
        VerificationOutcome::Passed,
        format!("issuer {issuer} is allowed"),
        Some(serde_json::json!({
            "issuer": issuer,
        })),
    )
}

fn check_operator_version_compatible(
    descriptor: &ArtifactDescriptor,
    advisory_set: Option<&AdvisorySet>,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    let required = verification_policy
        .minimum_operator_version
        .clone()
        .or_else(|| advisory_set.and_then(|advisory| advisory.minimum_operator_version.clone()))
        .or_else(|| minimum_operator_version_from_descriptor(descriptor));
    let Some(required) = required else {
        return make_check(
            "operator_version_compatible",
            VerificationOutcome::Skipped,
            "no minimum operator version requirement present",
            Some(serde_json::json!({
                "required": null,
                "actual": descriptor
                    .annotations
                    .get("operator_version")
                    .and_then(|value| value.as_str()),
            })),
        );
    };

    let actual = descriptor
        .annotations
        .get("operator_version")
        .and_then(|value| value.as_str())
        .map(|raw| raw.to_string());
    let Some(actual) = actual else {
        return make_check(
            "operator_version_compatible",
            VerificationOutcome::Warning,
            format!(
                "minimum operator version {required} is declared but actual operator version is unknown"
            ),
            Some(serde_json::json!({
                "required": required,
                "actual": null,
            })),
        );
    };

    let required_parsed = semver::Version::parse(&required);
    let actual_parsed = semver::Version::parse(&actual);
    match (required_parsed, actual_parsed) {
        (Ok(required), Ok(actual)) if actual >= required => make_check(
            "operator_version_compatible",
            VerificationOutcome::Passed,
            format!("operator version {actual} satisfies minimum {required}"),
            Some(serde_json::json!({
                "required": required.to_string(),
                "actual": actual.to_string(),
            })),
        ),
        (Ok(required), Ok(actual)) => {
            let outcome = match verification_policy.environment {
                VerificationEnvironment::Dev => VerificationOutcome::Warning,
                VerificationEnvironment::Staging | VerificationEnvironment::Prod => {
                    VerificationOutcome::Failed
                }
            };
            make_check(
                "operator_version_compatible",
                outcome,
                format!("operator version {actual} does not satisfy minimum {required}"),
                Some(serde_json::json!({
                    "required": required.to_string(),
                    "actual": actual.to_string(),
                    "environment": verification_environment_name(&verification_policy.environment),
                })),
            )
        }
        _ => make_check(
            "operator_version_compatible",
            VerificationOutcome::Warning,
            format!(
                "operator version metadata is not parseable (actual={actual}, required={required})"
            ),
            Some(serde_json::json!({
                "required": required,
                "actual": actual,
            })),
        ),
    }
}

fn check_content_digest_match(
    resolved_artifact: &ResolvedArtifact,
) -> Result<VerificationCheck, DistError> {
    let bytes = resolved_artifact.wasm_bytes()?;
    let computed = digest_for_bytes(bytes);
    Ok(if computed == resolved_artifact.descriptor.digest {
        make_check(
            "content_digest_match",
            VerificationOutcome::Passed,
            format!(
                "content digest matches {}",
                resolved_artifact.descriptor.digest
            ),
            None,
        )
    } else {
        make_check(
            "content_digest_match",
            VerificationOutcome::Failed,
            format!(
                "content digest {} did not match descriptor {}",
                computed, resolved_artifact.descriptor.digest
            ),
            Some(serde_json::json!({
                "expected": resolved_artifact.descriptor.digest,
                "actual": computed,
            })),
        )
    })
}

fn check_signature_present(
    descriptor: &ArtifactDescriptor,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    if descriptor.signature_refs.is_empty() {
        let outcome = match verification_policy.environment {
            VerificationEnvironment::Dev => VerificationOutcome::Warning,
            VerificationEnvironment::Staging | VerificationEnvironment::Prod => {
                if verification_policy.require_signature {
                    VerificationOutcome::Failed
                } else {
                    VerificationOutcome::Warning
                }
            }
        };
        return make_check(
            "signature_present",
            outcome,
            "no signature references are present",
            Some(serde_json::json!({
                "count": 0,
                "required": verification_policy.require_signature,
                "environment": verification_environment_name(&verification_policy.environment),
            })),
        );
    }
    make_check(
        "signature_present",
        VerificationOutcome::Passed,
        "signature references are present",
        Some(serde_json::json!({
            "count": descriptor.signature_refs.len(),
            "required": verification_policy.require_signature,
            "environment": verification_environment_name(&verification_policy.environment),
        })),
    )
}

fn check_signature_verified(
    descriptor: &ArtifactDescriptor,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    let detail = if descriptor.signature_refs.is_empty() {
        "signature verification could not run because no signature references are present"
    } else {
        "signature verification is not implemented in the open-source client"
    };
    let outcome = if verification_policy.require_signature {
        match verification_policy.environment {
            VerificationEnvironment::Dev => VerificationOutcome::Warning,
            VerificationEnvironment::Staging | VerificationEnvironment::Prod => {
                VerificationOutcome::Failed
            }
        }
    } else if descriptor.signature_refs.is_empty() {
        VerificationOutcome::Skipped
    } else {
        VerificationOutcome::Warning
    };
    make_check(
        "signature_verified",
        outcome,
        detail,
        Some(serde_json::json!({
            "implemented": false,
            "signature_count": descriptor.signature_refs.len(),
            "required": verification_policy.require_signature,
            "environment": verification_environment_name(&verification_policy.environment),
        })),
    )
}

fn check_sbom_present(
    descriptor: &ArtifactDescriptor,
    verification_policy: &VerificationPolicy,
) -> VerificationCheck {
    if descriptor.sbom_refs.is_empty() {
        return make_check(
            "sbom_present",
            if verification_policy.require_sbom {
                VerificationOutcome::Failed
            } else {
                VerificationOutcome::Warning
            },
            "no SBOM references are present",
            Some(serde_json::json!({
                "count": 0,
                "required": verification_policy.require_sbom,
            })),
        );
    }
    make_check(
        "sbom_present",
        VerificationOutcome::Passed,
        "SBOM references are present",
        Some(serde_json::json!({
            "count": descriptor.sbom_refs.len(),
            "required": verification_policy.require_sbom,
        })),
    )
}

fn verification_environment_name(environment: &VerificationEnvironment) -> &'static str {
    match environment {
        VerificationEnvironment::Dev => "dev",
        VerificationEnvironment::Staging => "staging",
        VerificationEnvironment::Prod => "prod",
    }
}

fn bundle_id_for_digest(digest: &str) -> String {
    format!("bundle:{}", normalize_digest(digest))
}

fn digest_from_bundle_id(bundle_id: &str) -> Option<String> {
    bundle_id
        .strip_prefix("bundle:")
        .map(normalize_digest)
        .filter(|digest| digest.starts_with("sha256:"))
}

fn manifest_summary_from_artifact(artifact: &ResolvedArtifact) -> BundleManifestSummary {
    BundleManifestSummary {
        component_id: artifact.component_id.clone(),
        abi_version: artifact.abi_version.clone(),
        describe_artifact_ref: artifact.describe_artifact_ref.clone(),
        artifact_type: artifact.descriptor.artifact_type.clone(),
        media_type: artifact.descriptor.media_type.clone(),
        size_bytes: artifact.descriptor.size_bytes,
    }
}

fn verification_failure_code(report: &VerificationReport) -> IntegrationErrorCode {
    for check in &report.checks {
        if !matches!(check.outcome, VerificationOutcome::Failed) {
            continue;
        }
        return match check.name.as_str() {
            "digest_allowed" => IntegrationErrorCode::DigestDenied,
            "media_type_allowed" => IntegrationErrorCode::MediaTypeRejected,
            "issuer_allowed" => IntegrationErrorCode::IssuerRejected,
            "content_digest_match" => IntegrationErrorCode::DigestMismatch,
            "signature_present" | "signature_verified" => IntegrationErrorCode::SignatureRequired,
            "operator_version_compatible" => IntegrationErrorCode::VerificationFailed,
            "sbom_present" => IntegrationErrorCode::VerificationFailed,
            _ => IntegrationErrorCode::VerificationFailed,
        };
    }
    IntegrationErrorCode::VerificationFailed
}

fn retention_decisions(input: &RetentionInput) -> Vec<RetentionDecision> {
    let mut decisions = Vec::with_capacity(input.entries.len());
    let protected_active = input
        .active_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_staged = input
        .staged_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_warming = input
        .warming_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_ready = input
        .ready_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_draining = input
        .draining_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_session = input
        .session_referenced_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let protected_rollback = rollback_protected_bundle_ids(
        &input.active_bundle_ids,
        &input.staged_bundle_ids,
        &input.entries,
        input.minimum_rollback_depth,
    );

    let now = unix_now();
    let mut candidate_indices = Vec::new();
    let mut total_bytes = input
        .entries
        .iter()
        .map(|entry| entry.size_bytes)
        .sum::<u64>();

    for (index, entry) in input.entries.iter().enumerate() {
        let bundle_id = bundle_id_for_digest(&entry.digest);
        let protection = if protected_active.contains(&bundle_id) {
            Some(("active_bundle", "bundle is currently active".to_string()))
        } else if protected_session.contains(&bundle_id) {
            Some((
                "session_reference",
                "bundle is referenced by a live session".to_string(),
            ))
        } else if protected_warming.contains(&bundle_id) {
            Some(("warming_bundle", "bundle is currently warming".to_string()))
        } else if protected_ready.contains(&bundle_id) {
            Some(("ready_bundle", "bundle is currently ready".to_string()))
        } else if protected_draining.contains(&bundle_id) {
            Some((
                "draining_bundle",
                "bundle is currently draining".to_string(),
            ))
        } else if protected_staged.contains(&bundle_id) {
            Some(("staged_bundle", "bundle is currently staged".to_string()))
        } else if protected_rollback.contains(&bundle_id) {
            Some((
                "rollback_depth",
                "bundle is protected for rollback depth".to_string(),
            ))
        } else {
            None
        };

        if let Some((reason_code, reason_detail)) = protection {
            decisions.push(RetentionDecision {
                cache_key: entry.cache_key.clone(),
                bundle_id,
                decision: RetentionDisposition::Protect,
                reason_code: reason_code.to_string(),
                reason_detail,
            });
            continue;
        }

        decisions.push(RetentionDecision {
            cache_key: entry.cache_key.clone(),
            bundle_id,
            decision: RetentionDisposition::Keep,
            reason_code: "within_policy".to_string(),
            reason_detail: "entry remains available".to_string(),
        });
        candidate_indices.push(index);
    }

    let mut candidate_order = candidate_indices
        .into_iter()
        .map(|index| {
            let entry = &input.entries[index];
            let is_corrupt = matches!(entry.state, CacheEntryState::Corrupt);
            let rollback_eligible =
                protected_rollback.contains(&bundle_id_for_digest(&entry.digest));
            let age_secs = now.saturating_sub(entry.last_accessed_at);
            (index, is_corrupt, rollback_eligible, age_secs)
        })
        .collect::<Vec<_>>();
    candidate_order.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| b.3.cmp(&a.3))
            .then_with(|| {
                input.entries[a.0]
                    .cache_key
                    .cmp(&input.entries[b.0].cache_key)
            })
    });

    for (index, is_corrupt, _, age_secs) in candidate_order {
        let entry = &input.entries[index];
        let aged_out = input
            .max_entry_age
            .is_some_and(|max_age| age_secs > max_age);
        let over_budget = input.max_cache_bytes > 0 && total_bytes > input.max_cache_bytes;
        if !is_corrupt && !aged_out && !over_budget {
            continue;
        }

        let decision = &mut decisions[index];
        decision.decision = RetentionDisposition::Evict;
        if is_corrupt {
            decision.reason_code = "corrupt_entry".to_string();
            decision.reason_detail = "corrupt entry can be evicted safely".to_string();
        } else if aged_out {
            decision.reason_code = "max_age_exceeded".to_string();
            decision.reason_detail = "entry exceeded retention age".to_string();
        } else {
            decision.reason_code = "cache_budget".to_string();
            decision.reason_detail =
                "entry selected for eviction under cache budget pressure".to_string();
        }
        total_bytes = total_bytes.saturating_sub(entry.size_bytes);
    }

    decisions
}

fn rollback_protected_bundle_ids(
    active_bundle_ids: &[String],
    staged_bundle_ids: &[String],
    entries: &[CacheEntry],
    minimum_rollback_depth: usize,
) -> std::collections::BTreeSet<String> {
    if minimum_rollback_depth == 0 {
        return std::collections::BTreeSet::new();
    }
    let active = active_bundle_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut protected = std::collections::BTreeSet::new();
    let ordered_bundle_ids = if staged_bundle_ids.is_empty() {
        let mut entries = entries.iter().collect::<Vec<_>>();
        entries.sort_by(|a, b| {
            a.fetched_at
                .cmp(&b.fetched_at)
                .then_with(|| a.cache_key.cmp(&b.cache_key))
        });
        entries
            .into_iter()
            .map(|entry| bundle_id_for_digest(&entry.digest))
            .collect::<Vec<_>>()
    } else {
        staged_bundle_ids.to_vec()
    };
    for bundle_id in ordered_bundle_ids.iter().rev() {
        if active.contains(bundle_id) {
            continue;
        }
        protected.insert(bundle_id.clone());
        if protected.len() >= minimum_rollback_depth {
            break;
        }
    }
    protected
}

fn digest_for_bytes(bytes: &[u8]) -> String {
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

fn compute_bytes_digest(bytes: &[u8]) -> String {
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

fn normalize_content_type(current: Option<&str>, fallback: &str) -> String {
    let value = current.unwrap_or("").trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn should_retry_store_as_pack(err: &OciComponentError) -> bool {
    let message = err.to_string();
    message.contains("Incompatible layer media type") || message.contains("component layer missing")
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

async fn download_store_artifact_with_client<C: StoreDownloadRegistryClient>(
    client: &C,
    source_ref: &str,
    mapped_reference: &str,
    auth: &StoreRegistryAuth,
) -> Result<DownloadedStoreArtifact, DistError> {
    let parsed = Reference::try_from(mapped_reference).map_err(|_| DistError::InvalidRef {
        reference: source_ref.to_string(),
    })?;
    let manifest = client
        .pull_manifest(&parsed, auth)
        .await
        .map_err(|err| DistError::Network(err.to_string()))?;
    let accepted_media_types = accepted_store_download_media_types(&manifest);
    let image = client
        .pull(&parsed, auth, &accepted_media_types)
        .await
        .map_err(|err| DistError::Network(err.to_string()))?;
    let layer = select_store_download_layer(&image.layers, mapped_reference)?;
    let digest = image
        .digest
        .clone()
        .or_else(|| layer.digest.clone())
        .unwrap_or_else(|| compute_bytes_digest(&layer.data));

    Ok(DownloadedStoreArtifact {
        source_ref: source_ref.to_string(),
        mapped_reference: format!("oci://{mapped_reference}"),
        canonical_ref: canonical_oci_ref(mapped_reference, &digest),
        digest,
        media_type: layer.media_type.clone(),
        bytes: layer.data.clone(),
        size_bytes: layer.data.len() as u64,
        manifest_digest: image.digest,
    })
}

fn accepted_store_download_media_types(manifest: &OciManifest) -> Vec<String> {
    let mut accepted = vec![
        "application/json".to_string(),
        "application/octet-stream".to_string(),
        WASM_CONTENT_TYPE.to_string(),
    ];
    if let OciManifest::Image(image_manifest) = manifest {
        for layer in &image_manifest.layers {
            if !accepted
                .iter()
                .any(|candidate| candidate == &layer.media_type)
            {
                accepted.push(layer.media_type.clone());
            }
        }
    }
    accepted
}

fn select_store_download_layer<'a>(
    layers: &'a [PulledLayer],
    reference: &str,
) -> Result<&'a PulledLayer, DistError> {
    if layers.is_empty() {
        return Err(DistError::Network(format!(
            "no layers returned for `{reference}`"
        )));
    }
    if let Some(layer) = layers
        .iter()
        .find(|layer| media_type_is_json(&layer.media_type))
    {
        return Ok(layer);
    }
    if let Some(layer) = layers
        .iter()
        .find(|layer| layer.media_type == WASM_CONTENT_TYPE)
    {
        return Ok(layer);
    }
    Ok(&layers[0])
}

fn media_type_is_json(media_type: &str) -> bool {
    media_type == "application/json" || media_type.ends_with("+json")
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GreenticBizStoreTarget {
    tenant: String,
    mapped_reference: String,
}

fn is_greentic_biz_store_target(target: &str) -> bool {
    target == "greentic-biz" || target.starts_with("greentic-biz/")
}

fn parse_greentic_biz_store_target(target: &str) -> Result<GreenticBizStoreTarget, DistError> {
    let remainder = target.strip_prefix("greentic-biz/").ok_or_else(|| {
        DistError::InvalidInput(
            "store://greentic-biz refs must include `<tenant>/<package-path>`".into(),
        )
    })?;
    let (tenant, package_path) = remainder.split_once('/').ok_or_else(|| {
        DistError::InvalidInput(
            "store://greentic-biz refs must include `<tenant>/<package-path>`".into(),
        )
    })?;
    let tenant = tenant.trim();
    let package_path = package_path.trim_matches('/');
    if tenant.is_empty() || package_path.is_empty() {
        return Err(DistError::InvalidInput(
            "store://greentic-biz refs must include non-empty `<tenant>/<package-path>`".into(),
        ));
    }
    Ok(GreenticBizStoreTarget {
        tenant: tenant.to_string(),
        mapped_reference: format!("ghcr.io/greentic-biz/{package_path}"),
    })
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
    use oci_distribution::manifest::{OciDescriptor, OciImageManifest};

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

    #[test]
    fn retries_store_resolution_as_pack_for_non_component_layers() {
        assert!(should_retry_store_as_pack(&OciComponentError::PullFailed {
            reference: "ghcr.io/greentic-biz/bundles/zain-x-bundle:latest".to_string(),
            source: oci_distribution::errors::OciDistributionError::GenericError(Some(
                "Incompatible layer media type: application/vnd.greentic.zain-x.bundle.v1+tar+gzip"
                    .to_string(),
            )),
        }));
    }

    #[test]
    fn canonical_oci_ref_preserves_registry_host_for_tagged_refs() {
        assert_eq!(
            canonical_oci_ref(
                "oci://ghcr.io/greenticai/components/templates:latest",
                "sha256:abc",
            ),
            "oci://ghcr.io/greenticai/components/templates@sha256:abc"
        );
    }

    #[test]
    fn store_download_treats_vendor_json_suffix_as_json() {
        assert!(media_type_is_json(
            "application/vnd.greentic.zain-x.catalog.root.v1+json"
        ));
    }

    #[tokio::test]
    async fn store_download_preserves_source_and_accepts_vendor_json_layer() {
        #[derive(Clone)]
        struct FakeClient {
            manifest: OciManifest,
            image: PulledImage,
        }

        #[async_trait]
        impl StoreDownloadRegistryClient for FakeClient {
            async fn pull_manifest(
                &self,
                _reference: &Reference,
                _auth: &StoreRegistryAuth,
            ) -> Result<OciManifest, OciDistributionError> {
                Ok(self.manifest.clone())
            }

            async fn pull(
                &self,
                _reference: &Reference,
                _auth: &StoreRegistryAuth,
                accepted_media_types: &[String],
            ) -> Result<PulledImage, OciDistributionError> {
                assert!(accepted_media_types.iter().any(|media_type| {
                    media_type == "application/vnd.greentic.zain-x.catalog.root.v1+json"
                }));
                Ok(self.image.clone())
            }
        }

        let media_type = "application/vnd.greentic.zain-x.catalog.root.v1+json".to_string();
        let payload = br#"{"kind":"catalog-root"}"#.to_vec();
        let digest = compute_bytes_digest(&payload);
        let client = FakeClient {
            manifest: OciManifest::Image(OciImageManifest {
                schema_version: 2,
                media_type: Some("application/vnd.oci.artifact.manifest.v1+json".to_string()),
                config: OciDescriptor {
                    media_type: "application/vnd.unknown.config.v1+json".to_string(),
                    digest: digest.clone(),
                    size: 2,
                    annotations: None,
                    urls: None,
                },
                layers: vec![OciDescriptor {
                    media_type: media_type.clone(),
                    digest: digest.clone(),
                    size: payload.len() as i64,
                    annotations: None,
                    urls: None,
                }],
                artifact_type: Some(media_type.clone()),
                annotations: None,
            }),
            image: PulledImage {
                digest: Some(digest.clone()),
                layers: vec![PulledLayer {
                    media_type: media_type.clone(),
                    data: payload.clone(),
                    digest: Some(digest.clone()),
                }],
            },
        };

        let downloaded = download_store_artifact_with_client(
            &client,
            "store://greentic-biz/3point/catalogs/zain-x:latest",
            "ghcr.io/greentic-biz/catalogs/zain-x:latest",
            &StoreRegistryAuth::Anonymous,
        )
        .await
        .unwrap();

        assert_eq!(
            downloaded.canonical_ref,
            format!("oci://ghcr.io/greentic-biz/catalogs/zain-x@{digest}")
        );
        assert_eq!(downloaded.media_type, media_type);
        assert_eq!(downloaded.bytes, payload);
    }
}
