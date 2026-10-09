#![cfg(feature = "oci-components")]

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use greentic_distributor_client::oci_components::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentError,
    OciComponentResolver, PulledImage, PulledLayer, RegistryClient, ResolvedComponent,
    ResolvedComponentDescriptor,
};
use oci_client::Reference;
use oci_client::errors::OciDistributionError;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct MockRegistryClient {
    pulls: Arc<AtomicUsize>,
    /// Cheap tag->digest lookups, counted separately from blob pulls: a
    /// manifest query is what `digest_for` is allowed to cost.
    digest_queries: Arc<AtomicUsize>,
    images: Arc<Mutex<HashMap<String, PulledImage>>>,
}

impl MockRegistryClient {
    fn with_image(reference: &str, image: PulledImage) -> Self {
        let client = Self::default();
        client
            .images
            .lock()
            .unwrap()
            .insert(reference.to_string(), image);
        client
    }

    fn pulls(&self) -> usize {
        self.pulls.load(Ordering::SeqCst)
    }

    fn digest_queries(&self) -> usize {
        self.digest_queries.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl RegistryClient for MockRegistryClient {
    fn default_client() -> Self {
        Self::default()
    }

    async fn digest_for(
        &self,
        reference: &Reference,
        _accepted_manifest_types: &[&str],
    ) -> Result<Option<String>, OciDistributionError> {
        self.digest_queries.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .images
            .lock()
            .unwrap()
            .get(&reference.whole())
            .and_then(|image| image.digest.clone()))
    }

    async fn pull(
        &self,
        reference: &Reference,
        accepted_manifest_types: &[&str],
    ) -> Result<PulledImage, OciDistributionError> {
        self.pulls.fetch_add(1, Ordering::SeqCst);
        let key = reference.whole();
        let image = self
            .images
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| OciDistributionError::GenericError(Some("not found".into())))?;
        if image
            .layers
            .iter()
            .all(|layer| !accepted_manifest_types.contains(&layer.media_type.as_str()))
        {
            return Err(OciDistributionError::GenericError(Some(format!(
                "Incompatible layer media type: {}",
                image
                    .layers
                    .first()
                    .map(|layer| layer.media_type.as_str())
                    .unwrap_or("<none>")
            ))));
        }
        Ok(image)
    }
}

fn options(temp: &TempDir) -> ComponentResolveOptions {
    ComponentResolveOptions {
        cache_dir: temp.path().to_path_buf(),
        ..ComponentResolveOptions::default()
    }
}

fn digest_for(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let mut rendered = String::with_capacity("sha256:".len() + digest.len() * 2);
    rendered.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut rendered, "{byte:02x}");
    }
    rendered
}

fn extension(refs: Vec<&str>) -> ComponentsExtension {
    ComponentsExtension {
        refs: refs.into_iter().map(|s| s.to_string()).collect(),
        mode: ComponentsMode::Eager,
    }
}

fn pulled_image(data: &[u8], media_type: &str, digest: &str) -> PulledImage {
    PulledImage {
        digest: Some(digest.to_string()),
        layers: vec![PulledLayer {
            media_type: media_type.to_string(),
            data: data.to_vec(),
            digest: Some(digest.to_string()),
        }],
        ..Default::default()
    }
}

fn pulled_image_with_layers(layers: Vec<PulledLayer>) -> PulledImage {
    PulledImage {
        digest: None,
        layers,
        ..Default::default()
    }
}

#[tokio::test]
async fn resolves_digest_pinned_and_caches() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"wasm-bytes";
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greentic/components@{digest}");

    let mock =
        MockRegistryClient::with_image(&reference, pulled_image(data, "application/wasm", &digest));
    let resolver = OciComponentResolver::with_client(mock.clone(), options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let ResolvedComponent {
        path,
        resolved_digest,
        fetched_from_network,
        manifest_digest,
        ..
    } = &results[0];
    assert_eq!(resolved_digest, &digest);
    assert!(path.exists());
    assert!(fetched_from_network);
    assert_eq!(manifest_digest.as_deref(), Some(digest.as_str()));
    assert_eq!(mock.pulls(), 1);

    // Second resolution should hit cache and avoid another pull.
    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();
    assert!(!results[0].fetched_from_network);
    assert_eq!(mock.pulls(), 1);
}

#[tokio::test]
async fn tag_refs_rejected_without_opt_in() {
    let temp = tempfile::tempdir().unwrap();
    let resolver = OciComponentResolver::with_client(MockRegistryClient::default(), options(&temp));
    let err = resolver
        .resolve_refs(&extension(vec!["ghcr.io/greentic/components:latest"]))
        .await
        .unwrap_err();
    assert!(matches!(err, OciComponentError::DigestRequired { .. }));
}

#[tokio::test]
async fn offline_mode_requires_cache() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"component bytes";
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greentic/components@{digest}");
    let mut opts = options(&temp);
    opts.offline = true;
    let resolver = OciComponentResolver::with_client(MockRegistryClient::default(), opts);

    let err = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap_err();
    assert!(matches!(err, OciComponentError::OfflineMissing { .. }));
}

/// A tag ref resolved twice must pull once: the first resolve learns the
/// digest, and the second must recognise that digest as already cached.
///
/// This is the per-render re-download seen in greentic-designer's Run Demo.
/// Measured there: ~5.5 MB pulled from ghcr.io on EVERY render, rewriting a
/// byte-identical file (md5 unchanged, mtime bumped), because
/// `resolve_refs` only probes the cache when the incoming ref already carries
/// a digest — a tag ref goes straight to `client.pull()` every time.
#[tokio::test]
async fn a_tag_ref_is_not_re_pulled_once_its_digest_is_cached() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"tagged component";
    let digest = digest_for(data);
    let reference = "ghcr.io/greentic/components:latest";

    let mut opts = options(&temp);
    opts.allow_tags = true;
    let mock =
        MockRegistryClient::with_image(reference, pulled_image(data, "application/wasm", &digest));
    let resolver = OciComponentResolver::with_client(mock.clone(), opts);

    let first = resolver
        .resolve_refs(&extension(vec![reference]))
        .await
        .unwrap();
    assert!(first[0].fetched_from_network, "first resolve pulls");
    assert_eq!(mock.pulls(), 1);

    let second = resolver
        .resolve_refs(&extension(vec![reference]))
        .await
        .unwrap();
    assert!(
        !second[0].fetched_from_network,
        "the second resolve of the same tag must be served from cache"
    );
    assert_eq!(
        mock.pulls(),
        1,
        "a tag whose digest is already cached must not be pulled again"
    );
    assert_eq!(
        mock.digest_queries(),
        2,
        "each resolve asks for the digest; only the blob fetch is skipped"
    );
}

#[tokio::test]
async fn allows_tag_refs_when_opted_in() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"tagged component";
    let digest = digest_for(data);
    let reference = "ghcr.io/greentic/components:latest";

    let mut opts = options(&temp);
    opts.allow_tags = true;
    let mock =
        MockRegistryClient::with_image(reference, pulled_image(data, "application/wasm", &digest));
    let resolver = OciComponentResolver::with_client(mock.clone(), opts);

    let results = resolver
        .resolve_refs(&extension(vec![reference]))
        .await
        .unwrap();
    assert_eq!(results[0].resolved_digest, digest);
    assert!(results[0].path.exists());
    assert_eq!(mock.pulls(), 1);
}

#[tokio::test]
async fn accepts_legacy_greentic_wasm_component_media_type() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"legacy greentic component";
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greentic/components@{digest}");

    let mock = MockRegistryClient::with_image(
        &reference,
        pulled_image(data, "application/vnd.greentic.wasm.component", &digest),
    );
    let resolver = OciComponentResolver::with_client(mock.clone(), options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(results[0].resolved_digest, digest);
    assert_eq!(
        results[0].media_type,
        "application/vnd.greentic.wasm.component"
    );
    assert!(results[0].path.exists());
    assert_eq!(mock.pulls(), 1);
}

/// The descriptor path has the same hole as `resolve_refs` did: it can read
/// the cache, but only when handed a digest. Asked for a TAG it pulls the whole
/// component every time — even when a previous resolve already materialised
/// exactly those bytes and all the caller wants is the metadata.
#[tokio::test]
async fn a_tag_ref_descriptor_is_served_from_cache_once_materialised() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"tagged component";
    let digest = digest_for(data);
    let reference = "ghcr.io/greentic/components:latest";

    let mut opts = options(&temp);
    opts.allow_tags = true;
    let mock =
        MockRegistryClient::with_image(reference, pulled_image(data, "application/wasm", &digest));
    let resolver = OciComponentResolver::with_client(mock.clone(), opts);

    // Materialise the blob, as a real caller does before asking about it.
    resolver
        .resolve_refs(&extension(vec![reference]))
        .await
        .unwrap();
    assert_eq!(mock.pulls(), 1);

    let descriptor = resolver.resolve_descriptor(reference).await.unwrap();
    assert_eq!(descriptor.resolved_digest, digest);
    assert!(
        !descriptor.fetched_from_network,
        "the descriptor of an already-cached tag must not be re-fetched"
    );
    assert_eq!(
        mock.pulls(),
        1,
        "asking for metadata must not re-pull the component"
    );
}

#[tokio::test]
async fn resolve_descriptor_does_not_require_cache_materialization() {
    let temp = tempfile::tempdir().unwrap();
    let data = b"descriptor-only";
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greentic/components@{digest}");

    let mock =
        MockRegistryClient::with_image(&reference, pulled_image(data, "application/wasm", &digest));
    let resolver = OciComponentResolver::with_client(mock.clone(), options(&temp));

    let ResolvedComponentDescriptor {
        resolved_digest,
        media_type,
        size_bytes,
        fetched_from_network,
        ..
    } = resolver.resolve_descriptor(&reference).await.unwrap();

    assert_eq!(resolved_digest, digest);
    assert_eq!(media_type, "application/wasm");
    assert_eq!(size_bytes, data.len() as u64);
    assert!(fetched_from_network);
    assert_eq!(mock.pulls(), 1);
    assert!(
        !temp
            .path()
            .join(digest.trim_start_matches("sha256:"))
            .exists()
    );
}

#[tokio::test]
async fn caches_manifest_and_wasm_with_expected_filenames() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_bytes = br#"{"name":"demo"}"#;
    let manifest_digest = digest_for(manifest_bytes);
    let manifest_ref = format!("ghcr.io/greentic/components@{manifest_digest}");
    let wasm_bytes = b"wasm-bytes";
    let wasm_digest = digest_for(wasm_bytes);
    let wasm_ref = format!("ghcr.io/greentic/components@{wasm_digest}");

    let mock = MockRegistryClient::with_image(
        &manifest_ref,
        pulled_image(
            manifest_bytes,
            "application/vnd.greentic.component.manifest+json",
            &manifest_digest,
        ),
    );
    mock.images.lock().unwrap().insert(
        wasm_ref.clone(),
        pulled_image(wasm_bytes, "application/wasm", &wasm_digest),
    );
    let resolver = OciComponentResolver::with_client(mock, options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&manifest_ref, &wasm_ref]))
        .await
        .unwrap();
    assert_eq!(
        results[0].path.file_name().and_then(|s| s.to_str()),
        Some("component.manifest.json")
    );
    assert_eq!(
        results[1].path.file_name().and_then(|s| s.to_str()),
        Some("component.wasm")
    );
}

#[tokio::test]
async fn prefers_wasm_layer_and_writes_wasm_path() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_bytes = br#"{"name":"demo"}"#;
    let manifest_digest = digest_for(manifest_bytes);
    let wasm_bytes = b"wasm-bytes";
    let wasm_digest = digest_for(wasm_bytes);
    let reference = format!("ghcr.io/greentic/components@{wasm_digest}");

    let image = pulled_image_with_layers(vec![
        PulledLayer {
            media_type: "application/vnd.greentic.component.manifest+json".to_string(),
            data: manifest_bytes.to_vec(),
            digest: Some(manifest_digest),
        },
        PulledLayer {
            media_type: "application/wasm".to_string(),
            data: wasm_bytes.to_vec(),
            digest: Some(wasm_digest.clone()),
        },
    ]);

    let mock = MockRegistryClient::with_image(&reference, image);
    let resolver = OciComponentResolver::with_client(mock, options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(results[0].media_type, "application/wasm");
    assert_eq!(
        results[0].path.file_name().and_then(|s| s.to_str()),
        Some("component.wasm")
    );
}

#[tokio::test]
async fn caches_manifest_layer_alongside_wasm() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_bytes = br#"{"name":"demo"}"#;
    let manifest_digest = digest_for(manifest_bytes);
    let wasm_bytes = b"wasm-bytes";
    let wasm_digest = digest_for(wasm_bytes);
    let reference = format!("ghcr.io/greentic/components@{wasm_digest}");

    let image = pulled_image_with_layers(vec![
        PulledLayer {
            media_type: "application/vnd.greentic.component.manifest+json".to_string(),
            data: manifest_bytes.to_vec(),
            digest: Some(manifest_digest),
        },
        PulledLayer {
            media_type: "application/wasm".to_string(),
            data: wasm_bytes.to_vec(),
            digest: Some(wasm_digest.clone()),
        },
    ]);

    let mock = MockRegistryClient::with_image(&reference, image);
    let resolver = OciComponentResolver::with_client(mock, options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(results[0].media_type, "application/wasm");
    let cache_dir = results[0].path.parent().unwrap();
    assert!(cache_dir.join("component.wasm").exists());
    assert!(cache_dir.join("component.manifest.json").exists());
}

#[tokio::test]
async fn caches_manifest_named_wasm_file() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_bytes = br#"{"artifacts":{"component_wasm":"component_templates.wasm"}}"#;
    let manifest_digest = digest_for(manifest_bytes);
    let wasm_bytes = b"wasm-bytes";
    let wasm_digest = digest_for(wasm_bytes);
    let reference = format!("ghcr.io/greentic/components@{wasm_digest}");

    let image = pulled_image_with_layers(vec![
        PulledLayer {
            media_type: "application/vnd.greentic.component.manifest+json".to_string(),
            data: manifest_bytes.to_vec(),
            digest: Some(manifest_digest),
        },
        PulledLayer {
            media_type: "application/wasm".to_string(),
            data: wasm_bytes.to_vec(),
            digest: Some(wasm_digest.clone()),
        },
    ]);

    let mock = MockRegistryClient::with_image(&reference, image);
    let resolver = OciComponentResolver::with_client(mock, options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(results[0].media_type, "application/wasm");
    let cache_dir = results[0].path.parent().unwrap();
    let manifest_path = cache_dir.join("component.manifest.json");
    let named_wasm_path = cache_dir.join("component_templates.wasm");
    let legacy_wasm_path = cache_dir.join("component.wasm");

    assert!(manifest_path.exists());
    assert!(named_wasm_path.exists());
    assert!(legacy_wasm_path.exists());

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let manifest_name = manifest
        .get("artifacts")
        .and_then(|v| v.get("component_wasm"))
        .and_then(|v| v.as_str())
        .unwrap();
    assert!(cache_dir.join(manifest_name).exists());
}

#[tokio::test]
async fn writes_manifest_path_when_only_manifest_layer_present() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_bytes = br#"{"name":"demo"}"#;
    let manifest_digest = digest_for(manifest_bytes);
    let reference = format!("ghcr.io/greentic/components@{manifest_digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "application/vnd.greentic.component.manifest+json".to_string(),
        data: manifest_bytes.to_vec(),
        digest: Some(manifest_digest.clone()),
    }]);

    let mock = MockRegistryClient::with_image(&reference, image);
    let resolver = OciComponentResolver::with_client(mock, options(&temp));

    let results = resolver
        .resolve_refs(&extension(vec![&reference]))
        .await
        .unwrap();

    assert_eq!(
        results[0].path.file_name().and_then(|s| s.to_str()),
        Some("component.manifest.json")
    );
}

#[tokio::test]
async fn invalid_reference_surfaces_error() {
    let temp = tempfile::tempdir().unwrap();
    let resolver = OciComponentResolver::with_client(MockRegistryClient::default(), options(&temp));
    let err = resolver
        .resolve_refs(&extension(vec!["not a ref"]))
        .await
        .unwrap_err();
    assert!(matches!(err, OciComponentError::InvalidReference { .. }));
}
