#![cfg(any(feature = "oci-components", feature = "pack-fetch"))]
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "oci-components")]
use greentic_distributor_client::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentResolver,
    ResolvedComponentDescriptor,
    oci_components::{PulledImage as ComponentPulledImage, PulledLayer as ComponentPulledLayer},
};
#[cfg(feature = "pack-fetch")]
use greentic_distributor_client::{
    OciPackFetcher, PackFetchOptions, ResolvedPack,
    oci_packs::{PulledImage as PackPulledImage, PulledLayer as PackPulledLayer},
};
use oci_distribution::Reference;
use oci_distribution::errors::OciDistributionError;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[cfg(feature = "oci-components")]
#[derive(Clone, Default)]
pub struct MockComponentRegistryClient {
    pulls: Arc<AtomicUsize>,
    images: Arc<Mutex<HashMap<String, ComponentPulledImage>>>,
}

#[cfg(feature = "oci-components")]
impl MockComponentRegistryClient {
    pub fn with_image(reference: &str, image: ComponentPulledImage) -> Self {
        let client = Self::default();
        client
            .images
            .lock()
            .unwrap()
            .insert(reference.to_string(), image);
        client
    }

    pub fn pulls(&self) -> usize {
        self.pulls.load(Ordering::SeqCst)
    }
}

#[cfg(feature = "oci-components")]
#[async_trait::async_trait]
impl greentic_distributor_client::oci_components::RegistryClient for MockComponentRegistryClient {
    fn default_client() -> Self {
        Self::default()
    }

    async fn pull(
        &self,
        reference: &Reference,
        _accepted_manifest_types: &[&str],
    ) -> Result<ComponentPulledImage, OciDistributionError> {
        self.pulls.fetch_add(1, Ordering::SeqCst);
        let key = reference.whole();
        self.images
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| OciDistributionError::GenericError(Some("not found".into())))
    }
}

#[cfg(feature = "pack-fetch")]
#[derive(Clone, Default)]
pub struct MockPackRegistryClient {
    pulls: Arc<AtomicUsize>,
    images: Arc<Mutex<HashMap<String, PackPulledImage>>>,
}

#[cfg(feature = "pack-fetch")]
impl MockPackRegistryClient {
    pub fn with_image(reference: &str, image: PackPulledImage) -> Self {
        let client = Self::default();
        client
            .images
            .lock()
            .unwrap()
            .insert(reference.to_string(), image);
        client
    }

    pub fn pulls(&self) -> usize {
        self.pulls.load(Ordering::SeqCst)
    }
}

#[cfg(feature = "pack-fetch")]
#[async_trait::async_trait]
impl greentic_distributor_client::oci_packs::RegistryClient for MockPackRegistryClient {
    fn default_client() -> Self {
        Self::default()
    }

    async fn pull(
        &self,
        reference: &Reference,
        _accepted_manifest_types: &[&str],
    ) -> Result<PackPulledImage, OciDistributionError> {
        self.pulls.fetch_add(1, Ordering::SeqCst);
        let key = reference.whole();
        self.images
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| OciDistributionError::GenericError(Some("not found".into())))
    }
}

pub fn digest_for(data: &[u8]) -> String {
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

#[cfg(feature = "oci-components")]
pub fn component_options(temp: &TempDir) -> ComponentResolveOptions {
    ComponentResolveOptions {
        cache_dir: temp.path().to_path_buf(),
        ..ComponentResolveOptions::default()
    }
}

#[cfg(feature = "pack-fetch")]
pub fn pack_options(temp: &TempDir) -> PackFetchOptions {
    PackFetchOptions {
        cache_dir: temp.path().to_path_buf(),
        ..PackFetchOptions::default()
    }
}

#[cfg(feature = "oci-components")]
pub fn component_extension(reference: &str) -> ComponentsExtension {
    ComponentsExtension {
        refs: vec![reference.to_string()],
        mode: ComponentsMode::Eager,
    }
}

#[cfg(feature = "oci-components")]
pub fn component_image(data: &[u8], media_type: &str, digest: &str) -> ComponentPulledImage {
    ComponentPulledImage {
        digest: Some(digest.to_string()),
        layers: vec![ComponentPulledLayer {
            media_type: media_type.to_string(),
            data: data.to_vec(),
            digest: Some(digest.to_string()),
        }],
    }
}

#[cfg(feature = "oci-components")]
pub fn component_image_with_layers(layers: Vec<ComponentPulledLayer>) -> ComponentPulledImage {
    ComponentPulledImage {
        digest: None,
        layers,
    }
}

#[cfg(feature = "pack-fetch")]
pub fn pack_image(data: &[u8], media_type: &str, digest: &str) -> PackPulledImage {
    PackPulledImage {
        digest: Some(digest.to_string()),
        layers: vec![PackPulledLayer {
            media_type: media_type.to_string(),
            data: data.to_vec(),
            digest: Some(digest.to_string()),
        }],
    }
}

#[cfg(feature = "oci-components")]
pub async fn warm_component_descriptor_cache(
    resolver: &OciComponentResolver<MockComponentRegistryClient>,
    reference: &str,
) -> ResolvedComponentDescriptor {
    resolver.resolve_descriptor(reference).await.unwrap()
}

#[cfg(feature = "oci-components")]
pub async fn warm_component_cache(
    resolver: &OciComponentResolver<MockComponentRegistryClient>,
    reference: &str,
) {
    let results = resolver
        .resolve_refs(&component_extension(reference))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].path.exists());
}

#[cfg(feature = "pack-fetch")]
pub async fn warm_pack_cache(
    fetcher: &OciPackFetcher<MockPackRegistryClient>,
    reference: &str,
) -> ResolvedPack {
    fetcher.fetch_pack_to_cache(reference).await.unwrap()
}
