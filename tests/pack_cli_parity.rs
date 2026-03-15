#![cfg(all(feature = "dist-cli", feature = "pack-fetch"))]

use std::collections::HashMap;
use std::io::Read;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use greentic_distributor_client::dist_cli::fetch_pack_for_cli;
use greentic_distributor_client::oci_packs::{
    OciPackFetcher, PackFetchOptions, PulledImage, PulledLayer, RegistryClient,
};
use oci_distribution::Reference;
use oci_distribution::errors::OciDistributionError;
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, Header};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct MockRegistryClient {
    pulls: Arc<AtomicUsize>,
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
}

#[async_trait::async_trait]
impl RegistryClient for MockRegistryClient {
    fn default_client() -> Self {
        Self::default()
    }

    async fn pull(
        &self,
        reference: &Reference,
        _accepted_manifest_types: &[&str],
    ) -> Result<PulledImage, OciDistributionError> {
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

fn options(temp: &TempDir) -> PackFetchOptions {
    PackFetchOptions {
        cache_dir: temp.path().to_path_buf(),
        ..PackFetchOptions::default()
    }
}

fn digest_for(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("sha256:{:x}", hasher.finalize())
}

fn pulled_image(data: &[u8], media_type: &str, digest: &str) -> PulledImage {
    PulledImage {
        digest: Some(digest.to_string()),
        layers: vec![PulledLayer {
            media_type: media_type.to_string(),
            data: data.to_vec(),
            digest: Some(digest.to_string()),
        }],
    }
}

fn tar_gzip_bundle(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);
    for (path, bytes) in entries {
        let mut header = Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_mode(0o644);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        builder.append(&header, *bytes).unwrap();
    }
    let encoder = builder.into_inner().unwrap();
    encoder.finish().unwrap()
}

fn read_tar_gzip_entry(bytes: &[u8], path: &str) -> Vec<u8> {
    let decoder = GzDecoder::new(bytes);
    let mut archive = Archive::new(decoder);
    let mut found = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let entry_path = entry.path().unwrap().to_string_lossy().into_owned();
        if entry_path == path {
            entry.read_to_end(&mut found).unwrap();
            return found;
        }
    }
    panic!("missing tar entry {path}");
}

#[tokio::test]
async fn cli_and_api_pack_fetch_match() {
    let data = b"zip-pack-bytes";
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image(data, "application/vnd.greentic.gtpack.v1+zip", &digest);
    let mock = MockRegistryClient::with_image(&reference, image);

    let temp_direct = tempfile::tempdir().unwrap();
    let fetcher = OciPackFetcher::with_client(mock.clone(), options(&temp_direct));
    let direct = fetcher.fetch_pack_to_cache(&reference).await.unwrap();

    let temp_cli = tempfile::tempdir().unwrap();
    let cli_resolved = fetch_pack_for_cli(
        &reference,
        false,
        Some(temp_cli.path().to_path_buf()),
        false,
        mock,
    )
    .await
    .unwrap();

    assert_eq!(cli_resolved.resolved_digest, direct.resolved_digest);
    assert_eq!(cli_resolved.media_type, direct.media_type);
    assert_eq!(
        cli_resolved.fetched_from_network,
        direct.fetched_from_network
    );

    let direct_bytes = std::fs::read(&direct.path).unwrap();
    let cli_bytes = std::fs::read(&cli_resolved.path).unwrap();
    assert_eq!(cli_bytes, direct_bytes);
}

#[tokio::test]
async fn cli_fetches_targzip_bundle_and_downstream_can_open_it() {
    let bundle_bytes = tar_gzip_bundle(&[
        ("bundle/manifest.json", br#"{"name":"zain-x"}"#),
        ("bundle/data.txt", b"hello-zain-x"),
    ]);
    let digest = digest_for(&bundle_bytes);
    let reference = "ghcr.io/greentic-biz/zain-x-bundle:latest";

    let image = pulled_image(
        &bundle_bytes,
        "application/vnd.greentic.zain-x.bundle.v1+tar+gzip",
        &digest,
    );
    let mock = MockRegistryClient::with_image(reference, image);

    let temp_cli = tempfile::tempdir().unwrap();
    let cli_resolved = fetch_pack_for_cli(
        reference,
        true,
        Some(temp_cli.path().to_path_buf()),
        false,
        mock,
    )
    .await
    .unwrap();

    let cli_bytes = std::fs::read(&cli_resolved.path).unwrap();
    assert_eq!(
        cli_resolved.media_type,
        "application/vnd.greentic.zain-x.bundle.v1+tar+gzip"
    );
    assert_eq!(
        read_tar_gzip_entry(&cli_bytes, "bundle/manifest.json"),
        br#"{"name":"zain-x"}"#
    );
    assert_eq!(
        read_tar_gzip_entry(&cli_bytes, "bundle/data.txt"),
        b"hello-zain-x"
    );
}
