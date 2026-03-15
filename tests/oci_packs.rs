#![cfg(feature = "pack-fetch")]

use std::collections::HashMap;
use std::io::Read;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use greentic_distributor_client::oci_packs::{
    OciPackError, OciPackFetcher, PackFetchOptions, PulledImage, PulledLayer, RegistryClient,
    default_pack_layer_media_types, fetch_pack_to_cache_with_options_and_client,
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

    fn pulls(&self) -> usize {
        self.pulls.load(Ordering::SeqCst)
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

fn pulled_image_with_layers(layers: Vec<PulledLayer>) -> PulledImage {
    PulledImage {
        digest: None,
        layers,
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
async fn resolves_digest_pinned_and_caches() {
    let temp = tempfile::tempdir().unwrap();
    let data = br#"{"pack":"bytes"}"#;
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let mock = MockRegistryClient::with_image(
        &reference,
        pulled_image(data, "application/octet-stream", &digest),
    );
    let fetcher = OciPackFetcher::with_client(mock.clone(), options(&temp));

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();

    assert_eq!(resolved.resolved_digest, digest);
    assert_eq!(
        resolved.path.file_name().and_then(|s| s.to_str()),
        Some("pack.gtpack")
    );
    assert!(resolved.fetched_from_network);
    assert!(resolved.path.exists());
    assert_eq!(mock.pulls(), 1);

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    assert!(!resolved.fetched_from_network);
    assert_eq!(mock.pulls(), 1);
}

#[tokio::test]
async fn tag_refs_rejected_without_opt_in() {
    let temp = tempfile::tempdir().unwrap();
    let fetcher = OciPackFetcher::with_client(MockRegistryClient::default(), options(&temp));
    let err = fetcher
        .fetch_pack_to_cache("ghcr.io/greenticai/greentic-packs/foo:latest")
        .await
        .unwrap_err();
    assert!(matches!(err, OciPackError::TagDisallowed { .. }));
}

#[tokio::test]
async fn offline_mode_requires_cache() {
    let temp = tempfile::tempdir().unwrap();
    let data = br#"{"pack":"bytes"}"#;
    let digest = digest_for(data);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");
    let mut opts = options(&temp);
    opts.offline = true;
    let fetcher = OciPackFetcher::with_client(MockRegistryClient::default(), opts);

    let err = fetcher.fetch_pack_to_cache(&reference).await.unwrap_err();
    assert!(matches!(err, OciPackError::OfflineMissing { .. }));
}

#[tokio::test]
async fn allows_tag_refs_when_opted_in() {
    let temp = tempfile::tempdir().unwrap();
    let data = br#"{"pack":"bytes"}"#;
    let digest = digest_for(data);
    let reference = "ghcr.io/greenticai/greentic-packs/foo:latest";

    let mut opts = options(&temp);
    opts.allow_tags = true;
    let mock = MockRegistryClient::with_image(
        reference,
        pulled_image(data, "application/octet-stream", &digest),
    );
    let fetcher = OciPackFetcher::with_client(mock.clone(), opts);

    let resolved = fetcher.fetch_pack_to_cache(reference).await.unwrap();
    assert_eq!(resolved.resolved_digest, digest);
    assert!(resolved.path.exists());
    assert_eq!(mock.pulls(), 1);
}

#[tokio::test]
async fn prefers_pack_media_type_when_present() {
    let temp = tempfile::tempdir().unwrap();
    let first_bytes = br#"{"pack":"fallback"}"#;
    let preferred_bytes = br#"{"pack":"preferred"}"#;
    let preferred_digest = digest_for(preferred_bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{preferred_digest}");

    let image = pulled_image_with_layers(vec![
        PulledLayer {
            media_type: "application/octet-stream".to_string(),
            data: first_bytes.to_vec(),
            digest: Some(digest_for(first_bytes)),
        },
        PulledLayer {
            media_type: "application/vnd.greentic.pack+json".to_string(),
            data: preferred_bytes.to_vec(),
            digest: Some(preferred_digest.clone()),
        },
    ]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let fetcher = OciPackFetcher::with_client(mock, options(&temp));

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, preferred_bytes);
}

#[tokio::test]
async fn accepts_zip_pack_media_type() {
    let temp = tempfile::tempdir().unwrap();
    let zip_bytes = b"zip-pack-bytes";
    let digest = digest_for(zip_bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "application/vnd.greentic.gtpack.v1+zip".to_string(),
        data: zip_bytes.to_vec(),
        digest: Some(digest.clone()),
    }]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let fetcher = OciPackFetcher::with_client(mock, options(&temp));

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, zip_bytes);
}

#[tokio::test]
async fn accepts_markdown_pack_media_type() {
    let temp = tempfile::tempdir().unwrap();
    let md_bytes = b"# pack\n";
    let digest = digest_for(md_bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "text/markdown".to_string(),
        data: md_bytes.to_vec(),
        digest: Some(digest.clone()),
    }]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let fetcher = OciPackFetcher::with_client(mock, options(&temp));

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, md_bytes);
}

#[tokio::test]
async fn accepts_additional_pack_media_types() {
    let temp = tempfile::tempdir().unwrap();
    let cases = [
        ("application/octet-stream", b"octet-pack".as_slice()),
        ("application/json", br#"{"pack":"json"}"#.as_slice()),
        (
            "application/vnd.oci.image.layer.v1.tar",
            b"tar-pack".as_slice(),
        ),
        (
            "application/vnd.oci.image.layer.v1.tar+gzip",
            b"targz-pack".as_slice(),
        ),
        (
            "application/vnd.oci.image.layer.v1.tar+zstd",
            b"tarzst-pack".as_slice(),
        ),
        (
            "application/vnd.greentic.pack+zip",
            b"packzip-pack".as_slice(),
        ),
    ];

    for (idx, (media_type, bytes)) in cases.iter().enumerate() {
        let digest = digest_for(bytes);
        let reference = format!("ghcr.io/greenticai/greentic-packs/foo{idx}@{digest}");
        let image = pulled_image_with_layers(vec![PulledLayer {
            media_type: (*media_type).to_string(),
            data: bytes.to_vec(),
            digest: Some(digest),
        }]);
        let mock = MockRegistryClient::with_image(&reference, image);
        let fetcher = OciPackFetcher::with_client(mock, options(&temp));

        let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
        let cached = std::fs::read(&resolved.path).unwrap();
        assert_eq!(cached, *bytes);
    }
}

#[tokio::test]
async fn accepts_custom_layer_media_type_override() {
    let temp = tempfile::tempdir().unwrap();
    let bytes = b"<html>pack</html>";
    let digest = digest_for(bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "text/html".to_string(),
        data: bytes.to_vec(),
        digest: Some(digest.clone()),
    }]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let mut opts = options(&temp);
    opts.accepted_layer_media_types = vec!["text/html".to_string()];
    let fetcher = OciPackFetcher::with_client(mock, opts);

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, bytes);
}

#[tokio::test]
async fn accepts_custom_tarball_media_type_when_appended() {
    let temp = tempfile::tempdir().unwrap();
    let bytes = b"zain-bundle";
    let digest = digest_for(bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "application/vnd.greentic.zain-x.bundle.v1+tar+gzip".to_string(),
        data: bytes.to_vec(),
        digest: Some(digest.clone()),
    }]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let opts = PackFetchOptions::default()
        .add_accepted_layer_media_type("application/vnd.greentic.zain-x.bundle.v1+tar+gzip");
    let fetcher = OciPackFetcher::with_client(
        mock,
        PackFetchOptions {
            cache_dir: temp.path().to_path_buf(),
            ..opts
        },
    );

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, bytes);
}

#[tokio::test]
async fn top_level_fetch_helper_accepts_custom_tarball_media_type_when_appended() {
    let temp = tempfile::tempdir().unwrap();
    let bytes = b"zain-bundle-top-level";
    let digest = digest_for(bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/top@{digest}");

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "application/vnd.greentic.zain-x.bundle.v1+tar+gzip".to_string(),
        data: bytes.to_vec(),
        digest: Some(digest.clone()),
    }]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let opts = PackFetchOptions {
        cache_dir: temp.path().to_path_buf(),
        ..PackFetchOptions::default()
            .add_accepted_layer_media_type("application/vnd.greentic.zain-x.bundle.v1+tar+gzip")
    };

    let via_helper = fetch_pack_to_cache_with_options_and_client(&reference, opts, mock)
        .await
        .unwrap();

    assert_eq!(std::fs::read(via_helper.path).unwrap(), bytes);
}

#[tokio::test]
async fn fetches_targzip_bundle_and_downstream_can_open_it() {
    let temp = tempfile::tempdir().unwrap();
    let bundle_bytes = tar_gzip_bundle(&[
        ("bundle/manifest.json", br#"{"name":"zain-x"}"#),
        ("bundle/data.txt", b"hello-zain-x"),
    ]);
    let digest = digest_for(&bundle_bytes);
    let reference = "ghcr.io/greentic-biz/zain-x-bundle:latest";

    let image = pulled_image_with_layers(vec![PulledLayer {
        media_type: "application/vnd.greentic.zain-x.bundle.v1+tar+gzip".to_string(),
        data: bundle_bytes.clone(),
        digest: Some(digest),
    }]);
    let mock = MockRegistryClient::with_image(reference, image);
    let mut opts = options(&temp);
    opts.allow_tags = true;
    let resolved = fetch_pack_to_cache_with_options_and_client(reference, opts, mock)
        .await
        .unwrap();

    let cached_bytes = std::fs::read(&resolved.path).unwrap();
    assert_eq!(
        resolved.media_type,
        "application/vnd.greentic.zain-x.bundle.v1+tar+gzip"
    );
    assert_eq!(
        read_tar_gzip_entry(&cached_bytes, "bundle/manifest.json"),
        br#"{"name":"zain-x"}"#
    );
    assert_eq!(
        read_tar_gzip_entry(&cached_bytes, "bundle/data.txt"),
        b"hello-zain-x"
    );
}

#[test]
fn default_pack_media_types_cover_current_exact_allowlist() {
    let media_types = default_pack_layer_media_types();
    assert!(media_types.contains(&"application/vnd.oci.image.layer.v1.tar".to_string()));
    assert!(media_types.contains(&"application/vnd.oci.image.layer.v1.tar+gzip".to_string()));
    assert!(media_types.contains(&"application/vnd.oci.image.layer.v1.tar+zstd".to_string()));
}

#[tokio::test]
async fn falls_back_to_first_layer_without_preferred_media_type() {
    let temp = tempfile::tempdir().unwrap();
    let first_bytes = br#"{"pack":"first"}"#;
    let second_bytes = br#"{"pack":"second"}"#;
    let digest = digest_for(first_bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/foo@{digest}");

    let image = pulled_image_with_layers(vec![
        PulledLayer {
            media_type: "application/octet-stream".to_string(),
            data: first_bytes.to_vec(),
            digest: Some(digest_for(first_bytes)),
        },
        PulledLayer {
            media_type: "application/json".to_string(),
            data: second_bytes.to_vec(),
            digest: Some(digest_for(second_bytes)),
        },
    ]);
    let mock = MockRegistryClient::with_image(&reference, image);
    let fetcher = OciPackFetcher::with_client(mock, options(&temp));

    let resolved = fetcher.fetch_pack_to_cache(&reference).await.unwrap();
    let cached = std::fs::read(&resolved.path).unwrap();
    assert_eq!(cached, first_bytes);
}

#[tokio::test]
async fn digest_mismatch_surfaces_error() {
    let temp = tempfile::tempdir().unwrap();
    let data = br#"{"pack":"bytes"}"#;
    let digest = digest_for(data);
    let reference = "ghcr.io/greenticai/greentic-packs/foo@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let mock = MockRegistryClient::with_image(
        reference,
        pulled_image(data, "application/octet-stream", &digest),
    );
    let fetcher = OciPackFetcher::with_client(mock, options(&temp));

    let err = fetcher.fetch_pack_to_cache(reference).await.unwrap_err();
    assert!(matches!(err, OciPackError::DigestMismatch { .. }));
}

#[tokio::test]
async fn invalid_reference_surfaces_error() {
    let temp = tempfile::tempdir().unwrap();
    let fetcher = OciPackFetcher::with_client(MockRegistryClient::default(), options(&temp));
    let err = fetcher.fetch_pack_to_cache("not a ref").await.unwrap_err();
    assert!(matches!(err, OciPackError::InvalidReference { .. }));
}
