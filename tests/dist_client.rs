#![cfg(feature = "dist-client")]

use async_trait::async_trait;
use greentic_distributor_client::dist::{
    DistClient, DistOptions, InjectedResolution, ResolveRefInjector,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

fn digest_for(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn options(dir: &TempDir) -> DistOptions {
    DistOptions {
        cache_dir: dir.path().to_path_buf(),
        allow_tags: true,
        offline: false,
        allow_insecure_local_http: true,
        cache_max_bytes: 3 * 1024 * 1024 * 1024,
        repo_registry_base: None,
        store_registry_base: None,
        #[cfg(feature = "fixture-resolver")]
        fixture_dir: None,
    }
}

#[tokio::test]
async fn caches_file_path_and_computes_digest() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"hello-component").unwrap();
    let expected = digest_for(b"hello-component");

    let client = DistClient::new(options(&temp));
    let resolved = client
        .ensure_cached(file_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(resolved.resolved_digest, expected);
    assert_eq!(resolved.digest, expected);
    assert_eq!(resolved.component_id, "component");
    assert_eq!(resolved.abi_version, None);
    assert!(resolved.wasm_bytes.is_none());
    assert!(resolved.wasm_path.is_some());
    assert_eq!(
        resolved.content_length,
        Some(b"hello-component".len() as u64)
    );
    assert_eq!(resolved.content_type.as_deref(), Some("application/wasm"));
    assert!(resolved.describe_artifact_ref.is_none());
    let cached = resolved.cache_path.unwrap();
    assert!(cached.exists());
    assert_eq!(fs::read(cached).unwrap(), b"hello-component");
}

#[tokio::test]
async fn caches_http_download() {
    let server = match std::panic::catch_unwind(httpmock::MockServer::start) {
        Ok(s) => s,
        Err(_) => {
            eprintln!(
                "skipping http download test: unable to bind mock server in this environment"
            );
            return;
        }
    };
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/component.wasm");
        then.status(200)
            .header("content-type", "application/vnd.custom.wasm")
            .body("from-http");
    });

    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));
    let url = format!("{}/component.wasm", server.base_url());
    let resolved = client.ensure_cached(&url).await.unwrap();

    let expected = digest_for(b"from-http");
    assert_eq!(resolved.resolved_digest, expected);
    assert_eq!(resolved.digest, expected);
    assert_eq!(resolved.component_id, "component");
    assert!(resolved.wasm_path.is_some());
    assert!(resolved.wasm_bytes.is_none());
    assert_eq!(resolved.content_length, Some(b"from-http".len() as u64));
    assert_eq!(
        resolved.content_type.as_deref(),
        Some("application/vnd.custom.wasm")
    );
    let cached = resolved.cache_path.unwrap();
    assert_eq!(fs::read(cached).unwrap(), b"from-http");
    mock.assert_async().await;
}

#[tokio::test]
async fn pulls_lockfile_entries() {
    let temp = tempfile::tempdir().unwrap();
    let file1 = temp.path().join("one.wasm");
    let file2 = temp.path().join("two.wasm");
    fs::write(&file1, b"one").unwrap();
    fs::write(&file2, b"two").unwrap();

    let lock_path = temp.path().join("pack.lock");
    let lock_contents = serde_json::json!({
        "components": [
            file1.to_str().unwrap(),
            { "reference": file2.to_str().unwrap() }
        ]
    });
    fs::write(&lock_path, serde_json::to_vec(&lock_contents).unwrap()).unwrap();

    let client = DistClient::new(options(&temp));
    let resolved = client.pull_lock(&lock_path).await.unwrap();

    assert_eq!(resolved.len(), 2);
    for item in resolved {
        assert!(item.cache_path.unwrap().exists());
    }
}

#[tokio::test]
async fn respects_canonical_lockfile_with_schema_version() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("hello.wasm");
    fs::write(&file, b"hello").unwrap();
    let digest = digest_for(b"hello");
    let lock_path = temp.path().join("pack.lock.json");
    let lock_contents = serde_json::json!({
        "schema_version": 1,
        "components": [
            {
                "name": "hello",
                "ref": file.to_str().unwrap(),
                "digest": digest
            }
        ]
    });
    fs::write(&lock_path, serde_json::to_vec(&lock_contents).unwrap()).unwrap();

    let client = DistClient::new(options(&temp));
    let resolved = client.pull_lock(&lock_path).await.unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].digest, digest);
    assert!(resolved[0].cache_path.as_ref().unwrap().exists());
}

#[tokio::test]
async fn offline_mode_blocks_http_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let mut opts = options(&temp);
    opts.offline = true;
    let client = DistClient::new(opts);
    let err = client
        .resolve_ref("http://example.com/component.wasm")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("offline"), "unexpected error: {msg}");
}

#[tokio::test]
async fn rejects_insecure_http_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));
    let err = client
        .resolve_ref("http://example.com/component.wasm")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("insecure url"), "unexpected error: {msg}");
}

#[tokio::test]
async fn wasm_bytes_helper_loads_from_wasm_path() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("foo__0_6_0.wasm");
    fs::write(&file_path, b"hello-component").unwrap();

    let client = DistClient::new(options(&temp));
    let resolved = client
        .ensure_cached(file_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(resolved.component_id, "foo");
    let loaded = resolved.wasm_bytes().unwrap();
    assert_eq!(loaded, b"hello-component");
}

#[tokio::test]
async fn file_resolution_exposes_describe_sidecar_ref_when_available() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("foo.wasm");
    let describe_path = temp.path().join("describe.cbor");
    fs::write(&file_path, b"hello-component").unwrap();
    fs::write(&describe_path, b"describe-cbor").unwrap();

    let client = DistClient::new(options(&temp));
    let resolved = client
        .ensure_cached(file_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        resolved.describe_artifact_ref.as_deref(),
        Some(describe_path.to_str().unwrap())
    );
}

#[tokio::test]
async fn evicts_oldest_entries_when_cache_cap_exceeded() {
    let temp = tempfile::tempdir().unwrap();
    let first_path = temp.path().join("one.wasm");
    let second_path = temp.path().join("two.wasm");
    fs::write(&first_path, b"11111111").unwrap();
    fs::write(&second_path, b"22222222").unwrap();

    let mut opts = options(&temp);
    opts.cache_max_bytes = 8;
    let client = DistClient::new(opts);

    let first = client
        .ensure_cached(first_path.to_str().unwrap())
        .await
        .unwrap();
    let second = client
        .ensure_cached(second_path.to_str().unwrap())
        .await
        .unwrap();

    let listed = client.list_cache();
    assert_eq!(listed, vec![second.resolved_digest.clone()]);
    assert!(!first.cache_path.unwrap().exists());
    assert!(second.cache_path.unwrap().exists());
}

#[tokio::test]
async fn repo_and_store_refs_use_registry_mapping_before_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let mut opts = options(&temp);
    opts.offline = true;
    opts.repo_registry_base = Some("ghcr.io/greentic/repo".into());
    opts.store_registry_base = Some("ghcr.io/greentic/store".into());
    let client = DistClient::new(opts);

    let repo_err = client.resolve_ref("repo://component-a").await.unwrap_err();
    assert!(
        format!("{repo_err}").contains("offline"),
        "unexpected error: {repo_err}"
    );

    let store_err = client.resolve_ref("store://component-b").await.unwrap_err();
    assert!(
        format!("{store_err}").contains("offline"),
        "unexpected error: {store_err}"
    );
}

struct RedirectInjector {
    target: String,
}

#[async_trait]
impl ResolveRefInjector for RedirectInjector {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<Option<InjectedResolution>, greentic_distributor_client::dist::DistError> {
        if reference == "repo://redirect-me" {
            Ok(Some(InjectedResolution::Redirect(self.target.clone())))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn resolve_ref_injector_can_redirect_resolution() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("redirected.wasm");
    fs::write(&file_path, b"redirected").unwrap();
    let target = file_path.to_str().unwrap().to_string();
    let injector = Arc::new(RedirectInjector { target });

    let client = DistClient::with_ref_injector(options(&temp), injector);
    let resolved = client.resolve_ref("repo://redirect-me").await.unwrap();
    assert_eq!(resolved.component_id, "redirected");
}

#[tokio::test]
async fn lock_hint_contains_expected_fields() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"lock-hint").unwrap();
    let client = DistClient::new(options(&temp));

    let resolved = client
        .ensure_cached(file_path.to_str().unwrap())
        .await
        .unwrap();
    let source_ref = file_path.to_string_lossy().to_string();
    let hint = resolved.lock_hint(source_ref.clone());

    assert_eq!(hint.source_ref, source_ref);
    assert_eq!(hint.resolved_digest, resolved.resolved_digest);
    assert_eq!(hint.content_length, Some(b"lock-hint".len() as u64));
    assert_eq!(hint.content_type.as_deref(), Some("application/wasm"));
    assert_eq!(hint.abi_version, None);
    assert_eq!(hint.component_id, resolved.component_id);
}

#[cfg(feature = "fixture-resolver")]
#[tokio::test]
async fn fixture_refs_resolve_from_fixture_dir() {
    let temp = tempfile::tempdir().unwrap();
    let fixture_dir = temp.path().join("fixtures");
    fs::create_dir_all(&fixture_dir).unwrap();
    fs::write(fixture_dir.join("fixture-a.wasm"), b"fixture-bytes").unwrap();
    fs::write(fixture_dir.join("describe.cbor"), b"fixture-describe").unwrap();

    let mut opts = options(&temp);
    opts.fixture_dir = Some(fixture_dir.clone());
    let client = DistClient::new(opts);

    let resolved = client.resolve_ref("fixture://fixture-a").await.unwrap();
    assert_eq!(resolved.component_id, "fixture-a");
    assert_eq!(resolved.wasm_bytes().unwrap(), b"fixture-bytes");
    assert_eq!(
        resolved.describe_artifact_ref.as_deref(),
        Some(fixture_dir.join("describe.cbor").to_str().unwrap())
    );
}
