#![cfg(feature = "dist-client")]
#![allow(deprecated)]

use async_trait::async_trait;
use greentic_distributor_client::dist::{
    AccessMode, AdvisorySet, ArtifactSource, ArtifactSourceKind, BundleLifecycleState, CachePolicy,
    DistClient, DistOptions, InjectedResolution, ResolvePolicy, ResolveRefInjector,
    RetentionDecision, RetentionDisposition, RetentionEnvironment, RetentionInput,
    RollbackBundleInput, StageBundleInput, VerificationEnvironment, VerificationPolicy,
    WarmBundleInput,
};
use greentic_distributor_client::{
    ArtifactOpenOutput, ArtifactOpenRequest, ArtifactOpener, BundleManifestSummary, BundleOpenMode,
    CacheEntry, CacheEntryState, IntegrationError, IntegrationErrorCode, ResolvedArtifact,
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
        store_auth_path: dir.path().join("store-auth.json"),
        store_state_path: dir.path().join("store-auth.json"),
        #[cfg(feature = "fixture-resolver")]
        fixture_dir: None,
    }
}

fn write_cache_entry(entry: &CacheEntry) {
    let entry_path = entry.local_path.parent().unwrap().join("entry.json");
    fs::write(entry_path, serde_json::to_vec_pretty(entry).unwrap()).unwrap();
}

fn bundle_record_path(dir: &TempDir, bundle_id: &str) -> std::path::PathBuf {
    dir.path()
        .join("bundles")
        .join(format!("{}.json", bundle_id.replace(':', "__")))
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
async fn pr01_lockfile_can_reopen_cached_digest_without_original_ref() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("hello.wasm");
    fs::write(&file, b"hello").unwrap();
    let digest = digest_for(b"hello");

    let client = DistClient::new(options(&temp));
    let _ = client.ensure_cached(file.to_str().unwrap()).await.unwrap();

    let lock_path = temp.path().join("digest-only.lock.json");
    let lock_contents = serde_json::json!({
        "schema_version": 1,
        "components": [
            {
                "name": "hello",
                "digest": digest
            }
        ]
    });
    fs::write(&lock_path, serde_json::to_vec(&lock_contents).unwrap()).unwrap();

    let resolved = client.pull_lock(&lock_path).await.unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].descriptor.digest, digest);
    assert_eq!(resolved[0].wasm_bytes().unwrap(), b"hello");
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
async fn pr01_resolve_returns_canonical_file_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"descriptor").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };

    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    assert_eq!(descriptor.source_kind, ArtifactSourceKind::File);
    assert_eq!(descriptor.digest, digest_for(b"descriptor"));
    assert!(descriptor.canonical_ref.contains("@sha256:"));
}

#[tokio::test]
async fn pr01_fetch_persists_entry_and_open_cached_reuses_it() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"open-cached").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };

    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    let fetched = client.fetch(&descriptor, CachePolicy).await.unwrap();
    let entry = client.stat_cache(&descriptor.digest).unwrap();
    let reopened = client.open_cached(&descriptor.digest).unwrap();

    assert_eq!(entry.digest, descriptor.digest);
    assert_eq!(entry.canonical_ref, descriptor.canonical_ref);
    assert_eq!(fetched.descriptor.digest, reopened.descriptor.digest);
    assert_eq!(reopened.wasm_bytes().unwrap(), b"open-cached");
}

#[tokio::test]
async fn pr01_repo_placeholder_requires_mapping_when_unconfigured() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: "repo://future-placeholder".to_string(),
        kind: ArtifactSourceKind::Repo,
        transport_hints: Default::default(),
        dev_mode: false,
    };

    let err = client.resolve(source, ResolvePolicy).await.unwrap_err();
    assert!(
        format!("{err}").contains("resolution unavailable"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn pr02_deny_digest_refuses_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"deny-me").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    let policy = VerificationPolicy {
        deny_digests: vec![descriptor.digest.clone()],
        ..VerificationPolicy::default()
    };

    let decision = client.apply_policy(&descriptor, None, &policy);
    assert!(!decision.passed);
    assert!(decision.errors.iter().any(|err| err.contains("denied")));
}

#[tokio::test]
async fn pr02_missing_issuer_yields_warning() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"issuerless").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();

    let decision = client.apply_policy(&descriptor, None, &VerificationPolicy::default());
    let issuer_check = decision
        .checks
        .iter()
        .find(|check| check.name == "issuer_allowed")
        .unwrap();
    assert!(issuer_check.detail.contains("missing"));
}

#[tokio::test]
async fn pr02_operator_version_mismatch_warns_in_dev() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"version-check").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let mut descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    descriptor.annotations.insert(
        "operator_version".to_string(),
        serde_json::Value::String("1.0.0".to_string()),
    );

    let policy = VerificationPolicy {
        minimum_operator_version: Some("2.0.0".to_string()),
        environment: VerificationEnvironment::Dev,
        ..VerificationPolicy::default()
    };

    let decision = client.apply_policy(&descriptor, None, &policy);
    assert!(decision.passed);
    assert!(
        decision
            .warnings
            .iter()
            .any(|warning| warning.contains("does not satisfy"))
    );
}

#[tokio::test]
async fn pr02_verify_artifact_updates_cache_verification_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("component.wasm");
    fs::write(&file_path, b"verified").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let mut descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    descriptor.sbom_refs.push("sbom://present".to_string());
    descriptor.signature_refs.push("sig://present".to_string());
    let fetched = client.fetch(&descriptor, CachePolicy).await.unwrap();

    let advisory = AdvisorySet {
        version: "7".to_string(),
        issued_at: 123,
        source: "test".to_string(),
        deny_digests: Vec::new(),
        deny_issuers: Vec::new(),
        minimum_operator_version: None,
        release_train: None,
        expires_at: None,
        next_refresh_hint: None,
    };
    let report = client
        .verify_artifact(&fetched, Some(&advisory), &VerificationPolicy::default())
        .unwrap();
    let entry = client.stat_cache(&fetched.descriptor.digest).unwrap();

    assert!(report.passed);
    assert_eq!(entry.advisory_epoch, Some(7));
    assert!(entry.last_verified_at.is_some());
    assert!(entry.signature_summary.is_some());
}

#[tokio::test]
async fn pr02_prod_rejects_unsigned_artifact_while_dev_warns() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("unsigned.wasm");
    fs::write(&file_path, b"unsigned").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    let fetched = client.fetch(&descriptor, CachePolicy).await.unwrap();

    let dev_policy = VerificationPolicy {
        require_signature: true,
        environment: VerificationEnvironment::Dev,
        ..VerificationPolicy::default()
    };
    let dev_report = client.verify_artifact(&fetched, None, &dev_policy).unwrap();
    let dev_signature_present = dev_report
        .checks
        .iter()
        .find(|check| check.name == "signature_present")
        .unwrap();
    assert!(dev_report.passed);
    assert_eq!(
        dev_signature_present.outcome,
        greentic_distributor_client::VerificationOutcome::Warning
    );

    let prod_policy = VerificationPolicy {
        require_signature: true,
        environment: VerificationEnvironment::Prod,
        ..VerificationPolicy::default()
    };
    let prod_report = client
        .verify_artifact(&fetched, None, &prod_policy)
        .unwrap();
    let prod_signature_verified = prod_report
        .checks
        .iter()
        .find(|check| check.name == "signature_verified")
        .unwrap();
    assert!(!prod_report.passed);
    assert_eq!(
        prod_signature_verified.outcome,
        greentic_distributor_client::VerificationOutcome::Failed
    );
}

#[tokio::test]
async fn pr02_cached_artifact_can_be_reverified_under_newer_advisory_without_redownload() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("advisory-shift.wasm");
    fs::write(&file_path, b"advisory-shift").unwrap();

    let client = DistClient::new(options(&temp));
    let source = ArtifactSource {
        raw_ref: file_path.to_string_lossy().to_string(),
        kind: ArtifactSourceKind::File,
        transport_hints: Default::default(),
        dev_mode: true,
    };
    let descriptor = client.resolve(source, ResolvePolicy).await.unwrap();
    client.fetch(&descriptor, CachePolicy).await.unwrap();
    let initial_entry = client.stat_cache(&descriptor.digest).unwrap();
    let reopened = client.open_cached(&descriptor.digest).unwrap();
    let reopened_entry = client.stat_cache(&descriptor.digest).unwrap();

    assert_eq!(reopened.descriptor.digest, descriptor.digest);
    assert_eq!(initial_entry.fetched_at, reopened_entry.fetched_at);

    let advisory_v1 = AdvisorySet {
        version: "1".to_string(),
        issued_at: 100,
        source: "test".to_string(),
        deny_digests: Vec::new(),
        deny_issuers: Vec::new(),
        minimum_operator_version: None,
        release_train: None,
        expires_at: None,
        next_refresh_hint: None,
    };
    let report_v1 = client
        .verify_artifact(
            &reopened,
            Some(&advisory_v1),
            &VerificationPolicy::default(),
        )
        .unwrap();
    let entry_after_v1 = client.stat_cache(&descriptor.digest).unwrap();

    assert!(report_v1.passed);
    assert_eq!(entry_after_v1.advisory_epoch, Some(1));

    let advisory_v2 = AdvisorySet {
        version: "2".to_string(),
        issued_at: 200,
        source: "test".to_string(),
        deny_digests: vec![descriptor.digest.clone()],
        deny_issuers: Vec::new(),
        minimum_operator_version: None,
        release_train: None,
        expires_at: None,
        next_refresh_hint: None,
    };
    let report_v2 = client
        .verify_artifact(
            &reopened,
            Some(&advisory_v2),
            &VerificationPolicy::default(),
        )
        .unwrap();
    let entry_after_v2 = client.stat_cache(&descriptor.digest).unwrap();

    assert!(!report_v2.passed);
    assert!(
        report_v2
            .errors
            .iter()
            .any(|error| error.contains("denied"))
    );
    assert_eq!(entry_after_v2.advisory_epoch, Some(2));
    assert_eq!(entry_after_v2.fetched_at, initial_entry.fetched_at);
}

#[tokio::test]
async fn pr03_repeated_stage_returns_stable_bundle_id_and_cache_identity() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("bundle.wasm");
    fs::write(&file_path, b"stage-me").unwrap();

    let client = DistClient::new(options(&temp));
    let input = StageBundleInput {
        bundle_ref: file_path.to_string_lossy().to_string(),
        requested_access_mode: AccessMode::Userspace,
        verification_policy_ref: "default".to_string(),
        cache_policy_ref: "default".to_string(),
        tenant: Some("tenant-a".to_string()),
        team: Some("team-a".to_string()),
    };

    let first = client
        .stage_bundle(&input, None, &VerificationPolicy::default(), CachePolicy)
        .await
        .unwrap();
    let second = client
        .stage_bundle(&input, None, &VerificationPolicy::default(), CachePolicy)
        .await
        .unwrap();

    assert_eq!(first.bundle_id, second.bundle_id);
    assert_eq!(first.cache_entry.cache_key, second.cache_entry.cache_key);
    assert_eq!(first.canonical_ref, second.canonical_ref);
}

#[tokio::test]
async fn pr03_warm_reopens_cached_artifact_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("warm.wasm");
    fs::write(&file_path, b"warm-me").unwrap();

    let input = StageBundleInput {
        bundle_ref: file_path.to_string_lossy().to_string(),
        requested_access_mode: AccessMode::Userspace,
        verification_policy_ref: "default".to_string(),
        cache_policy_ref: "default".to_string(),
        tenant: None,
        team: None,
    };
    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(&input, None, &VerificationPolicy::default(), CachePolicy)
        .await
        .unwrap();

    let mut restarted_opts = options(&temp);
    restarted_opts.offline = true;
    let restarted_client = DistClient::new(restarted_opts);
    let warm = restarted_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: stage.bundle_id.clone(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: true,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap();

    assert_eq!(warm.bundle_id, stage.bundle_id);
    assert!(warm.errors.is_empty());
    assert_eq!(
        warm.bundle_manifest_summary.component_id,
        stage.resolved_artifact.component_id
    );
}

#[tokio::test]
async fn pr03_rollback_reopens_from_cache_without_network_or_reresolution() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("rollback.wasm");
    fs::write(&file_path, b"rollback-me").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let mut offline_opts = options(&temp);
    offline_opts.offline = true;
    let offline_client = DistClient::new(offline_opts);
    let rollback = offline_client
        .rollback_bundle(
            &RollbackBundleInput {
                target_bundle_id: stage.bundle_id.clone(),
                expected_cache_key: Some(stage.cache_entry.cache_key.clone()),
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap();

    assert!(rollback.reopened_from_cache);
    assert_eq!(rollback.bundle_id, stage.bundle_id);
    assert_eq!(rollback.cache_entry.cache_key, stage.cache_entry.cache_key);
    assert!(rollback.verification_report.errors.is_empty());
}

#[tokio::test]
async fn pr04_bundle_record_persists_stage_identity_for_offline_rollback() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("indexed-rollback.wasm");
    fs::write(&file_path, b"indexed-rollback").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let record = stage_client.stat_bundle(&stage.bundle_id).unwrap();
    assert_eq!(record.cache_key, stage.cache_entry.cache_key);
    assert_eq!(record.canonical_ref, stage.canonical_ref);
    assert_eq!(record.lifecycle_state, BundleLifecycleState::Staged);
    assert_eq!(stage_client.list_bundles().unwrap(), vec![record.clone()]);

    let mut offline_opts = options(&temp);
    offline_opts.offline = true;
    let offline_client = DistClient::new(offline_opts);
    let rollback = offline_client
        .rollback_bundle(
            &RollbackBundleInput {
                target_bundle_id: stage.bundle_id.clone(),
                expected_cache_key: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap();

    assert_eq!(rollback.bundle_id, stage.bundle_id);
    assert_eq!(rollback.cache_entry.cache_key, stage.cache_entry.cache_key);
}

struct MountBundleOpener;

impl ArtifactOpener for MountBundleOpener {
    fn open(
        &self,
        artifact: &ResolvedArtifact,
        _request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError> {
        Ok(ArtifactOpenOutput {
            bundle_manifest_summary: BundleManifestSummary {
                component_id: format!("mounted:{}", artifact.component_id),
                abi_version: artifact.abi_version.clone(),
                describe_artifact_ref: artifact.describe_artifact_ref.clone(),
                artifact_type: artifact.descriptor.artifact_type.clone(),
                media_type: artifact.descriptor.media_type.clone(),
                size_bytes: artifact.descriptor.size_bytes,
            },
            bundle_open_mode: BundleOpenMode::Mount,
            warnings: vec!["custom opener used".to_string()],
        })
    }
}

struct FailingBundleOpener;

impl ArtifactOpener for FailingBundleOpener {
    fn open(
        &self,
        _artifact: &ResolvedArtifact,
        request: &ArtifactOpenRequest,
    ) -> Result<ArtifactOpenOutput, IntegrationError> {
        Err(IntegrationError {
            code: IntegrationErrorCode::BundleOpenFailed,
            summary: format!("failed to open {}", request.bundle_id),
            retryable: false,
            details: None,
        })
    }
}

#[tokio::test]
async fn pr03_warm_uses_custom_bundle_opener() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("mountable.wasm");
    fs::write(&file_path, b"mount-me").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Mount,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let warm_client = DistClient::with_artifact_opener(options(&temp), Arc::new(MountBundleOpener));
    let warm = warm_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: stage.bundle_id.clone(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap();

    assert_eq!(warm.bundle_open_mode, BundleOpenMode::Mount);
    assert!(
        warm.warnings
            .iter()
            .any(|warning| warning.contains("custom opener"))
    );
    assert!(
        warm.bundle_manifest_summary
            .component_id
            .starts_with("mounted:")
    );
}

#[tokio::test]
async fn pr03_warm_maps_bundle_opener_failures() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("broken-open.wasm");
    fs::write(&file_path, b"broken-open").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let warm_client =
        DistClient::with_artifact_opener(options(&temp), Arc::new(FailingBundleOpener));
    let err = warm_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: stage.bundle_id.clone(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::BundleOpenFailed);
    assert!(err.summary.contains(&stage.bundle_id));
}

#[tokio::test]
async fn pr03_warm_rejects_invalid_bundle_id() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("invalid-bundle-id.wasm");
    fs::write(&file_path, b"invalid-bundle-id").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let err = stage_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: "bundle:sha256:deadbeef".to_string(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::InvalidReference);
    assert!(err.summary.contains("does not match cache key"));
}

#[tokio::test]
async fn pr03_warm_distinguishes_corrupt_cached_blob_from_cache_miss() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("corrupt-cache.wasm");
    fs::write(&file_path, b"corrupt-cache").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    fs::remove_file(stage.cache_entry.local_path.clone()).unwrap();

    let err = stage_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: stage.bundle_id.clone(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::CacheCorrupt);
    assert!(err.summary.contains("cached blob is missing"));
}

#[tokio::test]
async fn pr03_warm_uses_expected_operator_version_for_prod_gate() {
    let temp = tempfile::tempdir().unwrap();
    let file_path = temp.path().join("operator-gate.wasm");
    fs::write(&file_path, b"operator-gate").unwrap();

    let stage_client = DistClient::new(options(&temp));
    let stage = stage_client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let err = stage_client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id: stage.bundle_id.clone(),
                cache_key: stage.cache_entry.cache_key.clone(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: Some("1.0.0".to_string()),
            },
            None,
            &VerificationPolicy {
                minimum_operator_version: Some("2.0.0".to_string()),
                environment: VerificationEnvironment::Prod,
                ..VerificationPolicy::default()
            },
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::VerificationFailed);
    assert!(err.summary.contains("does not satisfy minimum"));
    let failed_checks = err
        .details
        .as_ref()
        .and_then(|details| details.get("failed_checks"))
        .and_then(|value| value.as_array())
        .unwrap();
    assert_eq!(failed_checks.len(), 1);
    assert_eq!(
        failed_checks[0]
            .get("name")
            .and_then(|value| value.as_str()),
        Some("operator_version_compatible")
    );
}

#[tokio::test]
async fn pr03_warm_missing_cache_entry_is_cache_miss() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let err = client
        .warm_bundle(
            &WarmBundleInput {
                bundle_id:
                    "bundle:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                cache_key:
                    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                smoke_test: false,
                dry_run: false,
                expected_operator_version: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::CacheMiss);
    assert!(err.summary.contains("was not found"));
}

#[tokio::test]
async fn pr03_rollback_rejects_invalid_bundle_id() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let err = client
        .rollback_bundle(
            &RollbackBundleInput {
                target_bundle_id: "not-a-bundle-id".to_string(),
                expected_cache_key: None,
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::InvalidReference);
    assert!(err.summary.contains("invalid bundle id"));
}

#[tokio::test]
async fn pr03_rollback_missing_cache_entry_is_cache_miss() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let digest = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    let err = client
        .rollback_bundle(
            &RollbackBundleInput {
                target_bundle_id: format!("bundle:{digest}"),
                expected_cache_key: Some(digest.to_string()),
            },
            None,
            &VerificationPolicy::default(),
        )
        .unwrap_err();

    assert_eq!(err.code, IntegrationErrorCode::CacheMiss);
    assert!(err.summary.contains("was not found"));
}

#[tokio::test]
async fn pr04_retention_protects_active_and_session_bundles() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let first_path = temp.path().join("active.wasm");
    let second_path = temp.path().join("session.wasm");
    fs::write(&first_path, b"active").unwrap();
    fs::write(&second_path, b"session").unwrap();

    let active = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: first_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let session = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: second_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let outcome = client
        .evaluate_retention(&RetentionInput {
            entries: client.list_cache_entries(),
            active_bundle_ids: vec![active.bundle_id.clone()],
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: vec![session.bundle_id.clone()],
            max_cache_bytes: 1,
            max_entry_age: Some(0),
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Prod,
        })
        .unwrap();

    assert_eq!(outcome.report.protected, 2);
    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == active.bundle_id
            && matches!(decision.decision, RetentionDisposition::Protect)
            && decision.reason_code == "active_bundle"
    }));
    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == session.bundle_id
            && matches!(decision.decision, RetentionDisposition::Protect)
            && decision.reason_code == "session_reference"
    }));
}

#[tokio::test]
async fn pr04_retention_respects_rollback_depth() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let mut staged = Vec::new();
    for (name, bytes) in [
        ("a.wasm", b"a".as_slice()),
        ("b.wasm", b"b".as_slice()),
        ("c.wasm", b"c".as_slice()),
    ] {
        let path = temp.path().join(name);
        fs::write(&path, bytes).unwrap();
        staged.push(
            client
                .stage_bundle(
                    &StageBundleInput {
                        bundle_ref: path.to_string_lossy().to_string(),
                        requested_access_mode: AccessMode::Userspace,
                        verification_policy_ref: "default".to_string(),
                        cache_policy_ref: "default".to_string(),
                        tenant: None,
                        team: None,
                    },
                    None,
                    &VerificationPolicy::default(),
                    CachePolicy,
                )
                .await
                .unwrap(),
        );
    }

    for (index, staged_item) in staged.iter().enumerate() {
        let mut entry = client
            .stat_cache(&staged_item.cache_entry.cache_key)
            .unwrap();
        entry.fetched_at = (index as u64) + 1;
        write_cache_entry(&entry);
    }

    let outcome = client
        .evaluate_retention(&RetentionInput {
            entries: client.list_cache_entries(),
            active_bundle_ids: vec![staged[2].bundle_id.clone()],
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: 1,
            max_entry_age: Some(0),
            minimum_rollback_depth: 1,
            environment: RetentionEnvironment::Prod,
        })
        .unwrap();

    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == staged[1].bundle_id
            && matches!(decision.decision, RetentionDisposition::Protect)
            && decision.reason_code == "rollback_depth"
    }));
}

#[tokio::test]
async fn pr04_retention_prefers_safe_corrupt_eviction() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let first_path = temp.path().join("corrupt.wasm");
    let second_path = temp.path().join("healthy.wasm");
    fs::write(&first_path, b"corrupt").unwrap();
    fs::write(&second_path, b"healthy").unwrap();

    let corrupt = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: first_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let healthy = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: second_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let mut corrupt_entry = client.stat_cache(&corrupt.cache_entry.cache_key).unwrap();
    corrupt_entry.state = CacheEntryState::Corrupt;
    corrupt_entry.last_accessed_at = 1;
    write_cache_entry(&corrupt_entry);

    let mut healthy_entry = client.stat_cache(&healthy.cache_entry.cache_key).unwrap();
    healthy_entry.last_accessed_at = 1;
    write_cache_entry(&healthy_entry);

    let outcome = client
        .evaluate_retention(&RetentionInput {
            entries: client.list_cache_entries(),
            active_bundle_ids: Vec::new(),
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: healthy_entry.size_bytes + 1,
            max_entry_age: None,
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Prod,
        })
        .unwrap();

    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == corrupt.bundle_id
            && matches!(decision.decision, RetentionDisposition::Evict)
            && decision.reason_code == "corrupt_entry"
    }));
    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == healthy.bundle_id
            && !matches!(decision.decision, RetentionDisposition::Evict)
    }));
}

#[tokio::test]
async fn pr04_retention_budget_tie_breaks_by_cache_key() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let first_path = temp.path().join("tie-a.wasm");
    let second_path = temp.path().join("tie-b.wasm");
    fs::write(&first_path, b"same-size-a").unwrap();
    fs::write(&second_path, b"same-size-b").unwrap();

    let first = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: first_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let second = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: second_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    for cache_key in [&first.cache_entry.cache_key, &second.cache_entry.cache_key] {
        let mut entry = client.stat_cache(cache_key).unwrap();
        entry.last_accessed_at = 1;
        write_cache_entry(&entry);
    }

    let entries = client.list_cache_entries();
    let total = entries.iter().map(|entry| entry.size_bytes).sum::<u64>();
    let outcome = client
        .evaluate_retention(&RetentionInput {
            entries,
            active_bundle_ids: Vec::new(),
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: total.saturating_sub(first.cache_entry.size_bytes),
            max_entry_age: None,
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Prod,
        })
        .unwrap();

    let evicted = outcome
        .decisions
        .iter()
        .filter(|decision| matches!(decision.decision, RetentionDisposition::Evict))
        .collect::<Vec<&RetentionDecision>>();
    assert_eq!(evicted.len(), 1);
    let expected = [
        first.cache_entry.cache_key.clone(),
        second.cache_entry.cache_key.clone(),
    ]
    .into_iter()
    .min()
    .unwrap();
    assert_eq!(evicted[0].cache_key, expected);
}

#[tokio::test]
async fn pr04_apply_retention_removes_bundle_record_for_evicted_entry() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let file_path = temp.path().join("evicted-record.wasm");
    fs::write(&file_path, b"evicted-record").unwrap();
    let staged = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let mut entry = client.stat_cache(&staged.cache_entry.cache_key).unwrap();
    entry.last_accessed_at = 1;
    write_cache_entry(&entry);

    let outcome = client
        .apply_retention(&RetentionInput {
            entries: client.list_cache_entries(),
            active_bundle_ids: Vec::new(),
            staged_bundle_ids: Vec::new(),
            warming_bundle_ids: Vec::new(),
            ready_bundle_ids: Vec::new(),
            draining_bundle_ids: Vec::new(),
            session_referenced_bundle_ids: Vec::new(),
            max_cache_bytes: 0,
            max_entry_age: Some(0),
            minimum_rollback_depth: 0,
            environment: RetentionEnvironment::Prod,
        })
        .unwrap();

    assert!(outcome.decisions.iter().any(|decision| {
        decision.bundle_id == staged.bundle_id
            && matches!(decision.decision, RetentionDisposition::Evict)
    }));
    assert!(client.stat_bundle(&staged.bundle_id).is_err());
}

#[tokio::test]
async fn pr04_automatic_cache_cap_preserves_staged_bundle_records() {
    let temp = tempfile::tempdir().unwrap();
    let mut opts = options(&temp);
    opts.cache_max_bytes = 1;
    let client = DistClient::new(opts);

    let first_path = temp.path().join("auto-protect-a.wasm");
    let second_path = temp.path().join("auto-protect-b.wasm");
    fs::write(&first_path, b"aaaa").unwrap();
    fs::write(&second_path, b"bbbb").unwrap();

    let first = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: first_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let second = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: second_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    let cache_entries = client.list_cache_entries();
    assert_eq!(cache_entries.len(), 2);
    assert!(client.stat_bundle(&first.bundle_id).is_ok());
    assert!(client.stat_bundle(&second.bundle_id).is_ok());
}

#[tokio::test]
async fn pr04_inactive_bundle_state_allows_automatic_cap_eviction() {
    let temp = tempfile::tempdir().unwrap();
    let mut opts = options(&temp);
    opts.cache_max_bytes = 4;
    let client = DistClient::new(opts);

    let first_path = temp.path().join("inactive-a.wasm");
    let second_path = temp.path().join("inactive-b.wasm");
    fs::write(&first_path, b"aaaa").unwrap();
    fs::write(&second_path, b"bbbb").unwrap();

    let first = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: first_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    client
        .set_bundle_state(&first.bundle_id, BundleLifecycleState::Inactive)
        .unwrap();

    let second = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: second_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    assert!(client.stat_bundle(&first.bundle_id).is_err());
    assert!(client.stat_bundle(&second.bundle_id).is_ok());
}

#[tokio::test]
async fn pr04_list_bundles_errors_on_corrupt_bundle_record() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let file_path = temp.path().join("corrupt-record.wasm");
    fs::write(&file_path, b"corrupt-record").unwrap();
    let staged = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    fs::write(bundle_record_path(&temp, &staged.bundle_id), b"{not-json").unwrap();

    let err = client.list_bundles().unwrap_err();
    assert!(format!("{err}").contains("cache error"));
}

#[tokio::test]
async fn pr04_evict_cache_removes_bundle_record() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let file_path = temp.path().join("evict-record.wasm");
    fs::write(&file_path, b"evict-record").unwrap();
    let staged = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();
    let mut entry = client.stat_cache(&staged.cache_entry.cache_key).unwrap();
    entry.last_accessed_at = 1;
    write_cache_entry(&entry);

    let report = client
        .evict_cache(std::slice::from_ref(&staged.cache_entry.cache_key))
        .unwrap();

    assert_eq!(report.evicted, 1);
    assert!(client.stat_bundle(&staged.bundle_id).is_err());
}

#[tokio::test]
async fn pr04_gc_removes_orphaned_bundle_record() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::new(options(&temp));

    let file_path = temp.path().join("orphan-record.wasm");
    fs::write(&file_path, b"orphan-record").unwrap();
    let staged = client
        .stage_bundle(
            &StageBundleInput {
                bundle_ref: file_path.to_string_lossy().to_string(),
                requested_access_mode: AccessMode::Userspace,
                verification_policy_ref: "default".to_string(),
                cache_policy_ref: "default".to_string(),
                tenant: None,
                team: None,
            },
            None,
            &VerificationPolicy::default(),
            CachePolicy,
        )
        .await
        .unwrap();

    fs::remove_dir_all(staged.cache_entry.local_path.parent().unwrap()).unwrap();
    let removed = client.gc().unwrap();

    assert_eq!(removed, vec![staged.cache_entry.cache_key.clone()]);
    assert!(client.stat_bundle(&staged.bundle_id).is_err());
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

struct OciSourceInjector;

#[async_trait]
impl ResolveRefInjector for OciSourceInjector {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<Option<InjectedResolution>, greentic_distributor_client::dist::DistError> {
        if reference != "oci://ghcr.io/greenticai/components/templates:latest" {
            return Ok(None);
        }

        let wasm_bytes = b"oci-component".to_vec();
        Ok(Some(InjectedResolution::WasmBytes {
            resolved_digest: digest_for(&wasm_bytes),
            wasm_bytes,
            component_id: "templates".to_string(),
            abi_version: None,
            source: ArtifactSource {
                raw_ref: reference.to_string(),
                kind: ArtifactSourceKind::Oci,
                transport_hints: Default::default(),
                dev_mode: false,
            },
        }))
    }
}

#[tokio::test]
async fn resolve_ref_injector_preserves_registry_host_in_oci_canonical_ref() {
    let temp = tempfile::tempdir().unwrap();
    let client = DistClient::with_ref_injector(options(&temp), Arc::new(OciSourceInjector));

    let reference = "oci://ghcr.io/greenticai/components/templates:latest";
    let resolved = client.resolve_ref(reference).await.unwrap();

    assert_eq!(
        resolved.descriptor.canonical_ref,
        format!(
            "oci://ghcr.io/greenticai/components/templates@{}",
            resolved.resolved_digest
        )
    );
    assert_eq!(
        resolved.source_snapshot.canonical_ref,
        resolved.descriptor.canonical_ref
    );
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
