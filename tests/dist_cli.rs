#![cfg(feature = "dist-cli")]

use assert_cmd::assert::OutputAssertExt;
use greentic_distributor_client::dist::{
    ArtifactSourceKind, ArtifactType, CacheEntry, CacheEntryState, ReleaseChannel, ReleaseIndex,
    ReleaseIndexEntry, SourceSnapshot,
};
use greentic_distributor_client::store_auth::load_login;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

fn cache_env(temp: &TempDir) -> Vec<(&'static str, String)> {
    vec![
        (
            "GREENTIC_DIST_CACHE_DIR",
            temp.path().to_string_lossy().into(),
        ),
        ("XDG_CACHE_HOME", temp.path().to_string_lossy().into()),
    ]
}

fn digest_for(bytes: &[u8]) -> String {
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

fn cache_blob_path(temp: &TempDir, digest: &str) -> PathBuf {
    let trimmed = digest.trim_start_matches("sha256:");
    let (prefix, rest) = trimmed.split_at(2);
    temp.path()
        .join("artifacts")
        .join("sha256")
        .join(prefix)
        .join(rest)
        .join("blob")
}

fn seed_cached_blob(temp: &TempDir, reference: &str, bytes: &[u8]) -> String {
    let digest = digest_for(bytes);
    let blob_path = cache_blob_path(temp, &digest);
    fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
    fs::write(&blob_path, bytes).unwrap();
    let entry = CacheEntry {
        format_version: 1,
        cache_key: digest.clone(),
        digest: digest.clone(),
        media_type: "application/vnd.greentic.gtpack.v1+zip".to_string(),
        size_bytes: bytes.len() as u64,
        artifact_type: ArtifactType::Pack,
        source_kind: ArtifactSourceKind::Oci,
        raw_ref: reference.to_string(),
        canonical_ref: format!(
            "oci://{}@{}",
            reference
                .trim_start_matches("oci://")
                .rsplit_once(':')
                .unwrap()
                .0,
            digest
        ),
        fetched_at: 0,
        last_accessed_at: 0,
        last_verified_at: None,
        state: CacheEntryState::Ready,
        advisory_epoch: None,
        signature_summary: None,
        local_path: blob_path.clone(),
        source_snapshot: SourceSnapshot {
            raw_ref: reference.to_string(),
            canonical_ref: format!(
                "oci://{}@{}",
                reference
                    .trim_start_matches("oci://")
                    .rsplit_once(':')
                    .unwrap()
                    .0,
                digest
            ),
            source_kind: ArtifactSourceKind::Oci,
            authoritative: false,
        },
    };
    fs::write(
        blob_path.parent().unwrap().join("entry.json"),
        serde_json::to_vec_pretty(&entry).unwrap(),
    )
    .unwrap();
    digest
}

fn write_release_index(
    temp: &TempDir,
    release: &str,
    channel: ReleaseChannel,
    reference: &str,
    digest: &str,
) {
    let channel_path = match channel {
        ReleaseChannel::Stable => "stable",
        ReleaseChannel::Dev => "dev",
        ReleaseChannel::Rnd => "rnd",
    };
    let path = temp
        .path()
        .join("release-index")
        .join("v1")
        .join(channel_path)
        .join(format!("{release}.json"));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let index = ReleaseIndex {
        schema: "greentic.release-index.v1".to_string(),
        release: release.to_string(),
        channel,
        refs: std::collections::BTreeMap::from([(
            reference.to_string(),
            ReleaseIndexEntry {
                version: "0.5.4".to_string(),
                digest: digest.to_string(),
                canonical_ref: format!(
                    "oci://{}@{}",
                    reference
                        .trim_start_matches("oci://")
                        .rsplit_once(':')
                        .unwrap()
                        .0,
                    digest
                ),
            },
        )]),
    };
    fs::write(path, serde_json::to_vec_pretty(&index).unwrap()).unwrap();
}

#[test]
fn cache_ls_rm_gc_json() {
    let temp = tempfile::tempdir().unwrap();
    // seed cache with a digest dir
    let digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let dir = temp
        .path()
        .join("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("component.wasm"), b"cached").unwrap();

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("greentic-dist"));
    cmd.args(["--cache-dir", temp.path().to_str().unwrap()]);
    cmd.arg("cache").arg("ls").arg("--json");
    for (k, v) in cache_env(&temp) {
        cmd.env(k, v);
    }
    let output = cmd.assert().success().get_output().stdout.clone();
    let listed: Vec<String> = serde_json::from_slice(&output).unwrap();
    assert_eq!(listed, vec![digest.to_string()]);

    let mut rm = Command::new(assert_cmd::cargo::cargo_bin!("greentic-dist"));
    rm.args(["--cache-dir", temp.path().to_str().unwrap()]);
    rm.args(["cache", "rm", "--json", digest]);
    for (k, v) in cache_env(&temp) {
        rm.env(k, v);
    }
    rm.assert().success();

    let mut gc = Command::new(assert_cmd::cargo::cargo_bin!("greentic-dist"));
    gc.args(["--cache-dir", temp.path().to_str().unwrap()]);
    gc.args(["cache", "gc", "--json"]);
    for (k, v) in cache_env(&temp) {
        gc.env(k, v);
    }
    let gc_out = gc.assert().success().get_output().stdout.clone();
    let removed: Vec<String> = serde_json::from_slice(&gc_out).unwrap();
    // After explicit rm, GC should be a no-op.
    assert!(removed.is_empty());
}

#[test]
fn resolve_release_context_uses_local_release_index_offline() {
    let temp = tempfile::tempdir().unwrap();
    let reference = "oci://ghcr.io/greenticai/packs/apps/helpdesk-itsm:stable";
    let digest = seed_cached_blob(&temp, reference, b"cached-pack");
    write_release_index(&temp, "1.0.16", ReleaseChannel::Stable, reference, &digest);

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("greentic-dist"));
    cmd.args(["--cache-dir", temp.path().to_str().unwrap(), "--offline"]);
    cmd.args(["resolve", reference, "--release", "1.0.16"]);
    for (k, v) in cache_env(&temp) {
        cmd.env(k, v);
    }
    let output = cmd.assert().success().get_output().stdout.clone();

    assert_eq!(String::from_utf8(output).unwrap().trim(), digest);
}

#[test]
fn auth_login_persists_tenant_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let auth_path = temp.path().join("store-auth.json");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("greentic-dist"));
    cmd.args(["auth", "login", "tenant-a", "--token", "secret-token"]);
    cmd.env(
        "GREENTIC_DIST_STORE_SECRETS_PATH",
        auth_path.to_string_lossy().into_owned(),
    );
    cmd.env(
        "GREENTIC_DIST_STORE_STATE_PATH",
        auth_path.to_string_lossy().into_owned(),
    );
    cmd.assert().success();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let saved = rt
        .block_on(load_login(&auth_path, &auth_path, "tenant-a"))
        .expect("saved login");
    assert_eq!(saved.tenant, "tenant-a");
    assert_eq!(saved.username, "tenant-a");
    assert_eq!(saved.token, "secret-token");
}
