#![cfg(feature = "dist-cli")]

use assert_cmd::assert::OutputAssertExt;
use greentic_distributor_client::store_auth::load_login;
use std::fs;
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
