//! HTTP transport for store agentic-worker `.gtpack` artifacts.
//!
//! Fetches an agentic-worker pack from the store's public REST artifact endpoint
//! (`{base}/api/v1/agentic-workers/{name}/{version}/artifact`), verifies the
//! whole-archive `x-artifact-sha256` digest when advertised, and caches the
//! bytes on disk for offline reuse.
//!
//! This mirrors `store_ext` (which fetches store *extension* `.gtxpack`
//! artifacts): the transport lives here in the distributor-client so that the
//! client owns all artifact fetching. Reference resolution (parsing
//! `store://<name>@<version>` and resolving the store base URL from the
//! environment) stays in the caller (the bundle auto-wiring pass).
//!
//! Ed25519/DSSE signature verification is a separate, later task; this module
//! performs sha256 integrity checking + caching only.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};

/// Build the store artifact endpoint URL for an agentic worker `(name, version)`.
///
/// Shape: `{base}/api/v1/agentic-workers/{name}/{version}/artifact` (public, no auth).
/// A trailing slash on `store_base` is trimmed so the path joins cleanly.
pub fn agentic_worker_artifact_url(store_base: &str, name: &str, version: &str) -> String {
    let base = store_base.trim_end_matches('/');
    format!("{base}/api/v1/agentic-workers/{name}/{version}/artifact")
}

/// Fetch (and cache) the agentic-worker `.gtpack` from the store artifact endpoint.
///
/// In `offline` mode, returns the ref-keyed cached bytes for this exact
/// `name@version`, or an error telling the caller to run online once to populate
/// the cache. Otherwise performs an HTTP GET of the artifact URL, verifies the
/// whole-archive `x-artifact-sha256` digest when advertised (bailing on a
/// mismatch), caches the bytes under `cache_dir/agentic-workers/` keyed by BOTH
/// `sha256-<hex>.gtpack` AND a sanitized `name@version` ref-key, and returns
/// the bytes.
///
/// # Arguments
/// * `store_base` - Store base URL (e.g. `https://store.greentic.cloud`).
/// * `name` - Agentic-worker name (the `<name>` segment of `store://<name>@<version>`).
/// * `version` - Explicit, pinned agentic-worker version.
/// * `cache_dir` - Runtime cache root; artifacts land under `cache_dir/agentic-workers/`.
/// * `offline` - When true, never hit the network; serve only from cache.
///
/// # Errors
/// Returns an error if (offline) no cached artifact exists for the ref, if the
/// HTTP request fails or returns a non-200 status, if the advertised digest does
/// not match the body, or if the cache write fails.
pub fn fetch_store_agentic_worker(
    store_base: &str,
    name: &str,
    version: &str,
    cache_dir: &Path,
    offline: bool,
) -> Result<Vec<u8>> {
    if offline {
        // Offline: we cannot resolve the artifact sha without a download, so we
        // can only serve a previously cached artifact for this exact ref.
        return read_cached_store_artifact(cache_dir, name, version).ok_or_else(|| {
            anyhow!(
                "offline: no cached artifact for store agentic worker '{name}@{version}' under the cache dir; run online once to populate the cache"
            )
        });
    }

    let url = agentic_worker_artifact_url(store_base, name, version);

    let (bytes, advertised_sha) = http_get_artifact(&url)?;
    let actual_sha = hex::encode(Sha256::digest(&bytes));
    if let Some(advertised) = advertised_sha.as_deref()
        && !advertised.eq_ignore_ascii_case(&actual_sha)
    {
        bail!(
            "store artifact integrity check failed for '{name}@{version}': x-artifact-sha256 advertises '{advertised}' but body hashes to '{actual_sha}'"
        );
    }

    // Cache keyed by archive sha256 (+ ref-keyed copy for offline reuse).
    cache_store_artifact(cache_dir, name, version, &actual_sha, &bytes)?;
    Ok(bytes)
}

/// Directory under the runtime cache where store agentic-worker artifacts are kept.
fn store_artifact_cache_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("agentic-workers")
}

/// Filesystem-safe key for a store ref (`name@version` with separators escaped).
fn store_ref_cache_key(name: &str, version: &str) -> String {
    let sanitized = format!("{name}@{version}").replace(['/', '\\', ':', '@'], "_");
    format!("{sanitized}.gtpack")
}

/// Write the artifact into the cache under both its archive-sha name and a
/// ref-keyed name (so offline mode can find it by `name@version`).
fn cache_store_artifact(
    cache_dir: &Path,
    name: &str,
    version: &str,
    archive_sha: &str,
    bytes: &[u8],
) -> Result<()> {
    let dir = store_artifact_cache_dir(cache_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create store artifact cache dir {}", dir.display()))?;
    let sha_path = dir.join(format!("sha256-{archive_sha}.gtpack"));
    std::fs::write(&sha_path, bytes)
        .with_context(|| format!("write store artifact cache {}", sha_path.display()))?;
    let ref_path = dir.join(store_ref_cache_key(name, version));
    std::fs::write(&ref_path, bytes)
        .with_context(|| format!("write store artifact cache {}", ref_path.display()))?;
    Ok(())
}

/// Read a previously cached store artifact by ref key, if present.
fn read_cached_store_artifact(cache_dir: &Path, name: &str, version: &str) -> Option<Vec<u8>> {
    let path = store_artifact_cache_dir(cache_dir).join(store_ref_cache_key(name, version));
    std::fs::read(path).ok()
}

/// Blocking HTTP GET of the store artifact endpoint, returning the body bytes
/// and the optional `x-artifact-sha256` header value.
///
/// Runs `reqwest::blocking` on a dedicated thread so it is safe to call from
/// within a Tokio runtime.
fn http_get_artifact(url: &str) -> Result<(Vec<u8>, Option<String>)> {
    let url = url.to_string();
    std::thread::spawn(move || -> Result<(Vec<u8>, Option<String>)> {
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .context("build HTTP client for store agentic worker artifact")?;
        let response = client
            .get(&url)
            .send()
            .with_context(|| format!("request store agentic worker artifact {url}"))?;
        if response.status() != reqwest::StatusCode::OK {
            bail!(
                "store agentic worker artifact {url} request failed with status {}",
                response.status()
            );
        }
        let advertised_sha = response
            .headers()
            .get("x-artifact-sha256")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim().to_string());
        let bytes = response
            .bytes()
            .with_context(|| format!("read store agentic worker artifact response {url}"))?;
        Ok((bytes.to_vec(), advertised_sha))
    })
    .join()
    .map_err(|_| anyhow!("store artifact download thread panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agentic_worker_artifact_url_has_expected_shape() {
        let url = agentic_worker_artifact_url(
            "https://store.greentic.cloud",
            "agentic-research-tavily-agent",
            "0.1.0",
        );
        assert_eq!(
            url,
            "https://store.greentic.cloud/api/v1/agentic-workers/agentic-research-tavily-agent/0.1.0/artifact"
        );
    }

    #[test]
    fn agentic_worker_artifact_url_trims_trailing_slash() {
        let url = agentic_worker_artifact_url("https://store.greentic.cloud/", "agent", "1.2.3");
        assert_eq!(
            url,
            "https://store.greentic.cloud/api/v1/agentic-workers/agent/1.2.3/artifact"
        );
    }

    #[test]
    fn cache_round_trip_by_ref_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bytes = b"fake-gtpack-bytes".to_vec();
        let archive_sha = hex::encode(Sha256::digest(&bytes));

        cache_store_artifact(dir.path(), "my-agent", "0.1.0", &archive_sha, &bytes)
            .expect("cache write");

        // Ref-keyed read returns the same bytes.
        let read_back = read_cached_store_artifact(dir.path(), "my-agent", "0.1.0")
            .expect("cached bytes present");
        assert_eq!(read_back, bytes);

        // The sha-keyed copy is written too.
        let sha_path =
            store_artifact_cache_dir(dir.path()).join(format!("sha256-{archive_sha}.gtpack"));
        assert!(sha_path.exists(), "sha-keyed cache file should exist");
    }

    #[test]
    fn ref_cache_key_sanitizes_separators() {
        // Slashes, backslashes, colons, and `@` all collapse to `_`.
        let key = store_ref_cache_key("scope/name", "1.0.0");
        assert_eq!(key, "scope_name_1.0.0.gtpack");
    }

    #[test]
    fn offline_miss_reports_actionable_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = fetch_store_agentic_worker(
            "https://store.greentic.cloud",
            "missing-agent",
            "0.1.0",
            dir.path(),
            true,
        )
        .expect_err("offline miss should error");
        let message = err.to_string();
        assert!(
            message.contains("offline") && message.contains("missing-agent@0.1.0"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn offline_hit_serves_cached_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bytes = b"cached-archive".to_vec();
        let archive_sha = hex::encode(Sha256::digest(&bytes));
        cache_store_artifact(dir.path(), "cached-agent", "0.1.0", &archive_sha, &bytes)
            .expect("cache write");

        let read_back = fetch_store_agentic_worker(
            "https://store.greentic.cloud",
            "cached-agent",
            "0.1.0",
            dir.path(),
            true,
        )
        .expect("offline hit");
        assert_eq!(read_back, bytes);
    }
}
