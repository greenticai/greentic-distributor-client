# greentic-distributor-client

WIT-based client for the `greentic:distributor-api@1.0.0` world. Provides:
- `DistributorClient` async trait for resolving components, querying pack status, and warming packs.
- `WitDistributorClient` adapter that translates DTOs to `greentic-interfaces-guest` distributor-api bindings; use `GeneratedDistributorApiBindings` on WASM targets to call the distributor imports.
- Optional HTTP runtime client behind the `http-runtime` feature for JSON endpoints that mirror the runtime API.
- `greentic-dist` CLI (feature `dist-cli`) for resolving/pulling components into a shared cache, plus a library `DistClient` API for pack/runner integration.

Uses DTOs from `greentic-types`.

## Usage

```rust
use greentic_distributor_client::{
    DistributorApiBindings, DistributorClient, DistributorEnvironmentId, EnvId,
    GeneratedDistributorApiBindings, ResolveComponentRequest, TenantCtx, TenantId,
    WitDistributorClient,
};
use serde_json::json;

let bindings = GeneratedDistributorApiBindings::default();
let client = WitDistributorClient::new(bindings);
let resp = client.resolve_component(ResolveComponentRequest {
    tenant: TenantCtx::new(
        EnvId::try_from("prod").unwrap(),
        TenantId::try_from("tenant-a").unwrap(),
    ),
    environment_id: DistributorEnvironmentId::from("env-1"),
    pack_id: "pack-123".into(),
    component_id: "component-x".into(),
    version: "1.0.0".into(),
    extra: json!({}),
}).await?;
println!("artifact: {:?}", resp.artifact);
println!(
    "secret requirements present: {}",
    resp.secret_requirements.is_some()
);
```

`GeneratedDistributorApiBindings` calls the distributor imports on WASM targets. On non-WASM targets it returns an error; consumers can provide their own bindings implementation for testing.

`secret_requirements` is present when talking to distributor versions that support it; otherwise it is `None`. When requirements are returned, run `greentic-secrets init --pack <pack-id>` ahead of time so secrets are available to the runtime.

### HTTP runtime client (feature `http-runtime`)
Enable the feature and construct `HttpDistributorClient`:

```toml
[dependencies]
greentic-distributor-client = { version = "0.4", features = ["http-runtime"] }
```

```rust
use greentic_distributor_client::{
    DistributorClient, DistributorClientConfig, DistributorEnvironmentId, EnvId, HttpDistributorClient,
    ResolveComponentRequest, TenantCtx, TenantId,
};
use serde_json::json;

let config = DistributorClientConfig {
    base_url: Some("https://distributor.example.com".into()),
    environment_id: DistributorEnvironmentId::from("env-1"),
    tenant: TenantCtx::new(EnvId::try_from("prod").unwrap(), TenantId::try_from("tenant-a").unwrap()),
    auth_token: Some("token123".into()),
    extra_headers: None,
    request_timeout: None,
};
let client = HttpDistributorClient::new(config)?;
let resp = client.resolve_component(ResolveComponentRequest {
    tenant: TenantCtx::new(
        EnvId::try_from("prod").unwrap(),
        TenantId::try_from("tenant-a").unwrap(),
    ),
    environment_id: DistributorEnvironmentId::from("env-1"),
    pack_id: "pack-123".into(),
    component_id: "component-x".into(),
    version: "1.0.0".into(),
    extra: json!({}),
}).await?;
println!("artifact: {:?}", resp.artifact);
println!(
    "secret requirements present: {}",
    resp.secret_requirements.is_some()
);
```

Fetch typed pack status (includes secret requirements):

```rust
let status = client
    .get_pack_status_v2(
        &TenantCtx::new(EnvId::try_from("prod")?, TenantId::try_from("tenant-a")?),
        &DistributorEnvironmentId::from("env-1"),
        "pack-123",
    )
    .await?;
println!(
    "status: {}, secret requirements present: {}",
    status.status,
    status.secret_requirements.is_some()
);
```

## greentic-dist CLI (feature `dist-cli`)
Build with the CLI feature to get the `greentic-dist` binary:

```bash
cargo run --features dist-cli --bin greentic-dist -- resolve ghcr.io/greenticai/components/templates:latest
```

Commands:
- `resolve <REF>`: print digest (use `--json` for structured output).
- `pull <REF>`: ensure cached; prints path. Use `--lock <pack.lock>` to pull all components from a lockfile.
- `cache ls|rm|gc`: list cache entries, remove entries through retention-aware eviction, or clean orphaned cache state.
- `auth login <tenant> [--token <token>]`: save GHCR auth for `store://greentic-biz/<tenant>/...`; if `--token` is omitted, the CLI prompts without echoing the token.

Control cache location with `--cache-dir` or `GREENTIC_DIST_CACHE_DIR`; defaults to `${XDG_CACHE_HOME:-~/.cache}/greentic/components/<sha256>/component.wasm`. Set `GREENTIC_SILENCE_DEPRECATION_WARNINGS=1` to silence the temporary `greentic-distributor-client` shim binary warning.

Exit codes:
- `0` success
- `2` invalid input (bad ref/lockfile/missing args)
- `3` not found (cache miss)
- `4` offline blocked (network needed)
- `5` auth required/missing credentials (repo://, store://)
- `10` internal error

## Library API (feature `dist-client`)
Use `DistClient` to reuse the same resolution and cache logic programmatically. The `dist-client` feature also includes the OCI pack fetcher APIs (`fetch_pack`, `OciPackFetcher`).

`OciPackFetcher` accepts the default Greentic pack media types, including `application/vnd.greentic.gtpack.layer.v1+tar`, plus opaque tarball-style OCI layer media types ending in `+tar`, `+tar+gzip`, or `+tar+zstd`. Downstream clients can append exact custom layer media types through `PackFetchOptions::add_accepted_layer_media_type(...)` without copying the default allowlist, which is also exposed via `default_pack_layer_media_types()`.

For callers that prefer the crate-level helpers over constructing `OciPackFetcher` directly, use `fetch_pack_with_options(...)` or `fetch_pack_to_cache_with_options(...)`. Test-only or advanced integrations can also inject a custom registry client through `fetch_pack_with_options_and_client(...)` / `fetch_pack_to_cache_with_options_and_client(...)`.

The canonical PR-01 flow is:

1. build an `ArtifactSource`
2. call `resolve(...)` to get an `ArtifactDescriptor`
3. call `fetch(...)` to materialize a local `ResolvedArtifact`
4. later call `open_cached(...)` or `stat_cache(...)` using the digest/cache key

```rust
use greentic_distributor_client::dist::{
    ArtifactSource, ArtifactSourceKind, CachePolicy, DistClient, DistOptions, ResolvePolicy,
};

let client = DistClient::new(DistOptions::default());
let source = ArtifactSource {
    raw_ref: "file:///tmp/my-component.wasm".into(),
    kind: ArtifactSourceKind::File,
    transport_hints: Default::default(),
    dev_mode: true,
};

let descriptor = client.resolve(source, ResolvePolicy).await?;
let resolved = client.fetch(&descriptor, CachePolicy).await?;
let reopened = client.open_cached(&descriptor.digest)?;
println!("canonical ref: {}", descriptor.canonical_ref);
println!("path: {}", reopened.local_path.display());
```

Compatibility helpers like `resolve_ref(...)` and `ensure_cached(...)` still exist, but the digest-first descriptor/fetch/open flow is now the authoritative contract.

The cache entry format is now persisted alongside fetched artifacts as a versioned `entry.json` record under a digest-keyed cache tree.

The embedded lifecycle contract also exposes:
- `stage_bundle(...)` to resolve, fetch, verify, and persist a stable `bundle_id`
- `warm_bundle(...)` to reopen cached artifacts, rerun verification, and hand off to an `ArtifactOpener`
- `rollback_bundle(...)` to reopen a previously staged bundle by `bundle_id` without network access
- `stat_bundle(...)` / `list_bundles(...)` to inspect the persisted local `bundle_id -> cache_key/canonical_ref` reopen index
- `set_bundle_state(...)` to transition bundle records between `staged`, `warming`, `ready`, `draining`, and `inactive`
- `evaluate_retention(...)` / `apply_retention(...)` for deterministic GC decisions that protect active/session/rollback-relevant bundles

That means the production-safe offline path is now:
1. `stage_bundle(...)`
2. persist the returned `bundle_id` and `canonical_ref` in operator state
3. `warm_bundle(...)` for readiness checks and open-mode handoff
4. `rollback_bundle(...)` by `bundle_id` if a later activation must be reverted
5. `apply_retention(...)` when the operator wants explainable cache GC

The persisted bundle index is now strict rather than fail-open:
- malformed bundle record files surface as cache errors
- automatic cache-pressure protection uses bundle lifecycle state, not just bundle existence

`ArtifactOpener` is intentionally format-neutral. `greentic-distributor-client` owns reopen/verification/cache concerns; the owning crate for the artifact format should provide the actual open/parse implementation.

Older cache helpers such as `evict_cache(...)`, `remove_cached(...)`, and `gc()` remain for compatibility, but they are now wrappers around or adjacent to the retention-aware cache lifecycle and should not be the primary production integration surface.

The public source model supports:
- `oci://...`
- `https://...`
- `file://...` or existing local paths
- `fixture://...` when `fixture-resolver` is enabled
- `repo://...` and `store://...` as source kinds

`repo://...` is still a placeholder mapping source kind. `store://greentic-biz/<tenant>/<package-path>` maps to `ghcr.io/greentic-biz/<package-path>` and uses credentials saved with `auth login <tenant>`. For bundles published under a namespace, use paths such as `store://greentic-biz/<tenant>/bundles/zain-x-bundle:latest`.

Compatibility-only example:

```rust
use greentic_distributor_client::dist::{DistClient, DistOptions};

let client = DistClient::new(DistOptions::default());
let resolved = client.ensure_cached("file:///tmp/my-component.wasm").await?;
println!("digest: {}, path: {}", resolved.digest, resolved.cache_path.unwrap().display());
```

`ResolvedArtifact` now includes additive optional metadata:
- `describe_artifact_ref: Option<String>`
- `content_length: Option<u64>`
- `content_type: Option<String>`

When `describe_artifact_ref` is present, it is advisory only. WASM `describe()` remains authoritative; downstream tools must verify any cached describe artifact against the wasm-derived `describe_hash`.

### Integration examples
- Resolve a ref: `greentic-dist resolve oci://ghcr.io/greenticai/components/hello-world:1`
- Pull everything from a lockfile: `greentic-dist pull --lock pack.lock.json`
- Offline workflow: `greentic-dist pull --lock pack.lock.json` then `greentic-runner run mypack.gtpack --offline`

### Using greentic-config-types (host-resolved config)
Resolve configuration in your host binary with `greentic-config` and map it into the distributor client with `DistributorClientConfig::from_greentic`:

```rust
use greentic_config_types::GreenticConfig;
use greentic_distributor_client::{DistributorClientConfig, DistributorEnvironmentId, DistributorClient, TenantCtx, TenantId};

let cfg: GreenticConfig = /* resolved in the host via greentic-config */;
let tenant = TenantCtx::new(cfg.environment.env_id.clone(), TenantId::try_from("tenant-a")?);
let mut client_cfg = DistributorClientConfig::from_greentic(&cfg, tenant)
    .with_base_url("https://distributor.example.com"); // host still provides the endpoint/auth
// pass client_cfg to your chosen DistributorClient implementation
```

## Local dev distributor
Use the companion `greentic-distributor-dev` crate to serve packs/components from a local directory, useful for greentic-dev and conformance tests:

```rust
use greentic_distributor_client::{ChainedDistributorSource, DistributorSource, PackId, Version};
use greentic_distributor_dev::{DevConfig, DevDistributorSource};

let dev_source = DevDistributorSource::new(DevConfig::default());
let sources = ChainedDistributorSource::new(vec![Box::new(dev_source)]);

let pack_bytes = sources.fetch_pack(&PackId::try_from("dev.local.hello-flow")?, &Version::parse("0.1.0")?);
println!("Loaded {} bytes", pack_bytes.len());
```

## Repo maintenance
- Enable GitHub's "Allow auto-merge" setting for the repository.
- Configure branch protection with the required checks you want enforced before merges.
