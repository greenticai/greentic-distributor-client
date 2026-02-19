# Repository Overview

## 1. High-Level Purpose
- Workspace providing a WIT-based distributor client plus a dev-only filesystem-backed distributor source.
- `greentic-distributor-client`: async client trait with a WIT implementation; uses `greentic-types` DTOs and `greentic-interfaces-guest` bindings. Optional HTTP runtime client is available behind the `http-runtime` feature.
- `greentic-distributor-dev`: implements `DistributorSource` to serve packs/components from a local directory (flat or nested layouts) for local/dev flows.

## 2. Main Components and Functionality
- **Path:** `src/lib.rs`
  - **Role:** Library entrypoint exporting the `DistributorClient` trait plus the WIT client (and feature-gated HTTP client), shared types/config/errors, and source abstraction.
  - **Key functionality:** Async trait with `resolve_component`, `get_pack_status`, `warm_pack`; re-exports config/DTOs and `DistributorSource` composition helpers.
- **Path:** `src/types.rs`
  - **Role:** Re-exports distributor DTOs and IDs from `greentic-types` (TenantCtx, DistributorEnvironmentId, ComponentDigest/status, ArtifactLocation, SignatureSummary, CacheInfo, resolve request/response, EnvId/TenantId/ComponentId/PackId).
  - **Key functionality:** Leverages upstream helpers (e.g., `is_sha256_like` on ComponentDigest) and re-exports `semver::Version`.
- **Path:** `src/config.rs`
  - **Role:** Client configuration (base URL optional for HTTP, tenant/environment IDs, optional bearer token, extra headers, timeout).
- **Path:** `src/error.rs`
  - **Role:** `DistributorError` enum covering WIT/serde/invalid-response errors plus not-found/permission/other variants; HTTP-specific variants are gated behind the `http-runtime` feature.
- **Path:** `src/source.rs`
  - **Role:** `DistributorSource` trait for pack/component fetching plus `ChainedDistributorSource` for priority lookup; includes in-memory tests.
- **Path:** `src/oci_components.rs` (feature `oci-components`)
  - **Role:** Minimal OCI/GHCR component resolver with digest enforcement, HTTPS pulls (anon), caching under `${GREENTIC_HOME:-$HOME/.greentic}/cache/oci/<sha256>`, offline mode, and tag opt-in; exposes `ComponentsExtension` for `greentic.components` refs. Tested via `tests/oci_components.rs`.
- **Path:** `src/oci_packs.rs` (feature `pack-fetch`)
  - **Role:** Minimal OCI/GHCR pack fetcher with digest enforcement, HTTPS pulls (anon), caching under `${GREENTIC_HOME:-$HOME/.greentic}/cache/packs/<sha256>/pack.gtpack`, offline mode, and tag opt-in; prefers `application/vnd.greentic.pack+json` and falls back to the first layer. Tested via `tests/oci_packs.rs`.
- **Path:** `src/wit_client.rs`
  - **Role:** `WitDistributorClient` plus `DistributorApiBindings` trait to wrap actual WIT guest bindings; provides `GeneratedDistributorApiBindings` that calls distributor-api imports on WASM targets (errors on non-WASM) and handles DTO↔WIT conversions using `greentic-interfaces-guest::distributor_api` types and JSON parsing.
- **Path:** `src/dist.rs` (feature `dist-client`)
  - **Role:** `DistClient`/`DistOptions` reusable resolver/cache API for components (file/http/OCI), standardized cache layout, lockfile pulling, and digest computation.
- **Path:** `src/dist_cli.rs` + `src/bin/greentic-dist.rs` (feature `dist-cli`)
  - **Role:** `greentic-dist` CLI (with shim `greentic-distributor-client`) for resolve/pull/cache/auth (stub) commands plus `pack` fetch; defaults to `${XDG_CACHE_HOME:-~/.cache}/greentic/components/<sha256>/component.wasm`, supports `GREENTIC_DIST_CACHE_DIR`.
- **Path:** `src/http.rs` (feature `http-runtime`)
  - **Role:** `HttpDistributorClient` implementing the trait over JSON runtime endpoints (`/distributor-api/resolve-component`, `/pack-status`, `/warm-pack`); handles auth headers and status mapping.
- **Path:** `tests/wit_client.rs`
  - **Role:** WIT translation tests against a dummy binding verifying DTO↔WIT conversions and JSON parsing/warm-pack call-through.
- **Path:** `tests/http_client.rs` (feature `http-runtime`)
  - **Role:** HTTP client tests using `httpmock` for success, pack-status JSON, auth header propagation, 404/error mapping, and bad JSON handling.
- **Path:** `tests/oci_packs.rs` (feature `pack-fetch`)
  - **Role:** Tests for OCI pack fetching (preferred layer selection, caching, tag policy, offline mode, digest mismatch) using a mock registry client.
- **Path:** `greentic-distributor-dev/src/lib.rs`
  - **Role:** `DevDistributorSource` implementation reading packs/components from local disk using configurable `DevConfig` and `DevLayout` (Flat or ByIdAndVersion).
- **Path:** `greentic-distributor-dev/tests/dev_source.rs`
  - **Role:** Integration tests covering flat/nested layouts, happy paths, and not-found.
- **Path:** `README.md` / `LICENSE`
  - **Role:** Crate metadata for publication (MIT license, description/usage overview, local dev distributor usage example).
- **Path:** `.github/workflows/ci.yml`
  - **Role:** CI workflow running a bindings-path guard (`greentic_interfaces{,_guest}::bindings`), fmt, clippy, tests, and a packaging check on pushes/PRs.
- **Path:** `.github/workflows/publish.yml`
  - **Role:** Publish workflow for crates.io on tags or manual dispatch using `CARGO_REGISTRY_TOKEN`; packages/publishes both client and dev crates.
- **Path:** `ci/local_check.sh`
  - **Role:** Local helper script running a bindings-path guard (`greentic_interfaces{,_guest}::bindings`), fmt, clippy, and tests.
- **Path:** `docs/oci_packs.md`
  - **Role:** Documentation for public OCI pack fetching, cache layout, and limitations.
- **Path:** `README.md` / `LICENSE`
  - **Role:** Crate metadata for publication (MIT license, description/usage overview).

## 3. Work In Progress, TODOs, and Stubs
- WIT integration uses a pluggable `DistributorApiBindings` trait; `GeneratedDistributorApiBindings` works on WASM targets and errors on non-WASM. HTTP runtime client is feature-gated (`http-runtime`) for environments where the runtime JSON surface is available. Dev distributor is ready for greentic-dev wiring.

## 4. Broken, Failing, or Conflicting Areas
- `tests/oci_components_e2e.rs` can fail in restricted environments because it pulls `ghcr.io/greentic-ai/components/templates:latest` and currently returns `UnauthorizedError` without GHCR/network access. `ci/local_check.sh` fails when `OCI_E2E=1` under those constraints.

## 5. Notes for Future Work
- Confirm HTTP JSON field naming against the canonical distributor API and adjust serializers as needed.
- Keep packaging checks in CI; publish workflow already runs `cargo package` for both crates.
- Follow `.codex/PR-01-interfaces.md` in downstream repos: use `greentic_interfaces::canonical::*` only; do not import `greentic_interfaces::bindings::*` outside `greentic-interfaces`.
- Workspace currently patches `greentic-interfaces` to local `../greentic-interfaces` and uses local-path `greentic-interfaces-guest` for PR work.
