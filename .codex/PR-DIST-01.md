# PR-DIST-01 — greentic-distribution-client: 0.6.0-aware resolve API (digest-first) + optional contract metadata

Repo: `greentic-distribution-client`

## Goal
Ensure all downstream tools (flow/pack/operator/runner) can reliably resolve component artifacts for 0.6.0:
- Resolve to immutable digest and fetch wasm payload (bytes or cached path).
- Return minimal metadata helpful for locking and caching.
- Provide a common `ResolveRef` surface supporting `oci://`, `store://`, `repo://`, `file://`.
- Keep API deterministic and testable.

## Decisions locked (2026-02-13)
- Target ABI: **greentic:component@0.6.0** world `component-v0-v6-v0`.
- Contract authority: **WASM `describe()`** is source of truth (operations + inline SchemaIR + config_schema).
- Validation: strict by default (no silent accept). Any escape hatches must be explicit flags.
- Encodings: CBOR everywhere; use canonical CBOR encoding for stable hashing and deterministic artifacts.
- Hashes:
  - `describe_hash = sha256(canonical_cbor(typed_describe))`
  - `schema_hash = sha256(canonical_cbor({input, output, config}))` recomputed from typed SchemaIR values.
- i18n: `component-i18n.i18n-keys()` required for 0.6.0 components; QA specs must reference only known keys.
- Output type: use one unified `ResolvedArtifact` for all resolve APIs.
- Wasm payload rule: response must contain exactly one of `wasm_bytes` or `wasm_path`.
- Provide helper `fn wasm_bytes(&self) -> Result<&[u8]>` that loads from path when needed.
- `abi_version` is best-effort `Option<String>`; unknown is `None`.
- Backward compatibility: preserve existing public endpoints/fields; only additive changes.
- Semver: minor bump only (no major for this work).

## API shape
- `resolve_ref(...) -> ResolvedArtifact`
- `resolve_component(req) -> ResolvedArtifact`
- `ResolveRefRequest` and `ResolveComponentRequest` may differ as inputs, but output is unified.

## Scope
### In-scope
- Extend unified resolve response to include:
  - `resolved_digest` (mandatory)
  - exactly one of `wasm_bytes` or `wasm_path`
  - `component_id` (canonical)
  - `abi_version` (if known)
- Add `resolve_ref(ref: &str, opts: ResolveOpts)`.
- Add `ResolveRefRequest` while keeping existing request/endpoint surfaces.
- Support schemes as first-class inputs: `file://`, `oci://`, `store://`, `repo://`.
- Add `fixture://` resolver for tests, behind feature flag and/or dependency injection.
- Add deterministic disk cache keyed by `resolved_digest`.

### Out-of-scope
- Running WASM (no introspection here)
- Returning trusted describe bytes by default (that remains a tool responsibility)
- Schema/describe verification (handled in downstream tools)

## Canonicalization rules
- `component_id`: use canonical id from metadata/index/manifest/describe artifact when available without running wasm.
- Fallback deterministic derivation:
  - `oci://repo/name[:tag|@digest]` -> `repo/name` (drop tag/digest)
  - `store://<id>` -> `<id>`
  - `repo://<id>` -> `<id>`
  - `file://path` -> basename without extension (e.g., `foo__0_6_0` -> `foo`) unless an embedded deterministic id file exists.
- No smart guessing beyond these rules.

## Scheme behavior
- `file://`: load local artifact, compute/return digest, return `wasm_path` preferred (or `wasm_bytes`).
- `oci://`: fetch by tag/digest, return immutable `resolved_digest`, cache by digest.
- `store://`: resolve via store registry, then same flow as resolved artifact source.
- `repo://`: resolve via repo registry, then same flow as resolved artifact source.

## Implementation tasks
1) API updates
- `ResolveComponentRequest` supports ref strings and contextual resolution (tenant/pack/env optional).
- Add `ResolveRefRequest` path for direct refs.
- Return unified `ResolvedArtifact` from both resolve paths.
- Ensure strict additive backward compatibility.
- Add helper `ResolvedArtifact::wasm_bytes()`.

2) Deterministic caching
- Add disk cache enabled by default.
- Cache key: `resolved_digest`.
- Default location: `~/.greentic/cache/distribution/` (or existing greentic cache dir if already defined).
- Allow override via `GREENTIC_CACHE_DIR` and/or explicit option.
- Eviction: size-cap LRU (default cap 2-5 GB), configurable.
- No TTL.
- Optional small in-memory layer is allowed but disk cache is canonical.

3) Tests
- Unit tests for ref parsing and routing.
- Tests for unified `ResolvedArtifact` payload invariant (exactly one of bytes/path).
- Offline tests using fixture resolver (feature + DI coverage).
- Tests for deterministic `component_id` derivation rules.

4) Error taxonomy
- Add/standardize `ResolveError` variants:
  - `InvalidRef`
  - `NotFound`
  - `Unauthorized`
  - `Network`
  - `CorruptArtifact`
  - `UnsupportedAbi`
  - `CacheError`
- Keep verification failures outside dist client.

## Acceptance criteria
- Resolve returns digest + wasm payload reliably.
- Response always has exactly one of `wasm_bytes` or `wasm_path`.
- `resolve_ref` and `resolve_component` both return unified `ResolvedArtifact`.
- Supports required schemes: `file://`, `oci://`, `store://`, `repo://`.
- Optional `fixture://` remains test-only.
- Cache is deterministic and digest-keyed.
- `cargo test` passes.
