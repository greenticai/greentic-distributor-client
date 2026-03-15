# PR-00 Audit - Current `greentic-distributor-client` Contract

## Scope audited

This audit covers the current open-source crate as checked in on 2026-03-06. It maps:

- public API and feature surface
- module and entrypoint layout
- supported source kinds and parsing behavior
- fetch and cache pipelines
- trust and verification behavior
- tests and fixtures
- deletion and migration candidates for `PR-01` through `PR-04`

Primary code references:

- [Cargo.toml](/projects/ai/greentic-ng/greentic-distributor-client/Cargo.toml)
- [src/lib.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/lib.rs)
- [src/dist.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs)
- [src/oci_components.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs)
- [src/oci_packs.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs)
- [src/runner_api.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs)
- [src/http.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/http.rs)
- [src/wit_client.rs](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs)
- [greentic-distributor-dev/src/lib.rs](/projects/ai/greentic-ng/greentic-distributor-client/greentic-distributor-dev/src/lib.rs)

## Executive summary

The crate currently exposes three overlapping families of behavior rather than one coherent production artifact client:

1. `DistributorClient` for WIT and HTTP RPC access to a distributor service.
2. `DistClient` for local reference resolution, OCI/HTTP/file fetch, ad hoc cache management, and CLI support.
3. Separate low-level OCI fetchers for components, packs, and digest-addressed blobs.

That overlap is the main architectural issue. The crate already contains useful building blocks for `PR-01`:

- digest-aware OCI resolution
- offline cache hits for digest-pinned OCI refs
- simple cache metadata persistence
- lockfile parsing
- typed fetch errors at subsystem level

But it does not yet match the production contract required by `PR-00` onward:

- no canonical `ArtifactSource` / `ArtifactDescriptor` / `ResolvedArtifact` model
- no single cache layout across components, packs, and digest blobs
- no explicit descriptor-time versus post-download verification pipeline
- no signature, issuer, SBOM, advisory, or denylist enforcement
- no deterministic rollback-oriented retention model
- no typed operator lifecycle surface for stage/warm/activate/rollback

## 1. Public API map

### Feature flags

From [Cargo.toml:17](/projects/ai/greentic-ng/greentic-distributor-client/Cargo.toml#L17):

- `http-runtime`: enables `reqwest` HTTP runtime client.
- `oci-components`: enables OCI component resolver.
- `pack-fetch`: enables OCI pack fetcher.
- `runner-api`: enables digest-addressed blob fetcher.
- `dist-client`: enables `DistClient`; also pulls in `oci-components`, `reqwest`, and `pack-fetch`.
- `dist-cli`: enables CLI binaries and transitively `dist-client`.
- `fixture-resolver`: extends `DistClient` with `fixture://`.

Default feature is `dist-cli`, which means the default crate build is not a minimal library surface; it includes the CLI-oriented `DistClient` path by default.

### Top-level exports

From [src/lib.rs:1](/projects/ai/greentic-ng/greentic-distributor-client/src/lib.rs#L1):

- Always exported:
  - `config`, `error`, `source`, `types`
  - `DistributorClientConfig`
  - `DistributorError`
  - `ChainedDistributorSource`, `DistributorSource`
  - all items re-exported from `types`
  - `DistributorApiBindings`, `GeneratedDistributorApiBindings`, `WitDistributorClient`
  - `DistributorClient` trait
- Feature-gated exports:
  - `dist::{DistClient, DistOptions, InjectedResolution, LockHint, ResolveRefInjector, ResolveRefRequest, ResolvedArtifact}`
  - `HttpDistributorClient`
  - `oci_components::{ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentError, OciComponentResolver, ResolvedComponent}`
  - `oci_packs::{OciPackError, OciPackFetcher, PackFetchOptions, ResolvedPack, fetch_pack, fetch_pack_to_cache}`

### Public traits

- `DistributorClient`: async RPC-oriented trait for resolve/status/warm APIs, not artifact fetch APIs. [src/lib.rs:46](/projects/ai/greentic-ng/greentic-distributor-client/src/lib.rs#L46)
- `DistributorApiBindings`: abstraction over generated WIT imports. [src/wit_client.rs:15](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L15)
- `DistributorSource`: synchronous file-like source for pack/component bytes by `(id, version)`. [src/source.rs:4](/projects/ai/greentic-ng/greentic-distributor-client/src/source.rs#L4)
- Three separate `RegistryClient` traits exist in `oci_components`, `oci_packs`, and `runner_api`; they are unrelated duplicate abstractions.

### Public structs and enums

RPC/WIT surface:

- `DistributorClientConfig` [src/config.rs:11](/projects/ai/greentic-ng/greentic-distributor-client/src/config.rs#L11)
- `ResolveComponentResponse` [src/types.rs:16](/projects/ai/greentic-ng/greentic-distributor-client/src/types.rs#L16)
- `PackStatusResponse` [src/types.rs:28](/projects/ai/greentic-ng/greentic-distributor-client/src/types.rs#L28)
- `HttpDistributorClient` [src/http.rs:10](/projects/ai/greentic-ng/greentic-distributor-client/src/http.rs#L10)
- `WitDistributorClient` [src/wit_client.rs:45](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L45)
- `GeneratedDistributorApiBindings` [src/wit_client.rs:57](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L57)

Dist/CLI surface:

- `DistOptions` [src/dist.rs:18](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L18)
- `ResolvedArtifact` [src/dist.rs:60](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L60)
- `LockHint` [src/dist.rs:163](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L163)
- `ArtifactSource` [src/dist.rs:173](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L173)
- `DistClient` [src/dist.rs:182](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L182)
- `OciCacheInspection` [src/dist.rs:191](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L191)
- `ResolveRefRequest` [src/dist.rs:199](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L199)
- `dist::ResolveComponentRequest` [src/dist.rs:204](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L204)
- `InjectedResolution` [src/dist.rs:212](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L212)
- `DistError` [src/dist.rs:719](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L719)

OCI component surface:

- `ComponentsExtension`, `ComponentsMode`, `ComponentResolveOptions`, `ResolvedComponent`, `OciComponentResolver`, `OciComponentError` [src/oci_components.rs:47](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L47)

OCI pack surface:

- `PackFetchOptions`, `ResolvedPack`, `OciPackFetcher`, `OciPackError` [src/oci_packs.rs:46](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L46)

Runner digest surface:

- `DigestFetchOptions`, `DigestRef`, `DigestRefInput`, `CacheInfo`, `FetchMetadata`, `CachedDigest`, `DigestFetcher`, `RunnerApiError` [src/runner_api.rs:36](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L36)

Dev helper crate:

- `DevLayout`, `DevConfig`, `DevDistributorSource` [greentic-distributor-dev/src/lib.rs:9](/projects/ai/greentic-ng/greentic-distributor-client/greentic-distributor-dev/src/lib.rs#L9)

### Error model

There is no unified error taxonomy across the crate:

- `DistributorError` for WIT/HTTP RPC failures. [src/error.rs:8](/projects/ai/greentic-ng/greentic-distributor-client/src/error.rs#L8)
- `DistError` for CLI-oriented resolution/cache behavior. [src/dist.rs:719](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L719)
- `OciComponentError` and `OciPackError` for OCI fetch paths.
- `RunnerApiError` for digest-addressed blob fetches.

Implication: the current public contract does not give the operator one typed error surface for stage/warm/rollback mapping.

## 2. Module and entrypoint map

### Runtime entrypoints

- `DistributorClient` implementations:
  - `WitDistributorClient` maps to imported guest bindings. [src/wit_client.rs:153](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L153)
  - `HttpDistributorClient` maps to `/distributor-api/*` endpoints. [src/http.rs:76](/projects/ai/greentic-ng/greentic-distributor-client/src/http.rs#L76)
- `DistClient` is a separate artifact resolver/fetch/cache API. [src/dist.rs:235](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L235)
- Standalone low-level fetchers:
  - `OciComponentResolver`
  - `OciPackFetcher`
  - `DigestFetcher`

### CLI entrypoints

- `greentic-dist`
- `greentic-distributor-client`

Both binaries require `dist-cli` and therefore use the `DistClient` path rather than the RPC `DistributorClient` path.

### Important mismatch

There are two different `ResolveComponentRequest` types in the crate:

- RPC request from `greentic-types`, re-exported from `types`. [src/types.rs:4](/projects/ai/greentic-ng/greentic-distributor-client/src/types.rs#L4)
- `dist::ResolveComponentRequest` containing only `reference`, `tenant`, `pack`, and `environment`, but only `reference` is actually used. [src/dist.rs:203](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L203), [src/dist.rs:320](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L320)

This is a strong sign of overlapping, divergent contracts.

## 3. Source-kind matrix

### Currently supported source kinds

`DistClient` supports these reference kinds via `classify_reference`. [src/dist.rs:1123](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L1123)

| Source kind | Accepted form | Notes |
| --- | --- | --- |
| Digest | `sha256:<64hex>` | Cache-local only. No repository context. |
| HTTP/HTTPS | URL parse with `http` or `https` | HTTP rejected unless loopback override enabled. |
| File URL | `file://...` | Converted via `to_file_path()`. |
| Bare file path | existing local path | Only accepted if path exists at parse time. |
| OCI | `oci://...` or any string parseable as `oci_distribution::Reference` | Tag support depends on downstream options. |
| Repo alias | `repo://...` | Mapped to OCI by config. |
| Store alias | `store://...` | Mapped to OCI by config. |
| Fixture | `fixture://...` | Only with `fixture-resolver`. |

### Parsing and normalization behavior

- Bare digest is treated as `RefKind::Digest` and resolved only from local cache. [src/dist.rs:1124](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L1124)
- `oci://` is normalized by stripping the scheme before passing to `oci_distribution::Reference`. [src/dist.rs:1135](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L1135)
- `repo://` and `store://` are not first-class artifact identities; they are rewritten by `map_registry_target` into normal OCI references when a base is configured. [src/dist.rs:1102](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L1102)
- `fixture://` is a dev-only file indirection to `<fixture_dir>/<name>.wasm`. [src/dist.rs:574](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L574)
- `DigestFetcher` requires repository context for OCI pulls and rejects bare digests. [src/runner_api.rs:224](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L224)

### Tag support

- `DistOptions` defaults `allow_tags = true`. [src/dist.rs:43](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L43)
- `ComponentResolveOptions` defaults `allow_tags = false`. [src/oci_components.rs:73](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L73)
- `PackFetchOptions` defaults `allow_tags = false`. [src/oci_packs.rs:58](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L58)
- `DistClient` passes its `allow_tags` value through to `OciComponentResolver`, so the higher-level default is permissive for OCI component refs. [src/dist.rs:244](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L244)

### Canonical identity behavior

What exists:

- OCI component and pack fetchers compute or capture a `resolved_digest`.
- `DistClient` returns `resolved_digest` and `digest`.
- Lockfiles can carry both `reference` and `digest`.

What does not exist:

- no canonical reference DTO
- no rule that tag input must become `oci://...@sha256:...`
- no persisted distinction between raw ref and canonical ref
- no descriptor object that surfaces media type, digest, annotations, and source together

## 4. Fetch pipeline sequence map

### `DistClient` pipeline

Entry: `resolve_ref(reference)` [src/dist.rs:264](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L264)

Sequence:

1. Optional `ResolveRefInjector` redirect/materialization hook runs first. [src/dist.rs:267](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L267)
2. Reference classified into digest/http/file/oci/repo/store/fixture. [src/dist.rs:277](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L277)
3. Depending on kind:
   - digest: open local cache path only
   - HTTP: download bytes with `reqwest`, hash locally, cache as `component.wasm`
   - file: read bytes, hash locally, cache as `component.wasm`
   - OCI: delegate to `OciComponentResolver`
   - repo/store: rewrite then delegate to OCI
   - fixture: rewrite to file
4. Construct `ResolvedArtifact` with ad hoc metadata extraction from cache files. [src/dist.rs:529](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L529)
5. Enforce size cap after fetch by evicting old cache entries. [src/dist.rs:658](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L658)

Observations:

- `resolve_component(req)` ignores all fields except `reference`. [src/dist.rs:320](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L320)
- there is no separate `resolve` then `fetch` boundary
- there is no descriptor-time object
- cache eviction is coupled to successful fetch/open

### OCI component pipeline

Entry: `OciComponentResolver::resolve_refs` [src/oci_components.rs:159](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L159)

Sequence:

1. Parse OCI reference.
2. Enforce digest requirement unless `allow_tags`.
3. If digest pinned, try cache hit.
4. If offline and no digest-pinned cache hit, fail.
5. Pull via anonymous HTTPS `oci-distribution` client.
6. Pick preferred layer type, optionally parse component manifest for named wasm.
7. Compute/capture resolved digest.
8. Reject if expected digest mismatches resolved digest.
9. Write artifact and `metadata.json` to cache.

Important details:

- Resolver passes `preferred_layer_media_types` into the registry pull as accepted types, not `accepted_manifest_types`. [src/oci_components.rs:203](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L203)
- The configured `accepted_manifest_types` field is currently unused in the pull path.
- The cache hit path does not recompute digest from bytes before returning.

### OCI pack pipeline

Entry: `OciPackFetcher::fetch_pack_to_cache` [src/oci_packs.rs:153](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L153)

Sequence is similar to components:

1. Parse OCI ref.
2. Enforce tag policy/offline rules.
3. If digest pinned, try cache hit.
4. Pull via anonymous HTTPS registry client.
5. Choose preferred layer.
6. Capture resolved digest.
7. Reject digest mismatch.
8. Write `pack.gtpack` and `metadata.json`.

Unlike `runner_api`, cached pack hits are not byte-verified on reopen.

### Digest fetch pipeline

Entry: `DigestFetcher::ensure_cached` [src/runner_api.rs:180](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L180)

Sequence:

1. Parse digest-pinned OCI reference.
2. If cache hit exists, verify file digest on disk before returning. [src/runner_api.rs:185](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L185)
3. Pull via anonymous HTTPS registry client.
4. Find exact layer matching requested digest.
5. Verify downloaded bytes match digest.
6. Write `blob.bin` and `metadata.json`.

This is the only path with explicit cache-hit digest verification today.

## 5. Verification and trust assumptions

### Implemented

- Digest mismatch rejection after OCI download for components and packs. [src/oci_components.rs:239](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L239), [src/oci_packs.rs:210](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L210)
- Digest verification on cached `runner_api` digest blobs. [src/runner_api.rs:185](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L185)
- HTTPS-only transport for registry clients via `ClientProtocol::Https`. [src/oci_components.rs:584](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L584), [src/oci_packs.rs analogous], [src/runner_api.rs analogous]
- HTTPS-only or loopback HTTP enforcement for `DistClient` HTTP fetches. [src/dist.rs:1176](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L1176)
- Basic component manifest filename validation for `component_wasm`. [src/oci_components.rs:320](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L320)

### Partially implemented

- Media type handling exists, but as layer selection preference rather than strict policy enforcement.
  - components prefer wasm media types but can fall back to first layer. [src/oci_components.rs:218](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L218)
  - packs prefer pack types but can fall back to first layer if none of the preferred types are present. [src/oci_packs.rs:247](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L247)
- WIT/HTTP `ResolveComponentResponse` carries `SignatureSummary`, but the client only transports it. [src/types.rs:16](/projects/ai/greentic-ng/greentic-distributor-client/src/types.rs#L16), [src/wit_client.rs:250](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L250)

### Not implemented

- signature verification
- issuer allow/deny enforcement
- digest denylist/advisory ingestion
- SBOM discovery or requirement enforcement
- descriptor/manifest signature verification
- size limit enforcement against remote metadata
- canonical artifact type checking (`bundle`, `pack`, `component`)
- verification report generation
- structured trust policy DTOs

### Assumed but not enforced

- `describe_artifact_ref` is advisory only; downstream verification is deferred to consumers. [README.md:143](/projects/ai/greentic-ng/greentic-distributor-client/README.md#L143)
- `DistClient` assumes cached component bytes remain valid for digest-named entries; it does not rehash on cache hit. [src/dist.rs:278](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L278)
- WIT errors are flattened to generic `DistributorError::Wit`, with a TODO to add structured mapping later. [src/wit_client.rs:167](/projects/ai/greentic-ng/greentic-distributor-client/src/wit_client.rs#L167)

## 6. Cache and retention map

### Current cache roots

- `DistClient`: `<GREENTIC_HOME>/cache/distribution` or `~/.greentic/cache/distribution`. [src/dist.rs:704](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L704)
- OCI components: `${XDG_CACHE_HOME}/greentic/components` or fallback variants. [src/oci_components.rs:279](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L279)
- OCI packs: `${XDG_CACHE_HOME}/greentic/packs` or fallback variants. [src/oci_packs.rs:271](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L271)
- Runner digest blobs: `${XDG_CACHE_HOME}/greentic/digests` or fallback variants. [src/runner_api.rs around default_cache_root]

There is no unified cache root or shared on-disk schema.

### Current cache keying

- All three cache families are digest-keyed at directory level.
- Layout is flat per digest directory, not partitioned by algorithm prefix or sharded subdirectories.
- Example component layout:
  - `<root>/<sha256hex>/component.wasm`
  - optional `component.manifest.json`
  - `metadata.json`
  - `last_used` in `DistClient` cache only

### Persistent metadata

Components:

- `original_reference`
- `resolved_digest`
- `media_type`
- `fetched_at_unix_seconds`
- `size_bytes`
- optional `manifest_digest`
- optional `manifest_wasm_name`

Packs:

- same minus `manifest_wasm_name`

Digest blobs:

- `digest`
- `size_bytes`
- optional `media_type`
- `fetched_at_unix_seconds`

`DistClient` component cache itself does not persist a typed cache entry object. It only stores bytes and a `last_used` marker. [src/dist.rs:797](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L797)

### Temp files / atomicity / concurrency

Not implemented:

- no temp download directory
- no atomic move into final cache path
- no lock files
- no single-flight fetch protection
- no concurrent writer coordination

Writes use direct `fs::write` into final paths. [src/dist.rs:797](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L797), [src/oci_components.rs:369](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_components.rs#L369), [src/oci_packs.rs:316](/projects/ai/greentic-ng/greentic-distributor-client/src/oci_packs.rs#L316), [src/runner_api.rs:342](/projects/ai/greentic-ng/greentic-distributor-client/src/runner_api.rs#L342)

### Cleanup / GC / rollback behavior

`DistClient` only has:

- `list_cache`
- `remove_cached`
- `gc`
- size-cap eviction based on least-recently-used marker

Problems:

- no retention rules by lifecycle state
- no rollback depth protection
- no protection for active/session-referenced bundles
- `gc` only removes directories missing `component.wasm`; it is not a budget-based or policy-based GC. [src/dist.rs:402](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L402)
- size-cap eviction runs after successful resolution and can evict older entries without bundle/session awareness. [src/dist.rs:827](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L827)

## 7. Operator-coupled or operator-shaped logic

Leak points already present:

- `dist::ResolveComponentRequest` includes `tenant`, `pack`, and `environment`, but they are ignored. [src/dist.rs:204](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L204), [src/dist.rs:320](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L320)
- `LockHint` exists to feed lockfile-style downstream workflows, but the crate has no canonical artifact descriptor model. [src/dist.rs:150](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L150)
- `repo://` and `store://` aliases look like product-specific operator/distributor shortcuts rather than general artifact source kinds. [src/dist.rs:548](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L548)
- `resolve_component_id_from_cache`, `resolve_abi_version_from_cache`, and `resolve_describe_artifact_ref_from_cache` recursively scrape arbitrary JSON from cache files. [src/dist.rs:997](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L997)

That last point is especially important: metadata reconstruction is heuristic, not schema-driven.

## 8. Test inventory

### Unit tests in library modules

- `src/source.rs`: 3 tests for chained source fallback/error behavior.
- `src/config.rs`: 1 test for config mapping.
- `src/dist.rs`: 2 unit tests for metadata extraction helpers.
- `src/oci_components.rs`: 4 unit/integration-style internal tests for layer selection and cache file naming.
- `src/runner_api.rs`: 3 unit tests for digest parsing and digest mismatch logic.

### Integration tests

- [tests/dist_client.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/dist_client.rs): 13 tests
  - file caching
  - HTTP caching
  - lockfile parsing
  - offline/insecure HTTP rejection
  - `describe.cbor` sidecar detection
  - cache-cap eviction
  - repo/store mapping
  - injector behavior
- [tests/http_client.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/http_client.rs): 9 tests for HTTP RPC behavior/status mapping.
- [tests/wit_client.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/wit_client.rs): 4 tests for WIT DTO mapping.
- [tests/oci_components.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/oci_components.rs): 10 tests for component resolver behavior with mock registry.
- [tests/oci_components_e2e.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/oci_components_e2e.rs): 1 end-to-end-style component pull test.
- [tests/oci_packs.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/oci_packs.rs): 12 tests for pack fetch behavior.
- [tests/pack_cli_parity.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/pack_cli_parity.rs): 1 parity test.
- [tests/dist_cli.rs](/projects/ai/greentic-ng/greentic-distributor-client/tests/dist_cli.rs): 1 CLI test.

### Companion dev-source tests

- [greentic-distributor-dev/tests/dev_source.rs](/projects/ai/greentic-ng/greentic-distributor-client/greentic-distributor-dev/tests/dev_source.rs): 3 tests for flat and structured file layouts plus error handling.

### Fixture and fake infrastructure

- Mock registry clients exist for OCI components and packs in integration tests.
- `httpmock` is used for HTTP runtime and `DistClient` HTTP tests.
- `fixture://` support exists behind a feature but is only a local file indirection, not a replayable fake registry.

### Test gaps relative to PR-00 follow-ups

Missing today:

- concurrent fetch collision tests
- interrupted write recovery tests
- atomic cache promotion tests
- cache corruption classification beyond `runner_api`
- rollback reopen tests
- no-network rollback tests
- retention/GC policy tests with protected entries
- advisory/trust policy tests
- operator lifecycle harness for stage/warm/rollback

## 9. Deletion candidates

### High-confidence deletion or consolidation candidates

- `repo://` and `store://` alias handling in `DistClient`
  - product-specific shorthand; should live in operator/admin translation layer unless explicitly retained as public source kinds.
- `dist::ResolveComponentRequest`
  - collides with the RPC request type and most fields are ignored.
- `ResolveRefInjector` / `InjectedResolution`
  - useful for tests, but it is an implicit extension seam that bypasses descriptor/fetch verification boundaries.
- recursive metadata scraping helpers:
  - `resolve_component_id_from_cache`
  - `resolve_abi_version_from_cache`
  - `resolve_describe_artifact_ref_from_cache`
  - these should be replaced with explicit persisted DTOs.
- duplicate `RegistryClient` abstractions across three modules
  - can be unified or hidden.

### Medium-confidence candidates

- `DistributorSource` and `greentic-distributor-dev`
  - this `(id, version) -> bytes` API is orthogonal to the digest-first artifact model. Keep only if still needed for local dev workflows outside the production bundle contract.
- `greentic-dist` cache subcommands as currently modeled
  - current `gc` and `rm` semantics are not retention-aware.
- `pull_lock`
  - lockfile support probably remains useful, but current lock parsing and reopen semantics are not aligned with canonical staged descriptors.

### Likely legacy cache formats

- flat digest directories with `component.wasm` / `pack.gtpack` / `blob.bin`
- metadata files that differ across subsystems
- ad hoc `last_used` marker file

Migration to a new digest-keyed cache tree should be acceptable if old caches are treated as non-authoritative and can be lazily repopulated.

## 10. Migration risks

### API risk

- Existing consumers may use the RPC `DistributorClient` surface and the `DistClient` surface independently.
- Renaming or removing `dist::ResolveComponentRequest` is a source-breaking change.
- The default `dist-cli` feature means current downstream builds may accidentally rely on CLI-oriented behavior.

### Behavioral risk

- `DistOptions` currently defaults `allow_tags = true`; tightening to digest-first production semantics will change behavior immediately for existing `DistClient` users. [src/dist.rs:45](/projects/ai/greentic-ng/greentic-distributor-client/src/dist.rs#L45)
- Current cache paths are externally discoverable and may be baked into scripts or tests.
- `repo://` and `store://` may be in active use even though they are not a clean long-term public contract.

### Cache migration risk

- Existing caches are not versioned.
- There is no migration marker.
- There is no rollback-safe protection metadata in the current cache.
- There is no integrity state field, so corrupt versus partial versus ready cannot be reconstructed reliably.

## 11. Recommended reshape for PR-01 handoff

### Keep as raw material

- OCI digest mismatch logic
- OCI manifest/media-type knowledge
- digest fetcher cache-hit rehash behavior
- basic HTTP transport hardening
- test scaffolding with fake registry clients

### Replace or reshape early

- unify all artifact resolution around one descriptor DTO
- separate `resolve` from `fetch`
- replace heuristic cache metadata extraction with explicit persisted records
- make digest-pinned canonical ref the only authoritative staged identity
- collapse duplicate error models into one operator-facing taxonomy
- remove product-specific alias rewriting from the client unless deliberately retained
- introduce one cache layout for artifacts, metadata, temp files, and locks

## Audit deliverables checklist

- Public API map: complete
- Module / entrypoint map: complete
- Source-kind matrix: complete
- Fetch pipeline sequence map: complete
- Trust enforcement gap list: complete
- Cache format map: complete
- Deletion candidates: complete
- Migration risk notes: complete
