# PR-01 — Completion Note

## Implemented

- Added canonical artifact DTOs in `src/dist.rs`:
  - `ArtifactSource`
  - `ArtifactDescriptor`
  - `ResolvedArtifact`
  - `CacheEntry`
  - supporting enums for source kind, artifact type, integrity, and resolution path
- Added the digest-first API surface:
  - `parse_source(...)`
  - `resolve(...)`
  - `fetch(...)`
  - `open_cached(...)`
  - `stat_cache(...)`
- Switched first-party CLI resolution and pull paths onto the new descriptor/fetch flow.
- Added a versioned digest-keyed cache layout with `blob` + `entry.json`.
- Preserved `repo://` and `store://` as explicit placeholder source kinds with typed resolution-unavailable failure when no mapping is configured.
- Split OCI descriptor resolution away from cache materialization so `resolve(...)` and `fetch(...)` are distinct for OCI inputs too.
- Deprecated legacy overlap:
  - `ResolveRefRequest`
  - `dist::ResolveComponentRequest`
  - `resolve_ref(...)`
  - `resolve_component(...)`
  - `ensure_cached(...)`

## Remaining gap

- The compatibility wrappers still exist for downstream transition.
- The new cache path is digest-keyed and versioned, but it still does not implement explicit atomic temp-to-final moves or fetch locks.

## Verification

- `cargo test --features dist-client`
