# PR-01 — Canonical bundle reference resolution, descriptor model, and cache layout

## Goal

Turn `greentic-distributor-client` into the single open-source way the operator resolves and opens production bundle references.

This PR defines the public DTOs and cache layout needed for:
- stage
- warm
- activate
- rollback

without depending on private distributor internals.

This is also the contract-collapse PR:
- replace overlapping fetch/cache entrypoints with one canonical artifact contract
- deprecate ad hoc metadata scraping from cache files
- define the durable public shapes that later PRs build on

## Why this PR matters

The architecture now depends on the embedded client, not on a public distributor service contract.
So the client must own a precise and testable model for:
- reference parsing
- digest-pinned resolution
- descriptor retrieval
- local cache opening
- deterministic rollback reopen

## Public DTOs to add or normalize

### `ArtifactSource`
Represents the user/admin supplied source before resolution.

Required fields:
- `raw_ref: String`
- `kind: ArtifactSourceKind`
- `transport_hints: TransportHints`
- `dev_mode: bool`

`ArtifactSourceKind` must explicitly distinguish:
- `Oci`
- `Https`
- `File`
- `Fixture`
- `Repo`
- `Store`

`Repo` and `Store` are retained as public placeholder source kinds for future resolution paths.
They are accepted as pre-resolution inputs, but they are never authoritative staged identities.

### `ArtifactDescriptor`
Represents a resolved descriptor independent of local caching.

Required fields:
- `artifact_type` (`bundle`, `pack`, `component`)
- `source_kind`
- `raw_ref`
- `canonical_ref`
- `digest`
- `media_type`
- `size_bytes`
- `created_at` if available
- `annotations` / metadata map
- optional `manifest_digest`
- `resolved_via` (`direct`, `tag_resolution`, `repo_mapping`, `store_mapping`, `fixture`, `file`, `https`)
- optional `signature_refs`
- optional `sbom_refs`

### `ResolvedArtifact`
Represents a locally usable resolved artifact.

Required fields:
- `descriptor: ArtifactDescriptor`
- `cache_key`
- `local_path`
- `fetched_at`
- `integrity_state`
- `source_snapshot` (minimal record of where it came from)

### `CacheEntry`
Persistent on-disk metadata alongside the cached artifact.

Required fields:
- `format_version`
- `cache_key`
- `digest`
- `media_type`
- `size_bytes`
- `artifact_type`
- `source_kind`
- `raw_ref`
- `canonical_ref`
- `fetched_at`
- `last_accessed_at`
- `last_verified_at`
- `state` (`partial`, `ready`, `corrupt`, `evicted`)
- optional `advisory_epoch`
- optional `signature_summary`

The cache entry is the only authoritative persisted metadata format for the new client contract.
The client must not reconstruct canonical metadata by recursively scraping arbitrary JSON from cached artifacts.

## Reference normalization rules

### Production rules
- production staging must be **digest-first**
- tag refs may be accepted only as an explicit pre-resolution input, never as the final staged identity
- after resolution, the operator stores and uses only the canonical digest-pinned ref
- `repo://...` and `store://...` may be accepted as placeholder input refs, but stage must still end with a canonical digest-pinned identity

### Required normalization behavior
For `oci://...:tag`:
1. parse successfully
2. resolve descriptor
3. extract digest and media type
4. produce canonical `oci://...@sha256:...`
5. return both raw and canonical ref in the result
6. mark the original tag input as non-authoritative

For `repo://...` or `store://...`:
1. parse successfully as placeholder source kinds
2. attempt configured mapping or resolver-backed pre-resolution if available
3. if resolution succeeds, produce a canonical digest-pinned identity
4. if no resolver is configured, fail with a typed resolution-unavailable error rather than silently rewriting to an ambiguous identity

### Refusal cases
Reject at the client boundary:
- missing digest after required resolution
- mismatched media type
- unsupported artifact type
- ambiguous descriptor
- digest mismatch after download
- unresolved `repo://` / `store://` placeholder input when no configured resolution path exists

## Cache layout

Use a deterministic cache rooted under a caller-supplied directory, for example:

```text
<cache_root>/
  artifacts/
    sha256/
      ab/
        abcdef.../
          blob
          entry.json
          locks/
  descriptors/
  temp/
```

### Required properties
- digest-keyed, not URL-keyed
- atomic move from temp to final location
- reusable across process restarts
- safe concurrent fetch of same digest
- recoverable after interrupted downloads
- versioned on-disk format
- legacy cache layouts may be treated as non-authoritative and need not be migrated in place

## Operator integration contract

The client must expose an API shape equivalent to:

- `resolve(source, policy) -> ArtifactDescriptor`
- `fetch(descriptor, cache_policy) -> ResolvedArtifact`
- `open_cached(digest or cache_key) -> ResolvedArtifact`
- `stat_cache(digest) -> CacheEntry`
- `evict_cache(predicate) -> RetentionReport`

The new contract should replace the current overlapping entrypoints rather than sit beside them.
That includes replacing the operator-facing role currently spread across `DistClient`, OCI-specific fetchers, and cache-specific reopen helpers.

## Migration and deletion guidance

This PR should explicitly deprecate or remove legacy overlap where practical:
- `dist::ResolveComponentRequest`
- heuristic cache metadata reconstruction helpers
- duplicate public cache result shapes that do not map onto the new DTOs

This PR may keep `repo://` and `store://` in the public model as placeholders for future resolution work,
but they should be represented explicitly in the DTOs rather than as hidden ad hoc aliases.

## Error contract

This PR should also introduce the unified public error boundary that later PRs build on.

At minimum, the new contract should stop exposing unrelated top-level fetch/cache error families for the main operator path:
- `DistError`
- `OciComponentError`
- `OciPackError`
- `RunnerApiError`

The operator uses these APIs as follows:
- `stage_bundle` calls `resolve` + `fetch`
- `warm_bundle` uses the returned `ResolvedArtifact`
- `rollback` reopens the previously staged artifact via `open_cached`

## Tests

### Unit
- ref parsing matrix
- oci tag -> digest normalization
- digest mismatch rejection
- media type enforcement
- cache-entry serialization

### Integration
- fetch once, reopen after process restart
- concurrent fetch of same digest yields one final cache entry
- rollback reopen without network
- fixture source path parity with real path
- unresolved `repo://` / `store://` fails with typed resolution-unavailable behavior when no resolver is configured
