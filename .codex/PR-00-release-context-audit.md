# PR-00-audit: OCI Resolution, Cache Model, and Release-Context Extension Design

Repo: `greentic-distributor-client`

## Goal

Audit the current OCI resolution and cache behavior, then identify safe insertion points for release/channel-aware mutable tag resolution. This PR is documentation and test-planning only.

## Current Behavior To Document

- `oci://...@sha256:...`
  - Resolve by digest.
  - Check local cache first when the digest is known.
  - Use remote only when the digest is not already cached and offline mode is not active.
- `oci://...:<version>`
  - Treated as a mutable/tagged OCI reference.
  - Current flow resolves through the registry, then persists digest-addressed cache state.
- `oci://...:stable`
  - Same as any other tag today.
  - No local `stable -> digest` index exists, so it resolves remotely unless a later API adds a release context.

## Code Paths

- `src/oci_components.rs`
  - Raw OCI component resolver.
  - `OciComponentResolver::resolve_descriptor` and `resolve_single` parse OCI refs, cache-hit digest refs, and pull tags remotely.
- `src/oci_packs.rs`
  - Raw OCI pack fetcher.
  - `OciPackFetcher::fetch_pack_to_cache` has the same digest-first/tag-remote behavior for packs.
- `src/dist.rs`
  - Higher-level `DistClient` API and primary cache lifecycle.
  - Best insertion point for non-breaking release-context-aware behavior.

## Actual Cache Layout

The authoritative `DistClient` cache root is `DistOptions.cache_dir`.

`DistOptions::default()` chooses:

1. `GREENTIC_CACHE_DIR`
2. `GREENTIC_DIST_CACHE_DIR`
3. `GREENTIC_HOME/cache/distribution`
4. `~/.greentic/cache/distribution`
5. `.greentic/cache/distribution`

Current layout:

```text
<cache_dir>/
  artifacts/sha256/<aa>/<remaining-62-hex>/
    blob
    entry.json
    last_used
  bundles/
    <bundle_id>.json
  legacy-components/
    <sha256-hex>/
      component.wasm
      metadata.json
      component.manifest.json
  legacy-packs/
    <sha256-hex>/
      pack.gtpack
      metadata.json
```

Do not use a new top-level `blobs/` or `entries/` layout for this repo unless a migration PR explicitly introduces it.

## Extension Points

Preferred insertion point:

- `DistClient` before remote tag resolution for `ArtifactSourceKind::Oci`, `Repo`, and `Store` paths where the final mapped ref is an OCI ref.

Secondary insertion points:

- `OciComponentResolver` and `OciPackFetcher` only if lower-level users need the release-index behavior without `DistClient`.

Boundaries:

- Release index lookup should happen before remote mutable tag resolution.
- Cache validation should happen after index lookup and before returning a descriptor.
- Canonical refs returned from the index must be digest-pinned.
- Missing, malformed, or stale index entries must fall back to existing behavior unless offline mode forbids remote resolution.

## Audit Deliverables

- Document current resolution flow and cache layout.
- Identify public APIs that must remain unchanged.
- Confirm whether release-index support belongs at `DistClient`, raw OCI components/packs, or both.
- Define tests for PR-02 before implementation.

