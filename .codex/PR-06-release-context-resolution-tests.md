# PR-06: Release/Channel Resolution Tests

Repo: `greentic-distributor-client`

## Goal

Add focused tests for the `greentic-distributor-client` side of release/channel-aware resolution. Air-gap archive tests belong in `gtc`; this repo should test local index lookup, cache validation, fallback behavior, and offline semantics.

## Test Cases

### Resolution Without Context

- Given `oci://...:stable`
- When using the existing `resolve(...)` API
- Then behavior is unchanged and the resolver uses existing remote tag resolution.

### Resolution With Valid Context

- Given `<cache_dir>/release-index/v1/stable/1.0.16.json`
- And a matching `artifacts/sha256/.../blob` plus `entry.json`
- When resolving `oci://...:stable` through the new context-aware API
- Then it returns the indexed digest/canonical ref without network.

### Missing Index

- Given no release index file
- When resolving with context while online
- Then it falls back to existing remote resolution.

### Invalid Index Entry

- Cases:
  - invalid digest string
  - canonical ref is not digest-pinned
  - canonical ref digest does not match `digest`
  - malformed JSON
- Online mode: fall back to remote resolution.
- Offline mode: fail with the existing offline/cache-miss style error.

### Missing Cached Blob

- Given a valid index and `entry.json`, but no cached `blob`
- Online mode: fall back to remote resolution.
- Offline mode: fail without network.

### Corrupt Cache Metadata

- Given a valid index and `blob`, but malformed `entry.json`
- Online mode: fall back to remote resolution.
- Offline mode: fail without network.

### Digest-Pinned Regression

- Existing `oci://...@sha256:...` path remains cache-first.
- No release-index lookup should occur for digest-pinned refs.

## Fixtures

Tests should construct the real `DistClient` cache layout:

```text
<cache_dir>/artifacts/sha256/<aa>/<remaining-62-hex>/blob
<cache_dir>/artifacts/sha256/<aa>/<remaining-62-hex>/entry.json
<cache_dir>/release-index/v1/<channel>/<release>.json
```

Avoid adding a parallel test-only cache layout.

