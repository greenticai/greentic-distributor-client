# PR-04 — Completion Note

## Implemented

- Added typed retention DTOs:
  - `RetentionInput`
  - `RetentionDecision`
  - `RetentionDisposition`
  - `RetentionOutcome`
  - `RetentionEnvironment`
- Added cache/bundle inspection and retention APIs:
  - `list_cache_entries(...)`
  - `evaluate_retention(...)`
  - `apply_retention(...)`
  - `stat_bundle(...)`
  - `list_bundles(...)`
- Added persisted `BundleRecord` index for:
  - `bundle_id -> cache_key`
  - `bundle_id -> canonical_ref`
- Wired rollback to use the persisted bundle record when present.
- Made automatic cache-cap eviction retention-aware and protective of staged bundle records.
- Moved `cache rm` CLI behavior onto retention-aware eviction.
- Added cleanup of bundle records during:
  - retention eviction
  - explicit eviction
  - orphaned-cache GC
- Added tests for:
  - active/session protection
  - rollback depth
  - corrupt-first safe eviction
  - deterministic cache-key tie-breaking
  - offline rollback from persisted bundle index
  - bundle-record cleanup on eviction and GC
  - automatic cap preservation of staged bundle records

## Remaining gap

- Legacy flat cache compatibility remains best-effort; retention guarantees are defined around the new cache format and persisted bundle index.
- Older cache helpers (`evict_cache`, `remove_cached`, `gc`) still exist as deprecated compatibility paths.

## Verification

- `cargo test --features dist-client`
