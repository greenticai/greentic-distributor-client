# PR-04 — Offline rollback, retention rules, and cache GC decisions

## Goal

Make offline survival a first-class property of the embedded client:
- staged bundles remain reopenable without live registry access
- retention never breaks active or session-referenced bundles
- GC is deterministic, explainable, and testable

## Core principle

Once a bundle is successfully staged, later activation or rollback must not depend on:
- live registry access
- a working object store
- tag re-resolution
- private distributor availability

The cache becomes part of the runtime safety model.

This PR assumes `PR-01` introduced stable cache metadata and `PR-03` established a stable bundle-to-cache identity relationship.

## Public DTOs

### `RetentionInput`
Fields:
- `entries`
- `active_bundle_ids`
- `staged_bundle_ids`
- `warming_bundle_ids`
- `ready_bundle_ids`
- `draining_bundle_ids`
- `session_referenced_bundle_ids`
- `max_cache_bytes`
- `max_entry_age`
- `minimum_rollback_depth`
- `environment`

### `RetentionDecision`
For each entry:
- `cache_key`
- `bundle_id`
- `decision` (`keep`, `evict`, `protect`)
- `reason_code`
- `reason_detail`

### `RetentionReport`
Fields:
- `scanned_entries`
- `kept`
- `evicted`
- `protected`
- `bytes_reclaimed`
- `refusals`

## Required retention rules

### Never evict
- currently active bundle
- previous bundle eligible for rollback
- any bundle referenced by live session state
- any bundle currently in `Staged`, `Warming`, `Ready`, or `Draining`

### Eviction candidates
Only after the above protections are applied:
- corrupt entries
- superseded entries beyond rollback depth
- aged entries beyond retention window
- entries over cache budget, oldest-first or least-recently-used after protection filter

Corrupt entries are not automatically evictable if they are still protected by active, rollback, or session continuity rules.

## Session continuity integration

Because the production architecture stores `session.bundle_id_assigned`,
the operator must pass the set of session-referenced bundle ids into the client retention evaluator.

The client must not query Redis directly. That boundary stays in the operator.

## Deterministic ordering rules

The retention evaluator should define a stable decision order, for example:
1. protection class
2. integrity state
3. rollback eligibility
4. age or last access time
5. cache key as final tie-breaker

This is needed so GC behavior is explainable and testable under cache pressure.

## Required persisted relationships

This PR should require a stable local relationship such as:
- `bundle_id -> cache_key`
- `bundle_id -> canonical_ref`

without depending on network resolution or tag re-resolution at rollback time.

Retention should evaluate both:
- cached artifact entries
- bundle-associated stage records or reopen indices, if those are persisted separately

## Offline rollback tests

### Required scenarios
1. stage bundle A
2. stage bundle B
3. activate B
4. simulate remote outage
5. rollback to A from cache only
6. verify warm/activate succeeds without network

### Corruption scenario
1. cached entry metadata exists
2. blob is missing or digest mismatched
3. rollback fails with `CacheCorrupt`, not generic fetch failure
4. audit-safe error details preserved

## GC tests

- active bundle protected
- session-referenced bundle protected
- rollback depth respected
- corrupt entries evicted first when safe
- cache budget pressure produces deterministic decision order
- ties are broken deterministically by cache key or equivalent stable identity

## Operator updates implied

The existing operator PRs do not need new folders, but where they mention distributor behavior, replace that assumption with:
- the operator obtains remote artifacts only through `greentic-distributor-client`
- rollback uses client cache reopen
- GC uses client `RetentionReport`
- offline activation tests are integration points between operator and client

## Migration note

Existing legacy flat caches do not carry enough lifecycle metadata for production retention guarantees.
This PR should define retention behavior only for the new cache format introduced by `PR-01`, with legacy caches treated as best-effort compatibility inputs at most.
