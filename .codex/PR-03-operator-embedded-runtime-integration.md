# PR-03 — Embedded operator integration surface for stage / warm / activate / rollback

## Goal

Define and implement the exact embedded-runtime integration between `greentic-operator` and `greentic-distributor-client`
so there is one clear path for production bundle handling.

This PR is not about private service behavior. It is about the in-process seam.

## Why this is needed

Today the client is "embedded already", which usually leads to hidden coupling:
- helper functions with operator-specific assumptions
- fetch + verify + open mixed into one call
- lifecycle states not aligned with admin API states
- error messages that are fine for code but poor for audit/runtime reporting

This PR turns that into an explicit contract.

It must also define a strict ownership boundary so client and operator do not both persist or interpret the same lifecycle state in different ways.

## Required integration types

### `StageBundleInput`
Fields:
- `bundle_ref`
- `requested_access_mode` (`userspace`, `mount`)
- `verification_policy_ref`
- `cache_policy_ref`
- optional `tenant`
- optional `team`

### `StageBundleResult`
Fields:
- `bundle_id`
- `canonical_ref`
- `descriptor`
- `resolved_artifact`
- `verification_report`
- `cache_entry`
- `stage_audit_fields`

### `WarmBundleInput`
Fields:
- `bundle_id`
- `cache_key`
- `smoke_test`
- `dry_run`
- `expected_operator_version`

### `WarmBundleResult`
Fields:
- `bundle_id`
- `verification_report`
- `bundle_manifest_summary`
- `bundle_open_mode`
- `warnings`
- `errors`
- `warm_audit_fields`

### `RollbackBundleInput`
Fields:
- `target_bundle_id`
- optional `expected_cache_key`

### `RollbackBundleResult`
Fields:
- `bundle_id`
- `reopened_from_cache: bool`
- `cache_entry`
- `verification_report`
- `rollback_audit_fields`

## Required integration flow

### Stage
1. parse incoming `bundle_ref`
2. resolve to descriptor
3. fetch into cache
4. generate canonical digest-pinned ref
5. return stage metadata required for operator persistence
6. return all data required by warm

### Warm
1. reopen cached artifact
2. rerun verification as policy requires
3. open SquashFS in userspace mode
4. read manifest and summarize required extension/component metadata
5. return structured warm result

### Activate
The client should not flip operator state.
It only supplies the opened / verified artifact information that lets operator activate safely.

### Rollback
1. locate previous staged cache entry by bundle id / cache key
2. reopen without network
3. rerun policy verification if required
4. return structured rollback result

This PR must define whether `bundle_id` is:
- operator-issued and passed into the client, or
- client-derived and returned to the operator

That decision is required before rollback and retention semantics can be stable.

## Error model

Add a typed error taxonomy that the operator can map cleanly to admin API and audit events:

- `InvalidReference`
- `UnsupportedSource`
- `ResolutionFailed`
- `DownloadFailed`
- `ResolutionUnavailable`
- `DigestMismatch`
- `MediaTypeRejected`
- `IssuerRejected`
- `DigestDenied`
- `SignatureRequired`
- `CacheCorrupt`
- `CacheMiss`
- `OfflineRequiredButUnavailable`
- `UnsupportedArtifactType`
- `DescriptorCorrupt`
- `PolicyInputInvalid`
- `AdvisoryRejected`
- `VerificationFailed`
- `BundleOpenFailed`

Each error must carry:
- machine-readable code
- human-readable summary
- optional retryability hint
- optional audit-safe details

## Test strategy

### In client
- fake operator lifecycle harness around stage/warm/rollback inputs
- cache miss vs corrupt cache distinction
- open cached artifact after restart
- canonical ref stability across repeated staging
- repeated stage of the same canonical ref yields stable cache identity
- rollback path works with networking disabled and without tag re-resolution

### In operator
Update operator PRs to consume only the new typed results, not private helper state.

That means:
- admin `stage_bundle` handler stores `canonical_ref`
- warm report embeds `VerificationReport`
- rollback path is fixture-tested with networking disabled

## Ownership rules

The client owns:
- resolution
- fetch
- cache reopen
- verification
- typed artifact and cache results

The operator owns:
- persistence of stage/warm/admin lifecycle records unless explicitly delegated
- readiness/activation state transitions
- external audit emission
- environment-specific policy choice

The client contract should replace the current overlapping helper path rather than adding another seam beside it.
