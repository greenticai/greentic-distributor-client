# PR-DIST-02 — greentic-distribution-client: contract-aware optional channel (describe artifact pointers) + locking helpers

Repo: `greentic-distribution-client`

## Goal
Add optional helpers to reduce repeated work in tools while keeping WASM authoritative:
- Provide optional pointers to `describe.cbor` artifacts when available (not trusted unless verified).
- Provide convenience helpers for lock writing (digest + size + source).
- Keep default behavior unchanged: resolve returns wasm payload + digest.

## Decisions locked (2026-02-13)
- Target ABI: **greentic:component@0.6.0** world `component-v0-v6-v0`.
- Contract authority: **WASM `describe()`** is source of truth (operations + inline SchemaIR + config_schema).
- Validation: strict by default (no silent accept). Any escape hatches must be explicit flags.
- Encodings: CBOR everywhere; use canonical CBOR encoding for stable hashing and deterministic artifacts.
- Hashes:
  - `describe_hash = sha256(canonical_cbor(typed_describe))`
  - `schema_hash = sha256(canonical_cbor({input, output, config}))` recomputed from typed SchemaIR values.
- i18n: `component-i18n.i18n-keys()` required for 0.6.0 components; QA specs must reference only known keys.
- Resolve output remains unified `ResolvedArtifact` from PR-DIST-01.
- Verification of describe/schema remains downstream responsibility.

## Scope
### In-scope
- Extend resolve response with optional fields:
  - `describe_artifact_ref: Option<String>` (e.g., oci blob ref or local path)
  - `content_length`, `content_type`
- Add helper `LockHint` structure for pack/flow/operator.
- Ensure optional metadata remains additive and non-breaking.

### Out-of-scope
- Verification logic (tools must verify against WASM describe_hash)
- Schema hashing inside dist client

## Content type policy
- Source of truth for `content_type`:
  - registry/header MIME when present
  - otherwise fixed constants:
    - wasm: `application/wasm` (or canonical component wasm MIME if adopted in codebase)
    - describe: `application/cbor`

## Lock hint schema
- `LockHint` must include:
  - `source_ref: String`
  - `resolved_digest: String`
  - `content_length: Option<u64>`
  - `content_type: Option<String>`
  - `abi_version: Option<String>`
  - `component_id: String`

## Implementation tasks
1) Response extension (non-breaking)
- Add optional `describe_artifact_ref`, `content_length`, `content_type`.
- Keep existing fields unchanged.

2) Locking helper
- Add `LockHint` with required fields for flow/pack/operator lock writes.
- Ensure helpers include both `source_ref` and `resolved_digest`.

3) Documentation
- Document that pointers are advisory only and not trusted by default.
- Document mandatory downstream verification against wasm-derived `describe_hash` when using describe artifacts.

4) Tests
- Fixture-registry tests for optional describe artifact availability.
- Tests for `content_type` priority (header first, fallback constants).
- Tests that lock hints are populated deterministically when metadata is available.

## Acceptance criteria
- Optional describe pointers provided when available.
- `LockHint` includes source ref + resolved digest and optional metadata fields.
- `content_type` follows header-first then constant fallback policy.
- Clear docs that WASM remains authoritative and verification is required.
- Dist client does not perform schema/describe verification.
- `cargo test` passes.
