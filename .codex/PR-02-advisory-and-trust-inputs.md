# PR-02 — Advisory consumption, trust policy DTOs, and verification reports

## Goal

Add the public client-side models and enforcement points needed for production trust policy:
- signature verification inputs
- issuer allow / deny policy
- digest denylist policy
- minimum operator version / compatibility hints
- verification reports consumable by operator warm-stage logic

This PR must remain open-source and fixture-driven. It must not assume access to closed-source server code.

This PR should be explicit about two layers of work:
- trust DTO and verification report modeling
- enforcement only where the open-source client has enough public metadata or local bytes to make a defensible decision

## Public DTOs

### `VerificationPolicy`
Required fields:
- `require_signature: bool`
- `trusted_issuers: Vec<String>`
- `deny_issuers: Vec<String>`
- `deny_digests: Vec<String>`
- `allowed_media_types: Vec<String>`
- `require_sbom: bool`
- `minimum_operator_version: Option<String>`
- `environment: VerificationEnvironment` (`dev`, `staging`, `prod`)

### `AdvisorySet`
Represents the signed or versioned policy inputs delivered to the client.

Required fields:
- `version`
- `issued_at`
- `source`
- `deny_digests`
- `deny_issuers`
- `minimum_operator_version`
- optional release train metadata
- optional expiry / next refresh hints

### `VerificationReport`
Returned by descriptor and artifact verification.

Required fields:
- `artifact_digest`
- `canonical_ref`
- `checks: Vec<VerificationCheck>`
- `passed: bool`
- `warnings: Vec<String>`
- `errors: Vec<String>`
- `policy_fingerprint`
- optional `advisory_version`
- optional `cache_entry_fingerprint`

### `VerificationCheck`
Required fields:
- `name`
- `outcome` (`passed`, `failed`, `warning`, `skipped`)
- `detail`
- optional structured payload

Stable check names should be defined now so operator and audit output do not drift:
- `digest_allowed`
- `media_type_allowed`
- `issuer_allowed`
- `operator_version_compatible`
- `content_digest_match`
- `signature_present`
- `signature_verified`
- `sbom_present`

## Required verification pipeline

The client must provide verification hooks at two levels:

### Descriptor-time checks
Performed before blob download if possible:
- media type allowed
- issuer not denied
- digest not denied
- compatibility hints readable

If the necessary metadata is absent at descriptor time, the client must record a structured `warning` or `skipped` result rather than silently assume success.

### Post-download checks
Performed after download:
- content digest matches descriptor
- signature verification summary
- SBOM presence if required
- artifact-type specific sanity checks where public metadata allows

This PR should distinguish clearly between:
- locally verified facts
- advisory-provided facts
- upstream-provided claims not independently verified by the client

If signature verification is not performed locally, the report must say that explicitly rather than implying cryptographic verification happened.

## Advisory ingestion model

The client must support advisory inputs from:
- local file fixtures for tests
- embedded static config
- externally supplied JSON/CBOR payloads from operator/admin layer

Do **not** hardcode a single network fetch path into the client.

Recommended API shape:
- `load_advisory_set(bytes/source) -> AdvisorySet`
- `apply_policy(descriptor, advisory_set, verification_policy) -> PreliminaryDecision`
- `verify_artifact(resolved_artifact, advisory_set, verification_policy) -> VerificationReport`

The client must not hardcode advisory retrieval over the network.
Network acquisition, if any, remains outside the client boundary.

## Operator integration

The operator warm stage must consume `VerificationReport` directly and include it in the warm report.

The operator should not reimplement trust logic. The split should be:

### Client owns
- parse / normalize policy inputs
- check digest / issuer / media type / signature summary
- produce verification report

### Operator owns
- environment policy selection
- fail-open vs fail-closed behavior where policy allows it
- readiness / activation decisions
- audit event emission

## Release-train metadata

Because the closed-source distributor may publish release-train metadata later, the open-source client should only model it generically.

Add optional DTOs such as:

- `ReleaseTrainDescriptor`
  - `train_id`
  - `operator_digest`
  - `bundle_digests`
  - `required_extension_digests`
  - `baseline_observer_digest`

The client does not decide rollout; it only surfaces data cleanly.

## Required source-of-truth rules

The PR should explicitly document which inputs come from where:
- descriptor metadata
- advisory payloads
- locally computed post-download facts
- operator-supplied environment policy

Unknown or missing data must map to explicit `warning` or `skipped` checks, not implicit passes.

## Cache interaction

Verification results may be persisted into the cache metadata introduced by `PR-01`.

That means this PR should support:
- recording the last verification outcome against a cached artifact
- invalidating prior `ready` trust status when advisory or policy inputs change without deleting the blob itself

## Non-goals

- no closed-source distributor dependency
- no mandatory online advisory fetch path
- no pretending that a transported signature summary is equivalent to local signature verification unless the verification boundary is explicitly defined

## Tests

### Unit
- deny digest refusal
- deny issuer refusal
- media type refusal
- minimum operator version mismatch warning/failure by environment
- advisory-set parsing
- missing issuer metadata yields explicit warning/skipped outcome
- missing SBOM metadata yields explicit warning/failure according to policy

### Integration
- advisory update changes verification outcome without code change
- operator fixture consumes verification report in warm path
- prod policy rejects unsigned artifact while dev policy warns only
- cached artifact can be re-evaluated under a newer advisory/policy input without re-downloading
