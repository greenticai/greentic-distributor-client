# PR-00-AUDIT — greentic-distributor-client production contract audit

## Goal

Audit the existing open-source `greentic-distributor-client` as it is currently embedded into the operator,
so follow-up PRs can surgically reshape it into the production artifact fetch / verify / cache layer.

This audit is mandatory because there is likely legacy logic around:
- raw URL / OCI fetch
- tag-based references
- cache behavior
- implicit trust assumptions
- operator-coupled helper functions
- duplicate DTOs or dead code

## Audit scope

### 1. Public API and embedding points
Map the exact current public surface:
- crates / modules
- exported traits
- exported structs / enums
- feature flags
- helper constructors used by operator
- sync vs async APIs
- error model

### 2. Artifact source model
Document all currently supported source kinds:
- `oci://`
- `https://`
- `file://`
- cache-local references
- any dev / fixture schemes

For each:
- how parsing works
- where normalization happens
- whether tags are allowed
- whether digest pinning exists
- whether media type is surfaced

### 3. Fetch pipeline
Map the exact fetch path:
- reference parsing
- remote resolution
- descriptor retrieval
- blob/content download
- local cache write
- local reopen/read path

Capture all extension points and all places where operator-specific assumptions leak in.

### 4. Verification and trust assumptions
Identify current behavior for:
- digest checking
- signature checking
- issuer checking
- media type checking
- size checking
- manifest verification
- advisory / denylist support

Distinguish:
- implemented
- partially implemented
- assumed but not enforced

### 5. Cache and retention logic
Map:
- cache root resolution
- keying format
- temp download handling
- completed entry format
- lock/concurrency behavior
- partial failure recovery
- cleanup / GC behavior
- any rollback-related behavior

### 6. Test inventory
Catalog:
- unit tests
- integration tests
- fixture registries
- fake OCI/object-store helpers
- replayable operator tests already depending on the client

### 7. Deletion candidates
Produce an explicit deletion list for:
- dead source schemes
- tag-only runtime helpers
- duplicated fetch wrappers
- operator-specific branching that belongs in the operator, not the client
- legacy cache formats if migration is acceptable

## Deliverables

- public API map
- module / entrypoint map
- source-kind matrix
- fetch pipeline sequence map
- trust enforcement gap list
- cache format map
- deletion candidates
- migration risk notes
