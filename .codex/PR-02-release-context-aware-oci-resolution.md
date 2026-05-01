# PR-02: Add Release-Context-Aware OCI Resolution

Repo: `greentic-distributor-client`

## Goal

Add an opt-in, non-breaking way to resolve mutable OCI tags such as `:stable`, `:dev`, and `:rnd` through a local release index before falling back to existing remote resolution.

Existing APIs must behave exactly as they do today.

## New Public Types

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReleaseChannel {
    Stable,
    Dev,
    Rnd,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseResolutionContext {
    pub release: String,
    pub channel: ReleaseChannel,
}
```

Do not add fields to `DistOptions`, `StageBundleInput`, or other public structs in this PR. Public struct field additions can break downstream callers that use struct literals.

## Release Index Schema

Path:

```text
<cache_dir>/release-index/v1/<channel>/<release>.json
```

Schema:

```json
{
  "schema": "greentic.release-index.v1",
  "release": "1.0.16",
  "channel": "stable",
  "refs": {
    "ghcr.io/...:stable": {
      "version": "0.5.4",
      "digest": "sha256:abc",
      "canonical_ref": "ghcr.io/...@sha256:abc"
    }
  }
}
```

The index is machine-owned. `gtc` / `greentic-dev` should generate it. This crate should define the schema and lookup semantics needed by resolution.

## New API Shape

Prefer additive APIs:

```rust
impl DistClient {
    pub async fn resolve_with_release_context(
        &self,
        source: ArtifactSource,
        policy: ResolvePolicy,
        ctx: &ReleaseResolutionContext,
    ) -> Result<ArtifactDescriptor, DistError>;
}
```

Optionally add lower-level helpers if needed:

```rust
pub fn is_mutable_release_tag(reference: &str) -> bool;
```

Avoid `ctx.exists()` style APIs. Use a required `&ReleaseResolutionContext` on the explicit context-aware method, or an `Option<&ReleaseResolutionContext>` only for private helper plumbing.

## Resolution Logic

For context-aware resolution:

1. Parse/classify the source exactly as existing `resolve(...)` does.
2. If the effective OCI reference ends with `:stable`, `:dev`, or `:rnd`, load the matching release index.
3. Look up the full effective OCI ref without the `oci://` prefix.
4. If an entry exists:
   - validate the digest string
   - validate `canonical_ref` is digest-pinned
   - validate the digest exists in the `DistClient` cache via `stat_cache` / `open_cached`
   - return an `ArtifactDescriptor` based on `entry.json`, preserving the original source kind where appropriate
5. If lookup or validation fails, fall back to the existing remote resolution path.

Offline behavior:

- Valid index + valid cached artifact: succeeds without network.
- Missing index, stale index, missing blob, or corrupt entry: returns the existing offline/cache-miss style error; do not attempt remote network work.

## Mutable Tag Helper

```rust
fn is_mutable_release_tag(reference: &str) -> bool {
    reference.ends_with(":stable")
        || reference.ends_with(":dev")
        || reference.ends_with(":rnd")
}
```

This should only match tag suffixes, not digest refs or path fragments.

## Cache Layout To Use

Use the existing `DistClient` cache:

```text
<cache_dir>/
  artifacts/sha256/<aa>/<remaining-62-hex>/
    blob
    entry.json
  release-index/v1/<channel>/<release>.json
```

Do not introduce a separate `blobs/` or `entries/` root.

## Tests

- `:stable` without context still resolves through existing behavior.
- `:stable` with context and valid index returns local digest/canonical ref without network.
- Indexed digest with missing `blob` falls back remotely when online.
- Indexed digest with malformed `entry.json` falls back remotely when online.
- Missing index falls back remotely when online.
- Offline + valid index + cached blob succeeds.
- Offline + missing/stale index fails without network.
- Existing digest-pinned resolution remains cache-first.

