# Public components from GHCR (minimal support)

This crate now includes a minimal, extension-based resolver for public OCI components published to GHCR. Enable the `oci-components` feature to use it.

```toml
[dependencies]
greentic-distributor-client = { version = "0.4", features = ["oci-components"] }
```

## Referencing components in packs
Pack manifests can surface component refs via an extension block:

```yaml
extensions:
  greentic.components:
    refs:
      - "ghcr.io/greenticai/components/component-templates@sha256:..." # preferred
    mode: "eager" # or "lazy" (optional, defaults to eager)
```

Rules:
- Digest-pinned refs (`@sha256:<hex>`) are required by default. Tag refs are rejected unless `allow_tags` is enabled in resolver options.
- Offline mode only works with pinned refs that are already cached.

## Resolving components

```rust
use greentic_distributor_client::oci_components::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentResolver,
};

let ext = ComponentsExtension {
    refs: vec![ "ghcr.io/greenticai/components/component-templates@sha256:<digest>".into() ],
    mode: ComponentsMode::Eager,
};

let resolver = OciComponentResolver::new(ComponentResolveOptions {
    allow_tags: false,          // require digest pins
    offline: false,             // fail if network needed but unavailable
    cache_dir: "/home/user/.greentic/cache".into(), // default is ${GREENTIC_HOME:-$HOME/.greentic}/cache
    ..ComponentResolveOptions::default()
});

let resolved = resolver.resolve_refs(&ext).await?;
for component in resolved {
    println!("cached at {:?}, digest {}", component.path, component.resolved_digest);
}
```

Behavior:
- Anonymous HTTPS pulls using `oci-distribution`; GHCR works without credentials for public artifacts. Private auth hooks are intentionally not supported in this minimal scope—private registries will fail fast.
- Cache layout: `${cache_dir}/oci/<sha256>/component.wasm` with `metadata.json` (original ref, resolved digest, media type, fetch time, size).
- Preferred layer media types (validated by the resolver): `application/vnd.wasm.component.v1+wasm`, `application/vnd.module.wasm.content.layer.v1+wasm`, `application/vnd.greentic.component.manifest+json`, then fallback to the first layer.
- Errors are descriptive: missing digest pins, offline-miss, digest mismatch, invalid ref, or registry pull failures.

## Offline and enterprise-friendly workflows
- Prime the cache once online (digest-pinned refs), then run with `offline = true` to forbid network.
- All network fetches use HTTPS (client protocol forced to HTTPS). To use private registries later, add credentials via `oci-distribution` auth hooks; the current flow is anonymous-only by design.

## Known limitations
- No signature/provenance verification yet; `SignatureSummary` is still pass-through only.
- Pack schema is untouched; the resolver only consumes the `greentic.components` extension and returns cached file paths for runners to use.
- No push/publish helpers; use `oras`/`crane` to publish artifacts to GHCR and copy the digest into pack extensions.

## Optional public GHCR E2E test
Run an opt-in E2E against the public component template (requires outbound network):

```bash
OCI_E2E=1 OCI_E2E_REF=ghcr.io/greenticai/components/templates:latest \
  cargo test --features oci-components --test oci_components_e2e -- --nocapture
```

The test:
- Pulls anonymously from GHCR (tags allowed for this E2E).
- Caches under a temp dir and records the manifest digest for future verification.
- Skips automatically unless `OCI_E2E=1` is set.

