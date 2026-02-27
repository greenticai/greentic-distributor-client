# OCI Pack Fetching

This document describes the minimal OCI pack fetcher in `greentic-distributor-client`.

## Overview
- Anonymous HTTPS pulls only (no auth support).
- Packs are expected as a single layer containing the `.gtpack` bytes.
- Accepted layer media types: `application/vnd.greentic.pack+json`, `application/vnd.greentic.gtpack.v1+zip`, `application/vnd.greentic.gtpack+zip`, `application/vnd.greentic.pack+zip`, `text/markdown`, `application/octet-stream`, `application/json`, `application/vnd.oci.image.layer.v1.tar`, `application/vnd.oci.image.layer.v1.tar+gzip`, `application/vnd.oci.image.layer.v1.tar+zstd`.
- Preferred layer media types (selection order): `application/vnd.greentic.pack+json`, `application/vnd.greentic.gtpack.v1+zip`, `application/vnd.greentic.gtpack+zip`, `application/vnd.greentic.pack+zip`, `text/markdown`.
- If the preferred media type is missing, the first layer is used.
- Content-addressed cache writes `pack.gtpack` and `metadata.json`.

## Feature flag
Enable the `pack-fetch` feature (also included in `dist-client`):

```toml
greentic-distributor-client = { version = "0.4", features = ["pack-fetch"] }
```

## API
Use the helper APIs or the fetcher directly:

```rust
use greentic_distributor_client::fetch_pack;

let bytes = fetch_pack("ghcr.io/greenticai/greentic-packs/foo@sha256:...").await?;
```

```rust
use greentic_distributor_client::{OciPackFetcher, PackFetchOptions};

let fetcher = OciPackFetcher::new(PackFetchOptions::default());
let resolved = fetcher
    .fetch_pack_to_cache("ghcr.io/greenticai/greentic-packs/foo@sha256:...")
    .await?;
println!("cached at {:?}", resolved.path);
```

## Caching
Cache roots are resolved in order:
1. `GREENTIC_PACK_CACHE_DIR`
2. OS cache dir (`~/.cache/greentic/packs`)
3. `GREENTIC_HOME/cache/packs`
4. `.greentic/cache/packs` (project-relative)

Each digest is stored at `<cache>/<sha256>/pack.gtpack` with `metadata.json`.

## Limitations
- No registry auth (public GHCR only).
- Digest pins are enforced by default (tags require `allow_tags = true`).
- No signature/provenance verification.

