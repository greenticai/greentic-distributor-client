# OCI Pack Fetching

This document describes the minimal OCI pack fetcher in `greentic-distributor-client`.

## Overview
- Anonymous HTTPS pulls by default; basic auth is supported via `DefaultRegistryClient::with_basic_auth`.
- Packs are expected as a single layer containing the `.gtpack` bytes.
- Accepted layer media types: `application/vnd.greentic.pack+json`, `application/vnd.greentic.gtpack.v1+zip`, `application/vnd.greentic.gtpack+zip`, `application/vnd.greentic.pack+zip`, `application/vnd.greentic.gtpack.layer.v1+tar`, `text/markdown`, `application/octet-stream`, `application/json`, `application/vnd.oci.image.layer.v1.tar`, `application/vnd.oci.image.layer.v1.tar+gzip`, `application/vnd.oci.image.layer.v1.tar+zstd`.
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
- Anonymous pulls by default; `DefaultRegistryClient::with_basic_auth` supports basic-auth registries too (not GHCR-only).
- Digest pins are enforced by default (tags require `allow_tags = true`).
- No signature/provenance verification.

## Push/pull round-trip (E2E)
`tests/oci_push_e2e.rs` proves that an artifact pushed with `push_pack_with_client`
(`pack-push` feature, see `src/oci_push.rs`) is fetchable byte-for-byte through
this crate's own `OciPackFetcher` — the same fetch path `greentic-start` uses at
container boot. It is the only check that push and pull actually agree on media
type and manifest shape rather than merely being internally consistent with
themselves.

It runs the round-trip twice: once with a SquashFS-magic (`hsqs`) payload —
the shape every real `.gtbundle` push actually produces, which takes the
`application/octet-stream` fallback branch of `layer_media_type_for` — and
once with a ZIP-magic payload, which takes the dedicated gtpack-zip media
type. Both branches are proven against a real registry, not just unit-tested
in isolation.

The test is skipped **unless you set `OCI_PUSH_E2E=1` by hand** — it is not
wired into `ci/local_check.sh`, because this is the only container-based test
in the repo and the local gate must not require Docker. Run it explicitly:

```bash
docker run -d --rm -p 5000:5000 --name gtc-push-e2e registry:2
OCI_PUSH_E2E=1 cargo test --features pack-push --test oci_push_e2e -- --nocapture
docker rm -f gtc-push-e2e
```

Without `OCI_PUSH_E2E=1` the test passes trivially, printing a skip message —
`cargo test --features pack-push --test oci_push_e2e` needs no Docker at all,
so no one's gate newly depends on it.
