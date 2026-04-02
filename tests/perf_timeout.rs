#![cfg(all(feature = "oci-components", feature = "pack-fetch"))]

#[path = "perf_support.rs"]
mod perf_support;

use std::time::{Duration, Instant};

use greentic_distributor_client::{OciComponentResolver, OciPackFetcher};

use perf_support::{
    MockComponentRegistryClient, MockPackRegistryClient, component_image, component_options,
    digest_for, pack_image, pack_options, warm_component_cache, warm_pack_cache,
};

#[test]
fn cached_hot_paths_should_finish_quickly() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let component_temp = tempfile::tempdir().unwrap();
    let component_bytes = b"component timeout benchmark bytes";
    let component_digest = digest_for(component_bytes);
    let component_ref = format!("ghcr.io/greentic/components@{component_digest}");
    let component_client = MockComponentRegistryClient::with_image(
        &component_ref,
        component_image(component_bytes, "application/wasm", &component_digest),
    );
    let component_resolver =
        OciComponentResolver::with_client(component_client, component_options(&component_temp));
    runtime.block_on(warm_component_cache(&component_resolver, &component_ref));

    let pack_temp = tempfile::tempdir().unwrap();
    let pack_bytes = b"{\"pack\":\"timeout\"}";
    let pack_digest = digest_for(pack_bytes);
    let pack_ref = format!("ghcr.io/greenticai/greentic-packs/demo@{pack_digest}");
    let pack_client = MockPackRegistryClient::with_image(
        &pack_ref,
        pack_image(pack_bytes, "application/json", &pack_digest),
    );
    let pack_fetcher = OciPackFetcher::with_client(pack_client, pack_options(&pack_temp));
    runtime.block_on(warm_pack_cache(&pack_fetcher, &pack_ref));

    let start = Instant::now();
    for _ in 0..200 {
        let descriptor = runtime
            .block_on(component_resolver.resolve_descriptor(&component_ref))
            .unwrap();
        assert_eq!(descriptor.resolved_digest, component_digest);

        let resolved = runtime
            .block_on(pack_fetcher.fetch_pack_to_cache(&pack_ref))
            .unwrap();
        assert_eq!(resolved.resolved_digest, pack_digest);
    }
    let elapsed = start.elapsed();
    eprintln!("cached_hot_paths elapsed for 200 iters: {:?}", elapsed);

    assert!(
        elapsed < Duration::from_secs(2),
        "cached hot paths took too long: {elapsed:?}"
    );
}
