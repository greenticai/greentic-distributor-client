#![cfg(all(feature = "oci-components", feature = "pack-fetch"))]

#[path = "../tests/perf_support.rs"]
mod perf_support;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use greentic_distributor_client::{
    OciComponentResolver, OciPackFetcher, oci_components::PulledLayer as ComponentPulledLayer,
};
use perf_support::{
    MockComponentRegistryClient, MockPackRegistryClient, component_image,
    component_image_with_layers, component_options, digest_for, pack_image, pack_options,
    warm_component_cache, warm_pack_cache,
};

fn bench_component_descriptor_cache_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let bytes = b"criterion component cache hit bytes";
    let digest = digest_for(bytes);
    let reference = format!("ghcr.io/greentic/components@{digest}");
    let client = MockComponentRegistryClient::with_image(
        &reference,
        component_image(bytes, "application/wasm", &digest),
    );
    let resolver = OciComponentResolver::with_client(client, component_options(&temp));
    runtime.block_on(warm_component_cache(&resolver, &reference));

    c.bench_function("component_descriptor_cache_hit", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(resolver.resolve_descriptor(&reference))
                .unwrap();
            assert_eq!(result.resolved_digest, digest);
            assert!(!result.fetched_from_network);
        })
    });
}

fn bench_pack_cache_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let bytes = b"{\"pack\":\"criterion\"}";
    let digest = digest_for(bytes);
    let reference = format!("ghcr.io/greenticai/greentic-packs/demo@{digest}");
    let client = MockPackRegistryClient::with_image(
        &reference,
        pack_image(bytes, "application/json", &digest),
    );
    let fetcher = OciPackFetcher::with_client(client, pack_options(&temp));
    runtime.block_on(warm_pack_cache(&fetcher, &reference));

    c.bench_function("pack_cache_hit", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(fetcher.fetch_pack_to_cache(&reference))
                .unwrap();
            assert_eq!(result.resolved_digest, digest);
            assert!(!result.fetched_from_network);
        })
    });
}

fn bench_component_many_layers(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let layer_counts = [8usize, 64, 256];
    let mut group = c.benchmark_group("component_descriptor_many_layers");

    for layer_count in layer_counts {
        let temp = tempfile::tempdir().unwrap();
        let preferred = b"preferred-wasm-layer";
        let digest = digest_for(preferred);
        let reference = format!("ghcr.io/greentic/components@{digest}");
        let mut layers = Vec::with_capacity(layer_count);
        for idx in 0..(layer_count - 1) {
            let payload = format!("fallback-layer-{idx}");
            layers.push(ComponentPulledLayer {
                media_type: "application/octet-stream".to_string(),
                data: payload.into_bytes(),
                digest: None,
            });
        }
        layers.push(ComponentPulledLayer {
            media_type: "application/wasm".to_string(),
            data: preferred.to_vec(),
            digest: Some(digest.clone()),
        });
        let client = MockComponentRegistryClient::with_image(
            &reference,
            component_image_with_layers(layers),
        );
        let resolver = OciComponentResolver::with_client(client, component_options(&temp));

        group.bench_with_input(
            BenchmarkId::from_parameter(layer_count),
            &reference,
            |b, reference| {
                b.iter(|| {
                    let result = runtime
                        .block_on(resolver.resolve_descriptor(reference))
                        .unwrap();
                    assert_eq!(result.resolved_digest, digest);
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_component_descriptor_cache_hit,
    bench_pack_cache_hit,
    bench_component_many_layers
);
criterion_main!(benches);
