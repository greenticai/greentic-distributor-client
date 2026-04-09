#![cfg(feature = "oci-components")]

#[path = "perf_support.rs"]
mod perf_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use greentic_distributor_client::{OciComponentResolver, oci_components::PulledLayer};

use perf_support::{
    MockComponentRegistryClient, component_image_with_layers, component_options, digest_for,
    warm_component_cache,
};

fn run_cached_descriptor_workload(threads: usize, iters_per_thread: usize) -> Duration {
    let temp = tempfile::tempdir().unwrap();
    let payload = b"component bytes used for cached descriptor scaling";
    let digest = digest_for(payload);
    let reference = format!("ghcr.io/greentic/components@{digest}");
    let image = component_image_with_layers(vec![
        PulledLayer {
            media_type: "application/octet-stream".to_string(),
            data: b"fallback".to_vec(),
            digest: Some(digest_for(b"fallback")),
        },
        PulledLayer {
            media_type: "application/wasm".to_string(),
            data: payload.to_vec(),
            digest: Some(digest.clone()),
        },
    ]);
    let mock = MockComponentRegistryClient::with_image(&reference, image);
    let resolver = Arc::new(OciComponentResolver::with_client(
        mock.clone(),
        component_options(&temp),
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(warm_component_cache(&resolver, &reference));
    assert_eq!(mock.pulls(), 1);

    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let resolver = Arc::clone(&resolver);
            let reference = reference.clone();
            let expected_digest = digest.clone();
            scope.spawn(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                for _ in 0..iters_per_thread {
                    let result = runtime
                        .block_on(resolver.resolve_descriptor(&reference))
                        .unwrap();
                    assert_eq!(result.resolved_digest, expected_digest);
                    assert!(!result.fetched_from_network);
                }
            });
        }
    });
    start.elapsed()
}

fn nanos_per_op(elapsed: Duration, threads: usize, iters_per_thread: usize) -> f64 {
    elapsed.as_nanos() as f64 / (threads * iters_per_thread) as f64
}

#[test]
fn cached_descriptor_scaling_should_remain_reasonable() {
    let iters_per_thread = 40;
    let t1 = run_cached_descriptor_workload(1, iters_per_thread);
    let t4 = run_cached_descriptor_workload(4, iters_per_thread);
    let t8 = run_cached_descriptor_workload(8, iters_per_thread);

    let p1 = nanos_per_op(t1, 1, iters_per_thread);
    let p4 = nanos_per_op(t4, 4, iters_per_thread);
    let p8 = nanos_per_op(t8, 8, iters_per_thread);
    eprintln!("cached_descriptor_scaling ns/op: 1t={p1:.0} 4t={p4:.0} 8t={p8:.0}");

    assert!(
        p4 <= p1 * 2.5,
        "4-thread cached descriptor throughput regressed: 1t={p1:.0}ns/op 4t={p4:.0}ns/op"
    );
    assert!(
        p8 <= p1 * 3.5,
        "8-thread cached descriptor throughput regressed: 1t={p1:.0}ns/op 8t={p8:.0}ns/op"
    );
}
