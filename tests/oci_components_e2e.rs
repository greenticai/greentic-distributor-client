#![cfg(feature = "oci-components")]

use greentic_distributor_client::oci_components::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, OciComponentResolver,
};

/// Optional GHCR E2E: set `OCI_E2E=1` (and optionally `OCI_E2E_REF`) to run.
#[tokio::test]
async fn fetches_public_component_from_ghcr() {
    if std::env::var("OCI_E2E").as_deref() != Ok("1") {
        eprintln!("skipping public GHCR E2E (set OCI_E2E=1 to enable)");
        return;
    }

    let reference = std::env::var("OCI_E2E_REF")
        .unwrap_or_else(|_| "ghcr.io/greenticai/components/templates:latest".into());
    let temp = tempfile::tempdir().expect("tempdir");
    let resolver: OciComponentResolver<
        greentic_distributor_client::oci_components::DefaultRegistryClient,
    > = OciComponentResolver::new(ComponentResolveOptions {
        allow_tags: true, // public tag allowed for E2E
        offline: false,
        cache_dir: temp.path().into(),
        ..ComponentResolveOptions::default()
    });

    let ext = ComponentsExtension {
        refs: vec![reference.clone()],
        mode: ComponentsMode::Eager,
    };
    let results = resolver
        .resolve_refs(&ext)
        .await
        .unwrap_or_else(|e| match e {
            greentic_distributor_client::oci_components::OciComponentError::PullFailed {
                source,
                ..
            } if matches!(
                &source,
                oci_distribution::errors::OciDistributionError::RequestError(err)
                    if err.is_connect() || err.is_timeout()
            ) =>
            {
                eprintln!(
                    "skipping GHCR E2E due to network error: {source} (requires outbound network)"
                );
                Vec::new()
            }
            greentic_distributor_client::oci_components::OciComponentError::PullFailed {
                source,
                ..
            } => panic!("failed to pull {reference}: {source:?} (requires network to GHCR)"),
            other => panic!("failed to pull {reference}: {other:?} (requires network to GHCR)"),
        });
    if results.is_empty() {
        return;
    }
    let component = &results[0];
    assert!(component.path.exists(), "cached path missing");
    assert!(
        component.fetched_from_network,
        "expected network fetch on first E2E pull"
    );
    assert!(
        component.manifest_digest.is_some(),
        "manifest digest should be recorded for future verification"
    );
}

