use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use greentic_distributor_client::{
    ComponentResolveOptions, ComponentsExtension, ComponentsMode, DefaultRegistryClient,
    OciComponentResolver,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use wasmtime::component::{Component as WasmComponent, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

mod bindings {
    wasmtime::component::bindgen!({
        inline: r#"
        package greentic:component@0.6.0;

        interface node {
          type capability-id = string;
          type component-id = string;
          type flow-id = string;
          type step-id = string;
          type tenant-id = string;
          type team-id = string;
          type user-id = string;
          type env-id = string;
          type trace-id = string;
          type correlation-id = string;

          record node-error {
            code: string,
            message: string,
            retryable: bool,
            backoff-ms: option<u64>,
            details: option<list<u8>>,
          }

          record tenant-ctx {
            tenant-id: tenant-id,
            team-id: option<team-id>,
            user-id: option<user-id>,
            env-id: env-id,
            trace-id: trace-id,
            correlation-id: correlation-id,
            deadline-ms: u64,
            attempt: u32,
            idempotency-key: option<string>,
            i18n-id: string,
          }

          record invocation-envelope {
            ctx: tenant-ctx,
            flow-id: flow-id,
            step-id: step-id,
            component-id: component-id,
            attempt: u32,
            payload-cbor: list<u8>,
            metadata-cbor: option<list<u8>>,
          }

          record invocation-result {
            ok: bool,
            output-cbor: list<u8>,
            output-metadata-cbor: option<list<u8>>,
          }

          invoke: func(op: string, envelope: invocation-envelope) -> result<invocation-result, node-error>;
        }

        world component {
          export node;
        }
        "#,
        world: "component",
    });
}

#[derive(Debug, Deserialize)]
struct ComponentInvocationEnvelope {
    invocation_id: String,
    component_id: String,
    runtime: String,
    reference: String,
    #[serde(default)]
    interface: Option<String>,
    input: Value,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct ComponentInvocationResultEnvelope {
    invocation_id: String,
    component_id: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, Value>,
}

struct HostState {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
}

impl HostState {
    fn new() -> Result<Self> {
        let mut builder = WasiCtxBuilder::new();
        for preopen in component_runner_preopens()? {
            builder
                .preopened_dir(
                    &preopen.host_path,
                    &preopen.guest_path,
                    DirPerms::READ,
                    FilePerms::READ,
                )
                .map_err(|err| {
                    anyhow!(
                        "failed to preopen {} as {}: {err}",
                        preopen.host_path.display(),
                        preopen.guest_path
                    )
                })?;
        }
        Ok(Self {
            table: ResourceTable::new(),
            wasi_ctx: builder.build(),
        })
    }
}

#[derive(Debug)]
struct PreopenSpec {
    host_path: PathBuf,
    guest_path: String,
}

fn component_runner_preopens() -> Result<Vec<PreopenSpec>> {
    let Some(raw) = std::env::var("GREENTIC_COMPONENT_RUNNER_PREOPEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(Vec::new());
    };
    raw.split([',', ';'])
        .filter(|entry| !entry.trim().is_empty())
        .map(parse_preopen_spec)
        .collect()
}

fn parse_preopen_spec(entry: &str) -> Result<PreopenSpec> {
    let (host, guest) = entry.split_once('=').with_context(|| {
        format!("invalid preopen spec `{entry}`; expected host_path=guest_path")
    })?;
    let host_path = PathBuf::from(host.trim());
    if host_path.as_os_str().is_empty() {
        bail!("invalid preopen spec `{entry}`: host path is empty");
    }
    if !Path::new(&host_path).is_dir() {
        bail!(
            "invalid preopen spec `{entry}`: host path {} is not a directory",
            host_path.display()
        );
    }
    let guest_path = guest.trim();
    if !guest_path.starts_with('/') {
        bail!("invalid preopen spec `{entry}`: guest path must be absolute");
    }
    Ok(PreopenSpec {
        host_path,
        guest_path: guest_path.to_owned(),
    })
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}

fn main() {
    let result = run();
    match result {
        Ok(envelope) => {
            println!(
                "{}",
                serde_json::to_string(&envelope).expect("serialize result")
            );
        }
        Err(err) => {
            let fallback = ComponentInvocationResultEnvelope {
                invocation_id: "unknown".to_owned(),
                component_id: "unknown".to_owned(),
                status: "failed",
                output: None,
                error: Some(err.to_string()),
                warnings: Vec::new(),
                metadata: BTreeMap::new(),
            };
            println!(
                "{}",
                serde_json::to_string(&fallback).expect("serialize failure")
            );
            std::process::exit(1);
        }
    }
}

fn run() -> Result<ComponentInvocationResultEnvelope> {
    let mut stdin = String::new();
    io::stdin()
        .read_to_string(&mut stdin)
        .context("failed to read component invocation envelope from stdin")?;
    let envelope: ComponentInvocationEnvelope =
        serde_json::from_str(&stdin).context("failed to parse component invocation envelope")?;

    match invoke_component(&envelope) {
        Ok(output) => Ok(success_result(&envelope, output)),
        Err(err) => Ok(failed_result(&envelope, format!("{err:#}"))),
    }
}

fn invoke_component(envelope: &ComponentInvocationEnvelope) -> Result<Value> {
    if envelope.runtime != "wasm_wasi" && envelope.runtime != "WasmWasi" {
        bail!(
            "unsupported component runtime {}; expected wasm_wasi",
            envelope.runtime
        );
    }
    let resolved_path = resolve_component_path(envelope)?;

    invoke_wasm(envelope, &resolved_path).with_context(|| {
        format!(
            "failed to invoke component {} from {}",
            envelope.component_id,
            resolved_path.display()
        )
    })
}

fn resolve_component_path(envelope: &ComponentInvocationEnvelope) -> Result<PathBuf> {
    let reference = envelope
        .reference
        .strip_prefix("oci://")
        .unwrap_or(&envelope.reference)
        .to_owned();
    let component_ref = envelope.reference.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to create component resolver runtime")?;
    runtime.block_on(async move {
        let opts = ComponentResolveOptions {
            allow_tags: true,
            offline: std::env::var_os("GREENTIC_COMPONENT_RUNNER_OFFLINE").is_some(),
            ..Default::default()
        };
        let client = registry_client_from_env();
        let resolver: OciComponentResolver<DefaultRegistryClient> =
            OciComponentResolver::with_client(client, opts);
        let resolved = resolver
            .resolve_refs(&ComponentsExtension {
                refs: vec![reference],
                mode: ComponentsMode::Eager,
            })
            .await
            .with_context(|| format!("failed to resolve component OCI reference {component_ref}"))?
            .into_iter()
            .next()
            .context("component resolver returned no result")?;
        Ok(resolved.path)
    })
}

fn invoke_wasm(
    envelope: &ComponentInvocationEnvelope,
    wasm_path: &std::path::Path,
) -> Result<Value> {
    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine =
        Engine::new(&config).map_err(|err| anyhow!("failed to create wasmtime engine: {err}"))?;
    let component = WasmComponent::from_file(&engine, wasm_path).map_err(|err| {
        anyhow!(
            "failed to load component wasm {}: {err}",
            wasm_path.display()
        )
    })?;
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .map_err(|err| anyhow!("failed to add WASI p2 linker imports: {err}"))?;
    let mut store = Store::new(&engine, HostState::new()?);
    let bindings = bindings::Component::instantiate(&mut store, &component, &linker)
        .map_err(|err| anyhow!("failed to instantiate greentic component: {err}"))?;
    let node = bindings.greentic_component_node();
    let operation = operation_for(envelope);
    let invocation = to_node_invocation(envelope)?;
    let result = node
        .call_invoke(&mut store, &operation, &invocation)
        .map_err(|err| anyhow!("component invoke failed for operation {operation}: {err}"))?;
    match result {
        Ok(result) if result.ok => cbor_to_json(&result.output_cbor),
        Ok(result) => {
            let value = cbor_to_json(&result.output_cbor).unwrap_or(Value::Null);
            bail!("component returned ok=false: {value}")
        }
        Err(err) => {
            let details = err
                .details
                .as_ref()
                .and_then(|bytes| cbor_to_json(bytes).ok());
            bail!(
                "{}: {}{}",
                err.code,
                err.message,
                details.map(|v| format!(" details={v}")).unwrap_or_default()
            )
        }
    }
}

fn registry_client_from_env() -> DefaultRegistryClient {
    let token = std::env::var("GREENTIC_COMPONENT_REGISTRY_TOKEN")
        .ok()
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
        .or_else(|| std::env::var("GH_TOKEN").ok());
    let password = std::env::var("GREENTIC_COMPONENT_REGISTRY_PASSWORD")
        .ok()
        .or(token);
    if let Some(password) = password.filter(|value| !value.trim().is_empty()) {
        let username = std::env::var("GREENTIC_COMPONENT_REGISTRY_USERNAME")
            .ok()
            .or_else(|| std::env::var("GITHUB_ACTOR").ok())
            .unwrap_or_else(|| "greentic".to_owned());
        return DefaultRegistryClient::with_basic_auth(username, password);
    }
    DefaultRegistryClient::default()
}

fn operation_for(envelope: &ComponentInvocationEnvelope) -> String {
    envelope
        .metadata
        .get("operation")
        .and_then(Value::as_str)
        .or_else(|| {
            envelope
                .metadata
                .get("default_operation")
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| match envelope.component_id.as_str() {
            id if id.starts_with("tx.resolve.") => "resolve".to_owned(),
            id if id.starts_with("tx.query.") => "query".to_owned(),
            id if id.starts_with("tx.view.") => "render".to_owned(),
            _ => "analyse".to_owned(),
        })
}

fn to_node_invocation(
    envelope: &ComponentInvocationEnvelope,
) -> Result<bindings::exports::greentic::component::node::InvocationEnvelope> {
    let payload_cbor = json_to_cbor(&envelope.input)?;
    let metadata = json!({
        "interface": envelope.interface,
        "reference": envelope.reference,
        "run_id": envelope.run_id,
        "metadata": envelope.metadata,
    });
    let metadata_cbor = json_to_cbor(&metadata)?;
    Ok(
        bindings::exports::greentic::component::node::InvocationEnvelope {
            ctx: bindings::exports::greentic::component::node::TenantCtx {
                tenant_id: "demo".to_owned(),
                team_id: None,
                user_id: None,
                env_id: std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_owned()),
                trace_id: envelope.invocation_id.clone(),
                correlation_id: envelope.invocation_id.clone(),
                deadline_ms: u64::MAX,
                attempt: 1,
                idempotency_key: Some(envelope.invocation_id.clone()),
                i18n_id: "en".to_owned(),
            },
            flow_id: envelope
                .run_id
                .clone()
                .unwrap_or_else(|| "telco-x".to_owned()),
            step_id: envelope.invocation_id.clone(),
            component_id: envelope.component_id.clone(),
            attempt: 1,
            payload_cbor,
            metadata_cbor: Some(metadata_cbor),
        },
    )
}

fn json_to_cbor(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes).context("failed to encode JSON value as CBOR")?;
    Ok(bytes)
}

fn cbor_to_json(bytes: &[u8]) -> Result<Value> {
    ciborium::de::from_reader(bytes).context("failed to decode component CBOR output")
}

fn success_result(
    envelope: &ComponentInvocationEnvelope,
    output: Value,
) -> ComponentInvocationResultEnvelope {
    let mut metadata = BTreeMap::new();
    metadata.insert("runner".to_owned(), json!("greentic-component-runner"));
    metadata.insert("reference".to_owned(), json!(envelope.reference));
    ComponentInvocationResultEnvelope {
        invocation_id: envelope.invocation_id.clone(),
        component_id: envelope.component_id.clone(),
        status: "succeeded",
        output: Some(output),
        error: None,
        warnings: Vec::new(),
        metadata,
    }
}

fn failed_result(
    envelope: &ComponentInvocationEnvelope,
    error: String,
) -> ComponentInvocationResultEnvelope {
    ComponentInvocationResultEnvelope {
        invocation_id: envelope.invocation_id.clone(),
        component_id: envelope.component_id.clone(),
        status: "failed",
        output: None,
        error: Some(error),
        warnings: Vec::new(),
        metadata: BTreeMap::new(),
    }
}
