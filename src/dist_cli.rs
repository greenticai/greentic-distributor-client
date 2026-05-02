use crate::dist::{
    CachePolicy, DistClient, DistOptions, DownloadedStoreArtifact, ReleaseChannel,
    ReleaseResolutionContext, ResolvePolicy,
};
#[cfg(feature = "pack-fetch")]
use crate::oci_packs::{
    DefaultRegistryClient, OciPackFetcher, PackFetchOptions, RegistryClient, ResolvedPack,
};
use crate::store_auth::save_login;
use clap::{Parser, Subcommand, ValueEnum};
use rpassword::prompt_password;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "greentic-dist")]
#[command(about = "Greentic component resolver and cache manager")]
pub struct Cli {
    /// Override cache directory
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
    /// Offline mode (disable network fetches)
    #[arg(long, global = true)]
    pub offline: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Resolve a reference and print its digest
    Resolve {
        reference: String,
        /// Release version used for local release-index resolution of stable/dev/rnd tags
        #[arg(long)]
        release: Option<String>,
        /// Release channel used with --release
        #[arg(long, value_enum)]
        channel: Option<CliReleaseChannel>,
        #[arg(long)]
        json: bool,
    },
    /// Pull a reference or lockfile into the cache
    Pull {
        reference: Option<String>,
        #[arg(long)]
        lock: Option<PathBuf>,
        /// Release version used for local release-index resolution of stable/dev/rnd tags
        #[arg(long)]
        release: Option<String>,
        /// Release channel used with --release
        #[arg(long, value_enum)]
        channel: Option<CliReleaseChannel>,
        #[arg(long)]
        json: bool,
    },
    /// Cache management commands
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    /// Authentication commands (stub)
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Download a raw store artifact without adding it to the cache
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    /// Pull an OCI reference and report cached files
    Inspect {
        reference: String,
        /// Print the selected layer media type
        #[arg(long)]
        show_media_type: bool,
    },
    /// Fetch an OCI pack into the local cache
    #[cfg(feature = "pack-fetch")]
    Pack {
        reference: String,
        /// Allow tag references (digest pins preferred)
        #[arg(long)]
        allow_tags: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum CacheCommand {
    /// List cached digests
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Remove cached digests
    Rm {
        digests: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Garbage-collect broken cache entries
    Gc {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum AuthCommand {
    /// Save GHCR credentials for a tenant used by store://greentic-biz/<tenant>/...
    Login {
        tenant: String,
        #[arg(long)]
        token: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum StoreCommand {
    /// Download a raw artifact from store:// to a file without caching it
    Download {
        reference: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CliReleaseChannel {
    Stable,
    Dev,
    Rnd,
}

impl From<CliReleaseChannel> for ReleaseChannel {
    fn from(channel: CliReleaseChannel) -> Self {
        match channel {
            CliReleaseChannel::Stable => Self::Stable,
            CliReleaseChannel::Dev => Self::Dev,
            CliReleaseChannel::Rnd => Self::Rnd,
        }
    }
}

#[derive(Serialize)]
struct ResolveOutput<'a> {
    reference: &'a str,
    digest: &'a str,
}

#[derive(Serialize)]
struct PullOutput<'a> {
    reference: &'a str,
    digest: &'a str,
    cache_path: Option<&'a std::path::Path>,
    fetched: bool,
}

#[derive(Serialize)]
struct StoreDownloadOutput<'a> {
    reference: &'a str,
    mapped_reference: &'a str,
    canonical_ref: &'a str,
    digest: &'a str,
    media_type: &'a str,
    output_path: &'a std::path::Path,
    size_bytes: u64,
}

#[cfg(feature = "pack-fetch")]
#[derive(Serialize)]
struct PackOutput<'a> {
    reference: &'a str,
    digest: &'a str,
    media_type: &'a str,
    cache_path: &'a std::path::Path,
    fetched: bool,
}

#[derive(Debug, Deserialize)]
struct ComponentManifest {
    #[serde(default)]
    artifacts: Option<ComponentManifestArtifacts>,
}

#[derive(Debug, Deserialize)]
struct ComponentManifestArtifacts {
    #[serde(default)]
    component_wasm: Option<String>,
}

pub async fn run_from_env() -> Result<(), CliError> {
    let cli = Cli::parse();
    run(cli).await
}

#[derive(Debug)]
pub struct CliError {
    pub code: i32,
    pub message: String,
}

pub async fn run(cli: Cli) -> Result<(), CliError> {
    run_with_pack_client(cli, DefaultRegistryClient::default_client()).await
}

#[cfg(feature = "pack-fetch")]
#[allow(deprecated)]
pub async fn run_with_pack_client<C: RegistryClient>(
    cli: Cli,
    pack_client: C,
) -> Result<(), CliError> {
    let cache_dir_override = cli.cache_dir.clone();
    let offline = cli.offline;
    let mut opts = DistOptions::default();
    if let Some(dir) = cli.cache_dir {
        opts.cache_dir = dir;
    }
    opts.offline = offline || opts.offline;
    let store_auth_path = opts.store_auth_path.clone();
    let store_state_path = opts.store_state_path.clone();

    let client = DistClient::new(opts);

    match cli.command {
        Commands::Resolve {
            reference,
            release,
            channel,
            json,
        } => {
            let release_context = release_context_from_flags(release, channel)?;
            let source = client
                .parse_source(&reference)
                .map_err(CliError::from_dist)?;
            let descriptor = resolve_for_cli(&client, source, release_context.as_ref()).await?;
            if json {
                let out = ResolveOutput {
                    reference: &reference,
                    digest: &descriptor.digest,
                };
                println!("{}", serde_json::to_string_pretty(&out).unwrap());
            } else {
                println!("{}", descriptor.digest);
            }
        }
        Commands::Pull {
            reference,
            lock,
            release,
            channel,
            json,
        } => {
            let release_context = release_context_from_flags(release, channel)?;
            if let Some(lock_path) = lock {
                if release_context.is_some() {
                    return Err(CliError {
                        code: 2,
                        message: "--release is only supported when pulling a single reference"
                            .into(),
                    });
                }
                let resolved = client
                    .pull_lock(&lock_path)
                    .await
                    .map_err(CliError::from_dist)?;
                if json {
                    let payload: Vec<_> = resolved
                        .iter()
                        .map(|r| PullOutput {
                            reference: "",
                            digest: &r.digest,
                            cache_path: r.cache_path.as_deref(),
                            fetched: r.fetched,
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&payload).unwrap());
                } else {
                    for r in resolved {
                        let path = r
                            .cache_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default();
                        println!("{} {}", r.digest, path);
                    }
                }
            } else if let Some(reference) = reference {
                let source = client
                    .parse_source(&reference)
                    .map_err(CliError::from_dist)?;
                let descriptor = resolve_for_cli(&client, source, release_context.as_ref()).await?;
                let resolved = client
                    .fetch(&descriptor, CachePolicy)
                    .await
                    .map_err(CliError::from_dist)?;
                if json {
                    let out = PullOutput {
                        reference: &reference,
                        digest: &resolved.digest,
                        cache_path: resolved.cache_path.as_deref(),
                        fetched: resolved.fetched,
                    };
                    println!("{}", serde_json::to_string_pretty(&out).unwrap());
                } else if let Some(path) = &resolved.cache_path {
                    println!("{}", path.display());
                } else {
                    println!("{}", resolved.digest);
                }
            } else {
                return Err(CliError {
                    code: 2,
                    message: "pull requires either a reference or --lock".into(),
                });
            }
        }
        Commands::Cache { command } => match command {
            CacheCommand::Ls { json } => {
                let entries = client.list_cache();
                if json {
                    println!("{}", serde_json::to_string_pretty(&entries).unwrap());
                } else {
                    for digest in entries {
                        println!("{digest}");
                    }
                }
            }
            CacheCommand::Rm { digests, json } => {
                let report = client.evict_cache(&digests).map_err(CliError::from_dist)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report).unwrap());
                } else if report.evicted > 0 {
                    eprintln!(
                        "evicted {} entries, reclaimed {} bytes",
                        report.evicted, report.bytes_reclaimed
                    );
                }
            }
            CacheCommand::Gc { json } => {
                let removed = client.gc().map_err(CliError::from_dist)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&removed).unwrap());
                } else if !removed.is_empty() {
                    eprintln!("removed {}", removed.join(", "));
                }
            }
        },
        Commands::Auth { command } => match command {
            AuthCommand::Login { tenant, token } => {
                let token = match token {
                    Some(token) => token,
                    None => prompt_password(format!("GHCR token for tenant `{tenant}`: "))
                        .map_err(|err| CliError {
                            code: 10,
                            message: format!("failed to read token: {err}"),
                        })?,
                };
                save_login(&store_auth_path, &store_state_path, &tenant, &token)
                    .await
                    .map_err(|err| CliError {
                        code: 5,
                        message: err.to_string(),
                    })?;
                eprintln!("saved auth for tenant `{tenant}` for store://greentic-biz/{tenant}/...");
                return Ok(());
            }
        },
        Commands::Store { command } => match command {
            StoreCommand::Download {
                reference,
                output,
                token,
                json,
            } => {
                let downloaded = download_store_for_cli(
                    &client,
                    &reference,
                    token,
                    &store_auth_path,
                    &store_state_path,
                )
                .await?;
                let output_path =
                    write_store_download(&downloaded, output).map_err(|err| CliError {
                        code: 2,
                        message: format!("failed to write downloaded artifact: {err}"),
                    })?;
                if json {
                    let out = StoreDownloadOutput {
                        reference: &reference,
                        mapped_reference: &downloaded.mapped_reference,
                        canonical_ref: &downloaded.canonical_ref,
                        digest: &downloaded.digest,
                        media_type: &downloaded.media_type,
                        output_path: output_path.as_path(),
                        size_bytes: downloaded.size_bytes,
                    };
                    println!("{}", serde_json::to_string_pretty(&out).unwrap());
                } else {
                    println!("{}", output_path.display());
                }
            }
        },
        Commands::Inspect {
            reference,
            show_media_type,
        } => {
            let inspection = client
                .pull_oci_with_details(&reference)
                .await
                .map_err(CliError::from_dist)?;
            println!("cache dir: {}", inspection.cache_dir.display());
            println!("artifact type: {:?}", inspection.artifact_type);
            if inspection.artifact_type == crate::dist::ArtifactType::Component {
                let wasm_path = inspection.cache_dir.join("component.wasm");
                let manifest_path = inspection.cache_dir.join("component.manifest.json");
                println!("component.wasm: {}", wasm_path.exists());
                println!("component.manifest.json: {}", manifest_path.exists());
                if manifest_path.exists() {
                    let manifest_bytes = fs::read(&manifest_path).map_err(|err| CliError {
                        code: 2,
                        message: format!("failed to read component.manifest.json: {err}"),
                    })?;
                    let manifest: ComponentManifest = serde_json::from_slice(&manifest_bytes)
                        .map_err(|err| CliError {
                            code: 2,
                            message: format!("failed to parse component.manifest.json: {err}"),
                        })?;
                    let component_wasm = manifest
                        .artifacts
                        .and_then(|a| a.component_wasm)
                        .map(|name| name.trim().to_string())
                        .filter(|name| !name.is_empty());
                    if let Some(component_wasm) = component_wasm {
                        let manifest_wasm_path = inspection.cache_dir.join(&component_wasm);
                        let exists = manifest_wasm_path.exists();
                        println!("manifest component_wasm: {component_wasm}");
                        println!("manifest component_wasm exists: {exists}");
                        let mismatch = !exists;
                        println!("manifest component_wasm mismatch: {mismatch}");
                        if mismatch {
                            eprintln!(
                                "error: manifest component_wasm `{}` missing from cache",
                                component_wasm
                            );
                        }
                    } else {
                        println!("manifest component_wasm: <missing>");
                    }
                }
            } else {
                println!("artifact path: {}", inspection.artifact_path.display());
                println!("artifact exists: {}", inspection.artifact_path.exists());
            }
            if show_media_type {
                println!("selected media type: {}", inspection.selected_media_type);
            }
        }
        #[cfg(feature = "pack-fetch")]
        Commands::Pack {
            reference,
            allow_tags,
            json,
        } => {
            let resolved = fetch_pack_for_cli(
                &reference,
                allow_tags,
                cache_dir_override,
                offline,
                pack_client,
            )
            .await?;
            if json {
                let out = PackOutput {
                    reference: &reference,
                    digest: &resolved.resolved_digest,
                    media_type: &resolved.media_type,
                    cache_path: resolved.path.as_path(),
                    fetched: resolved.fetched_from_network,
                };
                println!("{}", serde_json::to_string_pretty(&out).unwrap());
            } else {
                println!("{}", resolved.path.display());
            }
        }
    }

    Ok(())
}

async fn resolve_for_cli(
    client: &DistClient,
    source: crate::dist::ArtifactSource,
    release_context: Option<&ReleaseResolutionContext>,
) -> Result<crate::dist::ArtifactDescriptor, CliError> {
    match release_context {
        Some(ctx) => client
            .resolve_with_release_context(source, ResolvePolicy, ctx)
            .await
            .map_err(CliError::from_dist),
        None => client
            .resolve(source, ResolvePolicy)
            .await
            .map_err(CliError::from_dist),
    }
}

fn release_context_from_flags(
    release: Option<String>,
    channel: Option<CliReleaseChannel>,
) -> Result<Option<ReleaseResolutionContext>, CliError> {
    match release {
        Some(release) => Ok(Some(ReleaseResolutionContext {
            release,
            channel: channel.unwrap_or(CliReleaseChannel::Stable).into(),
        })),
        None if channel.is_some() => Err(CliError {
            code: 2,
            message: "--channel requires --release".into(),
        }),
        None => Ok(None),
    }
}

#[cfg(feature = "pack-fetch")]
pub async fn fetch_pack_for_cli<C: RegistryClient>(
    reference: &str,
    allow_tags: bool,
    cache_dir_override: Option<PathBuf>,
    offline: bool,
    client: C,
) -> Result<ResolvedPack, CliError> {
    let mut opts = PackFetchOptions::default();
    if let Some(dir) = cache_dir_override {
        opts.cache_dir = dir;
    }
    opts.offline = offline;
    opts.allow_tags = allow_tags;

    let fetcher = OciPackFetcher::with_client(client, opts);
    fetcher
        .fetch_pack_to_cache(reference)
        .await
        .map_err(|err| CliError {
            code: 2,
            message: err.to_string(),
        })
}

async fn download_store_for_cli(
    client: &DistClient,
    reference: &str,
    token: Option<String>,
    store_auth_path: &std::path::Path,
    store_state_path: &std::path::Path,
) -> Result<DownloadedStoreArtifact, CliError> {
    match client.download_store_artifact(reference).await {
        Ok(downloaded) => Ok(downloaded),
        Err(crate::dist::DistError::StoreAuth(message))
            if is_missing_store_token_message(&message) =>
        {
            let tenant = tenant_from_store_reference(reference).ok_or_else(|| CliError {
                code: 5,
                message: message.clone(),
            })?;
            let token = match token {
                Some(token) => token,
                None => prompt_password(format!("GHCR token for tenant `{tenant}`: ")).map_err(
                    |err| CliError {
                        code: 10,
                        message: format!("failed to read token: {err}"),
                    },
                )?,
            };
            save_login(store_auth_path, store_state_path, &tenant, &token)
                .await
                .map_err(|err| CliError {
                    code: 5,
                    message: err.to_string(),
                })?;
            client
                .download_store_artifact(reference)
                .await
                .map_err(CliError::from_dist)
        }
        Err(err) => Err(CliError::from_dist(err)),
    }
}

fn write_store_download(
    downloaded: &DownloadedStoreArtifact,
    output: Option<PathBuf>,
) -> Result<PathBuf, std::io::Error> {
    let path = output.unwrap_or_else(|| default_store_download_path(downloaded));
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, &downloaded.bytes)?;
    Ok(path)
}

fn default_store_download_path(downloaded: &DownloadedStoreArtifact) -> PathBuf {
    let mapped = downloaded
        .mapped_reference
        .trim_start_matches("oci://")
        .split('@')
        .next()
        .unwrap_or(downloaded.mapped_reference.as_str());
    let last_segment = mapped.rsplit('/').next().unwrap_or("store-download");
    let stem = last_segment
        .split(':')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or("store-download");
    PathBuf::from(format!(
        "{stem}{}",
        extension_for_media_type(&downloaded.media_type)
    ))
}

fn extension_for_media_type(media_type: &str) -> &'static str {
    if media_type == "application/json" || media_type.ends_with("+json") {
        ".json"
    } else if media_type == "application/wasm" {
        ".wasm"
    } else if media_type.ends_with("+gzip") {
        ".tgz"
    } else if media_type.ends_with("+zstd") {
        ".tar.zst"
    } else if media_type.ends_with("+zip") {
        ".zip"
    } else {
        ".bin"
    }
}

fn tenant_from_store_reference(reference: &str) -> Option<String> {
    let target = reference.strip_prefix("store://greentic-biz/")?;
    let (tenant, _) = target.split_once('/')?;
    (!tenant.trim().is_empty()).then(|| tenant.to_string())
}

fn is_missing_store_token_message(message: &str) -> bool {
    message.contains("no saved store login") || message.contains("has no saved credentials")
}

impl CliError {
    pub fn from_dist(err: crate::dist::DistError) -> Self {
        Self {
            code: err.exit_code(),
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plus_json_media_types_download_as_json_files() {
        let downloaded = DownloadedStoreArtifact {
            source_ref: "store://greentic-biz/3point/catalogs/zain-x:latest".to_string(),
            mapped_reference: "oci://ghcr.io/greentic-biz/catalogs/zain-x:latest".to_string(),
            canonical_ref: "oci://ghcr.io/greentic-biz/catalogs/zain-x@sha256:abc".to_string(),
            digest: "sha256:abc".to_string(),
            media_type: "application/vnd.greentic.zain-x.catalog.root.v1+json".to_string(),
            bytes: br#"{"kind":"catalog-root"}"#.to_vec(),
            size_bytes: 23,
            manifest_digest: Some("sha256:abc".to_string()),
        };

        assert_eq!(
            default_store_download_path(&downloaded),
            PathBuf::from("zain-x.json")
        );
    }

    #[test]
    fn detects_missing_saved_store_tokens() {
        assert!(is_missing_store_token_message(
            "no saved store login found at `/tmp/store-auth.json`"
        ));
        assert!(is_missing_store_token_message(
            "tenant `3point` has no saved credentials"
        ));
    }
}
