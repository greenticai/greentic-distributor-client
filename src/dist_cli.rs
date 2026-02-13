use crate::dist::{DistClient, DistOptions};
#[cfg(feature = "pack-fetch")]
use crate::oci_packs::{
    DefaultRegistryClient, OciPackFetcher, PackFetchOptions, RegistryClient, ResolvedPack,
};
use clap::{Parser, Subcommand};
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
        #[arg(long)]
        json: bool,
    },
    /// Pull a reference or lockfile into the cache
    Pull {
        reference: Option<String>,
        #[arg(long)]
        lock: Option<PathBuf>,
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
    /// Stub login hook for future repo/store auth
    Login { target: String },
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

    let client = DistClient::new(opts);

    match cli.command {
        Commands::Resolve { reference, json } => {
            let resolved = client
                .resolve_ref(&reference)
                .await
                .map_err(CliError::from_dist)?;
            if json {
                let out = ResolveOutput {
                    reference: &reference,
                    digest: &resolved.digest,
                };
                println!("{}", serde_json::to_string_pretty(&out).unwrap());
            } else {
                println!("{}", resolved.digest);
            }
        }
        Commands::Pull {
            reference,
            lock,
            json,
        } => {
            if let Some(lock_path) = lock {
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
                let resolved = client
                    .ensure_cached(&reference)
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
                client
                    .remove_cached(&digests)
                    .map_err(CliError::from_dist)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&digests).unwrap());
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
            AuthCommand::Login { target } => {
                return Err(CliError {
                    code: 5,
                    message: format!(
                        "auth login for `{target}` is not implemented yet; stubbed for future store/repo"
                    ),
                });
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
            let wasm_path = inspection.cache_dir.join("component.wasm");
            let manifest_path = inspection.cache_dir.join("component.manifest.json");
            println!("cache dir: {}", inspection.cache_dir.display());
            println!("component.wasm: {}", wasm_path.exists());
            println!("component.manifest.json: {}", manifest_path.exists());
            if manifest_path.exists() {
                let manifest_bytes = fs::read(&manifest_path).map_err(|err| CliError {
                    code: 2,
                    message: format!("failed to read component.manifest.json: {err}"),
                })?;
                let manifest: ComponentManifest =
                    serde_json::from_slice(&manifest_bytes).map_err(|err| CliError {
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

impl CliError {
    pub fn from_dist(err: crate::dist::DistError) -> Self {
        Self {
            code: err.exit_code(),
            message: err.to_string(),
        }
    }
}
