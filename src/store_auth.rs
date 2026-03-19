use greentic_secrets_lib::{DevStore, SecretFormat, SecretsStore};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCredentials {
    pub tenant: String,
    pub username: String,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreAuth {
    auth_path: PathBuf,
    state_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoreAuthState {
    logins: Vec<StoreCredentials>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreAuthError {
    #[error("{0}")]
    Message(String),
    #[error("io error at `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("secret store error: {0}")]
    SecretStore(String),
}

pub fn default_store_auth_path() -> PathBuf {
    if let Ok(path) = std::env::var("GREENTIC_DIST_STORE_SECRETS_PATH") {
        return PathBuf::from(path);
    }
    default_store_auth_dir().join("store-auth.json")
}

pub fn default_store_state_path() -> PathBuf {
    default_store_auth_path()
}

impl Default for StoreAuth {
    fn default() -> Self {
        Self::new(default_store_auth_path(), default_store_state_path())
    }
}

impl StoreAuth {
    pub fn new(auth_path: impl Into<PathBuf>, state_path: impl Into<PathBuf>) -> Self {
        Self {
            auth_path: auth_path.into(),
            state_path: state_path.into(),
        }
    }

    pub fn from_env() -> Self {
        Self::default()
    }

    pub fn auth_path(&self) -> &Path {
        &self.auth_path
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    pub async fn save_login(&self, tenant: &str, token: &str) -> Result<(), StoreAuthError> {
        save_login(&self.auth_path, &self.state_path, tenant, token).await
    }

    pub async fn load_login(&self, tenant: &str) -> Result<StoreCredentials, StoreAuthError> {
        load_login(&self.auth_path, &self.state_path, tenant).await
    }
}

pub async fn save_login_default(tenant: &str, token: &str) -> Result<(), StoreAuthError> {
    StoreAuth::default().save_login(tenant, token).await
}

pub async fn load_login_default(tenant: &str) -> Result<StoreCredentials, StoreAuthError> {
    StoreAuth::default().load_login(tenant).await
}

fn default_store_auth_dir() -> PathBuf {
    if let Some(config) = dirs_next::config_dir() {
        return config.join("greentic").join("dist");
    }
    if let Ok(root) = std::env::var("GREENTIC_HOME") {
        return PathBuf::from(root).join("config").join("dist");
    }
    PathBuf::from(".greentic").join("config").join("dist")
}

pub async fn save_login(
    auth_path: &Path,
    _state_path: &Path,
    tenant: &str,
    token: &str,
) -> Result<(), StoreAuthError> {
    let tenant = tenant.trim();
    if tenant.is_empty() {
        return Err(StoreAuthError::Message("tenant cannot be empty".into()));
    }
    if token.is_empty() {
        return Err(StoreAuthError::Message("token cannot be empty".into()));
    }

    let credentials = StoreCredentials {
        tenant: tenant.to_string(),
        username: tenant.to_string(),
        token: token.to_string(),
    };
    let mut state = load_state(auth_path)
        .await?
        .unwrap_or(StoreAuthState { logins: Vec::new() });
    state.logins.retain(|login| login.tenant != tenant);
    state.logins.push(credentials);
    write_state(auth_path, &state).await?;
    Ok(())
}

pub async fn load_login(
    auth_path: &Path,
    _state_path: &Path,
    tenant: &str,
) -> Result<StoreCredentials, StoreAuthError> {
    let state = load_state(auth_path).await?.ok_or_else(|| {
        StoreAuthError::Message(format!(
            "no saved store login found at `{}`; run `greentic-dist auth login <tenant>` first",
            auth_path.display()
        ))
    })?;
    let active = state
        .logins
        .into_iter()
        .find(|login| login.tenant == tenant)
        .ok_or_else(|| {
            StoreAuthError::Message(format!(
                "tenant `{tenant}` has no saved credentials; run `greentic-dist auth login {tenant}` first"
            ))
        })?;
    if active.username.trim().is_empty() || active.token.is_empty() {
        return Err(StoreAuthError::Message(format!(
            "stored credentials for tenant `{}` are incomplete",
            active.tenant
        )));
    }
    Ok(active)
}

async fn load_state(path: &Path) -> Result<Option<StoreAuthState>, StoreAuthError> {
    ensure_parent_dir(path)?;
    let store = match open_store(path) {
        Ok(store) => store,
        Err(StoreAuthError::SecretStore(message)) if is_missing_store_path_error(&message) => {
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let bytes = match store.get("secrets://prod/dist/_/store/auth_state").await {
        Ok(bytes) => bytes,
        Err(err) if err.to_string().contains("not found") => return Ok(None),
        Err(err) => return Err(StoreAuthError::SecretStore(err.to_string())),
    };
    let state: StoreAuthState = serde_json::from_slice(&bytes)?;
    Ok(Some(state))
}

async fn write_state(path: &Path, state: &StoreAuthState) -> Result<(), StoreAuthError> {
    ensure_parent_dir(path)?;
    let store = open_store(path)?;
    let bytes = serde_json::to_vec(state)?;
    store
        .put(
            "secrets://prod/dist/_/store/auth_state",
            SecretFormat::Json,
            &bytes,
        )
        .await
        .map_err(|err| StoreAuthError::SecretStore(err.to_string()))?;
    Ok(())
}

fn open_store(path: &Path) -> Result<DevStore, StoreAuthError> {
    DevStore::with_path(path).map_err(|err| {
        StoreAuthError::SecretStore(format!(
            "failed to open secrets store `{}`: {err}",
            path.display()
        ))
    })
}

fn ensure_parent_dir(path: &Path) -> Result<(), StoreAuthError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| StoreAuthError::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    Ok(())
}

fn is_missing_store_path_error(message: &str) -> bool {
    message.contains("No such file or directory (os error 2)")
        || message.contains("The system cannot find the path specified. (os error 3)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_login_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let auth_path = temp.path().join("store-auth.json");
        let state_path = auth_path.clone();

        save_login(&auth_path, &state_path, "tenant-a", "secret-token")
            .await
            .unwrap();

        let loaded = load_login(&auth_path, &state_path, "tenant-a")
            .await
            .unwrap();
        assert_eq!(loaded.tenant, "tenant-a");
        assert_eq!(loaded.username, "tenant-a");
        assert_eq!(loaded.token, "secret-token");
    }

    #[tokio::test]
    async fn store_auth_wrapper_round_trips_login_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let auth = StoreAuth::new(
            temp.path().join("store-auth.json"),
            temp.path().join("store-auth.json"),
        );

        auth.save_login("tenant-b", "other-secret").await.unwrap();

        let loaded = auth.load_login("tenant-b").await.unwrap();
        assert_eq!(loaded.tenant, "tenant-b");
        assert_eq!(loaded.username, "tenant-b");
        assert_eq!(loaded.token, "other-secret");
    }

    #[tokio::test]
    async fn missing_store_directory_returns_login_required_message() {
        let temp = tempfile::tempdir().unwrap();
        let auth_path = temp.path().join("missing").join("store-auth.json");
        let state_path = auth_path.clone();

        let err = load_login(&auth_path, &state_path, "tenant-c")
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            format!(
                "no saved store login found at `{}`; run `greentic-dist auth login <tenant>` first",
                auth_path.display()
            )
        );
    }

    #[tokio::test]
    async fn load_login_creates_missing_parent_directory_before_opening_store() {
        let temp = tempfile::tempdir().unwrap();
        let auth_dir = temp.path().join("nested").join("auth");
        let auth_path = auth_dir.join("store-auth.json");
        let state_path = auth_path.clone();

        let err = load_login(&auth_path, &state_path, "tenant-d")
            .await
            .unwrap_err();

        assert!(auth_dir.is_dir());
        assert_eq!(
            err.to_string(),
            format!(
                "no saved store login found at `{}`; run `greentic-dist auth login <tenant>` first",
                auth_path.display()
            )
        );
    }
}
