//! Retry policy for transient OCI registry transport failures.
//!
//! Pulling an artifact set from a registry means one HTTP round trip per
//! artifact, and a single dropped connection anywhere in that sequence fails
//! the whole operation. Registries also throttle and reset connections under
//! load, so a lone transport blip is expected rather than exceptional.
//!
//! This module retries only failures that can plausibly succeed on a second
//! attempt. A definitive answer from the registry — "not found", "not
//! authorized", a malformed manifest — is returned to the caller immediately,
//! because retrying it only delays the same error.
//!
//! Note that [`OciDistributionError`] carries `reqwest::Error` from
//! `oci-client`'s own reqwest major, which differs from the one this
//! crate depends on directly. The two are distinct types, so classification
//! works off the error variant rather than reqwest's `is_timeout()`-style
//! predicates. That is sufficient here: `oci-client` maps every non-2xx
//! response onto a dedicated variant, so `RequestError` only ever represents a
//! transport-level failure.

use std::error::Error;
use std::future::Future;
use std::io::ErrorKind;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oci_client::errors::OciDistributionError;

const DEFAULT_ATTEMPTS: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 250;
const DEFAULT_MAX_BACKOFF_MS: u64 = 4_000;

const ATTEMPTS_ENV: &str = "GREENTIC_OCI_RETRY_ATTEMPTS";
const INITIAL_BACKOFF_ENV: &str = "GREENTIC_OCI_RETRY_INITIAL_MS";
const MAX_BACKOFF_ENV: &str = "GREENTIC_OCI_RETRY_MAX_MS";

/// How many times a transient registry call is re-attempted, and how long to
/// wait between attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first one. `1` disables retrying.
    pub attempts: u32,
    /// Backoff before the second attempt; doubles on each further attempt.
    pub initial_backoff: Duration,
    /// Ceiling the doubling backoff is clamped to.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: DEFAULT_ATTEMPTS,
            initial_backoff: Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
        }
    }
}

impl RetryPolicy {
    /// Policy that never retries, for callers that own their own retry loop.
    pub fn disabled() -> Self {
        Self {
            attempts: 1,
            ..Self::default()
        }
    }

    /// Read the policy from the environment, falling back to [`Default`] for
    /// any variable that is unset or unparseable.
    ///
    /// Unparseable values are ignored rather than rejected: a typo in an
    /// operator's environment should not turn a working pull into a hard
    /// failure. `GREENTIC_OCI_RETRY_ATTEMPTS=0` is clamped to `1` so the
    /// operation still runs once.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            attempts: env_u64(ATTEMPTS_ENV)
                .and_then(|value| u32::try_from(value).ok())
                .map(|attempts| attempts.max(1))
                .unwrap_or(defaults.attempts),
            initial_backoff: env_u64(INITIAL_BACKOFF_ENV)
                .map(Duration::from_millis)
                .unwrap_or(defaults.initial_backoff),
            max_backoff: env_u64(MAX_BACKOFF_ENV)
                .map(Duration::from_millis)
                .unwrap_or(defaults.max_backoff),
        }
    }

    /// Un-jittered backoff before attempt number `attempt` (1-based, so
    /// `attempt` 2 is the first retry).
    fn base_backoff_for(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(2);
        let factor = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let millis = u64::try_from(self.initial_backoff.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_mul(factor);
        Duration::from_millis(millis).min(self.max_backoff)
    }

    /// Backoff with equal jitter applied, landing uniformly in
    /// `[base / 2, base]`.
    ///
    /// Jitter matters more than the backoff curve here: CI fans this work out
    /// across a job matrix that starts at the same moment, so un-jittered
    /// retries would re-collide in lockstep on every round.
    fn backoff_for(&self, attempt: u32) -> Duration {
        let base = self.base_backoff_for(attempt).as_millis();
        let half = u64::try_from(base / 2).unwrap_or(u64::MAX);
        Duration::from_millis(half.saturating_add(jitter_millis(half)))
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.trim().parse::<u64>().ok()
}

/// Cheap jitter source in `[0, upper]`.
///
/// Deliberately not a real RNG: spreading retries across a few hundred
/// milliseconds does not need statistical quality, and this avoids pulling a
/// `rand` dependency into every consumer of the crate.
fn jitter_millis(upper: u64) -> u64 {
    if upper == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()))
        .unwrap_or(0);
    nanos % (upper + 1)
}

/// Whether `error` is worth another attempt.
///
/// Only transport-level failures and 5xx responses qualify. Anything the
/// registry answered definitively — 4xx, auth failures, unknown manifests — is
/// treated as final, as are decode and media-type errors, which would fail
/// identically on every retry.
pub fn is_retryable(error: &OciDistributionError) -> bool {
    match error {
        // The request never completed: connection reset, TLS failure, DNS
        // failure, timeout, truncated body.
        OciDistributionError::RequestError(_) => true,
        OciDistributionError::IoError(io_error) => is_retryable_io_kind(io_error.kind()),
        // `validate_registry_response` routes 4xx to `RegistryError` and only
        // sends genuine server-side failures here.
        OciDistributionError::ServerError { code, .. } => is_retryable_status(*code),
        _ => false,
    }
}

fn is_retryable_io_kind(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::BrokenPipe
            | ErrorKind::TimedOut
            | ErrorKind::Interrupted
            | ErrorKind::UnexpectedEof
            | ErrorKind::WouldBlock
    )
}

fn is_retryable_status(code: u16) -> bool {
    matches!(code, 500 | 502 | 503 | 504)
}

/// Render an error and its full `source` chain as a single line.
///
/// [`OciDistributionError`]'s own `Display` stops at the outermost message,
/// which for a transport failure reads `error sending request for url (...)`
/// and omits the cause that actually explains it. Flattening the chain keeps
/// that detail available to callers that log errors as strings.
pub fn error_chain(error: &dyn Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        // `#[error(transparent)]` wrappers repeat their source verbatim; adding
        // the duplicate would just make the line harder to read.
        if !rendered.ends_with(&text) {
            rendered.push_str(": ");
            rendered.push_str(&text);
        }
        source = cause.source();
    }
    rendered
}

/// Run `operation`, re-attempting it while it fails with a retryable error.
///
/// `label` identifies the operation in retry logs — typically the registry
/// reference being pulled. The last error is returned once attempts are
/// exhausted.
pub async fn retry_transient<T, F, Fut>(
    policy: RetryPolicy,
    label: &str,
    mut operation: F,
) -> Result<T, OciDistributionError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OciDistributionError>>,
{
    let attempts = policy.attempts.max(1);
    let mut attempt = 1;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= attempts || !is_retryable(&error) {
                    return Err(error);
                }
                let backoff = policy.backoff_for(attempt + 1);
                tracing::warn!(
                    target: "greentic::oci::retry",
                    reference = label,
                    attempt,
                    attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error_chain(&error),
                    "transient OCI registry failure; retrying"
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use oci_client::errors::{OciEnvelope, OciError, OciErrorCode};

    use super::*;

    fn transport_error() -> OciDistributionError {
        OciDistributionError::IoError(std::io::Error::new(
            ErrorKind::ConnectionReset,
            "connection reset by peer",
        ))
    }

    fn manifest_unknown() -> OciDistributionError {
        OciDistributionError::RegistryError {
            envelope: OciEnvelope {
                errors: vec![OciError {
                    code: OciErrorCode::ManifestUnknown,
                    message: "manifest unknown".to_string(),
                    detail: serde_json::Value::Null,
                }],
            },
            url: "https://ghcr.io/v2/example/manifests/1.0.0".to_string(),
        }
    }

    fn fast_policy(attempts: u32) -> RetryPolicy {
        RetryPolicy {
            attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    #[test]
    fn transport_and_server_failures_are_retryable() {
        assert!(is_retryable(&transport_error()));
        for code in [500, 502, 503, 504] {
            assert!(
                is_retryable(&OciDistributionError::ServerError {
                    code,
                    url: "https://ghcr.io".to_string(),
                    message: String::new(),
                }),
                "{code} should be retryable"
            );
        }
    }

    #[test]
    fn definitive_registry_answers_are_not_retryable() {
        assert!(!is_retryable(&manifest_unknown()));
        assert!(!is_retryable(&OciDistributionError::UnauthorizedError {
            url: "https://ghcr.io".to_string(),
        }));
        assert!(!is_retryable(&OciDistributionError::AuthenticationFailure(
            "bad token".to_string()
        )));
        assert!(!is_retryable(
            &OciDistributionError::ImageManifestNotFoundError("missing".to_string())
        ));
        assert!(!is_retryable(
            &OciDistributionError::UnsupportedMediaTypeError("text/plain".to_string())
        ));
        // 501 is a server code but not a transient one.
        assert!(!is_retryable(&OciDistributionError::ServerError {
            code: 501,
            url: "https://ghcr.io".to_string(),
            message: String::new(),
        }));
    }

    #[test]
    fn permanent_io_kinds_are_not_retryable() {
        for kind in [ErrorKind::NotFound, ErrorKind::PermissionDenied] {
            assert!(!is_retryable(&OciDistributionError::IoError(
                std::io::Error::new(kind, "nope")
            )));
        }
    }

    #[test]
    fn backoff_doubles_and_saturates_at_the_ceiling() {
        let policy = RetryPolicy {
            attempts: 6,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_millis(1_000),
        };
        assert_eq!(policy.base_backoff_for(2), Duration::from_millis(250));
        assert_eq!(policy.base_backoff_for(3), Duration::from_millis(500));
        assert_eq!(policy.base_backoff_for(4), Duration::from_millis(1_000));
        assert_eq!(policy.base_backoff_for(5), Duration::from_millis(1_000));
    }

    #[test]
    fn jitter_keeps_backoff_within_half_of_base() {
        let policy = RetryPolicy {
            attempts: 3,
            initial_backoff: Duration::from_millis(400),
            max_backoff: Duration::from_millis(400),
        };
        for _ in 0..64 {
            let backoff = policy.backoff_for(2);
            assert!(
                backoff >= Duration::from_millis(200) && backoff <= Duration::from_millis(400),
                "backoff {backoff:?} outside [200ms, 400ms]"
            );
        }
    }

    #[tokio::test]
    async fn retries_until_the_operation_succeeds() {
        let calls = AtomicU32::new(0);
        let result = retry_transient(fast_policy(3), "ghcr.io/example:1.0.0", || async {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(transport_error())
            } else {
                Ok("pulled")
            }
        })
        .await;

        assert_eq!(
            result.expect("should succeed on the third attempt"),
            "pulled"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_the_configured_attempts() {
        let calls = AtomicU32::new(0);
        let result: Result<(), _> =
            retry_transient(fast_policy(3), "ghcr.io/example:1.0.0", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(transport_error())
            })
            .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_a_definitive_failure() {
        let calls = AtomicU32::new(0);
        let result: Result<(), _> =
            retry_transient(fast_policy(5), "ghcr.io/example:1.0.0", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(manifest_unknown())
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a manifest-unknown answer must not be retried"
        );
    }

    #[tokio::test]
    async fn a_single_attempt_policy_runs_the_operation_once() {
        let calls = AtomicU32::new(0);
        let result: Result<(), _> =
            retry_transient(RetryPolicy::disabled(), "ghcr.io/example:1.0.0", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(transport_error())
            })
            .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn error_chain_appends_causes() {
        #[derive(Debug, thiserror::Error)]
        #[error("failed to pull `ghcr.io/example:1.0.0`")]
        struct Outer {
            #[source]
            source: std::io::Error,
        }

        let rendered = error_chain(&Outer {
            source: std::io::Error::new(ErrorKind::ConnectionReset, "connection reset by peer"),
        });
        assert_eq!(
            rendered,
            "failed to pull `ghcr.io/example:1.0.0`: connection reset by peer"
        );
    }

    #[test]
    fn error_chain_does_not_repeat_transparent_wrappers() {
        let rendered = error_chain(&transport_error());
        assert_eq!(rendered, "connection reset by peer");
    }
}
