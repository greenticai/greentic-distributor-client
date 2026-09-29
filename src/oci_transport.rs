//! Transport selection shared by every `oci-client`-backed registry client in
//! this crate ([`crate::oci_packs::DefaultRegistryClient`] and
//! [`crate::oci_components::DefaultRegistryClient`]).
//!
//! Transport (HTTPS vs plain HTTP) and credentials (anonymous vs basic auth)
//! are independent axes. Plain HTTP is only ever used for a registry the
//! caller listed explicitly; it is never inferred from the presence of
//! credentials, from the reference, or from a failed HTTPS attempt. Sending
//! basic-auth credentials over plain HTTP therefore always requires that same
//! explicit opt-in.

use std::fmt;

use oci_client::client::ClientProtocol;

/// Map a set of insecure-registry allowances to the transport protocol. An
/// empty list keeps the default HTTPS-everywhere behavior; a non-empty list
/// downgrades exactly those `host[:port]` registries to plain HTTP while HTTPS
/// stays the default for every other registry.
pub(crate) fn protocol_for_insecure_registries(insecure_registries: Vec<String>) -> ClientProtocol {
    if insecure_registries.is_empty() {
        ClientProtocol::Https
    } else {
        ClientProtocol::HttpsExcept(insecure_registries)
    }
}

/// An insecure-registry entry that can never match a registry.
///
/// `oci-client` compares each entry byte-for-byte with the `host[:port]` it
/// parses out of a reference. An entry such as `http://registry.local:5000` or
/// `registry.local:5000/team` therefore never matches, and the pull quietly
/// stays on HTTPS — against a plain-HTTP registry that surfaces as a TLS
/// failure that names neither the entry nor the allow-list. The checked
/// constructors refuse such an entry up front instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidInsecureRegistry {
    entry: String,
    reason: &'static str,
}

impl InvalidInsecureRegistry {
    /// The offending entry, verbatim.
    pub fn entry(&self) -> &str {
        &self.entry
    }

    /// Why the entry can never match a registry.
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl fmt::Display for InvalidInsecureRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "insecure registry entry `{}` {}; expected a bare `host[:port]` such as `registry.local:5000`",
            self.entry, self.reason
        )
    }
}

impl std::error::Error for InvalidInsecureRegistry {}

/// Check that every entry is a bare `host[:port]` that `oci-client` could
/// match against a reference's registry.
pub fn validate_insecure_registries(entries: &[String]) -> Result<(), InvalidInsecureRegistry> {
    for entry in entries {
        let reason = if entry.is_empty() {
            Some("is empty")
        } else if entry.contains("://") {
            Some("carries a URL scheme")
        } else if entry.contains('/') {
            Some("carries a path")
        } else if entry.contains('@') {
            Some("carries userinfo")
        } else if entry.chars().any(char::is_whitespace) {
            Some("contains whitespace")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(InvalidInsecureRegistry {
                entry: entry.clone(),
                reason,
            });
        }
    }
    Ok(())
}

/// Credentials a registry client presents. `Debug` never prints the password.
#[derive(Clone)]
pub(crate) enum RegistryClientAuth {
    Anonymous,
    Basic { username: String, password: String },
}

impl fmt::Debug for RegistryClientAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Anonymous => f.write_str("Anonymous"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

impl RegistryClientAuth {
    pub(crate) fn to_registry_auth(&self) -> oci_client::secrets::RegistryAuth {
        match self {
            Self::Anonymous => oci_client::secrets::RegistryAuth::Anonymous,
            Self::Basic { username, password } => {
                oci_client::secrets::RegistryAuth::Basic(username.clone(), password.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_insecure_list_keeps_https_default() {
        assert_eq!(
            protocol_for_insecure_registries(Vec::new()),
            ClientProtocol::Https
        );
    }

    #[test]
    fn bare_host_port_entries_are_accepted() {
        let entries = vec![
            "localhost:5000".to_string(),
            "gtc-oci-registry.gtc-local.svc.cluster.local:5000".to_string(),
            "10.0.0.7".to_string(),
        ];
        assert_eq!(validate_insecure_registries(&entries), Ok(()));
        assert_eq!(validate_insecure_registries(&[]), Ok(()));
    }

    #[test]
    fn entries_that_can_never_match_are_refused_with_the_entry_named() {
        for (entry, reason) in [
            ("", "is empty"),
            ("http://registry.local:5000", "carries a URL scheme"),
            ("registry.local:5000/team", "carries a path"),
            ("user@registry.local:5000", "carries userinfo"),
            ("registry.local :5000", "contains whitespace"),
        ] {
            let err = validate_insecure_registries(&[entry.to_string()]).unwrap_err();
            assert_eq!(err.entry(), entry);
            assert_eq!(err.reason(), reason);
            assert!(err.to_string().contains(entry));
        }
    }

    #[test]
    fn debug_never_prints_the_password() {
        let auth = RegistryClientAuth::Basic {
            username: "robot".into(),
            password: "s3cr3t-value".into(),
        };
        let rendered = format!("{auth:?}");
        assert!(rendered.contains("robot"));
        assert!(!rendered.contains("s3cr3t-value"), "{rendered}");
    }
}
