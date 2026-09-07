use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Selects which deployment-owned admission fences are applied by a transport.
///
/// This profile deliberately does not control protocol or data integrity validation. Both
/// profiles still require a valid wire frame, an unexpired signed capability, bounded deadlines,
/// and tenant/volume/object scope. `Development` is only safe for an explicitly loopback stack;
/// service configuration is responsible for enforcing that boundary before constructing a
/// transport.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportValidationProfile {
    /// Enforce workload identity, role/scope, and session/route generation fences.
    #[default]
    Strict,
    /// Loopback-only development profile that relaxes deployment-owned identity/generation fences.
    Development,
}

impl TransportValidationProfile {
    #[must_use]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }

    #[must_use]
    pub const fn is_development(self) -> bool {
        matches!(self, Self::Development)
    }
}

impl fmt::Display for TransportValidationProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Strict => "strict",
            Self::Development => "development",
        })
    }
}

impl FromStr for TransportValidationProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "strict" => Ok(Self::Strict),
            "development" => Ok(Self::Development),
            _ => Err(format!(
                "unknown validation profile {value:?}; expected strict or development"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransportValidationProfile;

    #[test]
    fn profile_round_trips_as_configuration_text() {
        for profile in [
            TransportValidationProfile::Strict,
            TransportValidationProfile::Development,
        ] {
            let encoded = profile.to_string();
            assert_eq!(
                encoded.parse::<TransportValidationProfile>().unwrap(),
                profile
            );
        }
    }

    #[test]
    fn unknown_profile_is_rejected() {
        assert!("permissive".parse::<TransportValidationProfile>().is_err());
    }
}
