use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{ProtocolError, ProtocolResult};

const DECIMAL_U64_PATTERN: &str = r"^(0|[1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-5])$";
const POSITIVE_DECIMAL_U64_PATTERN: &str = r"^([1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-5])$";

/// A numeric protocol revision carried in control envelopes and capability negotiation.
///
/// Unlike generations and counters, protocol versions remain JSON numbers because they are small,
/// closed discriminants rather than potentially large monotonic values.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct ProtocolVersion(u16);

impl ProtocolVersion {
    /// Protocol v1.
    pub const V1: Self = Self(1);

    /// Wraps a wire version. Unsupported values are retained so envelope validation can return the
    /// stable `PROTOCOL_UNSUPPORTED` error instead of a generic decode failure.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the numeric wire representation.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for ProtocolVersion {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        value.get()
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

fn parse_decimal_u64(kind: &'static str, value: &str, allow_zero: bool) -> ProtocolResult<u64> {
    if value == "0" || (!value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit()))
    {
        let parsed = value.parse().map_err(|_| ProtocolError::InvalidDecimal {
            kind,
            value: value.to_owned(),
        })?;
        if parsed == 0 && !allow_zero {
            Err(ProtocolError::InvalidDecimal {
                kind,
                value: value.to_owned(),
            })
        } else {
            Ok(parsed)
        }
    } else {
        Err(ProtocolError::InvalidDecimal {
            kind,
            value: value.to_owned(),
        })
    }
}

macro_rules! decimal_string {
    ($name:ident, $kind:literal) => {
        decimal_string!($name, $kind, DECIMAL_U64_PATTERN, true);
    };
    ($name:ident, $kind:literal, $pattern:expr) => {
        decimal_string!($name, $kind, $pattern, false);
    };
    ($name:ident, $kind:literal, $pattern:expr, $allow_zero:expr) => {
        #[doc = concat!("A ", $kind, " encoded as a canonical decimal JSON string.")]
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema)]
        #[schemars(transparent)]
        pub struct $name(
            #[schemars(
                                                                        with = "String",
                                                                        length(min = 1, max = 20),
                                                                        regex(pattern = $pattern)
                                                                    )]
            u64,
        );

        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = ProtocolError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_decimal_u64($kind, value, $allow_zero).map(Self)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

decimal_string!(DecimalU64, "unsigned integer");
decimal_string!(Generation, "generation", POSITIVE_DECIMAL_U64_PATTERN);
decimal_string!(
    SessionGeneration,
    "session generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(
    AssignmentGeneration,
    "assignment generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(
    MountGeneration,
    "mount generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(
    OwnerGeneration,
    "owner generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(
    PlacementGeneration,
    "placement generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(
    DecisionGeneration,
    "decision generation",
    POSITIVE_DECIMAL_U64_PATTERN
);
decimal_string!(ResourceVersion, "resource version");
decimal_string!(FencingToken, "fencing token", POSITIVE_DECIMAL_U64_PATTERN);
decimal_string!(IndexRevision, "index revision");
decimal_string!(SequenceNumber, "sequence number");
decimal_string!(UnixMillis, "Unix millisecond timestamp");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_values_are_json_strings() {
        let value = Generation::new(u64::MAX);
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#""18446744073709551615""#
        );
        assert_eq!(
            serde_json::from_str::<Generation>(r#""42""#).unwrap().get(),
            42
        );
    }

    #[test]
    fn decimal_values_reject_json_numbers_and_noncanonical_strings() {
        assert!(serde_json::from_str::<Generation>("42").is_err());
        assert!(serde_json::from_str::<Generation>(r#""042""#).is_err());
        assert!(serde_json::from_str::<Generation>(r#""+42""#).is_err());
        assert!(serde_json::from_str::<Generation>(r#""18446744073709551616""#).is_err());
        assert!(serde_json::from_str::<Generation>(r#""0""#).is_err());
        assert_eq!(
            serde_json::from_str::<DecimalU64>(r#""0""#).unwrap().get(),
            0
        );
    }
}
