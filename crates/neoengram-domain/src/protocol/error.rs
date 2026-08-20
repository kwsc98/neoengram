use std::{error::Error, fmt};

/// A wire-contract validation or canonicalization failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidIdentifier {
        kind: &'static str,
        value: String,
    },
    InvalidDecimal {
        kind: &'static str,
        value: String,
    },
    UnsupportedProtocolVersion(u16),
    UnsupportedMessageType(String),
    LimitExceeded {
        limit_name: &'static str,
        limit: usize,
        actual: usize,
    },
    InvalidField {
        field: &'static str,
        reason: String,
    },
    Serialization(String),
    InvalidDigest(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier { kind, value } => {
                write!(formatter, "invalid {kind}: {value:?}")
            }
            Self::InvalidDecimal { kind, value } => {
                write!(formatter, "invalid decimal-string {kind}: {value:?}")
            }
            Self::UnsupportedProtocolVersion(version) => {
                write!(formatter, "unsupported protocol version: {version}")
            }
            Self::UnsupportedMessageType(message_type) => {
                write!(
                    formatter,
                    "unsupported protocol message type: {message_type}"
                )
            }
            Self::LimitExceeded {
                limit_name,
                limit,
                actual,
            } => write!(
                formatter,
                "{limit_name} exceeds limit {limit}: observed {actual}"
            ),
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid field {field}: {reason}")
            }
            Self::Serialization(message) => write!(formatter, "serialization failed: {message}"),
            Self::InvalidDigest(message) => write!(formatter, "invalid digest: {message}"),
        }
    }
}

impl Error for ProtocolError {}

impl ProtocolError {
    /// Stable wire error code for adapters that translate validation failures into responses.
    #[must_use]
    pub const fn stable_code(&self) -> &'static str {
        match self {
            Self::UnsupportedProtocolVersion(_) | Self::UnsupportedMessageType(_) => {
                "PROTOCOL_UNSUPPORTED"
            }
            Self::LimitExceeded { .. } => "PROTOCOL_LIMIT_EXCEEDED",
            Self::InvalidDigest(_) => "PROTOCOL_DIGEST_MISMATCH",
            Self::InvalidIdentifier { .. }
            | Self::InvalidDecimal { .. }
            | Self::InvalidField { .. }
            | Self::Serialization(_) => "PROTOCOL_INVALID",
        }
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;
