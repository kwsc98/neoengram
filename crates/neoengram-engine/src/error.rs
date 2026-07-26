use std::{collections::BTreeMap, error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Stable engine-level failure category. Protocol adapters may map this to their wire error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArgument,
    InvalidPath,
    ResourceNotFound,
    AlreadyExists,
    Conflict,
    IndexVersionMismatch,
    WorktreeChangedDuringScan,
    LeaseExpired,
    FenceRejected,
    RecoveryRequired,
    IntegrityViolation,
    ObjectMissing,
    ObjectCorrupt,
    StorageUnavailable,
    Io,
    InjectedFailure,
    Internal,
}

impl ErrorCode {
    /// Conservative retry classification used unless a caller supplies a narrower one.
    #[must_use]
    pub const fn default_retry_class(self) -> RetryClass {
        match self {
            Self::StorageUnavailable | Self::Io => RetryClass::Backoff,
            Self::IndexVersionMismatch | Self::Conflict => RetryClass::AfterStateRefresh,
            Self::LeaseExpired | Self::FenceRejected => RetryClass::AfterLeaseRefresh,
            Self::RecoveryRequired => RetryClass::AfterRecovery,
            Self::InvalidArgument
            | Self::InvalidPath
            | Self::ResourceNotFound
            | Self::AlreadyExists
            | Self::WorktreeChangedDuringScan
            | Self::IntegrityViolation
            | Self::ObjectMissing
            | Self::ObjectCorrupt
            | Self::InjectedFailure
            | Self::Internal => RetryClass::Never,
        }
    }
}

/// What must happen before retrying a failed engine operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    Never,
    Immediate,
    Backoff,
    AfterStateRefresh,
    AfterLeaseRefresh,
    AfterRecovery,
}

/// Error returned by engine ports and orchestration code.
#[derive(Debug)]
pub struct EngineError {
    code: ErrorCode,
    retry_class: RetryClass,
    message: String,
    context: BTreeMap<String, String>,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl EngineError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            retry_class: code.default_retry_class(),
            message: message.into(),
            context: BTreeMap::new(),
            source: None,
        }
    }

    #[must_use]
    pub fn with_retry_class(mut self, retry_class: RetryClass) -> Self {
        self.retry_class = retry_class;
        self
    }

    #[must_use]
    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    /// Adds one stable, machine-readable context value without changing the display message.
    #[must_use]
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub const fn retry_class(&self) -> RetryClass {
        self.retry_class
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns structured diagnostic context suitable for logs and protocol error details.
    #[must_use]
    pub const fn context(&self) -> &BTreeMap<String, String> {
        &self.context
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<std::io::Error> for EngineError {
    fn from(error: std::io::Error) -> Self {
        Self::new(ErrorCode::Io, error.to_string()).with_source(error)
    }
}

impl From<neoengram_core::ValidationError> for EngineError {
    fn from(error: neoengram_core::ValidationError) -> Self {
        let code = match error.kind() {
            neoengram_core::ValidationErrorKind::InvalidPath => ErrorCode::InvalidPath,
            neoengram_core::ValidationErrorKind::InvalidDigest
            | neoengram_core::ValidationErrorKind::InvalidObject
            | neoengram_core::ValidationErrorKind::InvalidManifest
            | neoengram_core::ValidationErrorKind::InvalidDirectory
            | neoengram_core::ValidationErrorKind::InvalidCommit
            | neoengram_core::ValidationErrorKind::InvalidPublication
            | neoengram_core::ValidationErrorKind::InvalidIndex
            | neoengram_core::ValidationErrorKind::Overflow => ErrorCode::InvalidArgument,
            _ => ErrorCode::InvalidArgument,
        };
        Self::new(code, error.to_string()).with_source(error)
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use neoengram_core::LogicalPath;

    use super::*;

    #[test]
    fn retry_class_is_stable_and_conservative() {
        assert_eq!(
            EngineError::new(ErrorCode::StorageUnavailable, "offline").retry_class(),
            RetryClass::Backoff
        );
        assert_eq!(
            EngineError::new(ErrorCode::FenceRejected, "stale fence").retry_class(),
            RetryClass::AfterLeaseRefresh
        );
    }

    #[test]
    fn maps_core_path_validation_without_losing_the_source() {
        let core_error = LogicalPath::parse("../escape").unwrap_err();
        let error = EngineError::from(core_error);
        assert_eq!(error.code(), ErrorCode::InvalidPath);
        assert!(error.source().is_some());
    }

    #[test]
    fn structured_context_is_separate_from_the_operator_message() {
        let error = EngineError::new(ErrorCode::Conflict, "index publication conflicted")
            .with_context("artifact_id", "artifact-a")
            .with_context("expected_revision", "7");
        assert_eq!(error.message(), "index publication conflicted");
        assert_eq!(
            error.context().get("artifact_id").map(String::as_str),
            Some("artifact-a")
        );
        assert_eq!(
            error.context().get("expected_revision").map(String::as_str),
            Some("7")
        );
    }
}
