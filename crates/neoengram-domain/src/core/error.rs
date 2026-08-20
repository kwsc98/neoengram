use std::{error::Error, fmt};

/// Stable category for a core model validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ValidationErrorKind {
    /// A content digest or typed content identifier is malformed.
    InvalidDigest,
    /// A logical path or path component is not portable or canonical.
    InvalidPath,
    /// An immutable payload object description is invalid.
    InvalidObject,
    /// A file Manifest violates its canonical invariants.
    InvalidManifest,
    /// A Directory or one of its entries violates its canonical invariants.
    InvalidDirectory,
    /// A Commit violates its canonical invariants.
    InvalidCommit,
    /// A managed publication candidate is incomplete or internally inconsistent.
    InvalidPublication,
    /// An Index record, mutation, delta, or snapshot is invalid.
    InvalidIndex,
    /// Arithmetic required to validate or encode a model overflowed.
    Overflow,
}

/// A deterministic validation error returned by the environment-independent core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    kind: ValidationErrorKind,
    message: String,
}

impl ValidationError {
    /// Creates a validation error in the supplied stable category.
    pub fn new(kind: ValidationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the stable category suitable for mapping into an application error code.
    pub const fn kind(&self) -> ValidationErrorKind {
        self.kind
    }

    /// Returns the human-readable diagnostic detail.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ValidationError {}

/// Result type used by core model validation and canonical encoding.
pub type ValidationResult<T> = Result<T, ValidationError>;
