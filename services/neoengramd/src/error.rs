use std::{error::Error, fmt};

use neoengram_protocol::ProtocolError;

/// Stable machine-readable central control-plane error class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CentralErrorCode {
    Unauthorized,
    EnrollmentNotFound,
    EnrollmentIdReused,
    EnrollmentExpired,
    EnrollmentDecisionConflict,
    EnrollmentProbeFailed,
    BootstrapDenied,
    ApprovalRequired,
    AgentIdentityMismatch,
    AgentSessionActive,
    VolumeOwnerConflict,
    JobNotFound,
    JobAlreadyExists,
    JobIdReused,
    InvalidState,
    AssignmentMismatch,
    GenerationMismatch,
    DeadlineExceeded,
    ProtocolInvalid,
    BatchUndeclared,
    BatchIncomplete,
    BatchTampered,
    ObjectNotDurable,
    MetadataInvalid,
    ConcurrentUpdate,
    StorageFailure,
    Internal,
}

impl CentralErrorCode {
    /// Stable code suitable for wire adapters, logs, and retry policies.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "AUTHORIZATION_DENIED",
            Self::EnrollmentNotFound => "AGENT_ENROLLMENT_NOT_FOUND",
            Self::EnrollmentIdReused => "AGENT_ENROLLMENT_ID_REUSED",
            Self::EnrollmentExpired => "AGENT_ENROLLMENT_EXPIRED",
            Self::EnrollmentDecisionConflict => "AGENT_ENROLLMENT_DECISION_CONFLICT",
            Self::EnrollmentProbeFailed => "AGENT_ENROLLMENT_PROBE_FAILED",
            Self::BootstrapDenied => "AGENT_BOOTSTRAP_DENIED",
            Self::ApprovalRequired => "AGENT_APPROVAL_REQUIRED",
            Self::AgentIdentityMismatch => "AGENT_IDENTITY_MISMATCH",
            Self::AgentSessionActive => "AGENT_SESSION_ACTIVE",
            Self::VolumeOwnerConflict => "VOLUME_OWNER_CONFLICT",
            Self::JobNotFound => "JOB_NOT_FOUND",
            Self::JobAlreadyExists => "JOB_ALREADY_EXISTS",
            Self::JobIdReused => "JOB_ID_REUSED",
            Self::InvalidState => "JOB_INVALID_STATE",
            Self::AssignmentMismatch => "ASSIGNMENT_MISMATCH",
            Self::GenerationMismatch => "GENERATION_MISMATCH",
            Self::DeadlineExceeded => "JOB_DEADLINE_EXCEEDED",
            Self::ProtocolInvalid => "PROTOCOL_INVALID",
            Self::BatchUndeclared => "METADATA_BATCH_UNDECLARED",
            Self::BatchIncomplete => "METADATA_BATCH_INCOMPLETE",
            Self::BatchTampered => "METADATA_BATCH_TAMPERED",
            Self::ObjectNotDurable => "OBJECT_NOT_DURABLE",
            Self::MetadataInvalid => "METADATA_INVALID",
            Self::ConcurrentUpdate => "CONCURRENT_UPDATE",
            Self::StorageFailure => "STORAGE_FAILURE",
            Self::Internal => "INTERNAL",
        }
    }
}

/// A control-plane failure with a stable code separated from its operator-facing message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralError {
    code: CentralErrorCode,
    message: String,
    retryable: bool,
}

impl CentralError {
    #[must_use]
    pub fn new(code: CentralErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: matches!(
                code,
                CentralErrorCode::ConcurrentUpdate
                    | CentralErrorCode::StorageFailure
                    | CentralErrorCode::Internal
            ),
        }
    }

    #[must_use]
    pub const fn code(&self) -> CentralErrorCode {
        self.code
    }

    #[must_use]
    pub const fn stable_code(&self) -> &'static str {
        self.code.as_str()
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub const fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

impl fmt::Display for CentralError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.stable_code(), self.message)
    }
}

impl Error for CentralError {}

impl From<ProtocolError> for CentralError {
    fn from(error: ProtocolError) -> Self {
        let code = match error.stable_code() {
            "PROTOCOL_DIGEST_MISMATCH" => CentralErrorCode::BatchTampered,
            _ => CentralErrorCode::ProtocolInvalid,
        };
        Self::new(code, error.to_string())
    }
}

pub type CentralResult<T> = Result<T, CentralError>;
