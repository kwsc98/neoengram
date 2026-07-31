use std::{error::Error, fmt};

use neoengram_engine::EngineError;
use neoengram_protocol::ProtocolError;

/// Stable failure categories returned by the local Agent component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AgentErrorCode {
    InvalidAssignment,
    DeadlineExceeded,
    JobIdReused,
    AssignmentNotFound,
    AssignmentMismatch,
    ScopeMismatch,
    GenerationMismatch,
    MountUnavailable,
    InvalidState,
    LedgerConflict,
    ExecutionFailed,
    ObjectTransferFailed,
    ReportFailed,
    ProtocolInvalid,
    Internal,
}

impl AgentErrorCode {
    /// Stable machine-readable code suitable for a control-plane report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidAssignment => "ASSIGNMENT_INVALID",
            Self::DeadlineExceeded => "ASSIGNMENT_DEADLINE_EXCEEDED",
            Self::JobIdReused => "JOB_ID_REUSED",
            Self::AssignmentNotFound => "ASSIGNMENT_NOT_FOUND",
            Self::AssignmentMismatch => "ASSIGNMENT_MISMATCH",
            Self::ScopeMismatch => "ASSIGNMENT_SCOPE_MISMATCH",
            Self::GenerationMismatch => "GENERATION_MISMATCH",
            Self::MountUnavailable => "MOUNT_UNAVAILABLE",
            Self::InvalidState => "AGENT_STATE_INVALID",
            Self::LedgerConflict => "LEDGER_CONFLICT",
            Self::ExecutionFailed => "EXECUTION_FAILED",
            Self::ObjectTransferFailed => "OBJECT_TRANSFER_FAILED",
            Self::ReportFailed => "REPORT_FAILED",
            Self::ProtocolInvalid => "PROTOCOL_INVALID",
            Self::Internal => "AGENT_INTERNAL",
        }
    }
}

/// An Agent error with a stable code separated from its operator-facing message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentError {
    code: AgentErrorCode,
    message: String,
}

impl AgentError {
    #[must_use]
    pub fn new(code: AgentErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> AgentErrorCode {
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
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AgentError {}

impl From<EngineError> for AgentError {
    fn from(error: EngineError) -> Self {
        Self::new(AgentErrorCode::ExecutionFailed, error.to_string())
    }
}

impl From<ProtocolError> for AgentError {
    fn from(error: ProtocolError) -> Self {
        Self::new(AgentErrorCode::ProtocolInvalid, error.to_string())
    }
}

pub type AgentResult<T> = Result<T, AgentError>;
