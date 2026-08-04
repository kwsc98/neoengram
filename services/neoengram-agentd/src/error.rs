use std::{io, path::PathBuf};

use neoengram_agent::AgentError;

pub type AgentDaemonResult<T> = Result<T, AgentDaemonError>;

#[derive(Debug, thiserror::Error)]
pub enum AgentDaemonError {
    #[error("invalid Agent configuration: {0}")]
    Configuration(String),
    #[error("failed to read Agent configuration {path}: {source}")]
    ConfigIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid Agent configuration document: {0}")]
    ConfigDecode(String),
    #[error("Agent identity initialization failed: {0}")]
    Identity(String),
    #[error("Agent mount probe failed: {0}")]
    MountProbe(String),
    #[error("Agent enrollment request failed: {0}")]
    Enrollment(String),
    #[error("Agent enrollment response is invalid: {0}")]
    EnrollmentProtocol(String),
    #[error("Agent status timestamp state is invalid: {0}")]
    StatusClock(String),
    #[error("Agent enrollment was rejected")]
    EnrollmentRejected,
    #[error("Agent enrollment expired")]
    EnrollmentExpired,
    #[error("Agent health check failed: {0}")]
    Health(String),
    #[error("Agent runtime I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("Agent shutdown signal failed: {0}")]
    Signal(io::Error),
}

impl From<AgentError> for AgentDaemonError {
    fn from(error: AgentError) -> Self {
        Self::Identity(format!("{}: {}", error.stable_code(), error))
    }
}
