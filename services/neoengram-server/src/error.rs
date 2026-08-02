use fusen_rs::{Error, ErrorCategory};
use neoengramd::{CentralError, CentralErrorCode};

/// Maps a `CentralError` to a `fusen_rs::Error` using broad HTTP-compatible categories.
///
/// The mapping is deliberately coarse: domain errors are classified as invalid-argument,
/// not-found, conflict, unavailable, or internal. This avoids exposing fine-grained
/// internal state-machine details over the wire.
pub(crate) fn map_central_error(error: CentralError) -> fusen_rs::Error {
    let (category, code, message) = classify(&error);
    Error::application(category, code, message).unwrap_or_else(|_| {
        Error::application(
            ErrorCategory::Internal,
            "internal_error",
            "an unexpected error occurred",
        )
        .unwrap()
    })
}

fn classify(error: &CentralError) -> (ErrorCategory, String, String) {
    let code = error.code();
    let message = error.message().to_owned();
    match code {
        CentralErrorCode::JobNotFound | CentralErrorCode::EnrollmentNotFound => {
            (ErrorCategory::NotFound, code.as_str().to_owned(), message)
        }
        CentralErrorCode::ProtocolInvalid => (
            ErrorCategory::InvalidArgument,
            code.as_str().to_owned(),
            message,
        ),
        CentralErrorCode::JobAlreadyExists
        | CentralErrorCode::JobIdReused
        | CentralErrorCode::InvalidState
        | CentralErrorCode::AssignmentMismatch
        | CentralErrorCode::GenerationMismatch
        | CentralErrorCode::DeadlineExceeded
        | CentralErrorCode::ConcurrentUpdate
        | CentralErrorCode::EnrollmentIdReused
        | CentralErrorCode::EnrollmentExpired
        | CentralErrorCode::EnrollmentDecisionConflict
        | CentralErrorCode::EnrollmentProbeFailed
        | CentralErrorCode::VolumeOwnerConflict
        | CentralErrorCode::AgentIdentityMismatch
        | CentralErrorCode::AgentSessionActive
        | CentralErrorCode::ApprovalRequired
        | CentralErrorCode::BootstrapDenied => {
            (ErrorCategory::Conflict, code.as_str().to_owned(), message)
        }
        CentralErrorCode::StorageFailure => (
            ErrorCategory::Unavailable,
            code.as_str().to_owned(),
            message,
        ),
        CentralErrorCode::Unauthorized => (
            ErrorCategory::PermissionDenied,
            code.as_str().to_owned(),
            message,
        ),
        CentralErrorCode::BatchUndeclared
        | CentralErrorCode::BatchIncomplete
        | CentralErrorCode::BatchTampered
        | CentralErrorCode::ObjectNotDurable
        | CentralErrorCode::MetadataInvalid
        | CentralErrorCode::Internal => {
            (ErrorCategory::Internal, code.as_str().to_owned(), message)
        }
    }
}
