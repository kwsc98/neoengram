use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use neoengram_protocol::{AgentBootstrapRequest, AgentBootstrapStatusRequest};
use neoengramd::{
    AgentRegistryService, BootstrapStorageEnrollmentRequest, CentralError, CentralErrorCode,
};

use super::{AgentEnrollmentHandler, AgentHttpError};

/// Hyper adapter that shares the process-wide transport-neutral registry service.
pub struct RegistryAgentEnrollmentHandler {
    registry: Arc<AgentRegistryService>,
    accepting: Arc<AtomicBool>,
}

impl RegistryAgentEnrollmentHandler {
    #[must_use]
    pub fn new(registry: Arc<AgentRegistryService>) -> Self {
        Self {
            registry,
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }

    #[must_use]
    pub fn with_readiness(registry: Arc<AgentRegistryService>, accepting: Arc<AtomicBool>) -> Self {
        Self {
            registry,
            accepting,
        }
    }
}

#[async_trait]
impl AgentEnrollmentHandler for RegistryAgentEnrollmentHandler {
    async fn bootstrap(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        let request = AgentBootstrapRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .registry
            .bootstrap_storage_enrollment(BootstrapStorageEnrollmentRequest { request })
            .await
            .map_err(map_registry_error)?;
        serde_json::to_vec(&result.accepted).map_err(|_| AgentHttpError::unavailable())
    }

    async fn bootstrap_status(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        let request = AgentBootstrapStatusRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        request
            .verify()
            .map_err(|_| AgentHttpError::bootstrap_denied())?;
        let response = self
            .registry
            .bootstrap_status(request)
            .await
            .map_err(map_registry_error)?;
        serde_json::to_vec(&response).map_err(|_| AgentHttpError::unavailable())
    }
}

fn map_registry_error(error: CentralError) -> AgentHttpError {
    match error.code() {
        CentralErrorCode::StorageFailure | CentralErrorCode::Internal => {
            AgentHttpError::unavailable()
        }
        CentralErrorCode::ProtocolInvalid => AgentHttpError::protocol_invalid(),
        _ => AgentHttpError::bootstrap_denied(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_status_records_do_not_create_an_existence_oracle() {
        let mapped = map_registry_error(CentralError::new(
            CentralErrorCode::LegacyEnrollmentRequiresReissue,
            "sensitive legacy detail",
        ));
        assert_eq!(mapped.status, http::StatusCode::FORBIDDEN);
        assert_eq!(mapped.code, "AGENT_BOOTSTRAP_DENIED");
        assert!(!mapped.detail.contains("legacy"));
    }
}
