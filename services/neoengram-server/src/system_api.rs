use std::sync::Arc;

use fusen_rs::{interface, Error, ErrorCategory, Response};

use crate::{
    app_state::AppState,
    dto::{ApiVersionResponse, EmptyRequest, HealthStatus},
};

/// System-level endpoints: version discovery and health probes.
#[interface(name = "neoengram.system", group = "api", version = "1")]
pub trait SystemApi {
    /// Returns the supported API versions, agent protocol versions, and capabilities.
    #[fusen_rs::method(method = "POST", path = "/api/system/version/query")]
    async fn query_api_version(
        &self,
        #[param(body)] _body: EmptyRequest,
    ) -> Result<Response<ApiVersionResponse>, Error>;

    /// Liveness probe — always returns OK when the process is running.
    #[fusen_rs::method(method = "GET", path = "/health/live")]
    async fn live_probe(&self) -> Result<Response<HealthStatus>, Error>;

    /// Readiness probe — succeeds only when SQLite is open and integrity checks pass.
    #[fusen_rs::method(method = "GET", path = "/health/ready")]
    async fn ready_probe(&self) -> Result<Response<HealthStatus>, Error>;
}

pub struct SystemApiImpl {
    pub state: Arc<AppState>,
}

impl SystemApi for SystemApiImpl {
    async fn query_api_version(
        &self,
        _body: EmptyRequest,
    ) -> Result<Response<ApiVersionResponse>, Error> {
        Ok(Response::new(ApiVersionResponse {
            api_versions: vec![1],
            agent_protocol_versions: vec![1],
            capabilities: vec!["managed_add".to_owned(), "sqlite_authority".to_owned()],
        }))
    }

    async fn live_probe(&self) -> Result<Response<HealthStatus>, Error> {
        Ok(Response::new(HealthStatus {
            status: "ok".to_owned(),
        }))
    }

    async fn ready_probe(&self) -> Result<Response<HealthStatus>, Error> {
        if !self.state.is_ready() {
            return Err(Error::application(
                ErrorCategory::Unavailable,
                "not_ready",
                "server is starting",
            )
            .unwrap());
        }
        match self.state.authority.integrity_check().await {
            Ok(()) => Ok(Response::new(HealthStatus {
                status: "ok".to_owned(),
            })),
            Err(e) => Err(Error::application(
                ErrorCategory::Unavailable,
                "not_ready",
                format!("integrity check failed: {e}"),
            )
            .unwrap_or_else(|_| {
                Error::application(
                    ErrorCategory::Unavailable,
                    "not_ready",
                    "integrity check failed",
                )
                .unwrap()
            })),
        }
    }
}
