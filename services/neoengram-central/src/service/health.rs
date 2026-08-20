use std::sync::Arc;

use async_trait::async_trait;
use fusen_rs::Error;

use crate::dto::HealthStatus;

/// Backend readiness boundary used by the health application service.
#[async_trait]
pub trait ReadinessProbe: Send + Sync + 'static {
    async fn check(&self) -> Result<(), Error>;
}

/// Liveness and readiness application service.
pub struct HealthService {
    readiness: Arc<dyn ReadinessProbe>,
}

impl HealthService {
    pub fn new(readiness: Arc<dyn ReadinessProbe>) -> Self {
        Self { readiness }
    }

    pub fn live(&self) -> HealthStatus {
        HealthStatus {
            status: "ok".to_owned(),
        }
    }

    pub async fn ready(&self) -> Result<HealthStatus, Error> {
        self.readiness.check().await?;
        Ok(HealthStatus {
            status: "ok".to_owned(),
        })
    }
}
