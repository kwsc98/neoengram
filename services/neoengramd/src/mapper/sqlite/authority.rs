use std::{path::PathBuf, sync::Arc, time::Duration};

use neoengram_core::{ContentDigest, Manifest, ManifestId};
use neoengram_protocol::{ArtifactId, JobAssignment, TenantId};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::{
    datasource::sqlite::authority::SqliteAuthorityDataSource,
    mapper::sqlite::agent_registry::{
        open_sqlite_agent_registry, SqliteAgentRegistry, SqliteAgentRegistryConfig,
    },
    AgentEnrollmentAuditEvent, AuditEvent, AuthorityCapabilities, AuthorityStore, CentralError,
    CentralErrorCode, CentralResult, IndexKey, PublishedIndex,
};

const STORED_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct SqliteAuthorityConfig {
    pub path: PathBuf,
    pub busy_timeout: Duration,
}

impl SqliteAuthorityConfig {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

pub struct SqliteAuthority {
    inner: Arc<SqliteAuthorityStore>,
    agent_registry: SqliteAgentRegistry,
}

impl SqliteAuthority {
    #[must_use]
    pub fn authority_store(&self) -> AuthorityStore {
        AuthorityStore::from_parts(
            self.inner.clone(),
            self.inner.clone(),
            self.inner.clone(),
            self.inner.clone(),
            self.inner.clone(),
            self.inner.clone(),
            AuthorityCapabilities::SQLITE,
        )
        .with_precommits(self.inner.clone())
        .with_agent_registry(self.agent_registry.repository())
        .with_control_catalog(self.agent_registry.catalog_repository())
    }

    pub async fn integrity_check(&self) -> CentralResult<()> {
        self.inner.datasource.integrity_check().await?;
        self.agent_registry.integrity_check().await
    }

    pub async fn readiness_check(&self) -> CentralResult<()> {
        self.inner.datasource.readiness_check().await?;
        self.agent_registry.readiness_check().await
    }

    pub async fn close(&self) {
        self.inner.datasource.close().await;
        self.agent_registry.close().await;
    }

    pub async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        self.inner.published_index(key).await
    }

    pub async fn manifest(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        self.inner
            .manifest(tenant_id, artifact_id, manifest_id)
            .await
    }

    pub async fn published_assignments(&self) -> CentralResult<Vec<JobAssignment>> {
        self.inner.published_assignments().await
    }

    pub async fn audit_events(&self) -> CentralResult<Vec<AuditEvent>> {
        self.inner.audit_events().await
    }

    pub async fn enrollment_audit_events(&self) -> CentralResult<Vec<AgentEnrollmentAuditEvent>> {
        let mut events = self.inner.enrollment_audit_events().await?;
        for registry_event in self
            .agent_registry
            .repository()
            .enrollment_audit_events()
            .await?
        {
            if let Some(existing) = events.iter().find(|event| {
                event.tenant_id == registry_event.tenant_id
                    && event.event_id == registry_event.event_id
            }) {
                if existing != &registry_event {
                    return Err(storage_corruption(
                        "authority and Registry enrollment audit events disagree",
                    ));
                }
            } else {
                events.push(registry_event);
            }
        }
        events.sort_by(|left, right| {
            (&left.tenant_id, &left.event_id).cmp(&(&right.tenant_id, &right.event_id))
        });
        Ok(events)
    }
}

pub(super) struct SqliteAuthorityStore {
    pub(super) pool: SqlitePool,
    datasource: Arc<SqliteAuthorityDataSource>,
}

pub async fn open_sqlite_authority(
    config: SqliteAuthorityConfig,
) -> CentralResult<SqliteAuthority> {
    let agent_registry = open_sqlite_agent_registry(SqliteAgentRegistryConfig {
        path: config.path.clone(),
        busy_timeout: config.busy_timeout,
    })
    .await?;
    let datasource =
        Arc::new(SqliteAuthorityDataSource::open(&config.path, config.busy_timeout).await?);
    let pool = datasource.pool().clone();

    Ok(SqliteAuthority {
        inner: Arc::new(SqliteAuthorityStore { pool, datasource }),
        agent_registry,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredV1<T> {
    format: u32,
    value: T,
}

pub(super) fn encode<T: Serialize>(value: &T) -> CentralResult<Vec<u8>> {
    serde_json::to_vec(&StoredV1 {
        format: STORED_FORMAT_VERSION,
        value,
    })
    .map_err(|error| storage_corruption(format!("failed to encode authority record: {error}")))
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> CentralResult<T> {
    let stored: StoredV1<T> = serde_json::from_slice(bytes).map_err(|error| {
        storage_corruption(format!(
            "authority record is not valid current-format JSON: {error}"
        ))
    })?;
    if stored.format != STORED_FORMAT_VERSION {
        return Err(storage_corruption(format!(
            "authority record format {} is unsupported",
            stored.format
        )));
    }
    Ok(stored.value)
}

pub(super) fn parse_canonical_u64(value: String, field: &str) -> CentralResult<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(storage_corruption(format!(
            "stored {field} is not canonical decimal u64"
        )));
    }
    value
        .parse()
        .map_err(|_| storage_corruption(format!("stored {field} is outside the u64 range")))
}

pub(super) fn exact_32(bytes: Vec<u8>, field: &str) -> CentralResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| storage_corruption(format!("stored {field} is not exactly 32 bytes")))
}

pub(super) fn digest_from_blob(bytes: Vec<u8>, field: &str) -> CentralResult<ContentDigest> {
    Ok(ContentDigest::from_bytes(exact_32(bytes, field)?))
}

pub(super) fn manifest_id_from_blob(bytes: Vec<u8>) -> CentralResult<ManifestId> {
    Ok(ManifestId::from_bytes(exact_32(bytes, "ManifestId")?))
}

pub(super) fn storage_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        format!("SQLite authority storage operation failed: {error}"),
    )
}

pub(super) fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_releases_both_locks_while_the_old_handle_remains_alive() {
        let directory = tempfile::TempDir::new().unwrap();
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();

        authority.close().await;

        let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        reopened.close().await;
        drop(authority);
    }
}
