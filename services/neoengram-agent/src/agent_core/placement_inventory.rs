//! Durable Agent-local inventory of object placements.
//!
//! Central remains the authority for placement scheduling, but a source Agent must also prove
//! that the placement named by a signed batch ticket is present in its own Volume.  This module
//! stores that small piece of local evidence in the shared Agent SQLite database.  It deliberately
//! stores no object bytes and never accepts a path or placement metadata from the wire.

use std::{collections::BTreeMap, path::PathBuf, sync::RwLock};

use neoengram_domain::core::ObjectId;
use neoengram_domain::protocol::{
    materialization::ObjectPlacement, AgentId, ObjectNamespaceId, PlacementId, StorageVolumeId,
    TenantId,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use super::{
    sqlite_storage::{
        storage_corruption, storage_error, LockedSqlite, SqliteDefinition,
        AGENT_STATE_APPLICATION_ID, AGENT_STATE_DATABASE_FILE, AGENT_STATE_LOCK_FILE,
        AGENT_STATE_SCHEMA_VERSION,
    },
    AgentError, AgentErrorCode, AgentResult,
};

const DATABASE_FILE: &str = AGENT_STATE_DATABASE_FILE;
const LOCK_FILE: &str = AGENT_STATE_LOCK_FILE;
const APPLICATION_ID: i64 = AGENT_STATE_APPLICATION_ID;
const SCHEMA_VERSION: i64 = AGENT_STATE_SCHEMA_VERSION;
const INVENTORY_MAGIC: &str = "neoengram-agent-placement-inventory-v2";

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS placement_inventory_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    magic TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS placement_inventory (
    tenant_id TEXT NOT NULL,
    object_namespace_id TEXT NOT NULL,
    placement_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    placement_generation TEXT NOT NULL CHECK (
        placement_generation <> '' AND placement_generation NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, object_namespace_id, placement_id, object_id)
) STRICT;
CREATE INDEX IF NOT EXISTS placement_inventory_lookup
    ON placement_inventory (tenant_id, object_namespace_id, placement_id, object_id);
"#;

/// Local authority boundary used by source QUIC handlers and object publication adapters.
/// Implementations must be scoped to one Agent/Tenant/Volume and must fail closed on malformed
/// persisted records.
pub trait LocalPlacementInventory: std::fmt::Debug + Send + Sync {
    fn lookup(
        &self,
        tenant_id: &TenantId,
        placement_id: &PlacementId,
        object_namespace_id: &ObjectNamespaceId,
        object_id: ObjectId,
    ) -> AgentResult<Option<ObjectPlacement>>;

    fn record(&self, placement: ObjectPlacement) -> AgentResult<()>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct PlacementInventoryConfig {
    pub root: PathBuf,
    pub agent_id: AgentId,
    pub tenant_id: TenantId,
    pub storage_volume_id: StorageVolumeId,
}

impl PlacementInventoryConfig {
    #[must_use]
    pub fn new(
        root: impl Into<PathBuf>,
        agent_id: AgentId,
        tenant_id: TenantId,
        storage_volume_id: StorageVolumeId,
    ) -> Self {
        Self {
            root: root.into(),
            agent_id,
            tenant_id,
            storage_volume_id,
        }
    }
}

impl std::fmt::Debug for PlacementInventoryConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlacementInventoryConfig")
            .field("root_configured", &!self.root.as_os_str().is_empty())
            .field("agent_id", &self.agent_id)
            .field("tenant_id", &self.tenant_id)
            .field("storage_volume_id", &self.storage_volume_id)
            .finish()
    }
}

/// Durable inventory backed by the shared Agent SQLite database.
#[derive(Debug)]
pub struct SqlitePlacementInventory {
    storage: LockedSqlite,
    agent_id: AgentId,
    tenant_id: TenantId,
    storage_volume_id: StorageVolumeId,
}

impl SqlitePlacementInventory {
    pub fn open(config: PlacementInventoryConfig) -> AgentResult<Self> {
        let storage = LockedSqlite::open(
            &config.root,
            SqliteDefinition {
                database_file: DATABASE_FILE,
                lock_file: LOCK_FILE,
                application_id: APPLICATION_ID,
                schema_version: SCHEMA_VERSION,
                schema: SCHEMA,
                tables: &["placement_inventory_metadata", "placement_inventory"],
            },
        )?;
        let inventory = Self {
            storage,
            agent_id: config.agent_id,
            tenant_id: config.tenant_id,
            storage_volume_id: config.storage_volume_id,
        };
        inventory.bind_or_validate_identity()?;
        Ok(inventory)
    }

    pub fn integrity_check(&self) -> AgentResult<()> {
        self.storage.integrity_check()?;
        self.validate_metadata()
    }

    fn bind_or_validate_identity(&self) -> AgentResult<()> {
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let identity = transaction
            .query_row(
                "SELECT magic, agent_id, tenant_id, storage_volume_id \
                 FROM placement_inventory_metadata WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        match identity {
            None => {
                transaction
                    .execute(
                        "INSERT INTO placement_inventory_metadata \
                         (singleton, magic, agent_id, tenant_id, storage_volume_id) \
                         VALUES (1, ?1, ?2, ?3, ?4)",
                        params![
                            INVENTORY_MAGIC,
                            self.agent_id.as_str(),
                            self.tenant_id.as_str(),
                            self.storage_volume_id.as_str(),
                        ],
                    )
                    .map_err(storage_error)?;
            }
            Some((magic, agent_id, tenant_id, storage_volume_id))
                if magic == INVENTORY_MAGIC
                    && agent_id == self.agent_id.as_str()
                    && tenant_id == self.tenant_id.as_str()
                    && storage_volume_id == self.storage_volume_id.as_str() => {}
            Some(_) => return Err(identity_mismatch()),
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()
    }

    fn validate_metadata(&self) -> AgentResult<()> {
        let connection = self.storage.connection()?;
        let identity: (String, String, String, String) = connection
            .query_row(
                "SELECT magic, agent_id, tenant_id, storage_volume_id \
                 FROM placement_inventory_metadata WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(storage_error)?;
        if identity.0 != INVENTORY_MAGIC
            || identity.1 != self.agent_id.as_str()
            || identity.2 != self.tenant_id.as_str()
            || identity.3 != self.storage_volume_id.as_str()
        {
            return Err(identity_mismatch());
        }
        Ok(())
    }
}

impl LocalPlacementInventory for SqlitePlacementInventory {
    fn lookup(
        &self,
        tenant_id: &TenantId,
        placement_id: &PlacementId,
        object_namespace_id: &ObjectNamespaceId,
        object_id: ObjectId,
    ) -> AgentResult<Option<ObjectPlacement>> {
        self.validate_metadata()?;
        let connection = self.storage.connection()?;
        let payload = connection
            .query_row(
                "SELECT payload FROM placement_inventory \
                 WHERE tenant_id = ?1 AND object_namespace_id = ?2 \
                   AND placement_id = ?3 AND object_id = ?4",
                params![
                    tenant_id.as_str(),
                    object_namespace_id.as_str(),
                    placement_id.as_str(),
                    object_id.to_hex(),
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(storage_error)?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let placement: ObjectPlacement = serde_json::from_slice(&payload)
            .map_err(|_| storage_corruption("local placement inventory payload is invalid"))?;
        placement.validate().map_err(|error| {
            storage_corruption(format!(
                "local placement inventory record is invalid: {error}"
            ))
        })?;
        if *tenant_id != placement.tenant_id
            || placement.tenant_id != self.tenant_id
            || placement.object_namespace_id != *object_namespace_id
            || placement.placement_id != *placement_id
            || placement.object_id != object_id
            || placement.storage_volume_id.as_ref() != Some(&self.storage_volume_id)
            || placement.archive_id.is_some()
        {
            return Err(storage_corruption(
                "local placement inventory indexed columns differ from payload",
            ));
        }
        Ok(Some(placement))
    }

    fn record(&self, placement: ObjectPlacement) -> AgentResult<()> {
        placement
            .validate()
            .map_err(|error| AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string()))?;
        if placement.tenant_id != self.tenant_id
            || placement.storage_volume_id.as_ref() != Some(&self.storage_volume_id)
            || placement.archive_id.is_some()
        {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "placement does not belong to the local Agent Volume",
            ));
        }
        let payload = serde_json::to_vec(&placement).map_err(storage_error)?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO placement_inventory \
                 (tenant_id, object_namespace_id, placement_id, object_id, placement_generation, payload) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    placement.tenant_id.as_str(),
                    placement.object_namespace_id.as_str(),
                    placement.placement_id.as_str(),
                    placement.object_id.to_hex(),
                    placement.placement_generation.get().to_string(),
                    payload,
                ],
            )
            .map_err(storage_error)?;
        let stored: Vec<u8> = transaction
            .query_row(
                "SELECT payload FROM placement_inventory \
                 WHERE tenant_id = ?1 AND object_namespace_id = ?2 \
                   AND placement_id = ?3 AND object_id = ?4",
                params![
                    placement.tenant_id.as_str(),
                    placement.object_namespace_id.as_str(),
                    placement.placement_id.as_str(),
                    placement.object_id.to_hex(),
                ],
                |row| row.get(0),
            )
            .map_err(storage_error)?;
        if stored != payload {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "local placement inventory identity is already bound to different metadata",
            ));
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()
    }
}

fn identity_mismatch() -> AgentError {
    AgentError::new(
        AgentErrorCode::AssignmentMismatch,
        "local placement inventory database identity does not match the configured Agent/Tenant/Volume",
    )
}

/// Small deterministic inventory used by unit tests and embedded callers. Production startup
/// should use [`SqlitePlacementInventory`] so placement evidence survives a restart.
#[derive(Debug, Default)]
pub struct InMemoryPlacementInventory {
    entries: RwLock<BTreeMap<(String, String, PlacementId, ObjectId), ObjectPlacement>>,
}

impl LocalPlacementInventory for InMemoryPlacementInventory {
    fn lookup(
        &self,
        tenant_id: &TenantId,
        placement_id: &PlacementId,
        object_namespace_id: &ObjectNamespaceId,
        object_id: ObjectId,
    ) -> AgentResult<Option<ObjectPlacement>> {
        self.entries
            .read()
            .map_err(|_| {
                AgentError::new(
                    AgentErrorCode::Internal,
                    "placement inventory lock poisoned",
                )
            })
            .map(|entries| {
                entries
                    .get(&(
                        tenant_id.as_str().to_owned(),
                        object_namespace_id.as_str().to_owned(),
                        placement_id.clone(),
                        object_id,
                    ))
                    .cloned()
            })
    }

    fn record(&self, placement: ObjectPlacement) -> AgentResult<()> {
        placement
            .validate()
            .map_err(|error| AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string()))?;
        let key = (
            placement.tenant_id.as_str().to_owned(),
            placement.object_namespace_id.as_str().to_owned(),
            placement.placement_id.clone(),
            placement.object_id,
        );
        let mut entries = self.entries.write().map_err(|_| {
            AgentError::new(
                AgentErrorCode::Internal,
                "placement inventory lock poisoned",
            )
        })?;
        if let Some(existing) = entries.get(&key) {
            if existing != &placement {
                return Err(AgentError::new(
                    AgentErrorCode::AssignmentMismatch,
                    "local placement inventory identity is already bound to different metadata",
                ));
            }
            return Ok(());
        }
        entries.insert(key, placement);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neoengram_domain::protocol::{
        materialization::ObjectPlacementState, ObjectEncoding, PlacementGeneration,
    };

    fn placement() -> ObjectPlacement {
        let object_id = ObjectId::from_bytes([7; 32]);
        ObjectPlacement {
            placement_id: PlacementId::new("placement-local").unwrap(),
            tenant_id: TenantId::new("tenant-local").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("namespace-local").unwrap(),
            object_id,
            size: neoengram_domain::DecimalU64::new(3),
            encoding: ObjectEncoding::Raw,
            verified_digest: object_id.digest(),
            storage_volume_id: Some(StorageVolumeId::new("volume-local").unwrap()),
            archive_id: None,
            placement_generation: PlacementGeneration::new(2),
            state: ObjectPlacementState::Verified,
            failure_domain: "volume:volume-local".to_owned(),
        }
    }

    #[test]
    fn sqlite_inventory_is_scoped_idempotent_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let value = placement();
        let config = PlacementInventoryConfig::new(
            root.path(),
            AgentId::new("agent-local").unwrap(),
            value.tenant_id.clone(),
            value.storage_volume_id.clone().unwrap(),
        );
        let inventory = SqlitePlacementInventory::open(config.clone()).unwrap();
        inventory.record(value.clone()).unwrap();
        assert_eq!(
            inventory
                .lookup(
                    &value.tenant_id,
                    &value.placement_id,
                    &value.object_namespace_id,
                    value.object_id,
                )
                .unwrap(),
            Some(value.clone())
        );
        inventory.record(value.clone()).unwrap();
        assert!(inventory
            .lookup(
                &TenantId::new("tenant-other").unwrap(),
                &value.placement_id,
                &value.object_namespace_id,
                value.object_id,
            )
            .unwrap()
            .is_none());
        drop(inventory);
        let reopened = SqlitePlacementInventory::open(config).unwrap();
        assert_eq!(
            reopened
                .lookup(
                    &value.tenant_id,
                    &value.placement_id,
                    &value.object_namespace_id,
                    value.object_id,
                )
                .unwrap(),
            Some(value)
        );
    }

    #[test]
    fn in_memory_inventory_rejects_identity_reuse() {
        let inventory = InMemoryPlacementInventory::default();
        let value = placement();
        inventory.record(value.clone()).unwrap();
        let mut conflicting = value.clone();
        conflicting.placement_generation = PlacementGeneration::new(3);
        assert!(inventory.record(conflicting).is_err());
    }
}
