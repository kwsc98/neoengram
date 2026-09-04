//! Durable Agent-local inventory of object placements.
//!
//! Central remains the authority for placement scheduling, but a source Agent must also prove
//! that the placement named by a signed batch ticket is present in its own Volume.  This module
//! stores that small piece of local evidence in the shared Agent SQLite database.  It deliberately
//! stores no object bytes and never accepts a path or placement metadata from the wire.

use std::{collections::BTreeMap, num::NonZeroUsize, path::PathBuf, str::FromStr, sync::RwLock};

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
/// Upper bound for one placement inventory enumeration page.
pub const MAX_PLACEMENT_INVENTORY_PAGE_SIZE: usize = 4_096;

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

    /// Enumerates local placement evidence in a stable order.
    ///
    /// Implementations predating the integrity scanner can keep the default error and therefore
    /// remain source-compatible; a scanner will fail closed if a complete listing is unavailable.
    fn list_page(
        &self,
        _cursor: Option<&str>,
        _limit: NonZeroUsize,
    ) -> AgentResult<PlacementInventoryPage> {
        Err(AgentError::new(
            AgentErrorCode::InvalidState,
            "local placement inventory does not support enumeration",
        ))
    }

    /// Enumerates every placement using the bounded page API.
    fn list_all(&self) -> AgentResult<Vec<ObjectPlacement>> {
        let page_size = NonZeroUsize::new(MAX_PLACEMENT_INVENTORY_PAGE_SIZE)
            .expect("placement inventory page size is non-zero");
        let mut cursor = None;
        let mut placements = Vec::new();
        loop {
            let page = self.list_page(cursor.as_deref(), page_size)?;
            if page.placements.is_empty() && page.next_cursor.is_some() {
                return Err(AgentError::new(
                    AgentErrorCode::Internal,
                    "local placement inventory returned an empty page with a continuation",
                ));
            }
            placements.extend(page.placements);
            let Some(next_cursor) = page.next_cursor else {
                return Ok(placements);
            };
            if cursor.as_deref() == Some(next_cursor.as_str()) {
                return Err(AgentError::new(
                    AgentErrorCode::Internal,
                    "local placement inventory returned a non-advancing cursor",
                ));
            }
            cursor = Some(next_cursor);
        }
    }
}

/// A bounded page returned by [`LocalPlacementInventory::list_page`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementInventoryPage {
    pub placements: Vec<ObjectPlacement>,
    pub next_cursor: Option<String>,
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

    fn list_page(
        &self,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> AgentResult<PlacementInventoryPage> {
        self.validate_metadata()?;
        let limit = limit.get().min(MAX_PLACEMENT_INVENTORY_PAGE_SIZE);
        let page_limit = limit.checked_add(1).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                "placement inventory page size overflows",
            )
        })?;
        let decoded_cursor = cursor.map(decode_cursor).transpose()?;
        if let Some((tenant_id, _, _, _)) = &decoded_cursor {
            if tenant_id != &self.tenant_id {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "placement inventory cursor belongs to another tenant",
                ));
            }
        }

        let connection = self.storage.connection()?;
        let mut placements = Vec::with_capacity(page_limit);
        if let Some((tenant_id, namespace_id, placement_id, object_id)) = decoded_cursor {
            let mut statement = connection
                .prepare(
                    "SELECT tenant_id, object_namespace_id, placement_id, object_id, \
                            placement_generation, payload \
                     FROM placement_inventory \
                     WHERE tenant_id = ?1 AND (\
                           object_namespace_id > ?2 OR \
                           (object_namespace_id = ?2 AND placement_id > ?3) OR \
                           (object_namespace_id = ?2 AND placement_id = ?3 AND object_id > ?4)\
                     ) \
                     ORDER BY object_namespace_id, placement_id, object_id \
                     LIMIT ?5",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![
                    tenant_id.as_str(),
                    namespace_id.as_str(),
                    placement_id.as_str(),
                    object_id.to_hex(),
                    page_limit as i64,
                ])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                placements.push(decode_inventory_row(
                    row,
                    &self.tenant_id,
                    &self.storage_volume_id,
                )?);
            }
        } else {
            let mut statement = connection
                .prepare(
                    "SELECT tenant_id, object_namespace_id, placement_id, object_id, \
                            placement_generation, payload \
                     FROM placement_inventory \
                     WHERE tenant_id = ?1 \
                     ORDER BY object_namespace_id, placement_id, object_id \
                     LIMIT ?2",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![self.tenant_id.as_str(), page_limit as i64])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                placements.push(decode_inventory_row(
                    row,
                    &self.tenant_id,
                    &self.storage_volume_id,
                )?);
            }
        }

        let has_more = placements.len() > limit;
        if has_more {
            placements.truncate(limit);
        }
        let next_cursor = has_more.then(|| {
            encode_cursor(
                placements
                    .last()
                    .expect("a page with more placements cannot be empty"),
            )
        });
        Ok(PlacementInventoryPage {
            placements,
            next_cursor,
        })
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

    fn list_page(
        &self,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> AgentResult<PlacementInventoryPage> {
        let limit = limit.get().min(MAX_PLACEMENT_INVENTORY_PAGE_SIZE);
        let page_limit = limit.checked_add(1).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                "placement inventory page size overflows",
            )
        })?;
        let cursor = cursor.map(decode_cursor).transpose()?;
        let entries = self.entries.read().map_err(|_| {
            AgentError::new(
                AgentErrorCode::Internal,
                "placement inventory lock poisoned",
            )
        })?;
        let mut placements = Vec::with_capacity(page_limit);
        for ((tenant, namespace, placement_id, object_id), placement) in entries.iter() {
            if let Some((cursor_tenant, cursor_namespace, cursor_placement, cursor_object)) =
                &cursor
            {
                let current = (tenant.as_str(), namespace.as_str(), placement_id, object_id);
                let previous = (
                    cursor_tenant.as_str(),
                    cursor_namespace.as_str(),
                    cursor_placement,
                    cursor_object,
                );
                if current <= previous {
                    continue;
                }
            }
            placements.push(placement.clone());
            if placements.len() == page_limit {
                break;
            }
        }
        let has_more = placements.len() > limit;
        if has_more {
            placements.truncate(limit);
        }
        let next_cursor = has_more.then(|| {
            encode_cursor(
                placements
                    .last()
                    .expect("a page with more placements cannot be empty"),
            )
        });
        Ok(PlacementInventoryPage {
            placements,
            next_cursor,
        })
    }
}

fn encode_cursor(placement: &ObjectPlacement) -> String {
    format!(
        "{}|{}|{}|{}",
        placement.tenant_id,
        placement.object_namespace_id,
        placement.placement_id,
        placement.object_id.to_hex()
    )
}

fn decode_cursor(value: &str) -> AgentResult<(TenantId, ObjectNamespaceId, PlacementId, ObjectId)> {
    let mut parts = value.split('|');
    let tenant = parts.next().and_then(|part| TenantId::new(part).ok());
    let namespace = parts
        .next()
        .and_then(|part| ObjectNamespaceId::new(part).ok());
    let placement = parts.next().and_then(|part| PlacementId::new(part).ok());
    let object = parts.next().and_then(|part| ObjectId::from_str(part).ok());
    if parts.next().is_some() {
        return Err(AgentError::new(
            AgentErrorCode::ProtocolInvalid,
            "placement inventory cursor has too many components",
        ));
    }
    match (tenant, namespace, placement, object) {
        (Some(tenant), Some(namespace), Some(placement), Some(object)) => {
            Ok((tenant, namespace, placement, object))
        }
        _ => Err(AgentError::new(
            AgentErrorCode::ProtocolInvalid,
            "placement inventory cursor is invalid",
        )),
    }
}

fn decode_inventory_row(
    row: &rusqlite::Row<'_>,
    expected_tenant: &TenantId,
    expected_volume: &StorageVolumeId,
) -> AgentResult<ObjectPlacement> {
    let indexed_tenant = row.get::<_, String>(0).map_err(storage_error)?;
    let indexed_namespace = row.get::<_, String>(1).map_err(storage_error)?;
    let indexed_placement = row.get::<_, String>(2).map_err(storage_error)?;
    let indexed_object = row.get::<_, String>(3).map_err(storage_error)?;
    let indexed_generation = row.get::<_, String>(4).map_err(storage_error)?;
    let payload = row.get::<_, Vec<u8>>(5).map_err(storage_error)?;
    let placement: ObjectPlacement = serde_json::from_slice(&payload)
        .map_err(|_| storage_corruption("local placement inventory payload is invalid"))?;
    placement.validate().map_err(|error| {
        storage_corruption(format!(
            "local placement inventory record is invalid: {error}"
        ))
    })?;
    if indexed_tenant != placement.tenant_id.as_str()
        || indexed_tenant != expected_tenant.as_str()
        || indexed_namespace != placement.object_namespace_id.as_str()
        || indexed_placement != placement.placement_id.as_str()
        || indexed_object != placement.object_id.to_hex()
        || indexed_generation != placement.placement_generation.get().to_string()
        || placement.storage_volume_id.as_ref() != Some(expected_volume)
        || placement.archive_id.is_some()
    {
        return Err(storage_corruption(
            "local placement inventory indexed columns differ from payload",
        ));
    }
    Ok(placement)
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
