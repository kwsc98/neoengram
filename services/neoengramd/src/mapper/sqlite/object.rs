use async_trait::async_trait;
use neoengram_core::ObjectId;
use neoengram_protocol::{ArtifactId, ObjectDurabilityReceipt, TenantId};
use sqlx::Row;

use super::authority::*;
use crate::{
    validation::{durable_from_receipt, invalid},
    *,
};

#[async_trait]
impl ObjectCatalog for SqliteAuthorityStore {
    async fn record_durability(&self, receipt: &ObjectDurabilityReceipt) -> CentralResult<()> {
        let Some(durable) = durable_from_receipt(receipt)? else {
            return Ok(());
        };
        let result = sqlx::query(
            "INSERT OR IGNORE INTO durable_objects \
             (tenant_id, artifact_id, object_id, size, verified_digest, storage_version) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.artifact_id.as_str())
        .bind(durable.object_id.as_bytes().as_slice())
        .bind(durable.size.to_string())
        .bind(durable.verified_digest.as_bytes().as_slice())
        .bind(&durable.storage_version)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        let existing = self
            .durable_object(&receipt.tenant_id, &receipt.artifact_id, durable.object_id)
            .await?
            .ok_or_else(|| storage_corruption("conflicting durable object disappeared"))?;
        if existing == durable {
            Ok(())
        } else {
            Err(invalid(
                CentralErrorCode::MetadataInvalid,
                format!(
                    "object {} has conflicting durability metadata",
                    receipt.object.object_id
                ),
            ))
        }
    }

    async fn durable_object(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object_id: ObjectId,
    ) -> CentralResult<Option<DurableObject>> {
        let row = sqlx::query(
            "SELECT object_id, size, verified_digest, storage_version FROM durable_objects \
             WHERE tenant_id = ? AND artifact_id = ? AND object_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(artifact_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| {
            let stored_object =
                object_id_from_blob(row.try_get("object_id").map_err(storage_error)?)?;
            if stored_object != object_id {
                return Err(storage_corruption(
                    "durable object identity differs from query key",
                ));
            }
            Ok(DurableObject {
                object_id: stored_object,
                size: parse_canonical_u64(
                    row.try_get("size").map_err(storage_error)?,
                    "object size",
                )?,
                verified_digest: digest_from_blob(
                    row.try_get("verified_digest").map_err(storage_error)?,
                    "verified digest",
                )?,
                storage_version: row.try_get("storage_version").map_err(storage_error)?,
            })
        })
        .transpose()
    }
}
