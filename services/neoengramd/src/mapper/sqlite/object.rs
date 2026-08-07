use async_trait::async_trait;
use neoengram_core::ObjectId;
use neoengram_protocol::{ArtifactId, PlacementGeneration, StorageVolumeId, TenantId};

use super::authority::*;
use crate::{validation::invalid, *};

#[async_trait]
impl ObjectCatalog for SqliteAuthorityStore {
    async fn record_placement(&self, evidence: &ObjectPlacementEvidence) -> CentralResult<()> {
        let receipt = &evidence.receipt;
        if evidence.placement_generation.get() == 0 {
            return Err(invalid(
                CentralErrorCode::GenerationMismatch,
                "object placement generation must be greater than zero",
            ));
        }
        let payload = encode(evidence)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO object_placements \
             (tenant_id, receipt_id, artifact_id, job_id, storage_volume_id, \
              artifact_placement_id, placement_generation, object_id, size, \
              verified_at_unix_ms, payload) \
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM object_placements \
                 WHERE tenant_id = ? AND artifact_id = ? AND storage_volume_id = ? \
                   AND artifact_placement_id = ? AND placement_generation = ? \
                   AND object_id = ? AND size <> ? \
             )",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.receipt_id.as_str())
        .bind(receipt.artifact_id.as_str())
        .bind(receipt.job_id.as_str())
        .bind(receipt.storage_volume_id.as_str())
        .bind(receipt.artifact_placement_id.as_str())
        .bind(evidence.placement_generation.to_string())
        .bind(receipt.object_id.as_bytes().as_slice())
        .bind(receipt.size.to_string())
        .bind(receipt.verified_at_unix_ms.to_string())
        .bind(payload)
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.artifact_id.as_str())
        .bind(receipt.storage_volume_id.as_str())
        .bind(receipt.artifact_placement_id.as_str())
        .bind(evidence.placement_generation.to_string())
        .bind(receipt.object_id.as_bytes().as_slice())
        .bind(receipt.size.to_string())
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }

        let existing_payload: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM object_placements WHERE tenant_id = ? AND receipt_id = ?",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.receipt_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if let Some(existing_payload) = existing_payload {
            let existing: ObjectPlacementEvidence = decode(&existing_payload)?;
            return if &existing == evidence {
                Ok(())
            } else {
                Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!(
                        "object placement receipt {} has conflicting evidence",
                        receipt.receipt_id
                    ),
                ))
            };
        }
        if self
            .object_placement(
                &receipt.tenant_id,
                &receipt.artifact_id,
                &receipt.storage_volume_id,
                &receipt.artifact_placement_id,
                evidence.placement_generation,
                receipt.object_id,
            )
            .await?
            .is_some()
        {
            Err(invalid(
                CentralErrorCode::MetadataInvalid,
                format!(
                    "object {} has conflicting placement metadata",
                    receipt.object_id
                ),
            ))
        } else {
            Err(storage_corruption(
                "object placement insert was ignored without a stored conflict",
            ))
        }
    }

    async fn object_placement(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        storage_volume_id: &StorageVolumeId,
        artifact_placement_id: &neoengram_protocol::ArtifactPlacementId,
        placement_generation: PlacementGeneration,
        object_id: ObjectId,
    ) -> CentralResult<Option<ObjectPlacementEvidence>> {
        let payload: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM object_placements \
             WHERE tenant_id = ? AND artifact_id = ? AND storage_volume_id = ? \
               AND artifact_placement_id = ? AND placement_generation = ? AND object_id = ? \
             ORDER BY rowid DESC LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(artifact_id.as_str())
        .bind(storage_volume_id.as_str())
        .bind(artifact_placement_id.as_str())
        .bind(placement_generation.to_string())
        .bind(object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        payload
            .map(|payload| {
                let evidence: ObjectPlacementEvidence = decode(&payload)?;
                let receipt = &evidence.receipt;
                if &receipt.tenant_id != tenant_id
                    || &receipt.artifact_id != artifact_id
                    || &receipt.storage_volume_id != storage_volume_id
                    || &receipt.artifact_placement_id != artifact_placement_id
                    || evidence.placement_generation != placement_generation
                    || receipt.object_id != object_id
                {
                    return Err(storage_corruption(
                        "object placement payload differs from its lookup columns",
                    ));
                }
                Ok(evidence)
            })
            .transpose()
    }
}
