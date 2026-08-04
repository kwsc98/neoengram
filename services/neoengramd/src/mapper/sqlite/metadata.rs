use std::collections::BTreeMap;

use async_trait::async_trait;
use neoengram_protocol::{MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage, TenantId};
use sqlx::Row;

use super::authority::*;
use crate::{validation::invalid, *};

#[async_trait]
impl MetadataBatchStager for SqliteAuthorityStore {
    async fn stage_descriptor(&self, descriptor: MetadataBatchDescriptor) -> CentralResult<bool> {
        descriptor.validate()?;
        let payload = encode(&descriptor)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO metadata_batch_descriptors \
             (tenant_id, batch_id, job_id, artifact_id, playground_id, payload) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(descriptor.scope.tenant_id.as_str())
        .bind(descriptor.batch_id.as_str())
        .bind(descriptor.scope.job_id.as_str())
        .bind(descriptor.scope.artifact_id.as_str())
        .bind(descriptor.scope.playground_id.as_str())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(false);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM metadata_batch_descriptors \
             WHERE tenant_id = ? AND batch_id = ?",
        )
        .bind(descriptor.scope.tenant_id.as_str())
        .bind(descriptor.batch_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        if decode::<MetadataBatchDescriptor>(&existing)? == descriptor {
            Ok(true)
        } else {
            Err(invalid(
                CentralErrorCode::BatchTampered,
                format!("metadata batch {} descriptor changed", descriptor.batch_id),
            ))
        }
    }

    async fn stage_page(
        &self,
        descriptor: &MetadataBatchDescriptor,
        page: MetadataBatchPage,
    ) -> CentralResult<bool> {
        descriptor.validate_page(&page)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let descriptor_payload: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM metadata_batch_descriptors \
             WHERE tenant_id = ? AND batch_id = ?",
        )
        .bind(descriptor.scope.tenant_id.as_str())
        .bind(descriptor.batch_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(descriptor_payload) = descriptor_payload else {
            return Err(invalid(
                CentralErrorCode::BatchIncomplete,
                format!(
                    "metadata batch {} descriptor must be staged before its pages",
                    descriptor.batch_id
                ),
            ));
        };
        if decode::<MetadataBatchDescriptor>(&descriptor_payload)? != *descriptor {
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} descriptor differs from staging",
                    descriptor.batch_id
                ),
            ));
        }
        let payload = encode(&page)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO metadata_batch_pages \
             (tenant_id, batch_id, page_number, payload) VALUES (?, ?, ?, ?)",
        )
        .bind(descriptor.scope.tenant_id.as_str())
        .bind(descriptor.batch_id.as_str())
        .bind(i64::from(page.page_number))
        .bind(&payload)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM metadata_batch_pages \
             WHERE tenant_id = ? AND batch_id = ? AND page_number = ?",
        )
        .bind(descriptor.scope.tenant_id.as_str())
        .bind(descriptor.batch_id.as_str())
        .bind(i64::from(page.page_number))
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if decode::<MetadataBatchPage>(&existing)? == page {
            transaction.commit().await.map_err(storage_error)?;
            Ok(true)
        } else {
            Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} page {} changed",
                    descriptor.batch_id, page.page_number
                ),
            ))
        }
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        batch_id: &MetadataBatchId,
    ) -> CentralResult<Option<StagedMetadataBatch>> {
        let descriptor_row = sqlx::query(
            "SELECT job_id, artifact_id, playground_id, payload \
             FROM metadata_batch_descriptors WHERE tenant_id = ? AND batch_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(batch_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(row) = descriptor_row else {
            return Ok(None);
        };
        let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
        let descriptor: MetadataBatchDescriptor = decode(&payload)?;
        if descriptor.scope.tenant_id != *tenant_id
            || descriptor.batch_id != *batch_id
            || row.try_get::<String, _>("job_id").map_err(storage_error)?
                != descriptor.scope.job_id.as_str()
            || row
                .try_get::<String, _>("artifact_id")
                .map_err(storage_error)?
                != descriptor.scope.artifact_id.as_str()
            || row
                .try_get::<String, _>("playground_id")
                .map_err(storage_error)?
                != descriptor.scope.playground_id.as_str()
        {
            return Err(storage_corruption(
                "metadata descriptor relational identity differs from payload",
            ));
        }
        let rows = sqlx::query(
            "SELECT page_number, payload FROM metadata_batch_pages \
             WHERE tenant_id = ? AND batch_id = ? ORDER BY page_number",
        )
        .bind(tenant_id.as_str())
        .bind(batch_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut pages = BTreeMap::new();
        for row in rows {
            let stored_number: i64 = row.try_get("page_number").map_err(storage_error)?;
            let page_number = u32::try_from(stored_number)
                .map_err(|_| storage_corruption("metadata page number is outside u32"))?;
            let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
            let page: MetadataBatchPage = decode(&payload)?;
            if page.batch_id != *batch_id || page.page_number != page_number {
                return Err(storage_corruption(
                    "metadata page relational identity differs from payload",
                ));
            }
            descriptor.validate_page(&page)?;
            pages.insert(page_number, page);
        }
        Ok(Some(StagedMetadataBatch { descriptor, pages }))
    }
}
