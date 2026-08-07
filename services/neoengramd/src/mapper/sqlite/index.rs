use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use async_trait::async_trait;
use neoengram_core::{FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId};
use neoengram_protocol::{ArtifactId, TenantId, WireIndexVersion};
use sqlx::{Row, SqliteConnection};

use super::authority::*;
use crate::{
    validation::{invalid, same_index_version, validate_initial_index_snapshot},
    *,
};

#[async_trait]
impl IndexPublisher for SqliteAuthorityStore {
    async fn initialize_snapshot(
        &self,
        request: InitializeIndexSnapshotRequest,
    ) -> CentralResult<WireIndexVersion> {
        validate_initial_index_snapshot(&request.version, &request.records)?;
        let records = request
            .records
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let candidate = IndexSnapshot {
            version: request.version,
            records,
        };
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let inserted = sqlx::query(
            "INSERT INTO playground_indexes \
             (tenant_id, project_id, artifact_id, playground_id, revision, digest) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, project_id, artifact_id, playground_id) DO NOTHING",
        )
        .bind(request.index_key.tenant_id.as_str())
        .bind(request.index_key.project_id.as_str())
        .bind(request.index_key.artifact_id.as_str())
        .bind(request.index_key.playground_id.as_str())
        .bind(candidate.version.revision.get().to_string())
        .bind(candidate.version.digest.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;

        if inserted.rows_affected() == 0 {
            let existing = load_index(&mut transaction, &request.index_key).await?;
            if same_index_version(&existing.version, &candidate.version)
                && existing.records == candidate.records
            {
                transaction.commit().await.map_err(storage_error)?;
                return Ok(existing.version);
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Index for Playground {} is already initialized with a different snapshot",
                    request.index_key.playground_id
                ),
            ));
        }

        for record in candidate.records.values() {
            sqlx::query(
                "INSERT INTO playground_index_records \
                 (tenant_id, project_id, artifact_id, playground_id, path, manifest_id, \
                  total_size, chunk_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(request.index_key.tenant_id.as_str())
            .bind(request.index_key.project_id.as_str())
            .bind(request.index_key.artifact_id.as_str())
            .bind(request.index_key.playground_id.as_str())
            .bind(record.path.as_str())
            .bind(record.manifest_id.as_bytes().as_slice())
            .bind(record.total_size.to_string())
            .bind(record.chunk_count.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(candidate.version)
    }

    async fn compare_and_swap(
        &self,
        request: IndexPublishRequest,
    ) -> CentralResult<IndexPublishOutcome> {
        if request.job_key.tenant_id != request.index_key.tenant_id {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "Index publication Job and Index tenants differ",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let existing = sqlx::query(
            "SELECT request_payload, outcome_payload FROM index_publications \
             WHERE tenant_id = ? AND job_id = ?",
        )
        .bind(request.job_key.tenant_id.as_str())
        .bind(request.job_key.job_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing {
            let request_payload: Vec<u8> = row.try_get("request_payload").map_err(storage_error)?;
            let existing_request: IndexPublishRequest = decode(&request_payload)?;
            if existing_request != request {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    format!(
                        "job {} attempted a different Index publication",
                        request.job_key.job_id
                    ),
                ));
            }
            let outcome_payload: Vec<u8> = row.try_get("outcome_payload").map_err(storage_error)?;
            return decode(&outcome_payload);
        }

        let current = load_index(&mut transaction, &request.index_key).await?;
        if !same_index_version(&current.version, &request.expected_index_version) {
            let outcome = IndexPublishOutcome::Conflict(current.version);
            store_publication(&mut transaction, &request, &outcome).await?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(outcome);
        }

        let mut manifests = BTreeMap::new();
        for manifest in &request.manifests {
            let manifest_id = match manifest.canonical_id() {
                Ok(manifest_id) => manifest_id,
                Err(error) => {
                    return reject_publication(
                        transaction,
                        &request,
                        IndexPublishRejection::InvalidMetadata {
                            message: format!("invalid publication Manifest: {error}"),
                        },
                    )
                    .await;
                }
            };
            if manifests.insert(manifest_id, manifest.clone()).is_some() {
                return reject_publication(
                    transaction,
                    &request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("publication repeats Manifest {manifest_id}"),
                    },
                )
                .await;
            }
        }
        let mut referenced_manifests = BTreeSet::new();
        for mutation in &request.mutations {
            let neoengram_protocol::IndexDeltaRecord::Upsert {
                manifest_id,
                total_size,
                chunk_count,
                ..
            } = mutation
            else {
                continue;
            };
            referenced_manifests.insert(*manifest_id);
            if let Some(manifest) = manifests.get(manifest_id) {
                match manifest.chunk_count() {
                    Ok(observed)
                        if manifest.total_size == total_size.get()
                            && observed == chunk_count.get() => {}
                    Ok(_) => {
                        return reject_publication(
                            transaction,
                            &request,
                            IndexPublishRejection::InvalidMetadata {
                                message: format!(
                                    "Index upsert metadata differs from Manifest {manifest_id}"
                                ),
                            },
                        )
                        .await;
                    }
                    Err(error) => {
                        return reject_publication(
                            transaction,
                            &request,
                            IndexPublishRejection::InvalidMetadata {
                                message: format!("invalid publication Manifest: {error}"),
                            },
                        )
                        .await;
                    }
                }
            }
        }
        if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
            return reject_publication(
                transaction,
                &request,
                IndexPublishRejection::InvalidMetadata {
                    message: "publication Manifests are not the exact Index upsert set".to_owned(),
                },
            )
            .await;
        }
        for (manifest_id, manifest) in &manifests {
            let existing = load_manifest(
                &mut transaction,
                &request.index_key.tenant_id,
                &request.index_key.artifact_id,
                *manifest_id,
            )
            .await?;
            if existing.as_ref().is_some_and(|value| value != manifest) {
                return reject_publication(
                    transaction,
                    &request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!(
                            "immutable catalog already contains different content for Manifest {manifest_id}"
                        ),
                    },
                )
                .await;
            }
        }

        let mut records = current.records;
        for mutation in &request.mutations {
            match mutation {
                neoengram_protocol::IndexDeltaRecord::Upsert {
                    path,
                    manifest_id,
                    total_size,
                    chunk_count,
                    ..
                } => {
                    let record = match FileRecord::new(
                        path.clone(),
                        *manifest_id,
                        total_size.get(),
                        chunk_count.get(),
                    ) {
                        Ok(record) => record,
                        Err(error) => {
                            return reject_publication(
                                transaction,
                                &request,
                                IndexPublishRejection::InvalidMetadata {
                                    message: format!("invalid Index upsert: {error}"),
                                },
                            )
                            .await;
                        }
                    };
                    records.insert(path.clone(), record);
                }
                neoengram_protocol::IndexDeltaRecord::Delete { path, .. } => {
                    records.remove(path);
                }
            }
        }
        let Some(revision) = current.version.revision.get().checked_add(1) else {
            return reject_publication(
                transaction,
                &request,
                IndexPublishRejection::RevisionExhausted,
            )
            .await;
        };
        let ordered = records.values().cloned().collect::<Vec<_>>();
        let version = match IndexVersion::from_snapshot(revision, &ordered) {
            Ok(version) => WireIndexVersion::from(version),
            Err(error) => {
                return reject_publication(
                    transaction,
                    &request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("published Index is invalid: {error}"),
                    },
                )
                .await;
            }
        };
        if version.digest != request.expected_result_digest {
            return reject_publication(
                transaction,
                &request,
                IndexPublishRejection::ResultDigestMismatch {
                    expected_digest: request.expected_result_digest,
                    observed_digest: version.digest,
                },
            )
            .await;
        }

        for (manifest_id, manifest) in &manifests {
            sqlx::query(
                "INSERT OR IGNORE INTO immutable_manifests \
                 (tenant_id, artifact_id, manifest_id, payload) VALUES (?, ?, ?, ?)",
            )
            .bind(request.index_key.tenant_id.as_str())
            .bind(request.index_key.artifact_id.as_str())
            .bind(manifest_id.as_bytes().as_slice())
            .bind(encode(manifest)?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        sqlx::query(
            "INSERT INTO playground_indexes \
             (tenant_id, project_id, artifact_id, playground_id, revision, digest) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, project_id, artifact_id, playground_id) DO UPDATE SET \
             revision = excluded.revision, digest = excluded.digest",
        )
        .bind(request.index_key.tenant_id.as_str())
        .bind(request.index_key.project_id.as_str())
        .bind(request.index_key.artifact_id.as_str())
        .bind(request.index_key.playground_id.as_str())
        .bind(version.revision.get().to_string())
        .bind(version.digest.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "DELETE FROM playground_index_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?",
        )
        .bind(request.index_key.tenant_id.as_str())
        .bind(request.index_key.project_id.as_str())
        .bind(request.index_key.artifact_id.as_str())
        .bind(request.index_key.playground_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        for record in records.values() {
            sqlx::query(
                "INSERT INTO playground_index_records \
                 (tenant_id, project_id, artifact_id, playground_id, path, manifest_id, \
                  total_size, chunk_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(request.index_key.tenant_id.as_str())
            .bind(request.index_key.project_id.as_str())
            .bind(request.index_key.artifact_id.as_str())
            .bind(request.index_key.playground_id.as_str())
            .bind(record.path.as_str())
            .bind(record.manifest_id.as_bytes().as_slice())
            .bind(record.total_size.to_string())
            .bind(record.chunk_count.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        let outcome = IndexPublishOutcome::Published(version);
        store_publication(&mut transaction, &request, &outcome).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion> {
        Ok(self.published_index(key).await?.version)
    }

    async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        SqliteAuthorityStore::published_index(self, key).await
    }
}

impl SqliteAuthorityStore {
    pub(super) async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        let mut connection = self.pool.acquire().await.map_err(storage_error)?;
        let snapshot = load_index(&mut connection, key).await?;
        Ok(PublishedIndex {
            version: snapshot.version,
            records: snapshot.records.into_values().collect(),
        })
    }

    pub(super) async fn manifest(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        let mut connection = self.pool.acquire().await.map_err(storage_error)?;
        load_manifest(&mut connection, tenant_id, artifact_id, manifest_id).await
    }
}

struct IndexSnapshot {
    version: WireIndexVersion,
    records: BTreeMap<LogicalPath, FileRecord>,
}

async fn load_index(
    connection: &mut SqliteConnection,
    key: &IndexKey,
) -> CentralResult<IndexSnapshot> {
    let header = sqlx::query(
        "SELECT revision, digest FROM playground_indexes \
         WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?",
    )
    .bind(key.tenant_id.as_str())
    .bind(key.project_id.as_str())
    .bind(key.artifact_id.as_str())
    .bind(key.playground_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(storage_error)?;
    let Some(header) = header else {
        return empty_snapshot();
    };
    let revision = parse_canonical_u64(
        header.try_get("revision").map_err(storage_error)?,
        "Index revision",
    )?;
    let digest = digest_from_blob(
        header.try_get("digest").map_err(storage_error)?,
        "Index digest",
    )?;
    let rows = sqlx::query(
        "SELECT path, manifest_id, total_size, chunk_count FROM playground_index_records \
         WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ? \
         ORDER BY path",
    )
    .bind(key.tenant_id.as_str())
    .bind(key.project_id.as_str())
    .bind(key.artifact_id.as_str())
    .bind(key.playground_id.as_str())
    .fetch_all(&mut *connection)
    .await
    .map_err(storage_error)?;
    let mut records = BTreeMap::new();
    for row in rows {
        let path_text: String = row.try_get("path").map_err(storage_error)?;
        let path = LogicalPath::from_str(&path_text)
            .map_err(|error| storage_corruption(format!("invalid stored Index path: {error}")))?;
        let record = FileRecord::new(
            path.clone(),
            manifest_id_from_blob(row.try_get("manifest_id").map_err(storage_error)?)?,
            parse_canonical_u64(
                row.try_get("total_size").map_err(storage_error)?,
                "Index record total size",
            )?,
            parse_canonical_u64(
                row.try_get("chunk_count").map_err(storage_error)?,
                "Index record chunk count",
            )?,
        )
        .map_err(|error| storage_corruption(format!("invalid stored Index record: {error}")))?;
        records.insert(path, record);
    }
    let ordered = records.values().cloned().collect::<Vec<_>>();
    let computed = IndexVersion::from_snapshot(revision, &ordered)
        .map_err(|error| storage_corruption(format!("invalid stored Index: {error}")))?;
    if computed.digest != digest {
        return Err(storage_corruption(
            "stored Index digest differs from its canonical records",
        ));
    }
    Ok(IndexSnapshot {
        version: WireIndexVersion::from(computed),
        records,
    })
}

async fn load_manifest(
    connection: &mut SqliteConnection,
    tenant_id: &TenantId,
    artifact_id: &ArtifactId,
    manifest_id: ManifestId,
) -> CentralResult<Option<Manifest>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM immutable_manifests \
         WHERE tenant_id = ? AND artifact_id = ? AND manifest_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(artifact_id.as_str())
    .bind(manifest_id.as_bytes().as_slice())
    .fetch_optional(&mut *connection)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| {
            let manifest: Manifest = decode(&payload)?;
            let computed = manifest
                .canonical_id()
                .map_err(|error| storage_corruption(format!("invalid stored Manifest: {error}")))?;
            if computed != manifest_id {
                return Err(storage_corruption(
                    "stored Manifest identity differs from its payload",
                ));
            }
            Ok(manifest)
        })
        .transpose()
}

async fn store_publication(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &IndexPublishRequest,
    outcome: &IndexPublishOutcome,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO index_publications \
         (tenant_id, job_id, request_payload, outcome_payload) VALUES (?, ?, ?, ?)",
    )
    .bind(request.job_key.tenant_id.as_str())
    .bind(request.job_key.job_id.as_str())
    .bind(encode(request)?)
    .bind(encode(outcome)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn reject_publication(
    mut transaction: sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &IndexPublishRequest,
    rejection: IndexPublishRejection,
) -> CentralResult<IndexPublishOutcome> {
    let outcome = IndexPublishOutcome::Rejected(rejection);
    store_publication(&mut transaction, request, &outcome).await?;
    transaction.commit().await.map_err(storage_error)?;
    Ok(outcome)
}

fn empty_snapshot() -> CentralResult<IndexSnapshot> {
    let version = IndexVersion::from_snapshot(0, &[])
        .map(WireIndexVersion::from)
        .map_err(|error| storage_corruption(format!("failed to construct empty Index: {error}")))?;
    Ok(IndexSnapshot {
        version,
        records: BTreeMap::new(),
    })
}
