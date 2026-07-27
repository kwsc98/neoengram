use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use fs2::FileExt;
use neoengram_core::{
    ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId, ObjectId,
};
use neoengram_protocol::{
    ArtifactId, AssignmentOperation, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectDurabilityReceipt, TenantId, WireIndexVersion,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
    Row, SqliteConnection, SqlitePool,
};

use crate::{
    validation::{durable_from_receipt, invalid, same_index_version},
    AssignmentOutbox, AssignmentPublishOutcome, AssignmentReserveOutcome, AuditEvent, AuditKind,
    AuditSink, AuthorityCapabilities, AuthorityStore, CentralError, CentralErrorCode,
    CentralResult, DurableObject, IndexKey, IndexPublishOutcome, IndexPublishRejection,
    IndexPublishRequest, IndexPublisher, JobInsertOutcome, JobKey, JobRecord, JobRepository,
    MetadataBatchStager, ObjectCatalog, PublishedIndex, StagedMetadataBatch,
};

const DATABASE_FILE_NAME: &str = "authority.sqlite3";
const LOCK_FILE_NAME: &str = "authority.lock";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f41;
const SQLITE_SCHEMA_VERSION: i64 = 1;
const STORED_FORMAT_VERSION: u32 = 1;

const SCHEMA_SQL: &str = r#"
CREATE TABLE control_jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;

CREATE TABLE assignment_outbox (
    tenant_id TEXT NOT NULL,
    assignment_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
    PRIMARY KEY (tenant_id, assignment_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES control_jobs (tenant_id, job_id)
) STRICT;

CREATE TABLE metadata_batch_descriptors (
    tenant_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, batch_id)
) STRICT;

CREATE TABLE metadata_batch_pages (
    tenant_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    page_number INTEGER NOT NULL CHECK (page_number >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, batch_id, page_number),
    FOREIGN KEY (tenant_id, batch_id)
        REFERENCES metadata_batch_descriptors (tenant_id, batch_id)
        ON DELETE CASCADE
) STRICT;

CREATE TABLE durable_objects (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    size TEXT NOT NULL CHECK (size <> '' AND size NOT GLOB '*[^0-9]*'),
    verified_digest BLOB NOT NULL CHECK (length(verified_digest) = 32),
    storage_version TEXT NOT NULL CHECK (storage_version <> ''),
    PRIMARY KEY (tenant_id, artifact_id, object_id)
) STRICT;

CREATE TABLE playground_indexes (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK (revision <> '' AND revision NOT GLOB '*[^0-9]*'),
    digest BLOB NOT NULL CHECK (length(digest) = 32),
    PRIMARY KEY (tenant_id, artifact_id, playground_id)
) STRICT;

CREATE TABLE playground_index_records (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    path TEXT NOT NULL,
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    total_size TEXT NOT NULL CHECK (total_size <> '' AND total_size NOT GLOB '*[^0-9]*'),
    chunk_count TEXT NOT NULL CHECK (chunk_count <> '' AND chunk_count NOT GLOB '*[^0-9]*'),
    PRIMARY KEY (tenant_id, artifact_id, playground_id, path),
    FOREIGN KEY (tenant_id, artifact_id, playground_id)
        REFERENCES playground_indexes (tenant_id, artifact_id, playground_id)
        ON DELETE CASCADE
) STRICT;

CREATE TABLE immutable_manifests (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id, manifest_id)
) STRICT;

CREATE TABLE index_publications (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    request_payload BLOB NOT NULL,
    outcome_payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;

CREATE TABLE audit_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL CHECK (occurred_at <> '' AND occurred_at NOT GLOB '*[^0-9]*'),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
) STRICT;
"#;

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
    }

    pub async fn integrity_check(&self) -> CentralResult<()> {
        validate_integrity(&self.inner.pool).await
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
        let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM assignment_outbox WHERE published = 1 \
             ORDER BY tenant_id, assignment_id",
        )
        .fetch_all(&self.inner.pool)
        .await
        .map_err(storage_error)?;
        payloads.iter().map(|payload| decode(payload)).collect()
    }

    pub async fn audit_events(&self) -> CentralResult<Vec<AuditEvent>> {
        let payloads: Vec<Vec<u8>> =
            sqlx::query_scalar("SELECT payload FROM audit_events ORDER BY tenant_id, event_id")
                .fetch_all(&self.inner.pool)
                .await
                .map_err(storage_error)?;
        payloads.iter().map(|payload| decode(payload)).collect()
    }
}

struct SqliteAuthorityStore {
    pool: SqlitePool,
    _lock: File,
}

pub async fn open_sqlite_authority(
    config: SqliteAuthorityConfig,
) -> CentralResult<SqliteAuthority> {
    let root = prepare_root(&config.path)?;
    let lock = open_lock(&root.join(LOCK_FILE_NAME))?;
    let database = root.join(DATABASE_FILE_NAME);
    validate_database_path(&database)?;
    let initialize =
        !database.exists() || fs::metadata(&database).map_err(storage_error)?.len() == 0;

    let options = SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(config.busy_timeout);
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(storage_error)?;
    initialize_or_validate(&pool, initialize).await?;
    secure_database_file(&database)?;
    validate_integrity(&pool).await?;

    Ok(SqliteAuthority {
        inner: Arc::new(SqliteAuthorityStore { pool, _lock: lock }),
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredV1<T> {
    format: u32,
    value: T,
}

fn encode<T: Serialize>(value: &T) -> CentralResult<Vec<u8>> {
    serde_json::to_vec(&StoredV1 {
        format: STORED_FORMAT_VERSION,
        value,
    })
    .map_err(|error| storage_corruption(format!("failed to encode authority record: {error}")))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> CentralResult<T> {
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

#[async_trait]
impl JobRepository for SqliteAuthorityStore {
    async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        let row = sqlx::query(
            "SELECT state, resource_version, payload FROM control_jobs \
             WHERE tenant_id = ? AND job_id = ?",
        )
        .bind(key.tenant_id.as_str())
        .bind(key.job_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| decode_job_row(key, &row)).transpose()
    }

    async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        validate_job_identity(&job)?;
        let payload = encode(&job)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO control_jobs \
             (tenant_id, job_id, state, resource_version, payload) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .bind(job_state_name(job.state))
        .bind(job.resource_version.get().to_string())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(JobInsertOutcome::Inserted(job));
        }
        JobRepository::get(self, &job.key())
            .await?
            .map(JobInsertOutcome::Existing)
            .ok_or_else(|| storage_corruption("conflicting Job disappeared"))
    }

    async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        validate_job_identity(&job)?;
        if job.resource_version.get() != expected.saturating_add(1) {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replacement Job does not advance ResourceVersion by one",
            ));
        }
        let payload = encode(&job)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let result = sqlx::query(
            "UPDATE control_jobs SET state = ?, resource_version = ?, payload = ? \
             WHERE tenant_id = ? AND job_id = ? AND resource_version = ?",
        )
        .bind(job_state_name(job.state))
        .bind(job.resource_version.get().to_string())
        .bind(payload)
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .bind(expected.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            let exists: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM control_jobs WHERE tenant_id = ? AND job_id = ?")
                    .bind(job.spec.tenant_id.as_str())
                    .bind(job.spec.job_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
            return Err(invalid(
                if exists.is_some() {
                    CentralErrorCode::ConcurrentUpdate
                } else {
                    CentralErrorCode::JobNotFound
                },
                "Job ResourceVersion changed during authority CAS",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(job)
    }
}

#[async_trait]
impl AssignmentOutbox for SqliteAuthorityStore {
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome> {
        let AssignmentOperation::Add { input, .. } = &assignment.assignment;
        let payload = encode(&assignment)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO assignment_outbox \
             (tenant_id, assignment_id, job_id, payload, published) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .bind(input.job_id.as_str())
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(AssignmentReserveOutcome::Reserved);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM assignment_outbox WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        if decode::<JobAssignment>(&existing)? == assignment {
            Ok(AssignmentReserveOutcome::Existing)
        } else {
            Err(invalid(
                CentralErrorCode::JobIdReused,
                "AssignmentId was reused with another payload",
            ))
        }
    }

    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome> {
        let AssignmentOperation::Add { input, .. } = &assignment.assignment;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published FROM assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "Assignment must be reserved first",
            )
        })?;
        let existing: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
        if decode::<JobAssignment>(&existing)? != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                "AssignmentId was reused with another payload",
            ));
        }
        let published: i64 = row.try_get("published").map_err(storage_error)?;
        if published == 1 {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        sqlx::query(
            "UPDATE assignment_outbox SET published = 1 \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssignmentPublishOutcome::Published)
    }
}

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

#[async_trait]
impl IndexPublisher for SqliteAuthorityStore {
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
             (tenant_id, artifact_id, playground_id, revision, digest) VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, artifact_id, playground_id) DO UPDATE SET \
             revision = excluded.revision, digest = excluded.digest",
        )
        .bind(request.index_key.tenant_id.as_str())
        .bind(request.index_key.artifact_id.as_str())
        .bind(request.index_key.playground_id.as_str())
        .bind(version.revision.get().to_string())
        .bind(version.digest.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "DELETE FROM playground_index_records \
             WHERE tenant_id = ? AND artifact_id = ? AND playground_id = ?",
        )
        .bind(request.index_key.tenant_id.as_str())
        .bind(request.index_key.artifact_id.as_str())
        .bind(request.index_key.playground_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        for record in records.values() {
            sqlx::query(
                "INSERT INTO playground_index_records \
                 (tenant_id, artifact_id, playground_id, path, manifest_id, total_size, chunk_count) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(request.index_key.tenant_id.as_str())
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
}

#[async_trait]
impl AuditSink for SqliteAuthorityStore {
    async fn record(&self, event: AuditEvent) -> CentralResult<bool> {
        let payload = encode(&event)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO audit_events \
             (tenant_id, event_id, job_id, kind, state, occurred_at, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.job_key.tenant_id.as_str())
        .bind(&event.event_id)
        .bind(event.job_key.job_id.as_str())
        .bind(audit_kind_name(event.kind))
        .bind(job_state_name(event.state))
        .bind(event.occurred_at_unix_ms.get().to_string())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(false);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM audit_events WHERE tenant_id = ? AND event_id = ?",
        )
        .bind(event.job_key.tenant_id.as_str())
        .bind(&event.event_id)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        let existing: AuditEvent = decode(&existing)?;
        if existing.kind == event.kind
            && existing.job_key == event.job_key
            && existing.state == event.state
        {
            Ok(true)
        } else {
            Err(invalid(
                CentralErrorCode::Internal,
                format!("audit event ID {} was reused", event.event_id),
            ))
        }
    }
}

impl SqliteAuthorityStore {
    async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        let mut connection = self.pool.acquire().await.map_err(storage_error)?;
        let snapshot = load_index(&mut connection, key).await?;
        Ok(PublishedIndex {
            version: snapshot.version,
            records: snapshot.records.into_values().collect(),
        })
    }

    async fn manifest(
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
         WHERE tenant_id = ? AND artifact_id = ? AND playground_id = ?",
    )
    .bind(key.tenant_id.as_str())
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
         WHERE tenant_id = ? AND artifact_id = ? AND playground_id = ? ORDER BY path",
    )
    .bind(key.tenant_id.as_str())
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

fn prepare_root(path: &Path) -> CentralResult<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(storage_corruption("SQLite authority path must be explicit"));
    }
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(storage_corruption(
                "SQLite authority path must be a real directory",
            ));
        }
    } else {
        fs::create_dir_all(path).map_err(storage_error)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(storage_error)?;
    }
    fs::canonicalize(path).map_err(storage_error)
}

fn open_lock(path: &Path) -> CentralResult<File> {
    if path.exists()
        && fs::symlink_metadata(path)
            .map_err(storage_error)?
            .file_type()
            .is_symlink()
    {
        return Err(storage_corruption(
            "SQLite authority lock cannot be a symbolic link",
        ));
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(storage_error)?;
    file.try_lock_exclusive().map_err(|error| {
        CentralError::new(
            CentralErrorCode::StorageFailure,
            format!("SQLite authority is already open: {error}"),
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    Ok(file)
}

fn validate_database_path(path: &Path) -> CentralResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(storage_corruption(
            "SQLite authority database must be a regular file, not a symbolic link",
        ));
    }
    Ok(())
}

async fn initialize_or_validate(pool: &SqlitePool, initialize: bool) -> CentralResult<()> {
    let application_id: i64 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(pool)
        .await
        .map_err(storage_error)?;
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await
        .map_err(storage_error)?;
    if initialize {
        if application_id != 0 || user_version != 0 {
            return Err(storage_corruption(
                "empty SQLite authority database carries unexpected schema identity",
            ));
        }
        let mut transaction = pool.begin().await.map_err(storage_error)?;
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA application_id = 1313165121")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA user_version = 1")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        return Ok(());
    }
    if application_id != SQLITE_APPLICATION_ID {
        return Err(storage_corruption(format!(
            "SQLite authority application_id {application_id} is unsupported"
        )));
    }
    if user_version != SQLITE_SCHEMA_VERSION {
        return Err(storage_corruption(format!(
            "SQLite authority user_version {user_version} is unsupported"
        )));
    }
    let expected = [
        "control_jobs",
        "assignment_outbox",
        "metadata_batch_descriptors",
        "metadata_batch_pages",
        "durable_objects",
        "playground_indexes",
        "playground_index_records",
        "immutable_manifests",
        "index_publications",
        "audit_events",
    ];
    let actual: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?;
    let expected_names = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual_names = actual.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if actual_names != expected_names {
        return Err(storage_corruption(
            "SQLite authority table set differs from the current schema",
        ));
    }
    for expected_statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let table = expected_statement
            .split_ascii_whitespace()
            .nth(2)
            .ok_or_else(|| storage_corruption("invalid embedded SQLite authority schema"))?;
        let stored: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_one(pool)
                .await
                .map_err(storage_error)?;
        if normalize_schema_sql(&stored) != normalize_schema_sql(expected_statement) {
            return Err(storage_corruption(format!(
                "SQLite authority table {table} differs from the current schema"
            )));
        }
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect()
}

fn secure_database_file(path: &Path) -> CentralResult<()> {
    validate_database_path(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    }
    Ok(())
}

async fn validate_integrity(pool: &SqlitePool) -> CentralResult<()> {
    let results: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if results.as_slice() != ["ok"] {
        return Err(storage_corruption(format!(
            "SQLite authority integrity check failed: {}",
            results.join("; ")
        )));
    }
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if !violations.is_empty() {
        return Err(storage_corruption(
            "SQLite authority foreign-key check failed",
        ));
    }
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        format!("SQLite authority operation failed: {error}"),
    )
}

fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}

fn validate_job_identity(job: &JobRecord) -> CentralResult<()> {
    job.spec
        .job_id
        .as_str()
        .parse::<neoengram_protocol::JobId>()?;
    job.spec.tenant_id.as_str().parse::<TenantId>()?;
    Ok(())
}

fn decode_job_row(key: &JobKey, row: &SqliteRow) -> CentralResult<JobRecord> {
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let job: JobRecord = decode(&payload)?;
    let state: String = row.try_get("state").map_err(storage_error)?;
    let resource_version = parse_canonical_u64(
        row.try_get("resource_version").map_err(storage_error)?,
        "Job ResourceVersion",
    )?;
    if job.key() != *key
        || state != job_state_name(job.state)
        || resource_version != job.resource_version.get()
    {
        return Err(storage_corruption(
            "Job relational identity differs from its payload",
        ));
    }
    Ok(job)
}

const fn job_state_name(state: neoengram_protocol::JobState) -> &'static str {
    use neoengram_protocol::JobState;
    match state {
        JobState::Queued => "queued",
        JobState::Assigned => "assigned",
        JobState::Accepted => "accepted",
        JobState::Running => "running",
        JobState::Prepared => "prepared",
        JobState::Publishing => "publishing",
        JobState::CancelRequested => "cancel_requested",
        JobState::Succeeded => "succeeded",
        JobState::Conflicted => "conflicted",
        JobState::Rejected => "rejected",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::TimedOut => "timed_out",
        JobState::RecoveryRequired => "recovery_required",
        JobState::Unknown => "unknown",
    }
}

const fn audit_kind_name(kind: AuditKind) -> &'static str {
    match kind {
        AuditKind::JobCreated => "job_created",
        AuditKind::AssignmentQueued => "assignment_queued",
        AuditKind::ReportReceived => "report_received",
        AuditKind::MetadataStaged => "metadata_staged",
        AuditKind::AddExpired => "add_expired",
        AuditKind::AddFinalized => "add_finalized",
    }
}

fn parse_canonical_u64(value: String, field: &str) -> CentralResult<u64> {
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

fn exact_32(bytes: Vec<u8>, field: &str) -> CentralResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| storage_corruption(format!("stored {field} is not exactly 32 bytes")))
}

fn digest_from_blob(bytes: Vec<u8>, field: &str) -> CentralResult<ContentDigest> {
    Ok(ContentDigest::from_bytes(exact_32(bytes, field)?))
}

fn object_id_from_blob(bytes: Vec<u8>) -> CentralResult<ObjectId> {
    Ok(ObjectId::from_bytes(exact_32(bytes, "ObjectId")?))
}

fn manifest_id_from_blob(bytes: Vec<u8>) -> CentralResult<ManifestId> {
    Ok(ManifestId::from_bytes(exact_32(bytes, "ManifestId")?))
}
