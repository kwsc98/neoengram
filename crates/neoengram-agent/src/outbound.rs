use std::{path::PathBuf, sync::Arc};

use neoengram_protocol::{jcs_blake3, JobId, MessageId, TenantId, UnixMillis};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{
    sqlite_storage::{storage_corruption, storage_error, LockedSqlite, SqliteDefinition},
    AgentError, AgentErrorCode, AgentReport, AgentResult, Clock, OutboundReportQueue, ReportSink,
};

const DATABASE_FILE: &str = "outbound.sqlite3";
const LOCK_FILE: &str = "outbound.lock";
const APPLICATION_ID: i64 = 0x4e45_4f51;
const SCHEMA_VERSION: i64 = 1;
const REPORT_FORMAT: u32 = 1;
const OUTBOUND_MAGIC: &str = "neoengram-agent-outbound-v1";
const MAX_OUTBOUND_PAGE: usize = 256;

const SCHEMA: &str = r#"
CREATE TABLE outbound_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    magic TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL
) STRICT;
CREATE TABLE outbound_reports (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    job_id TEXT NOT NULL,
    enqueued_at_unix_ms TEXT NOT NULL CHECK (
        enqueued_at_unix_ms <> '' AND enqueued_at_unix_ms NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL
) STRICT;
CREATE INDEX outbound_reports_job_order ON outbound_reports (job_id, sequence);
"#;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedAgentReport {
    pub sequence: u64,
    pub message_id: MessageId,
    pub enqueued_at_unix_ms: UnixMillis,
    pub report: AgentReport,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SqliteOutboundReportQueueConfig {
    pub root: PathBuf,
    pub agent_id: neoengram_protocol::AgentId,
    pub tenant_id: TenantId,
}

impl SqliteOutboundReportQueueConfig {
    #[must_use]
    pub fn new(
        root: impl Into<PathBuf>,
        agent_id: neoengram_protocol::AgentId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            root: root.into(),
            agent_id,
            tenant_id,
        }
    }
}

impl std::fmt::Debug for SqliteOutboundReportQueueConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteOutboundReportQueueConfig")
            .field("root_configured", &!self.root.as_os_str().is_empty())
            .field("agent_id", &self.agent_id)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

#[derive(Debug)]
pub struct SqliteOutboundReportQueue {
    storage: LockedSqlite,
    agent_id: neoengram_protocol::AgentId,
    tenant_id: TenantId,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReportV1 {
    format: u32,
    value: AgentReport,
}

impl SqliteOutboundReportQueue {
    pub fn open(config: SqliteOutboundReportQueueConfig) -> AgentResult<Self> {
        let storage = LockedSqlite::open(
            &config.root,
            SqliteDefinition {
                database_file: DATABASE_FILE,
                lock_file: LOCK_FILE,
                application_id: APPLICATION_ID,
                schema_version: SCHEMA_VERSION,
                schema: SCHEMA,
                tables: &["outbound_metadata", "outbound_reports"],
            },
        )?;
        let queue = Self {
            storage,
            agent_id: config.agent_id,
            tenant_id: config.tenant_id,
        };
        queue.bind_or_validate_identity()?;
        Ok(queue)
    }

    pub fn integrity_check(&self) -> AgentResult<()> {
        self.storage.integrity_check()?;
        self.validate_metadata()
    }

    /// Lists every durable report for one Job in send order.
    ///
    /// This is intentionally separate from the bounded transport page. Long-lived observations
    /// use it to replace only their own older reports without disturbing ordered Job messages.
    pub fn list_for_job(&self, job_id: &JobId) -> AgentResult<Vec<QueuedAgentReport>> {
        let connection = self.storage.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT sequence, message_id, enqueued_at_unix_ms, payload \
                 FROM outbound_reports WHERE job_id = ?1 ORDER BY sequence",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![job_id.as_str()], decode_row)
            .map_err(storage_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|_| storage_corruption("outbound report payload is invalid"))
    }

    /// Atomically enqueues a newer observation and removes exact reports it supersedes.
    ///
    /// The new report is persisted before any supplied identity is removed. Callers must only
    /// supply observational messages whose semantics are replaced by the new report.
    pub fn enqueue_superseding(
        &self,
        report: AgentReport,
        enqueued_at_unix_ms: UnixMillis,
        superseded: &[MessageId],
    ) -> AgentResult<QueuedAgentReport> {
        self.persist(report, enqueued_at_unix_ms, superseded)
    }

    fn bind_or_validate_identity(&self) -> AgentResult<()> {
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let identity = transaction
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM outbound_metadata WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        match identity {
            None => {
                transaction
                    .execute(
                        "INSERT INTO outbound_metadata (singleton, magic, agent_id, tenant_id) \
                         VALUES (1, ?1, ?2, ?3)",
                        params![
                            OUTBOUND_MAGIC,
                            self.agent_id.as_str(),
                            self.tenant_id.as_str()
                        ],
                    )
                    .map_err(storage_error)?;
            }
            Some((magic, agent_id, tenant_id))
                if magic == OUTBOUND_MAGIC
                    && agent_id == self.agent_id.as_str()
                    && tenant_id == self.tenant_id.as_str() => {}
            Some(_) => return Err(identity_mismatch()),
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()
    }

    fn validate_metadata(&self) -> AgentResult<()> {
        let connection = self.storage.connection()?;
        let identity: (String, String, String) = connection
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM outbound_metadata WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(storage_error)?;
        if identity.0 != OUTBOUND_MAGIC
            || identity.1 != self.agent_id.as_str()
            || identity.2 != self.tenant_id.as_str()
        {
            return Err(identity_mismatch());
        }
        Ok(())
    }

    fn persist(
        &self,
        report: AgentReport,
        enqueued_at_unix_ms: UnixMillis,
        superseded: &[MessageId],
    ) -> AgentResult<QueuedAgentReport> {
        let message_id = report_message_id(&report)?;
        let job_id = report_job_id(&report);
        if report_tenant_id(&report).is_some_and(|tenant| tenant != &self.tenant_id) {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "outbound report does not belong to the configured Tenant",
            ));
        }
        let payload = serde_json::to_vec(&StoredReportV1 {
            format: REPORT_FORMAT,
            value: report.clone(),
        })
        .map_err(storage_error)?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO outbound_reports \
                 (message_id, job_id, enqueued_at_unix_ms, payload) VALUES (?1, ?2, ?3, ?4)",
                params![
                    message_id.as_str(),
                    job_id.as_str(),
                    enqueued_at_unix_ms.get().to_string(),
                    payload,
                ],
            )
            .map_err(storage_error)?;
        let queued = load_message(&transaction, &message_id)?.ok_or_else(|| {
            storage_corruption("outbound report disappeared after an idempotent insert")
        })?;
        if queued.report != report {
            return Err(storage_corruption(
                "outbound message identity resolved to another report",
            ));
        }
        for stale in superseded {
            if stale != &message_id {
                transaction
                    .execute(
                        "DELETE FROM outbound_reports WHERE message_id = ?1 AND job_id = ?2",
                        params![stale.as_str(), job_id.as_str()],
                    )
                    .map_err(storage_error)?;
            }
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(queued)
    }
}

impl OutboundReportQueue for SqliteOutboundReportQueue {
    fn enqueue(
        &self,
        report: AgentReport,
        enqueued_at_unix_ms: UnixMillis,
    ) -> AgentResult<QueuedAgentReport> {
        self.persist(report, enqueued_at_unix_ms, &[])
    }

    fn list(&self, limit: usize) -> AgentResult<Vec<QueuedAgentReport>> {
        if limit == 0 || limit > MAX_OUTBOUND_PAGE {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                format!("outbound report page limit must be in 1..={MAX_OUTBOUND_PAGE}"),
            ));
        }
        let connection = self.storage.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT sequence, message_id, enqueued_at_unix_ms, payload \
                 FROM outbound_reports ORDER BY sequence LIMIT ?1",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![limit as u64], decode_row)
            .map_err(storage_error)?;
        let reports = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| storage_corruption("outbound report payload is invalid"))?;
        Ok(reports)
    }

    fn acknowledge(&self, message_id: &MessageId) -> AgentResult<bool> {
        let connection = self.storage.connection()?;
        let changed = connection
            .execute(
                "DELETE FROM outbound_reports WHERE message_id = ?1",
                params![message_id.as_str()],
            )
            .map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(changed == 1)
    }
}

#[derive(Debug)]
pub struct DurableReportSink {
    queue: Arc<dyn OutboundReportQueue>,
    clock: Arc<dyn Clock>,
}

impl DurableReportSink {
    #[must_use]
    pub fn new(queue: Arc<dyn OutboundReportQueue>, clock: Arc<dyn Clock>) -> Self {
        Self { queue, clock }
    }
}

impl ReportSink for DurableReportSink {
    fn send(&self, report: AgentReport) -> AgentResult<()> {
        let enqueued_at = UnixMillis::new(self.clock.now_unix_ms()?);
        self.queue.enqueue(report, enqueued_at).map(|_| ())
    }
}

fn report_message_id(report: &AgentReport) -> AgentResult<MessageId> {
    let digest = jcs_blake3(report)?;
    Ok(MessageId::new(format!("report-{digest}"))?)
}

fn report_job_id(report: &AgentReport) -> &neoengram_protocol::JobId {
    match report {
        AgentReport::Accepted(value) => &value.job_id,
        AgentReport::Progress(value) => &value.job_id,
        AgentReport::Prepared(value) => &value.job_id,
        AgentReport::Finalized(value) => &value.job_id,
        AgentReport::Failed(value) => &value.job_id,
    }
}

fn report_tenant_id(report: &AgentReport) -> Option<&TenantId> {
    match report {
        AgentReport::Failed(value) => Some(&value.tenant_id),
        _ => None,
    }
}

fn load_message(
    transaction: &rusqlite::Transaction<'_>,
    message_id: &MessageId,
) -> AgentResult<Option<QueuedAgentReport>> {
    transaction
        .query_row(
            "SELECT sequence, message_id, enqueued_at_unix_ms, payload \
             FROM outbound_reports WHERE message_id = ?1",
            params![message_id.as_str()],
            decode_row,
        )
        .optional()
        .map_err(storage_error)
}

fn decode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedAgentReport> {
    let sequence = row.get::<_, u64>(0)?;
    let message_id =
        MessageId::new(row.get::<_, String>(1)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let enqueued_at = row
        .get::<_, String>(2)?
        .parse::<u64>()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let payload = row.get::<_, Vec<u8>>(3)?;
    let stored: StoredReportV1 =
        serde_json::from_slice(&payload).map_err(|_| rusqlite::Error::InvalidQuery)?;
    if stored.format != REPORT_FORMAT {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(QueuedAgentReport {
        sequence,
        message_id,
        enqueued_at_unix_ms: UnixMillis::new(enqueued_at),
        report: stored.value,
    })
}

fn identity_mismatch() -> AgentError {
    AgentError::new(
        AgentErrorCode::AssignmentMismatch,
        "Agent outbound database identity does not match the configured Agent/Tenant",
    )
}

#[cfg(test)]
mod tests {
    use neoengram_protocol::{AssignmentGeneration, AssignmentId, Extensions, JobAccepted, JobId};

    use super::*;

    fn report() -> AgentReport {
        report_at("job-a", 100)
    }

    fn report_at(job_id: &str, accepted_at: u64) -> AgentReport {
        AgentReport::Accepted(JobAccepted {
            job_id: JobId::new(job_id).unwrap(),
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            accepted_at_unix_ms: UnixMillis::new(accepted_at),
            request_digest: "11".repeat(32).parse().unwrap(),
            extensions: Extensions::new(),
        })
    }

    #[test]
    fn sqlite_queue_is_ordered_idempotent_and_reopenable() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SqliteOutboundReportQueueConfig::new(
            temporary.path(),
            neoengram_protocol::AgentId::new("agent-a").unwrap(),
            TenantId::new("tenant-a").unwrap(),
        );
        let queue = SqliteOutboundReportQueue::open(config.clone()).unwrap();
        let first = queue.enqueue(report(), UnixMillis::new(200)).unwrap();
        let replay = queue.enqueue(report(), UnixMillis::new(201)).unwrap();
        assert_eq!(first, replay);
        assert_eq!(queue.list(32).unwrap(), vec![first.clone()]);
        drop(queue);

        let reopened = SqliteOutboundReportQueue::open(config).unwrap();
        assert_eq!(reopened.list(32).unwrap(), vec![first.clone()]);
        assert!(reopened.acknowledge(&first.message_id).unwrap());
        assert!(!reopened.acknowledge(&first.message_id).unwrap());
        assert!(reopened.list(32).unwrap().is_empty());
    }

    #[test]
    fn superseding_enqueue_replaces_only_exact_reports_for_the_same_job() {
        let temporary = tempfile::tempdir().unwrap();
        let queue = SqliteOutboundReportQueue::open(SqliteOutboundReportQueueConfig::new(
            temporary.path(),
            neoengram_protocol::AgentId::new("agent-a").unwrap(),
            TenantId::new("tenant-a").unwrap(),
        ))
        .unwrap();
        let first = queue
            .enqueue(report_at("job-a", 100), UnixMillis::new(100))
            .unwrap();
        let other = queue
            .enqueue(report_at("job-b", 100), UnixMillis::new(100))
            .unwrap();
        let latest = queue
            .enqueue_superseding(
                report_at("job-a", 200),
                UnixMillis::new(200),
                &[first.message_id.clone(), other.message_id.clone()],
            )
            .unwrap();

        assert_eq!(
            queue.list_for_job(&JobId::new("job-a").unwrap()).unwrap(),
            vec![latest.clone()]
        );
        assert_eq!(queue.list(32).unwrap(), vec![other, latest]);
    }
}
