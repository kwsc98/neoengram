use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use neoengram_agent::{
    Agent, AgentError, AgentErrorCode, AgentResult, AssignmentKey, OutboundReportQueue,
};
use neoengram_core::{FileRecord, IndexVersion, ObjectId, ObjectSpec};
use neoengram_protocol::{
    AgentHeartbeatReportPayload, AgentIndexPageQueryPayload, AgentJobReportCreatePayload,
    AgentMessageListQueryPayload, AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload,
    AgentObjectMissingQueryPayload, AgentObjectUploadPayload, AgentSessionClosePayload,
    AgentSessionOpenPayload, ControlEnvelope, ControlMessage, DecimalU64, DurabilityState,
    Extensions, IndexDeltaRecord, JobAssignment, JobDecision, MetadataBatchDescriptor,
    MetadataBatchPage, MountGeneration, OwnerGeneration, ProtocolVersion, ResourceVersion,
    TenantId, UnixMillis, WireObjectSpec, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_METADATA_BATCH_STAGE_PATH, AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    AGENT_JOB_OBJECT_MISSING_QUERY_PATH, AGENT_JOB_OBJECT_UPLOAD_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CLOSE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH, MAX_RECORDS_PER_PAGE,
};
use tokio::runtime::Handle;

use crate::{
    AgentDaemonError, AgentDaemonResult, AgentRequestSigner, AgentSessionClient, AgentSessionFence,
    AuthoritativeIndexSnapshot, ExecutionBridge, MissingObjectSet, ObjectUploadEvidence,
};

#[async_trait]
pub trait AgentMessageProcessor: Send + Sync {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()>;
    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()>;
}

#[derive(Debug)]
pub struct CoreAgentMessageProcessor {
    agent: Arc<Agent>,
}

impl CoreAgentMessageProcessor {
    #[must_use]
    pub fn new(agent: Arc<Agent>) -> Self {
        Self { agent }
    }
}

#[async_trait]
impl AgentMessageProcessor for CoreAgentMessageProcessor {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()> {
        let neoengram_protocol::AssignmentOperation::Add { input, .. } = assignment.assignment;
        let agent = Arc::clone(&self.agent);
        tokio::task::spawn_blocking(move || agent.handle_assignment(input))
            .await
            .map_err(join_error)?
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()> {
        let agent = Arc::clone(&self.agent);
        let tenant_id = tenant_id.clone();
        tokio::task::spawn_blocking(move || agent.handle_decision(&tenant_id, decision))
            .await
            .map_err(join_error)?
            .map(|_| ())
            .map_err(Into::into)
    }
}

/// Session generation shared by control transport and synchronous data-plane calls.
#[derive(Debug, Clone, Default)]
pub struct SharedSessionFence(Arc<RwLock<Option<AgentSessionFence>>>);

impl SharedSessionFence {
    fn get(&self) -> AgentDaemonResult<AgentSessionFence> {
        self.0
            .read()
            .map_err(|_| AgentDaemonError::Session("session fence lock is poisoned".to_owned()))?
            .clone()
            .ok_or_else(|| AgentDaemonError::Session("Agent session is not open".to_owned()))
    }

    fn replace(&self, fence: Option<AgentSessionFence>) -> AgentDaemonResult<()> {
        *self.0.write().map_err(|_| {
            AgentDaemonError::Session("session fence lock is poisoned".to_owned())
        })? = fence;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SharedResourceVersion(Arc<RwLock<ResourceVersion>>);

impl SharedResourceVersion {
    #[must_use]
    pub fn new(version: ResourceVersion) -> Self {
        Self(Arc::new(RwLock::new(version)))
    }

    fn get(&self) -> AgentDaemonResult<ResourceVersion> {
        self.0
            .read()
            .map(|version| *version)
            .map_err(|_| AgentDaemonError::Session("resource version lock is poisoned".to_owned()))
    }

    fn replace(&self, version: ResourceVersion) -> AgentDaemonResult<()> {
        *self.0.write().map_err(|_| {
            AgentDaemonError::Session("resource version lock is poisoned".to_owned())
        })? = version;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionBinding {
    pub fence: AgentSessionFence,
    pub agent_mount_id: neoengram_protocol::AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
}

/// Stateful driver for one Agent boot. Executor and data-plane adapters remain injected behind the
/// message processor; this type owns only signed HTTP transport and durable report acknowledgement.
pub struct AgentSessionTransport<C> {
    tenant_id: TenantId,
    signer: Arc<AgentRequestSigner>,
    client: Arc<C>,
    reports: Arc<dyn OutboundReportQueue>,
    processor: Arc<dyn AgentMessageProcessor>,
    fence: SharedSessionFence,
    resource_version: SharedResourceVersion,
}

impl<C> std::fmt::Debug for AgentSessionTransport<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSessionTransport")
            .field("tenant_id", &self.tenant_id)
            .field("signer", &self.signer)
            .field("fence", &self.fence)
            .field("resource_version", &self.resource_version.get().ok())
            .finish_non_exhaustive()
    }
}

impl<C: AgentSessionClient> AgentSessionTransport<C> {
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        signer: AgentRequestSigner,
        client: C,
        reports: Arc<dyn OutboundReportQueue>,
        processor: Arc<dyn AgentMessageProcessor>,
        resource_version: ResourceVersion,
    ) -> Self {
        Self::new_shared(
            tenant_id,
            Arc::new(signer),
            Arc::new(client),
            reports,
            processor,
            SharedSessionFence::default(),
            SharedResourceVersion::new(resource_version),
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_shared(
        tenant_id: TenantId,
        signer: Arc<AgentRequestSigner>,
        client: Arc<C>,
        reports: Arc<dyn OutboundReportQueue>,
        processor: Arc<dyn AgentMessageProcessor>,
        fence: SharedSessionFence,
        resource_version: SharedResourceVersion,
    ) -> Self {
        Self {
            tenant_id,
            signer,
            client,
            reports,
            processor,
            fence,
            resource_version,
        }
    }

    #[must_use]
    pub fn fence(&self) -> Option<AgentSessionFence> {
        self.fence.get().ok()
    }

    pub fn resource_version(&self) -> AgentDaemonResult<ResourceVersion> {
        self.resource_version.get()
    }

    pub async fn open(
        &mut self,
        signed_at_unix_ms: UnixMillis,
        mut payload: AgentSessionOpenPayload,
    ) -> AgentDaemonResult<AgentSessionBinding> {
        payload.expected_resource_version = self.resource_version.get()?;
        let request = self
            .signer
            .sign(AGENT_SESSION_OPEN_PATH, None, signed_at_unix_ms, payload)
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.open(&request)).await?;
        if response.agent_id != request.agent_id || response.request_id != request.request_id {
            return Err(AgentDaemonError::Session(
                "session open response identity differs from the request".to_owned(),
            ));
        }
        if response.session_generation.get() == 0
            || response.mount_generation.get() == 0
            || response.owner_generation.get() == 0
            || response.resource_version.get() == 0
        {
            return Err(AgentDaemonError::Session(
                "session open response contains a zero generation or resource version".to_owned(),
            ));
        }
        self.resource_version.replace(response.resource_version)?;
        let fence = AgentSessionFence {
            session_id: response.session_id,
            session_generation: response.session_generation,
        };
        self.fence.replace(Some(fence.clone()))?;
        Ok(AgentSessionBinding {
            fence,
            agent_mount_id: response.agent_mount_id,
            mount_generation: response.mount_generation,
            owner_generation: response.owner_generation,
        })
    }

    pub async fn heartbeat(
        &mut self,
        signed_at_unix_ms: UnixMillis,
        payload: AgentHeartbeatReportPayload,
    ) -> AgentDaemonResult<()> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_HEARTBEAT_REPORT_PATH,
                Some(&fence),
                signed_at_unix_ms,
                payload,
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.heartbeat(&request)).await?;
        self.resource_version.replace(response.resource_version)?;
        Ok(())
    }

    /// Sends durable reports oldest-first. A report is deleted only after the center returns a
    /// successful idempotent acknowledgement.
    pub async fn flush_reports(
        &mut self,
        signed_at_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<usize> {
        let fence = self.require_fence()?.clone();
        let queued = self.reports.list(32)?;
        let mut acknowledged = 0;
        for queued_report in queued {
            let envelope = ControlEnvelope {
                protocol_version: ProtocolVersion::V1,
                message_id: queued_report.message_id.clone(),
                session_generation: fence.session_generation,
                resource_version: None,
                request_id: None,
                trace_id: None,
                sent_at_unix_ms: queued_report.enqueued_at_unix_ms,
                message: queued_report.report.into_control_message(),
                extensions: Extensions::new(),
            };
            envelope
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            let request = self
                .signer
                .sign(
                    AGENT_JOB_REPORT_CREATE_PATH,
                    Some(&fence),
                    signed_at_unix_ms,
                    AgentJobReportCreatePayload {
                        tenant_id: self.tenant_id.clone(),
                        report: envelope,
                        extensions: Extensions::new(),
                    },
                )
                .map_err(session_error)?;
            let response = retry_session_action(|| self.client.create_report(&request)).await?;
            // This is the Job resource version, not the Agent registry/session version.
            let _ = response.resource_version;
            if !self.reports.acknowledge(&queued_report.message_id)? {
                return Err(AgentDaemonError::Session(
                    "acknowledged outbound report disappeared from the local queue".to_owned(),
                ));
            }
            acknowledged += 1;
        }
        Ok(acknowledged)
    }

    pub async fn poll_once(&self, signed_at_unix_ms: UnixMillis) -> AgentDaemonResult<usize> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_MESSAGE_LIST_QUERY_PATH,
                Some(&fence),
                signed_at_unix_ms,
                AgentMessageListQueryPayload {
                    max_messages: 32,
                    extensions: Extensions::new(),
                },
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.poll(&request)).await?;
        for envelope in &response.messages {
            envelope
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            if envelope.session_generation != fence.session_generation {
                return Err(AgentDaemonError::Session(
                    "center returned a message for another session generation".to_owned(),
                ));
            }
            match &envelope.message {
                ControlMessage::Assignment(assignment) => {
                    self.processor
                        .handle_assignment((**assignment).clone())
                        .await?;
                }
                ControlMessage::Decision(decision) => {
                    self.processor
                        .handle_decision(&self.tenant_id, decision.clone())
                        .await?;
                }
                _ => {
                    return Err(AgentDaemonError::Session(
                        "center poll returned a non-deliverable control message".to_owned(),
                    ));
                }
            }
        }
        Ok(response.messages.len())
    }

    pub async fn close(&mut self, signed_at_unix_ms: UnixMillis) -> AgentDaemonResult<()> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_CLOSE_PATH,
                Some(&fence),
                signed_at_unix_ms,
                AgentSessionClosePayload {
                    expected_resource_version: self.resource_version.get()?,
                    extensions: Extensions::new(),
                },
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.close(&request)).await?;
        self.resource_version.replace(response.resource_version)?;
        self.fence.replace(None)?;
        Ok(())
    }

    fn require_fence(&self) -> AgentDaemonResult<AgentSessionFence> {
        self.fence.get()
    }
}

/// Typed action/RPC adapter used only from Agent `spawn_blocking` execution.
pub struct SessionExecutionBridge<C> {
    tenant_id: TenantId,
    client: Arc<C>,
    signer: Arc<AgentRequestSigner>,
    fence: SharedSessionFence,
    reports: Arc<dyn OutboundReportQueue>,
    runtime: Handle,
    index_cache: RwLock<BTreeMap<AssignmentKey, AuthoritativeIndexSnapshot>>,
}

impl<C> std::fmt::Debug for SessionExecutionBridge<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionExecutionBridge")
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

impl<C: AgentSessionClient + 'static> SessionExecutionBridge<C> {
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        client: Arc<C>,
        signer: Arc<AgentRequestSigner>,
        fence: SharedSessionFence,
        reports: Arc<dyn OutboundReportQueue>,
        runtime: Handle,
    ) -> Self {
        Self {
            tenant_id,
            client,
            signer,
            fence,
            reports,
            runtime,
            index_cache: RwLock::new(BTreeMap::new()),
        }
    }

    fn signed<T: serde::Serialize>(
        &self,
        path: &'static str,
        payload: T,
    ) -> AgentResult<neoengram_protocol::AgentAuthenticatedRequest<T>> {
        let fence = self.fence.get().map_err(data_plane_error)?;
        self.signer
            .sign(path, Some(&fence), UnixMillis::new(system_now()?), payload)
            .map_err(data_plane_error)
    }

    fn call<T, F>(&self, mut action: F) -> AgentResult<T>
    where
        F: FnMut(
            Arc<C>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, crate::AgentSessionClientError>> + Send>>,
    {
        self.runtime.block_on(async {
            let mut delay = Duration::from_millis(100);
            for attempt in 0..3 {
                match action(Arc::clone(&self.client)).await {
                    Ok(value) => return Ok(value),
                    Err(error) if error.retryable() && attempt < 2 => {
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                    }
                    Err(error) => return Err(data_plane_error(error)),
                }
            }
            Err(data_plane_error("Agent action retry loop exhausted"))
        })
    }

    fn load_index(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        let mut page_number = 0_u32;
        let mut page_count = None;
        let mut records = Vec::new();
        loop {
            let request = self.signed(
                AGENT_JOB_INDEX_PAGE_QUERY_PATH,
                AgentIndexPageQueryPayload {
                    tenant_id: assignment.tenant_id.clone(),
                    job_id: assignment.job_id.clone(),
                    artifact_id: assignment.artifact_id.clone(),
                    playground_id: assignment.playground_id.clone(),
                    index_version: assignment.expected_index_version.clone(),
                    page_number,
                    max_records: u16::try_from(MAX_RECORDS_PER_PAGE)
                        .map_err(|_| data_plane_error("Index page limit exceeds u16"))?,
                    extensions: Extensions::new(),
                },
            )?;
            let response = self.call(|client| {
                let request = request.clone();
                Box::pin(async move { client.query_index_page(&request).await })
            })?;
            if response.index_version != assignment.expected_index_version
                || response.page_number != page_number
                || response.page_count == 0
                || page_count.is_some_and(|count| count != response.page_count)
            {
                return Err(data_plane_error(
                    "Index page response changed identity or pagination",
                ));
            }
            page_count = Some(response.page_count);
            for record in response.records {
                match record {
                    IndexDeltaRecord::Upsert {
                        path,
                        manifest_id,
                        total_size,
                        chunk_count,
                        ..
                    } => records.push(
                        FileRecord::new(path, manifest_id, total_size.get(), chunk_count.get())
                            .map_err(|error| data_plane_error(error.to_string()))?,
                    ),
                    IndexDeltaRecord::Delete { .. } => {
                        return Err(data_plane_error(
                            "authoritative Index snapshot contains a delete record",
                        ));
                    }
                }
            }
            page_number = page_number
                .checked_add(1)
                .ok_or_else(|| data_plane_error("Index page number overflow"))?;
            if page_number == page_count.unwrap_or_default() {
                break;
            }
            if page_number > page_count.unwrap_or_default() {
                return Err(data_plane_error("Index page response exceeded page_count"));
            }
        }
        AuthoritativeIndexSnapshot::new(
            IndexVersion::from(assignment.expected_index_version.clone()),
            records,
        )
    }

    fn flush_reports(&self) -> AgentResult<usize> {
        let fence = self.fence.get().map_err(data_plane_error)?;
        let queued = self.reports.list(32)?;
        let mut acknowledged = 0;
        for queued_report in queued {
            let envelope = ControlEnvelope {
                protocol_version: ProtocolVersion::V1,
                message_id: queued_report.message_id.clone(),
                session_generation: fence.session_generation,
                resource_version: None,
                request_id: None,
                trace_id: None,
                sent_at_unix_ms: queued_report.enqueued_at_unix_ms,
                message: queued_report.report.into_control_message(),
                extensions: Extensions::new(),
            };
            envelope.validate().map_err(data_plane_error)?;
            let request = self
                .signer
                .sign(
                    AGENT_JOB_REPORT_CREATE_PATH,
                    Some(&fence),
                    UnixMillis::new(system_now()?),
                    AgentJobReportCreatePayload {
                        tenant_id: self.tenant_id.clone(),
                        report: envelope,
                        extensions: Extensions::new(),
                    },
                )
                .map_err(data_plane_error)?;
            let response = self.call(|client| {
                let request = request.clone();
                Box::pin(async move { client.create_report(&request).await })
            })?;
            // This is the Job resource version, not the Agent registry/session version.
            let _ = response.resource_version;
            if !self.reports.acknowledge(&queued_report.message_id)? {
                return Err(data_plane_error(
                    "acknowledged report disappeared from durable outbox",
                ));
            }
            acknowledged += 1;
        }
        Ok(acknowledged)
    }
}

impl<C: AgentSessionClient + 'static> ExecutionBridge for SessionExecutionBridge<C> {
    fn authoritative_index(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        let key = AssignmentKey::from_assignment(assignment);
        if let Some(snapshot) = self
            .index_cache
            .read()
            .map_err(|_| data_plane_error("Index cache lock is poisoned"))?
            .get(&key)
            .cloned()
        {
            return Ok(snapshot);
        }
        let snapshot = self.load_index(assignment)?;
        self.index_cache
            .write()
            .map_err(|_| data_plane_error("Index cache lock is poisoned"))?
            .insert(key, snapshot.clone());
        Ok(snapshot)
    }

    fn query_missing_objects(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        objects: &[ObjectSpec],
    ) -> AgentResult<MissingObjectSet> {
        let mut all_missing = Vec::new();
        for chunk in objects.chunks(MAX_RECORDS_PER_PAGE) {
            let request = self.signed(
                AGENT_JOB_OBJECT_MISSING_QUERY_PATH,
                AgentObjectMissingQueryPayload {
                    tenant_id: assignment.tenant_id.clone(),
                    project_id: assignment.project_id.clone(),
                    artifact_id: assignment.artifact_id.clone(),
                    job_id: assignment.job_id.clone(),
                    objects: chunk.iter().copied().map(wire_object).collect(),
                    extensions: Extensions::new(),
                },
            )?;
            let response = self.call(|client| {
                let request = request.clone();
                Box::pin(async move { client.query_missing_objects(&request).await })
            })?;
            let expected = chunk
                .iter()
                .map(|object| (object.id, object.size))
                .collect::<BTreeMap<_, _>>();
            let missing = response
                .missing
                .into_iter()
                .map(local_object)
                .collect::<Vec<_>>();
            let mut observed = BTreeSet::<ObjectId>::new();
            for object in missing
                .iter()
                .copied()
                .chain(response.already_durable.into_iter().map(local_object))
            {
                if expected.get(&object.id) != Some(&object.size) || !observed.insert(object.id) {
                    return Err(data_plane_error(
                        "missing-object response changed, duplicated, or invented an object",
                    ));
                }
            }
            if observed.len() != expected.len() {
                return Err(data_plane_error(
                    "missing-object response omitted negotiated objects",
                ));
            }
            all_missing.extend(missing);
        }
        Ok(MissingObjectSet {
            missing: all_missing,
        })
    }

    fn upload_object(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        object: ObjectSpec,
        content: &[u8],
    ) -> AgentResult<ObjectUploadEvidence> {
        let request = self.signed(
            AGENT_JOB_OBJECT_UPLOAD_PATH,
            AgentObjectUploadPayload {
                tenant_id: assignment.tenant_id.clone(),
                project_id: assignment.project_id.clone(),
                artifact_id: assignment.artifact_id.clone(),
                job_id: assignment.job_id.clone(),
                object: wire_object(object),
                content_base64: STANDARD.encode(content),
                extensions: Extensions::new(),
            },
        )?;
        let response = self.call(|client| {
            let request = request.clone();
            Box::pin(async move { client.upload_object(&request).await })
        })?;
        response.receipt.validate().map_err(data_plane_error)?;
        if response.receipt.tenant_id != assignment.tenant_id
            || response.receipt.artifact_id != assignment.artifact_id
            || response.receipt.job_id != assignment.job_id
            || response.receipt.session_id
                != request.session_id.clone().ok_or_else(|| {
                    data_plane_error("signed upload request omitted session identity")
                })?
            || local_object(response.receipt.object.clone()) != object
            || !matches!(response.receipt.state, DurabilityState::Durable { .. })
        {
            return Err(data_plane_error(
                "object upload response is not durable or changed object scope",
            ));
        }
        Ok(ObjectUploadEvidence {
            object,
            verified_at_unix_ms: response.receipt.checked_at_unix_ms.get(),
        })
    }

    fn stage_metadata_descriptor(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        descriptor: &MetadataBatchDescriptor,
    ) -> AgentResult<()> {
        // Prepared is enqueued before ObjectTransfer::stage_metadata. The center rejects metadata
        // until Accepted/Running/Prepared have been observed in durable order.
        while self.flush_reports()? == 32 {}
        let request = self.signed(
            AGENT_JOB_METADATA_BATCH_STAGE_PATH,
            AgentMetadataBatchStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                descriptor: descriptor.clone(),
                extensions: Extensions::new(),
            },
        )?;
        let response = self.call(|client| {
            let request = request.clone();
            Box::pin(async move { client.stage_metadata_batch(&request).await })
        })?;
        if response.batch_id != descriptor.batch_id {
            return Err(data_plane_error(
                "metadata descriptor acknowledgement changed batch ID",
            ));
        }
        Ok(())
    }

    fn stage_metadata_page(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        page: &MetadataBatchPage,
    ) -> AgentResult<()> {
        let request = self.signed(
            AGENT_JOB_METADATA_PAGE_STAGE_PATH,
            AgentMetadataPageStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                page: page.clone(),
                extensions: Extensions::new(),
            },
        )?;
        let response = self.call(|client| {
            let request = request.clone();
            Box::pin(async move { client.stage_metadata_page(&request).await })
        })?;
        if response.batch_id != page.batch_id {
            return Err(data_plane_error(
                "metadata page acknowledgement changed batch ID",
            ));
        }
        Ok(())
    }

    fn now_unix_ms(&self) -> AgentResult<u64> {
        system_now()
    }
}

fn wire_object(object: ObjectSpec) -> WireObjectSpec {
    WireObjectSpec {
        object_id: object.id,
        size: DecimalU64::new(object.size),
        extensions: Extensions::new(),
    }
}

fn local_object(object: WireObjectSpec) -> ObjectSpec {
    ObjectSpec::new(object.object_id, object.size.get())
}

fn system_now() -> AgentResult<u64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| data_plane_error(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| data_plane_error("system timestamp exceeds u64"))
}

fn data_plane_error(error: impl std::fmt::Display) -> AgentError {
    AgentError::new(AgentErrorCode::ObjectTransferFailed, error.to_string())
}

fn join_error(error: tokio::task::JoinError) -> AgentDaemonError {
    AgentDaemonError::Session(format!("Agent execution task failed: {error}"))
}

async fn retry_session_action<T, F, Fut>(mut action: F) -> AgentDaemonResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, crate::AgentSessionClientError>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 0..3 {
        match action().await {
            Ok(value) => return Ok(value),
            Err(error) if error.retryable() && attempt < 2 => {
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(session_error(error)),
        }
    }
    Err(session_error("Agent action retry loop exhausted"))
}

fn session_error(error: impl std::fmt::Display) -> AgentDaemonError {
    AgentDaemonError::Session(error.to_string())
}
