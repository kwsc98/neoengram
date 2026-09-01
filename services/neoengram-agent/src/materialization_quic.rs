//! Agent-side guards for the v2 multi-source materialization data plane.
//!
//! The QUIC byte stream remains the existing bounded object stream.  These helpers keep the v2
//! control objects (ticket, paged manifest, durable checkpoint and receipt) separate from that
//! stream while enforcing the identity and generation fences before a CAS operation is started.

use std::collections::{BTreeMap, BTreeSet};

use neoengram_domain::protocol::{
    BatchManifest, BatchManifestPage, CloseTransfer, MaterializationBatch,
    MaterializationBatchTicket, MaterializationObject, MaterializationObjectReceipt,
    MaterializationObjectState, ObjectAck, ObjectChunk, ObjectProof, ObjectReceiptId, ObjectRef,
    ObjectRequest, PlacementId, ProtocolError, ProtocolResult, SignedMaterializationBatchTicket,
    TransferFrame, TransferId, UnixMillis, MAX_TRANSFER_CHUNK_BYTES,
};
use neoengram_runtime::{ObjectBackend, ObjectRange, ObjectSpec, TransferSource};

use crate::{AgentTransferError, CentralCommandTrustBundle};

/// A durable per-object checkpoint.  Checkpoints are monotonic and scoped to one batch attempt;
/// changing source or route therefore does not reset the target staging key or offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationCheckpoint {
    pub materialization_id: neoengram_domain::MaterializationId,
    pub batch_id: neoengram_domain::MaterializationBatchId,
    pub plan_revision: neoengram_domain::Generation,
    pub batch_attempt: neoengram_domain::Generation,
    pub object_id: neoengram_domain::ObjectId,
    pub confirmed_offset: u64,
}

fn invalid(field: &'static str, reason: impl Into<String>) -> ProtocolError {
    ProtocolError::InvalidField {
        field,
        reason: reason.into(),
    }
}

/// Validates a complete v2 batch descriptor and all of its manifest pages.  This is deliberately
/// stricter than validating each object independently: a page from another plan, a reordered page
/// or a manifest that omits an object must be rejected before opening a source object.
pub fn validate_materialization_batch(
    ticket: &MaterializationBatchTicket,
    batch: &MaterializationBatch,
    manifest: &BatchManifest,
    pages: &[BatchManifestPage],
) -> ProtocolResult<()> {
    ticket.validate()?;
    batch.validate()?;
    manifest.validate()?;
    if ticket.materialization_id != batch.materialization_id
        || ticket.batch_id != batch.batch_id
        || ticket.plan_revision != batch.plan_revision
        || ticket.batch_attempt != batch.batch_attempt
        || ticket.source != batch.source
        || ticket.target != batch.target
    {
        return Err(invalid(
            "batch_ticket",
            "ticket identity, source, target or plan does not match batch",
        ));
    }
    if ticket.manifest_digest != manifest.manifest_digest
        || batch.manifest_digest != manifest.manifest_digest
    {
        return Err(ProtocolError::InvalidDigest(
            "batch manifest digest does not match signed ticket".to_owned(),
        ));
    }
    let reconstructed = BatchManifest::from_pages(pages)?;
    if reconstructed != *manifest {
        return Err(invalid(
            "manifest",
            "manifest descriptor does not match supplied pages",
        ));
    }
    let mut object_ids = pages
        .iter()
        .flat_map(|page| page.objects.iter().map(|object| object.object_id))
        .collect::<Vec<_>>();
    object_ids.sort_unstable();
    let mut expected_ids = batch.object_ids.clone();
    expected_ids.sort_unstable();
    if object_ids != expected_ids {
        return Err(invalid(
            "manifest.objects",
            "manifest object IDs do not match the batch plan",
        ));
    }
    if manifest.object_count != batch.object_count || manifest.total_bytes != batch.total_bytes {
        return Err(invalid(
            "manifest.counts",
            "manifest counters do not match the batch plan",
        ));
    }
    Ok(())
}

/// Checks the signed ticket and the durable batch identity before the manifest is read.  The
/// manifest digest is intentionally deferred until all bounded pages have arrived; constructing
/// a synthetic empty manifest here would make a malformed preflight indistinguishable from a
/// real batch and could hide a ticket/batch mismatch.
fn validate_materialization_ticket_batch(
    trust: &CentralCommandTrustBundle,
    ticket: &SignedMaterializationBatchTicket,
    batch: &MaterializationBatch,
    now_unix_ms: UnixMillis,
) -> crate::AgentDaemonResult<()> {
    trust.verify_materialization_ticket(ticket, now_unix_ms)?;
    ticket
        .validate()
        .map_err(|error| crate::AgentDaemonError::Session(error.to_string()))?;
    batch
        .validate()
        .map_err(|error| crate::AgentDaemonError::Session(error.to_string()))?;
    if ticket.ticket.materialization_id != batch.materialization_id
        || ticket.ticket.batch_id != batch.batch_id
        || ticket.ticket.plan_revision != batch.plan_revision
        || ticket.ticket.batch_attempt != batch.batch_attempt
        || ticket.ticket.source != batch.source
        || ticket.ticket.target != batch.target
    {
        return Err(crate::AgentDaemonError::Session(
            "materialization ticket identity does not match durable batch".to_owned(),
        ));
    }
    Ok(())
}

/// Verifies a Central-signed v2 ticket and exact manifest before a transfer starts.
pub fn validate_signed_materialization_batch(
    ticket: &SignedMaterializationBatchTicket,
    batch: &MaterializationBatch,
    manifest: &BatchManifest,
    pages: &[BatchManifestPage],
) -> ProtocolResult<()> {
    ticket.validate()?;
    neoengram_domain::protocol::materialization::validate_materialization_assignment(
        &ticket.ticket,
        batch,
        manifest,
        pages,
    )
}

/// As above, but also checks the Central keyring before a transport session is opened. Keeping
/// this call adjacent to manifest validation makes it difficult for a caller to accidentally
/// stage bytes after only structural (unsigned) validation.
pub fn validate_signed_materialization_batch_with_trust(
    trust: &crate::CentralCommandTrustBundle,
    ticket: &SignedMaterializationBatchTicket,
    batch: &MaterializationBatch,
    manifest: &BatchManifest,
    pages: &[BatchManifestPage],
    now_unix_ms: UnixMillis,
) -> crate::AgentDaemonResult<()> {
    trust.verify_materialization_ticket(ticket, now_unix_ms)?;
    neoengram_domain::protocol::materialization::validate_materialization_assignment(
        &ticket.ticket,
        batch,
        manifest,
        pages,
    )
    .map_err(|error| crate::AgentDaemonError::Session(error.to_string()))
}

/// Derives the transfer namespace used by the runtime CAS for one materialization.  The runtime
/// backend keeps staging under `(tenant, transfer_id, object_id)`; deriving the ID from the
/// materialization and object namespace makes it stable across source failover, route changes and
/// batch attempts while retaining the existing backend API.
pub fn materialization_transfer_id(
    materialization_id: &neoengram_domain::MaterializationId,
    object_namespace_id: &neoengram_domain::ObjectNamespaceId,
) -> ProtocolResult<TransferId> {
    #[derive(serde::Serialize)]
    struct TransferScope<'a> {
        materialization_id: &'a neoengram_domain::MaterializationId,
        object_namespace_id: &'a neoengram_domain::ObjectNamespaceId,
    }
    let digest = neoengram_domain::protocol::jcs_blake3(&TransferScope {
        materialization_id,
        object_namespace_id,
    })?;
    TransferId::new(format!("materialization-{}", &digest.to_hex()[..32]))
}

fn transfer_unexpected(expected: &'static str, frame: &TransferFrame) -> AgentTransferError {
    let actual = match frame {
        TransferFrame::OpenTransfer(_) => "OpenTransfer",
        TransferFrame::OpenTransferSigned(_) => "OpenTransferSigned",
        TransferFrame::OpenMaterializationSigned(_) => "OpenMaterializationSigned",
        TransferFrame::MaterializationManifest(_) => "MaterializationManifest",
        TransferFrame::MaterializationManifestPage(_) => "MaterializationManifestPage",
        TransferFrame::ObjectRequest(_) => "ObjectRequest",
        TransferFrame::ObjectChunk(_) => "ObjectChunk",
        TransferFrame::ObjectProof(_) => "ObjectProof",
        TransferFrame::ObjectAck(_) => "ObjectAck",
        TransferFrame::CommitObjectSet(_) => "CommitObjectSet",
        TransferFrame::TransferError(_) => "TransferError",
        TransferFrame::CloseTransfer(_) => "CloseTransfer",
    };
    AgentTransferError::UnexpectedFrame { expected, actual }
}

fn transfer_remote(error: neoengram_domain::protocol::TransferError) -> AgentTransferError {
    AgentTransferError::Remote {
        code: error.code,
        message: error.message,
    }
}

fn materialization_ticket_error(error: impl std::fmt::Display) -> AgentTransferError {
    AgentTransferError::Ticket(error.to_string())
}

/// Source-side v2 session. The source accepts exactly one signed batch, receives the paged
/// manifest from the target, and serves ranges only for ObjectRefs in that manifest. It never
/// resolves a path or object outside the artifact-scoped runtime backend supplied by the caller.
#[derive(Debug)]
pub struct MaterializationSourceSession<R, W> {
    channel: crate::AgentTransferFrameChannel<R, W>,
}

impl<R, W> MaterializationSourceSession<R, W> {
    #[must_use]
    pub fn new(recv: R, send: W) -> Self {
        Self {
            channel: crate::AgentTransferFrameChannel::new(recv, send),
        }
    }

    #[must_use]
    pub fn channel(&self) -> &crate::AgentTransferFrameChannel<R, W> {
        &self.channel
    }

    pub fn channel_mut(&mut self) -> &mut crate::AgentTransferFrameChannel<R, W> {
        &mut self.channel
    }
}

impl<R, W> MaterializationSourceSession<R, W>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    /// Serves one batch until the target closes it. `batch` is obtained from Central's durable
    /// plan, while the manifest descriptor/pages are supplied by the target and checked against
    /// the signed digest before the first source read.
    pub async fn serve(
        &mut self,
        trust: &CentralCommandTrustBundle,
        batch: &MaterializationBatch,
        tenant_id: &neoengram_domain::TenantId,
        source: &dyn TransferSource,
        now_unix_ms: UnixMillis,
    ) -> Result<(), AgentTransferError> {
        let first = self.channel.recv_frame().await?;
        let signed = match first {
            TransferFrame::OpenMaterializationSigned(ticket) => ticket,
            other => return Err(transfer_unexpected("OpenMaterializationSigned", &other)),
        };
        validate_materialization_ticket_batch(trust, &signed, batch, now_unix_ms)
            .map_err(materialization_ticket_error)?;

        let manifest = match self.channel.recv_frame().await? {
            TransferFrame::MaterializationManifest(manifest) => manifest,
            other => return Err(transfer_unexpected("MaterializationManifest", &other)),
        };
        let page_count = usize::try_from(manifest.page_count.get()).map_err(|_| {
            AgentTransferError::Scope("manifest page count cannot fit usize".to_owned())
        })?;
        if page_count == 0 || page_count > 65_535 {
            return Err(AgentTransferError::Scope(
                "manifest page count is outside the bounded transfer limit".to_owned(),
            ));
        }
        let mut pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            let frame = self.channel.recv_frame().await?;
            match frame {
                TransferFrame::MaterializationManifestPage(page) => pages.push(page),
                other => return Err(transfer_unexpected("MaterializationManifestPage", &other)),
            }
        }
        validate_signed_materialization_batch_with_trust(
            trust,
            &signed,
            batch,
            &manifest,
            &pages,
            now_unix_ms,
        )
        .map_err(materialization_ticket_error)?;
        if signed.ticket.tenant_id != *tenant_id {
            return Err(AgentTransferError::Scope(
                "materialization ticket tenant does not match source Agent".to_owned(),
            ));
        }
        let objects = pages
            .iter()
            .flat_map(|page| page.objects.iter().cloned())
            .map(|object| (object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        // A resumed target may start at a non-zero durable offset, but every subsequent range
        // for that object must be contiguous.  Keeping this state at the source prevents a peer
        // from acknowledging only the final range and making an incomplete object look complete.
        let mut next_offsets = BTreeMap::<neoengram_domain::ObjectId, u64>::new();
        let mut completed = BTreeSet::new();
        let mut served_bytes = 0_u64;
        loop {
            let frame = self.channel.recv_frame().await?;
            match frame {
                TransferFrame::ObjectAck(ack)
                    if ack.accepted
                        && ack.length == 0
                        && objects.values().any(|object| {
                            object.object_id == ack.object_id && object.size.get() == ack.offset
                        }) =>
                {
                    // A reconnecting target may already have a durable/published object.  The
                    // zero-length acknowledgement is a resume marker, so no source request or
                    // proof is expected for it.
                    completed.insert(ack.object_id);
                    continue;
                }
                TransferFrame::ObjectRequest(request) => {
                    let object = objects.get(&request.object_id).ok_or_else(|| {
                        AgentTransferError::Scope(
                            "requested object is outside the materialization manifest".to_owned(),
                        )
                    })?;
                    if completed.contains(&request.object_id) {
                        return Err(AgentTransferError::Scope(
                            "requested object was already completed".to_owned(),
                        ));
                    }
                    if object.size.get() == 0 {
                        if request.offset != 0 || request.length != 0 {
                            return Err(AgentTransferError::Scope(
                                "empty object requests must use a zero-length range at offset zero"
                                    .to_owned(),
                            ));
                        }
                        self.channel
                            .send_frame(&TransferFrame::ObjectProof(ObjectProof {
                                object_id: object.object_id,
                                digest: object.object_id.digest(),
                                size: 0,
                            }))
                            .await?;
                        let ack = self.channel.recv_frame().await?;
                        let TransferFrame::ObjectAck(ack) = ack else {
                            if let TransferFrame::TransferError(error) = ack {
                                return Err(transfer_remote(error));
                            }
                            return Err(transfer_unexpected("ObjectAck", &ack));
                        };
                        if ack.object_id != object.object_id
                            || ack.offset != 0
                            || ack.length != 0
                            || !ack.accepted
                        {
                            return Err(AgentTransferError::Scope(
                                "empty object acknowledgement does not match the proof".to_owned(),
                            ));
                        }
                        completed.insert(object.object_id);
                        continue;
                    }
                    if let Some(next) = next_offsets.get(&request.object_id) {
                        if *next != request.offset {
                            return Err(AgentTransferError::Scope(
                                "requested range is not contiguous with the acknowledged offset"
                                    .to_owned(),
                            ));
                        }
                    }
                    if request.length == 0 {
                        return Err(AgentTransferError::Scope(
                            "non-empty object requests must use a positive range length".to_owned(),
                        ));
                    }
                    let expected = ObjectSpec::new(object.object_id, object.size.get());
                    let range = ObjectRange::new(request.offset, request.length)
                        .map_err(|error| AgentTransferError::Scope(error.to_string()))?;
                    if range.end() > expected.size {
                        return Err(AgentTransferError::Scope(
                            "requested range exceeds the manifest ObjectRef size".to_owned(),
                        ));
                    }
                    let next_served_bytes =
                        served_bytes.checked_add(range.length).ok_or_else(|| {
                            AgentTransferError::Scope("served byte count overflows u64".to_owned())
                        })?;
                    if next_served_bytes > signed.ticket.max_bytes.get() {
                        return Err(AgentTransferError::Scope(
                            "source transfer exceeds the signed byte limit".to_owned(),
                        ));
                    }
                    let mut bytes = Vec::with_capacity(range.length as usize);
                    let copied = source
                        .open_object(tenant_id, &expected, range, &mut bytes)
                        .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                    if copied != range.length || bytes.len() as u64 != range.length {
                        return Err(AgentTransferError::Backend(
                            "source returned an unexpected range length".to_owned(),
                        ));
                    }
                    self.channel
                        .send_frame(&TransferFrame::ObjectChunk(ObjectChunk::new(
                            request.object_id,
                            request.offset,
                            bytes,
                        )?))
                        .await?;
                    if range.end() == expected.size {
                        self.channel
                            .send_frame(&TransferFrame::ObjectProof(ObjectProof {
                                object_id: expected.id,
                                digest: expected.id.digest(),
                                size: expected.size,
                            }))
                            .await?;
                    }
                    let ack = self.channel.recv_frame().await?;
                    let TransferFrame::ObjectAck(ack) = ack else {
                        if let TransferFrame::TransferError(error) = ack {
                            return Err(transfer_remote(error));
                        }
                        return Err(transfer_unexpected("ObjectAck", &ack));
                    };
                    if ack.object_id != request.object_id
                        || ack.offset != request.offset
                        || ack.length != request.length
                        || !ack.accepted
                    {
                        return Err(AgentTransferError::Scope(
                            "ObjectAck does not match the requested materialization range"
                                .to_owned(),
                        ));
                    }
                    let next = request.offset.checked_add(request.length).ok_or_else(|| {
                        AgentTransferError::Scope("requested range end overflows u64".to_owned())
                    })?;
                    next_offsets.insert(request.object_id, next);
                    if next == expected.size {
                        completed.insert(request.object_id);
                    }
                    served_bytes = next_served_bytes;
                }
                TransferFrame::CloseTransfer(close) => {
                    if !close.committed || completed.len() != objects.len() {
                        return Err(AgentTransferError::Scope(
                            "materialization closed before all objects were acknowledged"
                                .to_owned(),
                        ));
                    }
                    return Ok(());
                }
                TransferFrame::TransferError(error) => return Err(transfer_remote(error)),
                other => {
                    return Err(transfer_unexpected(
                        "ObjectRequest or CloseTransfer",
                        &other,
                    ));
                }
            }
        }
    }
}

/// Target-side v2 session. It sends the signed batch and its paged manifest, then requests each
/// object and commits it through the runtime backend. The returned receipts are ready for the
/// normal authenticated Agent report path; Central remains the only placement authority.
#[derive(Debug)]
pub struct MaterializationTargetSession<R, W> {
    channel: crate::AgentTransferFrameChannel<R, W>,
    ticket: Option<SignedMaterializationBatchTicket>,
    transferred_bytes: u64,
    chunk_bytes: usize,
}

impl<R, W> MaterializationTargetSession<R, W> {
    #[must_use]
    pub fn new(recv: R, send: W) -> Self {
        Self {
            channel: crate::AgentTransferFrameChannel::new(recv, send),
            ticket: None,
            transferred_bytes: 0,
            chunk_bytes: MAX_TRANSFER_CHUNK_BYTES,
        }
    }

    pub fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Result<Self, AgentTransferError> {
        if chunk_bytes == 0 || chunk_bytes > MAX_TRANSFER_CHUNK_BYTES {
            return Err(AgentTransferError::Scope(format!(
                "chunk size must be in 1..={MAX_TRANSFER_CHUNK_BYTES}"
            )));
        }
        self.chunk_bytes = chunk_bytes;
        Ok(self)
    }

    #[must_use]
    pub fn channel(&self) -> &crate::AgentTransferFrameChannel<R, W> {
        &self.channel
    }

    pub fn channel_mut(&mut self) -> &mut crate::AgentTransferFrameChannel<R, W> {
        &mut self.channel
    }
}

impl<R, W> MaterializationTargetSession<R, W>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    pub async fn open(
        &mut self,
        trust: &CentralCommandTrustBundle,
        signed_ticket: SignedMaterializationBatchTicket,
        batch: &MaterializationBatch,
        manifest: &BatchManifest,
        pages: &[BatchManifestPage],
        now_unix_ms: UnixMillis,
    ) -> Result<(), AgentTransferError> {
        validate_signed_materialization_batch_with_trust(
            trust,
            &signed_ticket,
            batch,
            manifest,
            pages,
            now_unix_ms,
        )
        .map_err(materialization_ticket_error)?;
        self.channel
            .send_frame(&TransferFrame::OpenMaterializationSigned(
                signed_ticket.clone(),
            ))
            .await?;
        self.channel
            .send_frame(&TransferFrame::MaterializationManifest(manifest.clone()))
            .await?;
        for page in pages {
            self.channel
                .send_frame(&TransferFrame::MaterializationManifestPage(page.clone()))
                .await?;
        }
        self.ticket = Some(signed_ticket);
        self.transferred_bytes = 0;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn copy_batch(
        &mut self,
        tenant_id: &neoengram_domain::TenantId,
        batch: &MaterializationBatch,
        manifest: &BatchManifest,
        pages: &[BatchManifestPage],
        objects: &[MaterializationObject],
        target: &dyn ObjectBackend,
        now_unix_ms: UnixMillis,
    ) -> Result<Vec<MaterializationObjectReceipt>, AgentTransferError> {
        let ticket = self
            .ticket
            .as_ref()
            .ok_or_else(|| AgentTransferError::Scope("materialization session is not open".into()))?
            .clone();
        neoengram_domain::protocol::materialization::validate_materialization_assignment(
            &ticket.ticket,
            batch,
            manifest,
            pages,
        )
        .map_err(materialization_ticket_error)?;
        if ticket.ticket.tenant_id != *tenant_id {
            return Err(AgentTransferError::Scope(
                "materialization ticket tenant does not match target Agent".to_owned(),
            ));
        }
        let manifest_objects = pages
            .iter()
            .flat_map(|page| page.objects.iter().cloned())
            .map(|object| (object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        let supplied_ids = objects
            .iter()
            .map(|task| task.object.object_id)
            .collect::<BTreeSet<_>>();
        if supplied_ids.len() != objects.len()
            || supplied_ids != manifest_objects.keys().copied().collect::<BTreeSet<_>>()
        {
            return Err(AgentTransferError::Scope(
                "materialization task list must exactly match the signed manifest".to_owned(),
            ));
        }
        let transfer_id = materialization_transfer_id(
            &ticket.ticket.materialization_id,
            &ticket.ticket.object_namespace_id,
        )
        .map_err(materialization_ticket_error)?;
        let mut receipts = Vec::with_capacity(objects.len());
        for task in objects {
            task.validate().map_err(materialization_ticket_error)?;
            if task.materialization_id != ticket.ticket.materialization_id
                || task.plan_revision != ticket.ticket.plan_revision
                || task.attempt != ticket.ticket.batch_attempt
                || task.current_batch_id.as_ref() != Some(&ticket.ticket.batch_id)
            {
                return Err(AgentTransferError::Scope(
                    "materialization object is outside the signed batch fence".to_owned(),
                ));
            }
            let object = manifest_objects
                .get(&task.object.object_id)
                .ok_or_else(|| {
                    AgentTransferError::Scope(
                        "materialization object is absent from the signed manifest".to_owned(),
                    )
                })?;
            if object != &task.object {
                return Err(AgentTransferError::Scope(
                    "materialization task and manifest ObjectRef differ".to_owned(),
                ));
            }
            let expected = ObjectSpec::new(object.object_id, object.size.get());
            if let Some(metadata) = target
                .inspect(tenant_id, &expected.id)
                .map_err(|error| AgentTransferError::Backend(error.to_string()))?
            {
                if metadata.id == expected.id && metadata.size == expected.size {
                    // A path named by the digest is not sufficient evidence: a damaged file can
                    // retain the same name and size.  Reuse the backend's verified publication
                    // path so it re-hashes existing bytes before accepting a replayed receipt.
                    target
                        .verify_and_publish(&transfer_id, tenant_id, &expected)
                        .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                    self.channel
                        .send_frame(&TransferFrame::ObjectAck(ObjectAck {
                            object_id: expected.id,
                            offset: expected.size,
                            length: 0,
                            accepted: true,
                        }))
                        .await?;
                    receipts.push(materialization_receipt_for(
                        &ticket.ticket,
                        object,
                        expected.size,
                        ticket.ticket.target.placement_generation,
                        now_unix_ms,
                    )?);
                    continue;
                }
                return Err(AgentTransferError::Backend(
                    "target contains an object with an unexpected size or digest".to_owned(),
                ));
            }
            let mut offset = target
                .staged_size(&transfer_id, tenant_id, &expected.id)
                .map_err(|error| AgentTransferError::Backend(error.to_string()))?
                .unwrap_or(0);
            if offset < task.confirmed_offset.get() {
                return Err(AgentTransferError::Backend(
                    "durable target staging regressed below the Central checkpoint".to_owned(),
                ));
            }
            if offset > expected.size {
                return Err(AgentTransferError::Backend(
                    "durable target staging exceeds the manifest ObjectRef size".to_owned(),
                ));
            }
            if offset == expected.size && expected.size == 0 {
                self.channel
                    .send_frame(&TransferFrame::ObjectRequest(ObjectRequest {
                        object_id: expected.id,
                        offset: 0,
                        length: 0,
                    }))
                    .await?;
                let proof = self.channel.recv_frame().await?;
                let TransferFrame::ObjectProof(proof) = proof else {
                    return Err(transfer_unexpected("ObjectProof", &proof));
                };
                if proof.object_id != expected.id
                    || proof.size != 0
                    || proof.digest != expected.id.digest()
                {
                    return Err(AgentTransferError::Scope(
                        "empty ObjectProof does not match the manifest ObjectRef".to_owned(),
                    ));
                }
                self.channel
                    .send_frame(&TransferFrame::ObjectAck(ObjectAck {
                        object_id: expected.id,
                        offset: 0,
                        length: 0,
                        accepted: true,
                    }))
                    .await?;
                target
                    .verify_and_publish(&transfer_id, tenant_id, &expected)
                    .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                receipts.push(materialization_receipt_for(
                    &ticket.ticket,
                    object,
                    0,
                    ticket.ticket.target.placement_generation,
                    now_unix_ms,
                )?);
                continue;
            }
            if offset == expected.size {
                // The previous connection may have completed the durability barrier but lost the
                // response before publication/receipt enqueue.  Re-run the idempotent finalize
                // fence locally and do not wait for a proof that the source has no reason to send.
                target
                    .verify_and_publish(&transfer_id, tenant_id, &expected)
                    .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                self.channel
                    .send_frame(&TransferFrame::ObjectAck(ObjectAck {
                        object_id: expected.id,
                        offset,
                        length: 0,
                        accepted: true,
                    }))
                    .await?;
                receipts.push(materialization_receipt_for(
                    &ticket.ticket,
                    object,
                    offset,
                    ticket.ticket.target.placement_generation,
                    now_unix_ms,
                )?);
                continue;
            }
            while offset < expected.size {
                let length = (expected.size - offset).min(self.chunk_bytes as u64);
                self.channel
                    .send_frame(&TransferFrame::ObjectRequest(ObjectRequest {
                        object_id: expected.id,
                        offset,
                        length,
                    }))
                    .await?;
                let request_end = offset + length;
                loop {
                    let frame = self.channel.recv_frame().await?;
                    match frame {
                        TransferFrame::ObjectChunk(chunk) => {
                            if chunk.object_id != expected.id
                                || chunk.offset != offset
                                || chunk.bytes.is_empty()
                                || chunk.bytes.len() as u64 > request_end - offset
                            {
                                return Err(AgentTransferError::Scope(
                                    "ObjectChunk does not match the requested materialization range"
                                    .to_owned(),
                                ));
                            }
                            let chunk_len = chunk.bytes.len() as u64;
                            let next_transferred = self
                                .transferred_bytes
                                .checked_add(chunk_len)
                                .ok_or_else(|| {
                                    AgentTransferError::Scope(
                                        "transferred byte count overflows u64".to_owned(),
                                    )
                                })?;
                            if next_transferred > ticket.ticket.max_bytes.get() {
                                return Err(AgentTransferError::Scope(
                                    "target transfer exceeds the signed byte limit".to_owned(),
                                ));
                            }
                            let staged = target
                                .stage_write(
                                    &transfer_id,
                                    tenant_id,
                                    &expected,
                                    offset,
                                    &chunk.bytes,
                                )
                                .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                            let next = offset + chunk_len;
                            if staged.accepted_bytes != chunk_len || staged.staged_size != next {
                                return Err(AgentTransferError::Backend(
                                    "target acknowledged an unexpected durable offset".to_owned(),
                                ));
                            }
                            self.channel
                                .send_frame(&TransferFrame::ObjectAck(ObjectAck {
                                    object_id: expected.id,
                                    offset,
                                    length: chunk_len,
                                    accepted: true,
                                }))
                                .await?;
                            offset = next;
                            self.transferred_bytes = next_transferred;
                            if offset == request_end {
                                break;
                            }
                        }
                        TransferFrame::TransferError(error) => return Err(transfer_remote(error)),
                        other => return Err(transfer_unexpected("ObjectChunk", &other)),
                    }
                }
            }
            if offset == expected.size {
                if expected.size > 0 {
                    let proof = self.channel.recv_frame().await?;
                    let TransferFrame::ObjectProof(proof) = proof else {
                        return Err(transfer_unexpected("ObjectProof", &proof));
                    };
                    if proof.object_id != expected.id
                        || proof.size != expected.size
                        || proof.digest != expected.id.digest()
                    {
                        return Err(AgentTransferError::Scope(
                            "ObjectProof does not match the manifest ObjectRef".to_owned(),
                        ));
                    }
                }
                target
                    .verify_and_publish(&transfer_id, tenant_id, &expected)
                    .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                let checkpoint = MaterializationCheckpoint {
                    materialization_id: ticket.ticket.materialization_id.clone(),
                    batch_id: ticket.ticket.batch_id.clone(),
                    plan_revision: ticket.ticket.plan_revision,
                    batch_attempt: ticket.ticket.batch_attempt,
                    object_id: expected.id,
                    confirmed_offset: offset,
                };
                receipts.push(materialization_receipt_for(
                    &ticket.ticket,
                    object,
                    checkpoint.confirmed_offset,
                    ticket.ticket.target.placement_generation,
                    now_unix_ms,
                )?);
            }
        }
        self.channel
            .send_frame(&TransferFrame::CloseTransfer(CloseTransfer {
                committed: true,
            }))
            .await?;
        Ok(receipts)
    }
}

/// Applies an fsync-confirmed checkpoint to a materialization object.  The update is monotonic,
/// and state changes use the domain state machine so a stale or terminal report cannot revive an
/// object silently.
pub fn apply_materialization_checkpoint(
    object: &mut MaterializationObject,
    checkpoint: &MaterializationCheckpoint,
) -> ProtocolResult<()> {
    object.validate()?;
    if object.materialization_id != checkpoint.materialization_id
        || object.object.object_id != checkpoint.object_id
        || object.plan_revision != checkpoint.plan_revision
    {
        return Err(invalid(
            "checkpoint",
            "checkpoint identity does not match materialization object",
        ));
    }
    if object.attempt != checkpoint.batch_attempt {
        return Err(invalid(
            "batch_attempt",
            "checkpoint attempt does not match materialization object",
        ));
    }
    if let Some(current_batch_id) = &object.current_batch_id {
        if current_batch_id != &checkpoint.batch_id {
            return Err(invalid(
                "batch_id",
                "checkpoint batch does not match materialization object",
            ));
        }
    } else {
        // Bind an unassigned object to the first durable checkpoint. Every later checkpoint is
        // then fenced to this exact batch, including retries that reuse the staging offset.
        object.current_batch_id = Some(checkpoint.batch_id.clone());
    }
    if checkpoint.confirmed_offset < object.confirmed_offset.get() {
        return Err(invalid(
            "confirmed_offset",
            "checkpoint offset must be monotonic",
        ));
    }
    if checkpoint.confirmed_offset > object.object.size.get() {
        return Err(invalid(
            "confirmed_offset",
            "checkpoint offset exceeds object size",
        ));
    }
    if checkpoint.confirmed_offset == 0
        && object.object.size.get() == 0
        && object.state == MaterializationObjectState::Missing
    {
        object.state = MaterializationObjectState::AlreadyPresent;
    } else if checkpoint.confirmed_offset > 0
        && matches!(
            object.state,
            MaterializationObjectState::Missing | MaterializationObjectState::Failed
        )
    {
        if !object
            .state
            .can_transition_to(MaterializationObjectState::Reserved)
        {
            return Err(invalid("state", "object cannot be reserved for checkpoint"));
        }
        object.state = MaterializationObjectState::Reserved;
        object.state = MaterializationObjectState::Transferring;
    } else if checkpoint.confirmed_offset > 0
        && object.state == MaterializationObjectState::Reserved
    {
        object.state = MaterializationObjectState::Transferring;
    }
    object.confirmed_offset = neoengram_domain::DecimalU64::new(checkpoint.confirmed_offset);
    if checkpoint.confirmed_offset == object.object.size.get() && !object.complete() {
        if !object
            .state
            .can_transition_to(MaterializationObjectState::Verified)
        {
            return Err(invalid("state", "complete checkpoint cannot verify object"));
        }
        object.state = MaterializationObjectState::Verified;
    }
    object.validate()
}

/// Switches a failed object task to one of its Central-selected fallback placements. The target
/// staging identity and confirmed offset remain untouched; only the batch attempt changes. This
/// prevents a source disconnect from forcing a restart from byte zero or allowing an arbitrary
/// Agent-selected source.
pub fn switch_materialization_source(
    object: &mut MaterializationObject,
    fallback: &PlacementId,
) -> ProtocolResult<()> {
    object.validate()?;
    if !object.fallback_sources.contains(fallback) {
        return Err(invalid(
            "fallback_sources",
            "source is not an authority-selected fallback",
        ));
    }
    if !matches!(
        object.state,
        MaterializationObjectState::Failed | MaterializationObjectState::Reserved
    ) {
        return Err(invalid(
            "state",
            "source may only switch before a new transfer attempt",
        ));
    }
    let old_primary = object.primary_source.replace(fallback.clone());
    object.fallback_sources.retain(|source| source != fallback);
    if let Some(old_primary) = old_primary {
        if old_primary != *fallback && !object.fallback_sources.contains(&old_primary) {
            object.fallback_sources.push(old_primary);
        }
    }
    object.attempt = neoengram_domain::Generation::new(
        object
            .attempt
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid("attempt", "exceeds u64"))?,
    );
    object.current_batch_id = None;
    if object.state == MaterializationObjectState::Failed {
        object.state = MaterializationObjectState::Reserved;
    }
    object.validate()
}

/// Creates the receipt that is allowed to cross the control plane after the target durability
/// barrier.  Callers still need to persist/send it through the normal signed Agent report path.
pub fn materialization_receipt_from_checkpoint(
    ticket: &MaterializationBatchTicket,
    object: &ObjectRef,
    receipt_id: ObjectReceiptId,
    placement_generation: neoengram_domain::PlacementGeneration,
    checkpoint: &MaterializationCheckpoint,
    verified_at_unix_ms: UnixMillis,
) -> ProtocolResult<MaterializationObjectReceipt> {
    ticket.validate()?;
    if checkpoint.materialization_id != ticket.materialization_id
        || checkpoint.batch_id != ticket.batch_id
        || checkpoint.plan_revision != ticket.plan_revision
        || checkpoint.batch_attempt != ticket.batch_attempt
        || checkpoint.object_id != object.object_id
    {
        return Err(invalid(
            "checkpoint",
            "checkpoint does not match batch ticket or object",
        ));
    }
    let receipt = MaterializationObjectReceipt {
        receipt_id,
        materialization_id: ticket.materialization_id.clone(),
        batch_id: ticket.batch_id.clone(),
        plan_revision: ticket.plan_revision,
        batch_attempt: ticket.batch_attempt,
        tenant_id: ticket.tenant_id.clone(),
        object_namespace_id: object.object_namespace_id.clone(),
        object_id: object.object_id,
        size: object.size,
        encoding: object.encoding,
        verified_digest: object.object_id.digest(),
        target_storage_volume_id: ticket.target.storage_volume_id.clone(),
        target_placement_generation: placement_generation,
        committed_offset: neoengram_domain::DecimalU64::new(checkpoint.confirmed_offset),
        verified_at_unix_ms,
    };
    receipt.validate_against_ticket(ticket, object)?;
    Ok(receipt)
}

fn materialization_receipt_for(
    ticket: &MaterializationBatchTicket,
    object: &ObjectRef,
    confirmed_offset: u64,
    placement_generation: neoengram_domain::PlacementGeneration,
    verified_at_unix_ms: UnixMillis,
) -> Result<MaterializationObjectReceipt, AgentTransferError> {
    let receipt_id = ObjectReceiptId::new(format!(
        "receipt-{}",
        &blake3::hash(
            format!(
                "{}:{}:{}:{}:{}",
                ticket.materialization_id,
                ticket.batch_id,
                ticket.plan_revision,
                ticket.batch_attempt,
                object.object_id
            )
            .as_bytes(),
        )
        .to_hex()[..32]
    ))
    .map_err(materialization_ticket_error)?;
    materialization_receipt_from_checkpoint(
        ticket,
        object,
        receipt_id,
        placement_generation,
        &MaterializationCheckpoint {
            materialization_id: ticket.materialization_id.clone(),
            batch_id: ticket.batch_id.clone(),
            plan_revision: ticket.plan_revision,
            batch_attempt: ticket.batch_attempt,
            object_id: object.object_id,
            confirmed_offset,
        },
        verified_at_unix_ms,
    )
    .map_err(materialization_ticket_error)
}

/// Keeps this module's protocol surface visibly tied to the published v2 schema. This function is
/// useful to transport adapters that need a compile-time schema root without accepting arbitrary
/// JSON values at the Agent boundary.
#[must_use]
pub const fn materialization_protocol_version() -> u16 {
    neoengram_domain::protocol::MATERIALIZATION_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentTransferFrameChannel;
    use neoengram_domain::protocol::{
        ArtifactId, EdgeClusterId, GatewayPoolId, Generation, MaterializationBatchId,
        MaterializationId, MaterializationManifestSource, MaterializationSource,
        MaterializationTarget, ObjectEncoding, PlacementGeneration, PlacementId, StorageVolumeId,
        TenantId,
    };
    use neoengram_domain::{
        CentralSignedPayload, ContentDigest, Ed25519PublicKeySpki, Ed25519Signature, Extensions,
        GatewayOpaqueBytes, ObjectId,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use tokio::io::duplex;

    use neoengram_runtime::VolumeCasBackend;

    fn ids() -> (
        MaterializationBatchTicket,
        MaterializationBatch,
        BatchManifest,
        Vec<BatchManifestPage>,
        ObjectRef,
    ) {
        let artifact = ArtifactId::new("artifact-v2").unwrap();
        let namespace = neoengram_domain::ObjectNamespaceId::from_artifact(&artifact);
        ids_for_object(ObjectRef::new(
            namespace.clone(),
            ObjectId::for_bytes(b"data"),
            4,
            ObjectEncoding::Raw,
            0,
        ))
    }

    fn empty_ids() -> (
        MaterializationBatchTicket,
        MaterializationBatch,
        BatchManifest,
        Vec<BatchManifestPage>,
        ObjectRef,
    ) {
        let artifact = ArtifactId::new("artifact-v2").unwrap();
        let namespace = neoengram_domain::ObjectNamespaceId::from_artifact(&artifact);
        ids_for_object(ObjectRef::new(
            namespace,
            ObjectId::for_bytes([]),
            0,
            ObjectEncoding::Raw,
            0,
        ))
    }

    fn ids_for_object(
        object: ObjectRef,
    ) -> (
        MaterializationBatchTicket,
        MaterializationBatch,
        BatchManifest,
        Vec<BatchManifestPage>,
        ObjectRef,
    ) {
        let tenant = TenantId::new("tenant-v2").unwrap();
        let artifact = ArtifactId::new("artifact-v2").unwrap();
        let namespace = neoengram_domain::ObjectNamespaceId::from_artifact(&artifact);
        assert_eq!(object.object_namespace_id, namespace);
        let materialization_id = MaterializationId::new("materialization-v2").unwrap();
        let batch_id = MaterializationBatchId::new("batch-v2").unwrap();
        let source = MaterializationSource {
            placement_id: PlacementId::new("source-placement").unwrap(),
            tenant_id: tenant.clone(),
            object_namespace_id: namespace.clone(),
            storage_volume_id: Some(StorageVolumeId::new("source-volume").unwrap()),
            archive_id: None,
            agent_id: neoengram_domain::AgentId::new("source-agent").unwrap(),
            edge_cluster_id: EdgeClusterId::new("source-cluster").unwrap(),
            gateway_pool_id: GatewayPoolId::new("source-pool").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: neoengram_domain::SessionGeneration::new(1),
            mount_generation: neoengram_domain::MountGeneration::new(1),
            route_generation: neoengram_domain::RouteGeneration::new(1),
        };
        let target = MaterializationTarget {
            tenant_id: tenant.clone(),
            object_namespace_id: namespace.clone(),
            storage_volume_id: StorageVolumeId::new("target-volume").unwrap(),
            agent_id: neoengram_domain::AgentId::new("target-agent").unwrap(),
            edge_cluster_id: EdgeClusterId::new("target-cluster").unwrap(),
            gateway_pool_id: GatewayPoolId::new("target-pool").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: neoengram_domain::SessionGeneration::new(1),
            mount_generation: neoengram_domain::MountGeneration::new(1),
            route_generation: neoengram_domain::RouteGeneration::new(1),
        };
        let (manifest, pages) = BatchManifest::paginate_with_sources(
            materialization_id.clone(),
            batch_id.clone(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            vec![object.clone()],
            vec![MaterializationManifestSource {
                object_id: object.object_id,
                placement_id: source.placement_id.clone(),
                placement_generation: source.placement_generation,
            }],
            16,
        )
        .unwrap();
        let batch = MaterializationBatch {
            batch_id: batch_id.clone(),
            materialization_id: materialization_id.clone(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            source: source.clone(),
            target: target.clone(),
            manifest_digest: manifest.manifest_digest,
            object_ids: vec![object.object_id],
            object_count: 1.into(),
            total_bytes: object.size,
            state: neoengram_domain::MaterializationBatchState::Transferring,
            max_bytes: neoengram_domain::DecimalU64::new(object.size.get().max(1)),
            deadline_unix_ms: UnixMillis::new(300_001),
        };
        let ticket = MaterializationBatchTicket {
            ticket_id: neoengram_domain::ObjectTicketId::new("ticket-v2").unwrap(),
            materialization_id,
            batch_id,
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: tenant,
            artifact_id: artifact,
            object_namespace_id: namespace,
            commit_id: neoengram_domain::CommitId::from_bytes([1; 32]),
            manifest_digest: manifest.manifest_digest,
            source,
            target,
            max_bytes: neoengram_domain::DecimalU64::new(object.size.get().max(1)),
            deadline_unix_ms: UnixMillis::new(300_001),
            capability: neoengram_domain::protocol::COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        };
        (ticket, batch, manifest, pages, object)
    }

    fn signed_ticket(
        ticket: &MaterializationBatchTicket,
    ) -> (CentralCommandTrustBundle, SignedMaterializationBatchTicket) {
        let key = Ed25519KeyPair::from_seed_unchecked(&[0x31; 32]).unwrap();
        let payload = GatewayOpaqueBytes::new(ticket.payload_bytes().unwrap()).unwrap();
        let mut central_signature = CentralSignedPayload {
            key_id: "central-materialization-test".to_owned(),
            certificate_generation: neoengram_domain::CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(1),
            expires_at_unix_ms: ticket.deadline_unix_ms,
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        central_signature.signature = Ed25519Signature::new(
            key.sign(&central_signature.signing_bytes().unwrap())
                .as_ref()
                .to_vec(),
        )
        .unwrap();
        let trust = CentralCommandTrustBundle::from_test_keys(vec![(
            "central-materialization-test".to_owned(),
            neoengram_domain::CertificateGeneration::new(1),
            Ed25519PublicKeySpki::from_public_key_bytes(
                key.public_key().as_ref().try_into().unwrap(),
            ),
            crate::command_trust::TrustKeyState::Active,
        )])
        .unwrap();
        let signed =
            SignedMaterializationBatchTicket::new(ticket.clone(), central_signature).unwrap();
        (trust, signed)
    }

    fn channel_pair() -> (
        AgentTransferFrameChannel<tokio::io::DuplexStream, tokio::io::DuplexStream>,
        AgentTransferFrameChannel<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    ) {
        let (left_to_right_write, left_to_right_read) = duplex(64 * 1024);
        let (right_to_left_write, right_to_left_read) = duplex(64 * 1024);
        (
            AgentTransferFrameChannel::new(right_to_left_read, left_to_right_write),
            AgentTransferFrameChannel::new(left_to_right_read, right_to_left_write),
        )
    }

    #[tokio::test]
    async fn source_and_target_sessions_copy_an_empty_object_with_zero_length_request() {
        let (ticket, batch, manifest, pages, object) = empty_ids();
        let (trust, signed) = signed_ticket(&ticket);
        let tenant = ticket.tenant_id.clone();
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let source_backend = VolumeCasBackend::open_or_create(source_root.path()).unwrap();
        let target_backend = VolumeCasBackend::open_or_create(target_root.path()).unwrap();
        let (source_channel, target_channel) = channel_pair();
        let (source_recv, source_send) = source_channel.into_parts();
        let source_batch = batch.clone();
        let source_trust = trust.clone();
        let source_tenant = tenant.clone();
        let source = tokio::spawn(async move {
            let mut session = MaterializationSourceSession::new(source_recv, source_send);
            session
                .serve(
                    &source_trust,
                    &source_batch,
                    &source_tenant,
                    &source_backend,
                    UnixMillis::new(2),
                )
                .await
        });

        let (target_recv, target_send) = target_channel.into_parts();
        let mut target = MaterializationTargetSession::new(target_recv, target_send);
        target
            .open(
                &trust,
                signed,
                &batch,
                &manifest,
                &pages,
                UnixMillis::new(2),
            )
            .await
            .unwrap();
        let mut task = MaterializationObject::new(
            ticket.materialization_id.clone(),
            object.clone(),
            Generation::new(1),
        );
        task.current_batch_id = Some(ticket.batch_id.clone());
        let receipts = target
            .copy_batch(
                &tenant,
                &batch,
                &manifest,
                &pages,
                &[task],
                &target_backend,
                UnixMillis::new(2),
            )
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].committed_offset.get(), 0);
        assert_eq!(receipts[0].size.get(), 0);
        assert_eq!(
            target_backend.inspect(&tenant, &object.object_id).unwrap(),
            Some(neoengram_runtime::ObjectMetadata {
                id: object.object_id,
                size: 0,
            })
        );
        source.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn source_session_rejects_zero_length_request_for_non_empty_object() {
        let (ticket, batch, manifest, pages, object) = ids();
        let (trust, signed) = signed_ticket(&ticket);
        let source_root = tempfile::tempdir().unwrap();
        let source_backend = VolumeCasBackend::open_or_create(source_root.path()).unwrap();
        let (source_channel, mut peer) = channel_pair();
        let (source_recv, source_send) = source_channel.into_parts();
        let source = tokio::spawn(async move {
            let mut session = MaterializationSourceSession::new(source_recv, source_send);
            session
                .serve(
                    &trust,
                    &batch,
                    &ticket.tenant_id,
                    &source_backend,
                    UnixMillis::new(2),
                )
                .await
        });

        peer.send_frame(&TransferFrame::OpenMaterializationSigned(signed))
            .await
            .unwrap();
        peer.send_frame(&TransferFrame::MaterializationManifest(manifest))
            .await
            .unwrap();
        for page in pages {
            peer.send_frame(&TransferFrame::MaterializationManifestPage(page))
                .await
                .unwrap();
        }
        peer.send_frame(&TransferFrame::ObjectRequest(ObjectRequest {
            object_id: object.object_id,
            offset: 0,
            length: 0,
        }))
        .await
        .unwrap();

        let error = source.await.unwrap().unwrap_err();
        assert!(error
            .to_string()
            .contains("non-empty object requests must use a positive range length"));
    }

    #[test]
    fn batch_manifest_and_ticket_are_bound_together() {
        let (ticket, batch, manifest, pages, _) = ids();
        validate_materialization_batch(&ticket, &batch, &manifest, &pages).unwrap();
        neoengram_domain::protocol::materialization::validate_materialization_assignment(
            &ticket, &batch, &manifest, &pages,
        )
        .unwrap();
        let mut bad = pages.clone();
        bad[0].objects[0].ordinal = 1.into();
        assert!(validate_materialization_batch(&ticket, &batch, &manifest, &bad).is_err());

        // The generic helper remains useful for structural fixtures, but an actual assignment
        // must bind every non-empty object to the exact Central-selected source Placement.
        let (unbound_manifest, unbound_pages) = BatchManifest::paginate(
            ticket.materialization_id.clone(),
            ticket.batch_id.clone(),
            ticket.plan_revision,
            ticket.batch_attempt,
            ticket.object_namespace_id.clone(),
            pages[0].objects.clone(),
            16,
        )
        .unwrap();
        let mut unbound_batch = batch;
        unbound_batch.manifest_digest = unbound_manifest.manifest_digest;
        let mut unbound_ticket = ticket;
        unbound_ticket.manifest_digest = unbound_manifest.manifest_digest;
        assert!(
            neoengram_domain::protocol::materialization::validate_materialization_assignment(
                &unbound_ticket,
                &unbound_batch,
                &unbound_manifest,
                &unbound_pages,
            )
            .is_err()
        );
    }

    #[test]
    fn checkpoints_are_monotonic_and_receipts_require_full_size() {
        let (ticket, _, _, _, object) = ids();
        let mut task = MaterializationObject::new(
            ticket.materialization_id.clone(),
            object.clone(),
            Generation::new(1),
        );
        let checkpoint = MaterializationCheckpoint {
            materialization_id: ticket.materialization_id.clone(),
            batch_id: ticket.batch_id.clone(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            object_id: object.object_id,
            confirmed_offset: 2,
        };
        apply_materialization_checkpoint(&mut task, &checkpoint).unwrap();
        assert_eq!(task.confirmed_offset.get(), 2);
        assert_eq!(task.current_batch_id, Some(checkpoint.batch_id.clone()));
        assert!(apply_materialization_checkpoint(
            &mut task,
            &MaterializationCheckpoint {
                batch_id: MaterializationBatchId::new("other-batch").unwrap(),
                ..checkpoint.clone()
            }
        )
        .is_err());
        assert!(apply_materialization_checkpoint(
            &mut task,
            &MaterializationCheckpoint {
                batch_attempt: Generation::new(2),
                ..checkpoint.clone()
            }
        )
        .is_err());
        assert!(apply_materialization_checkpoint(
            &mut task,
            &MaterializationCheckpoint {
                confirmed_offset: 1,
                ..checkpoint.clone()
            }
        )
        .is_err());
        assert!(materialization_receipt_from_checkpoint(
            &ticket,
            &object,
            ObjectReceiptId::new("receipt-v2").unwrap(),
            PlacementGeneration::new(1),
            &checkpoint,
            UnixMillis::new(1)
        )
        .is_err());
        let complete = MaterializationCheckpoint {
            confirmed_offset: 4,
            ..checkpoint
        };
        apply_materialization_checkpoint(&mut task, &complete).unwrap();
        let receipt = materialization_receipt_from_checkpoint(
            &ticket,
            &object,
            ObjectReceiptId::new("receipt-v2").unwrap(),
            PlacementGeneration::new(1),
            &complete,
            UnixMillis::new(1),
        )
        .unwrap();
        assert_eq!(receipt.committed_offset.get(), 4);
    }

    #[test]
    fn failed_object_switches_only_to_authorized_fallback_and_keeps_offset() {
        let (ticket, _, _, _, object) = ids();
        let mut task = MaterializationObject::new(
            ticket.materialization_id.clone(),
            object,
            Generation::new(1),
        );
        let fallback = PlacementId::new("fallback-placement").unwrap();
        task.primary_source = Some(PlacementId::new("primary-placement").unwrap());
        task.fallback_sources = vec![fallback.clone()];
        task.state = MaterializationObjectState::Failed;
        task.confirmed_offset = 2.into();
        switch_materialization_source(&mut task, &fallback).unwrap();
        assert_eq!(task.primary_source, Some(fallback));
        assert_eq!(task.confirmed_offset.get(), 2);
        assert_eq!(task.attempt.get(), 2);
        assert!(
            switch_materialization_source(&mut task, &PlacementId::new("unlisted").unwrap())
                .is_err()
        );
    }
}
