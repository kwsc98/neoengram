use std::{collections::BTreeMap, sync::Mutex};

use async_trait::async_trait;
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentEnrollmentId, AgentEnrollmentState, AgentId, AgentInstallationId, EdgeClusterId,
    PvcIdentityDigest, RequestId, StorageVolumeId, TenantId, UnixMillis,
};

use crate::{
    checked_next_registry_resource_version, ensure_immutable_registry_scope,
    transition_storage_enrollment, validate_registry_insert, validate_registry_record,
    validate_registry_replace_transition, validate_registry_replacement_transition,
    AgentEnrollmentExpiryReconciliation, AgentEnrollmentLifecycleAuditKind,
    AgentEnrollmentListCursor, AgentEnrollmentListPage, AgentEnrollmentListRequest,
    AgentRegistryInsertOutcome, AgentRegistryRecord, AgentRegistryReplacementRecords,
    AgentRegistryRepository, CentralError, CentralErrorCode, CentralResult, PvcVolumeBinding,
    StorageEnrollmentState, AGENT_ENROLLMENT_MAX_PAGE_SIZE, AGENT_ENROLLMENT_MAX_QUERY_CHARS,
};

#[derive(Debug, Default)]
pub struct InMemoryAgentRegistry {
    records: Mutex<BTreeMap<AgentEnrollmentId, AgentRegistryRecord>>,
    bootstrap_status_watermarks: Mutex<BTreeMap<AgentEnrollmentId, UnixMillis>>,
}

impl InMemoryAgentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn records(&self) -> CentralResult<Vec<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl AgentRegistryRepository for InMemoryAgentRegistry {
    async fn get(
        &self,
        enrollment_id: &AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .get(enrollment_id)
            .cloned())
    }

    async fn get_for_tenant(
        &self,
        tenant_id: &TenantId,
        enrollment_id: &AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .get(enrollment_id)
            .filter(|record| &record.enrollment.tenant_id == tenant_id)
            .cloned())
    }

    async fn list_for_tenant(
        &self,
        request: &AgentEnrollmentListRequest,
    ) -> CentralResult<AgentEnrollmentListPage> {
        if request.limit == 0 || request.limit > AGENT_ENROLLMENT_MAX_PAGE_SIZE {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Agent enrollment page size is outside the supported range",
            )
            .with_retryable(false));
        }
        if request.query.as_ref().is_some_and(|query| {
            query.is_empty() || query.chars().count() > AGENT_ENROLLMENT_MAX_QUERY_CHARS
        }) {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Agent enrollment query must contain 1 to 256 characters",
            )
            .with_retryable(false));
        }
        let records = self.records.lock().map_err(lock_error)?;
        let normalized_query = request.query.as_ref().map(|query| query.to_lowercase());
        let mut matches =
            records
                .values()
                .filter(|record| record.enrollment.tenant_id == request.tenant_id)
                .filter(|record| record.storage_enrollment.state.is_some())
                .filter(|record| {
                    request
                        .state
                        .is_none_or(|state| record.storage_enrollment.state == Some(state))
                })
                .filter(|record| {
                    request.registration_kind.is_none_or(|kind| {
                        record.storage_enrollment.registration_kind == Some(kind)
                    })
                })
                .filter(|record| {
                    normalized_query.as_ref().is_none_or(|query| {
                        record
                            .enrollment
                            .enrollment_id
                            .as_str()
                            .to_lowercase()
                            .contains(query)
                            || record
                                .enrollment
                                .storage_volume_id
                                .as_str()
                                .to_lowercase()
                                .contains(query)
                            || record.storage_enrollment.descriptor.as_ref().is_some_and(
                                |descriptor| descriptor.display_name.to_lowercase().contains(query),
                            )
                    })
                })
                .filter(|record| {
                    request.after.as_ref().is_none_or(|cursor| {
                        public_enrollment_created_at(record).get() < cursor.created_at_unix_ms.get()
                            || (public_enrollment_created_at(record) == cursor.created_at_unix_ms
                                && record.enrollment.enrollment_id > cursor.enrollment_id)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            public_enrollment_created_at(right)
                .cmp(&public_enrollment_created_at(left))
                .then_with(|| {
                    left.enrollment
                        .enrollment_id
                        .cmp(&right.enrollment.enrollment_id)
                })
        });
        let has_more = matches.len() > request.limit;
        matches.truncate(request.limit);
        let next = has_more.then(|| {
            let last = matches
                .last()
                .expect("a non-empty page has a last enrollment");
            AgentEnrollmentListCursor {
                created_at_unix_ms: public_enrollment_created_at(last),
                enrollment_id: last.enrollment.enrollment_id.clone(),
            }
        });
        Ok(AgentEnrollmentListPage {
            records: matches,
            next,
        })
    }

    async fn get_by_agent(&self, agent_id: &AgentId) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| &record.enrollment.reserved_agent_id == agent_id)
            .cloned())
    }

    async fn get_by_token_request_id(
        &self,
        tenant_id: &TenantId,
        token_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| {
                &record.enrollment.tenant_id == tenant_id
                    && &record.enrollment.token_request_id == token_request_id
            })
            .cloned())
    }

    async fn get_by_token_digest(
        &self,
        token_digest: &ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| &record.enrollment.bootstrap_token_digest == token_digest)
            .cloned())
    }

    async fn get_by_bootstrap_request_id(
        &self,
        tenant_id: &TenantId,
        bootstrap_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| {
                &record.enrollment.tenant_id == tenant_id
                    && record.enrollment.bootstrap_request_id.as_ref() == Some(bootstrap_request_id)
            })
            .cloned())
    }

    async fn get_by_decision_request_id(
        &self,
        tenant_id: &TenantId,
        decision_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| {
                &record.enrollment.tenant_id == tenant_id
                    && record
                        .enrollment
                        .decision_request
                        .as_ref()
                        .is_some_and(|request| &request.decision_request_id == decision_request_id)
            })
            .cloned())
    }

    async fn get_by_installation_id(
        &self,
        installation_id: &AgentInstallationId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| {
                record
                    .candidate
                    .as_ref()
                    .is_some_and(|candidate| &candidate.installation_id == installation_id)
            })
            .cloned())
    }

    async fn get_by_public_key_fingerprint(
        &self,
        public_key_fingerprint: &ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        Ok(self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .find(|record| {
                record.candidate.as_ref().is_some_and(|candidate| {
                    &candidate.public_key_fingerprint == public_key_fingerprint
                })
            })
            .cloned())
    }

    async fn get_current_by_volume(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let records = self.records.lock().map_err(lock_error)?;
        let mut matching = records.values().filter(|record| {
            &record.enrollment.tenant_id == tenant_id
                && &record.enrollment.storage_volume_id == storage_volume_id
                && record.enrollment.state == AgentEnrollmentState::Approved
        });
        let current = matching.next().cloned();
        if matching.next().is_some() {
            return Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "multiple approved Agents own one Tenant Volume",
            )
            .with_retryable(false));
        }
        Ok(current)
    }

    async fn get_pvc_binding(
        &self,
        edge_cluster_id: &EdgeClusterId,
        pvc_identity_digest: &PvcIdentityDigest,
    ) -> CentralResult<Option<PvcVolumeBinding>> {
        let records = self.records.lock().map_err(lock_error)?;
        let mut bindings = records.values().filter(|record| {
            &record.enrollment.edge_cluster_id == edge_cluster_id
                && &record.enrollment.pvc_identity_digest == pvc_identity_digest
                && !is_terminal(record.enrollment.state)
        });
        let first = bindings.next().map(pvc_binding);
        for record in bindings {
            if first.as_ref() != Some(&pvc_binding(record)) {
                return Err(CentralError::new(
                    CentralErrorCode::StorageFailure,
                    "one EdgeCluster PVC identity is bound to multiple Volumes",
                )
                .with_retryable(false));
            }
        }
        Ok(first)
    }

    async fn expire_stale_token_intents(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
        edge_cluster_id: &EdgeClusterId,
        pvc_identity_digest: &PvcIdentityDigest,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<usize> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let mut staged = Vec::new();
        for record in records.values() {
            let conflicts = (&record.enrollment.tenant_id == tenant_id
                && &record.enrollment.storage_volume_id == storage_volume_id)
                || (&record.enrollment.edge_cluster_id == edge_cluster_id
                    && &record.enrollment.pvc_identity_digest == pvc_identity_digest);
            if conflicts
                && record.enrollment.state == AgentEnrollmentState::TokenIssued
                && record.enrollment.expires_at_unix_ms.get() <= now_unix_ms.get()
            {
                let mut expired = record.clone();
                let next = checked_next_registry_resource_version(record.resource_version.get())?;
                expired.enrollment.state = AgentEnrollmentState::Expired;
                expired.resource_version = neoengram_protocol::ResourceVersion::new(next);
                transition_storage_enrollment(
                    &mut expired,
                    None,
                    AgentEnrollmentLifecycleAuditKind::Expired,
                    now_unix_ms,
                    None,
                );
                validate_registry_record(&expired)?;
                staged.push((record.enrollment.enrollment_id.clone(), expired));
            }
        }
        let expired = staged.len();
        for (enrollment_id, record) in staged {
            records.insert(enrollment_id, record);
        }
        Ok(expired)
    }

    async fn expire_stale_review_enrollments(
        &self,
        tenant_id: &TenantId,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<usize> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let mut staged = Vec::new();
        for record in records.values() {
            if &record.enrollment.tenant_id != tenant_id
                || record.enrollment.state != AgentEnrollmentState::PendingApproval
                || record
                    .enrollment
                    .review_expires_at_unix_ms
                    .is_none_or(|expires_at| expires_at.get() > now_unix_ms.get())
            {
                continue;
            }
            let mut expired = record.clone();
            let next = checked_next_registry_resource_version(record.resource_version.get())?;
            expired.enrollment.state = AgentEnrollmentState::Expired;
            expired.resource_version = neoengram_protocol::ResourceVersion::new(next);
            transition_storage_enrollment(
                &mut expired,
                Some(StorageEnrollmentState::Expired),
                AgentEnrollmentLifecycleAuditKind::Expired,
                now_unix_ms,
                None,
            );
            validate_registry_record(&expired)?;
            staged.push((record.enrollment.enrollment_id.clone(), expired));
        }
        let expired = staged.len();
        for (enrollment_id, record) in staged {
            records.insert(enrollment_id, record);
        }
        Ok(expired)
    }

    async fn reconcile_expired_enrollments(
        &self,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<AgentEnrollmentExpiryReconciliation> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let mut staged = Vec::new();
        let mut result = AgentEnrollmentExpiryReconciliation::default();
        for record in records.values() {
            let public_state = match record.enrollment.state {
                AgentEnrollmentState::TokenIssued
                    if record.enrollment.expires_at_unix_ms.get() <= now_unix_ms.get() =>
                {
                    result.expired_token_intents += 1;
                    None
                }
                AgentEnrollmentState::PendingApproval
                    if record
                        .enrollment
                        .review_expires_at_unix_ms
                        .is_some_and(|expires_at| expires_at.get() <= now_unix_ms.get()) =>
                {
                    result.expired_review_enrollments += 1;
                    Some(StorageEnrollmentState::Expired)
                }
                _ => continue,
            };
            let mut expired = record.clone();
            let next = checked_next_registry_resource_version(record.resource_version.get())?;
            expired.enrollment.state = AgentEnrollmentState::Expired;
            expired.resource_version = neoengram_protocol::ResourceVersion::new(next);
            transition_storage_enrollment(
                &mut expired,
                public_state,
                AgentEnrollmentLifecycleAuditKind::Expired,
                now_unix_ms,
                None,
            );
            validate_registry_record(&expired)?;
            staged.push((record.enrollment.enrollment_id.clone(), expired));
        }
        for (enrollment_id, record) in staged {
            records.insert(enrollment_id, record);
        }
        Ok(result)
    }

    async fn enrollment_audit_events(
        &self,
    ) -> CentralResult<Vec<crate::AgentEnrollmentAuditEvent>> {
        let mut events = self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .filter_map(|record| record.decision_audit_event.clone())
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            (&left.tenant_id, &left.event_id).cmp(&(&right.tenant_id, &right.event_id))
        });
        Ok(events)
    }

    async fn enrollment_lifecycle_audit_events(
        &self,
        tenant_id: &TenantId,
    ) -> CentralResult<Vec<crate::AgentEnrollmentLifecycleAuditEvent>> {
        let mut events = self
            .records
            .lock()
            .map_err(lock_error)?
            .values()
            .filter(|record| &record.enrollment.tenant_id == tenant_id)
            .flat_map(|record| record.storage_enrollment.lifecycle_audit_events.clone())
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            (&left.occurred_at_unix_ms, &left.event_id)
                .cmp(&(&right.occurred_at_unix_ms, &right.event_id))
        });
        Ok(events)
    }

    async fn consume_bootstrap_status_signed_at(
        &self,
        enrollment_id: &AgentEnrollmentId,
        signed_at_unix_ms: UnixMillis,
    ) -> CentralResult<()> {
        let mut watermarks = self
            .bootstrap_status_watermarks
            .lock()
            .map_err(lock_error)?;
        if watermarks
            .get(enrollment_id)
            .is_some_and(|stored| signed_at_unix_ms.get() <= stored.get())
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Agent bootstrap status replay watermark changed",
            ));
        }
        watermarks.insert(enrollment_id.clone(), signed_at_unix_ms);
        Ok(())
    }

    async fn insert_or_load(
        &self,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryInsertOutcome> {
        validate_registry_insert(&record)?;
        let mut records = self.records.lock().map_err(lock_error)?;
        let enrollment_id = record.enrollment.enrollment_id.clone();
        if let Some(existing) = records.get(&enrollment_id) {
            return Ok(AgentRegistryInsertOutcome::Existing(existing.clone()));
        }
        if let Some(existing) = records.values().find(|existing| {
            existing.enrollment.tenant_id == record.enrollment.tenant_id
                && existing.enrollment.token_request_id == record.enrollment.token_request_id
        }) {
            return Ok(AgentRegistryInsertOutcome::Existing(existing.clone()));
        }
        if records
            .values()
            .any(|existing| identity_conflicts(existing, &record))
            || records
                .values()
                .any(|existing| volume_intent_conflicts(existing, &record))
            || records
                .values()
                .any(|existing| pvc_identity_conflicts(existing, &record))
        {
            return Err(CentralError::new(
                CentralErrorCode::VolumeOwnerConflict,
                "Agent identity or Tenant Volume already has an active enrollment",
            )
            .with_retryable(false));
        }
        if records
            .values()
            .any(|existing| bootstrap_request_identity_conflicts(existing, &record))
        {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentIdReused,
                "Agent bootstrap request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        if records
            .values()
            .any(|existing| decision_request_identity_conflicts(existing, &record))
        {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentDecisionConflict,
                "Agent decision request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        if records
            .values()
            .any(|existing| candidate_identity_conflicts(existing, &record))
        {
            return Err(CentralError::new(
                CentralErrorCode::AgentIdentityMismatch,
                "Agent candidate identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        records.insert(enrollment_id, record.clone());
        Ok(AgentRegistryInsertOutcome::Inserted(record))
    }

    async fn replace(
        &self,
        expected_resource_version: u64,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryRecord> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let enrollment_id = record.enrollment.enrollment_id.clone();
        let existing = records.get(&enrollment_id).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::EnrollmentNotFound,
                "Agent enrollment disappeared during update",
            )
        })?;
        validate_registry_record(&record)?;
        let next_resource_version =
            checked_next_registry_resource_version(expected_resource_version)?;
        if existing.resource_version.get() != expected_resource_version
            || record.resource_version.get() != next_resource_version
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Agent registry ResourceVersion changed",
            ));
        }
        ensure_immutable_registry_scope(existing, &record)?;
        validate_registry_replace_transition(existing, &record)?;
        if records.values().any(|other| {
            other.enrollment.enrollment_id != enrollment_id
                && bootstrap_request_identity_conflicts(other, &record)
        }) {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentIdReused,
                "Agent registry request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        if records.values().any(|other| {
            other.enrollment.enrollment_id != enrollment_id
                && decision_request_identity_conflicts(other, &record)
        }) {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentDecisionConflict,
                "Agent decision request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        if records.values().any(|other| {
            other.enrollment.enrollment_id != enrollment_id
                && candidate_identity_conflicts(other, &record)
        }) {
            return Err(CentralError::new(
                CentralErrorCode::AgentIdentityMismatch,
                "Agent candidate identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        records.insert(enrollment_id, record.clone());
        Ok(record)
    }

    async fn activate_replacement(
        &self,
        expected_previous_resource_version: u64,
        revoked: AgentRegistryRecord,
        expected_replacement_resource_version: u64,
        replacement: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryReplacementRecords> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let stored_previous = records
            .get(&revoked.enrollment.enrollment_id)
            .ok_or_else(|| missing("previous Agent enrollment disappeared during replacement"))?;
        let stored_replacement = records
            .get(&replacement.enrollment.enrollment_id)
            .ok_or_else(|| missing("replacement Agent enrollment disappeared during approval"))?;
        validate_registry_record(&revoked)?;
        validate_registry_record(&replacement)?;
        validate_registry_replacement_transition(
            stored_previous,
            expected_previous_resource_version,
            &revoked,
            stored_replacement,
            expected_replacement_resource_version,
            &replacement,
        )?;
        ensure_immutable_registry_scope(stored_previous, &revoked)?;
        ensure_immutable_registry_scope(stored_replacement, &replacement)?;
        if records.values().any(|other| {
            other.enrollment.enrollment_id != replacement.enrollment.enrollment_id
                && bootstrap_request_identity_conflicts(other, &replacement)
        }) {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentIdReused,
                "Agent registry request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        if records.values().any(|other| {
            other.enrollment.enrollment_id != replacement.enrollment.enrollment_id
                && decision_request_identity_conflicts(other, &replacement)
        }) {
            return Err(CentralError::new(
                CentralErrorCode::EnrollmentDecisionConflict,
                "Agent decision request identity is already bound to another enrollment",
            )
            .with_retryable(false));
        }
        records.insert(revoked.enrollment.enrollment_id.clone(), revoked.clone());
        records.insert(
            replacement.enrollment.enrollment_id.clone(),
            replacement.clone(),
        );
        Ok(AgentRegistryReplacementRecords {
            revoked,
            replacement,
        })
    }
}

fn identity_conflicts(existing: &AgentRegistryRecord, candidate: &AgentRegistryRecord) -> bool {
    existing.enrollment.reserved_agent_id == candidate.enrollment.reserved_agent_id
        || existing.enrollment.token_id == candidate.enrollment.token_id
        || (existing.enrollment.tenant_id == candidate.enrollment.tenant_id
            && existing.enrollment.token_request_id == candidate.enrollment.token_request_id)
        || existing.enrollment.bootstrap_token_digest == candidate.enrollment.bootstrap_token_digest
}

fn bootstrap_request_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id {
        return false;
    }
    existing.enrollment.bootstrap_request_id.is_some()
        && existing.enrollment.bootstrap_request_id == candidate.enrollment.bootstrap_request_id
}

fn decision_request_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id {
        return false;
    }
    existing
        .enrollment
        .decision_request
        .as_ref()
        .zip(candidate.enrollment.decision_request.as_ref())
        .is_some_and(|(left, right)| left.decision_request_id == right.decision_request_id)
}

fn candidate_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    existing
        .candidate
        .as_ref()
        .zip(candidate.candidate.as_ref())
        .is_some_and(|(left, right)| {
            left.installation_id == right.installation_id
                || left.public_key_fingerprint == right.public_key_fingerprint
        })
}

fn volume_intent_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id
        || existing.enrollment.storage_volume_id != candidate.enrollment.storage_volume_id
        || matches!(
            existing.enrollment.state,
            AgentEnrollmentState::Rejected
                | AgentEnrollmentState::Expired
                | AgentEnrollmentState::Revoked
        )
    {
        return false;
    }
    candidate.enrollment.replaces_enrollment_id.as_ref() != Some(&existing.enrollment.enrollment_id)
        || existing.enrollment.state != AgentEnrollmentState::Approved
}

fn pvc_identity_conflicts(existing: &AgentRegistryRecord, candidate: &AgentRegistryRecord) -> bool {
    if existing.enrollment.edge_cluster_id != candidate.enrollment.edge_cluster_id
        || existing.enrollment.pvc_identity_digest != candidate.enrollment.pvc_identity_digest
        || matches!(
            existing.enrollment.state,
            AgentEnrollmentState::Rejected
                | AgentEnrollmentState::Expired
                | AgentEnrollmentState::Revoked
        )
    {
        return false;
    }
    existing.enrollment.tenant_id != candidate.enrollment.tenant_id
        || existing.enrollment.storage_volume_id != candidate.enrollment.storage_volume_id
        || candidate.enrollment.replaces_enrollment_id.as_ref()
            != Some(&existing.enrollment.enrollment_id)
        || existing.enrollment.state != AgentEnrollmentState::Approved
}

fn pvc_binding(record: &AgentRegistryRecord) -> PvcVolumeBinding {
    PvcVolumeBinding {
        pvc_identity_digest: record.enrollment.pvc_identity_digest,
        tenant_id: record.enrollment.tenant_id.clone(),
        edge_cluster_id: record.enrollment.edge_cluster_id.clone(),
        storage_volume_id: record.enrollment.storage_volume_id.clone(),
    }
}

fn is_terminal(state: AgentEnrollmentState) -> bool {
    matches!(
        state,
        AgentEnrollmentState::Rejected
            | AgentEnrollmentState::Expired
            | AgentEnrollmentState::Revoked
    )
}

fn public_enrollment_created_at(record: &AgentRegistryRecord) -> UnixMillis {
    record
        .enrollment
        .bootstrapped_at_unix_ms
        .unwrap_or(record.enrollment.created_at_unix_ms)
}

fn missing(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::EnrollmentNotFound, message).with_retryable(false)
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> CentralError {
    CentralError::new(
        CentralErrorCode::Internal,
        "in-memory Agent registry lock poisoned",
    )
}
