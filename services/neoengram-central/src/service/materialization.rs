//! Object-level Commit materialization control path.
//!
//! This module is intentionally independent from the legacy whole-Commit replication service.
//! It only moves immutable metadata and scheduling decisions through Central; object bytes remain
//! exclusively on Agents and are transported through Gateway relay streams.

use std::collections::{BTreeMap, BTreeSet};

use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::materialization::{
    AvailabilityStatus, BatchManifest, CoverageGoal, CoverageState, DurabilityPolicy,
    MaterializationBatch, MaterializationBatchState, MaterializationBatchTicket,
    MaterializationJob, MaterializationJobKey, MaterializationJobState,
    MaterializationManifestSource, MaterializationObject, MaterializationObjectState,
    MaterializationSource, MaterializationTarget, NamespaceObjectSet, ObjectPlacement,
    ObjectReadLease, ObjectRef, SignedMaterializationBatchTicket, StagingLease, ViewReadiness,
    VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    object_read_lease_id, staging_lease_id, AgentId, ArtifactId, CommitObject, DecimalU64,
    Generation, MaterializationBatchId, MaterializationId, ObjectNamespaceId, ObjectSet,
    ObjectTicketId, OperationTask, PlacementGeneration, RequestId, StorageVolumeId, TaskAttemptId,
    TaskId, TaskIntent, TaskPurpose, TaskResourceKind, TaskResourceRole, TaskScope, TaskState,
    TenantId, UnixMillis,
};

use crate::dto::{
    CancelCommitMaterializationRequest, CancelCommitMaterializationResponse,
    CommitAvailabilityV2View, CreateCommitMaterializationRequest,
    CreateCommitMaterializationResponse, MaterializationView, MissingObjectView,
    QueryCommitAvailabilityV2Request, QueryCommitAvailabilityV2Response,
    QueryCommitCoverageRequest, QueryCommitCoverageResponse, QueryCommitMaterializationListRequest,
    QueryCommitMaterializationListResponse, QueryCommitMaterializationRequest,
    QueryCommitMaterializationResponse, RetryCommitMaterializationRequest,
    RetryCommitMaterializationResponse, VolumeCommitCoverageView,
};
use crate::error::{application_error, invalid_request, map_central_error};
use crate::identity::{AuthenticatedIdentity, Permission};
use crate::service::CatalogService;
use crate::{
    MaterializationPlan, MaterializationPlanInsertOutcome, MaterializationPlanReplacement,
    PlacementRepository,
};

const MAX_MATERIALIZATION_PAGE_SIZE: usize = 100;
/// A materialization is a durable multi-object job. Its parent deadline must cover Agent/Gateway
/// reconnects and source failover; individual signed tickets still use the command-signing TTL.
pub(crate) const MATERIALIZATION_PLAN_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;

/// A materialization reserves the configured volume copy reserve for staging.  A bounded second
/// reserve covers concurrent transfer metadata/checkpoint growth; it is derived from the bytes
/// being materialized so a threshold goal does not reserve the whole Commit.
fn materialization_capacity_requirement(
    missing_bytes: u64,
    staging_reserve: u64,
) -> Result<u64, Error> {
    let concurrency_reserve = missing_bytes.min(staging_reserve);
    missing_bytes
        .checked_add(staging_reserve)
        .and_then(|value| value.checked_add(concurrency_reserve))
        .ok_or_else(|| invalid_request("materialization capacity requirement overflow"))
}

fn not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_namespace(value: String) -> Result<ObjectNamespaceId, Error> {
    ObjectNamespaceId::new(value)
        .map_err(|error| invalid_request(format!("object_namespace_id: {error}")))
}

fn parse_volume(value: String, field: &'static str) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value).map_err(|error| invalid_request(format!("{field}: {error}")))
}

fn parse_commit(value: String) -> Result<ContentDigest, Error> {
    value
        .parse()
        .map_err(|_| invalid_request("commit_id must be a 64-character digest"))
}

fn parse_materialization_id(value: String) -> Result<MaterializationId, Error> {
    MaterializationId::new(value)
        .map_err(|error| invalid_request(format!("materialization_id: {error}")))
}

fn parse_generation(value: String, field: &'static str) -> Result<Generation, Error> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return Err(invalid_request(format!(
            "{field} must be canonical unsigned integer"
        )));
    }
    value
        .parse::<u64>()
        .map(Generation::new)
        .map_err(|_| invalid_request(format!("{field} must be canonical unsigned integer")))
}

fn materialization_id_for_key(key: &MaterializationJobKey) -> Result<MaterializationId, Error> {
    let digest = neoengram_domain::protocol::jcs_blake3(key)
        .map_err(|error| invalid_request(format!("materialization key: {error}")))?;
    MaterializationId::new(format!("materialization-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("materialization_id: {error}")))
}

/// Derives an execution-specific materialization identity. Copy plans retain the stable key
/// identity, while repair plans include the observation fence so a later corruption observation
/// cannot be deduplicated against an earlier successful repair.
fn materialization_id_for_request(
    key: &MaterializationJobKey,
    purpose: TaskPurpose,
    repair_observation_digest: Option<&str>,
    target_placement_generation: Option<&str>,
) -> Result<MaterializationId, Error> {
    if purpose == TaskPurpose::Copy {
        return materialization_id_for_key(key);
    }
    let fence = repair_observation_digest
        .or(target_placement_generation)
        .ok_or_else(|| {
            invalid_request(
                "repair requires repair_observation_digest or target_placement_generation",
            )
        })?;
    let key_digest = neoengram_domain::protocol::jcs_blake3(key)
        .map_err(|error| invalid_request(format!("materialization key: {error}")))?;
    let material = format!("repair\0{}\0{}", key_digest, fence);
    let digest = ContentDigest::hash(material.as_bytes());
    MaterializationId::new(format!("materialization-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("materialization_id: {error}")))
}

fn task_attempt_generation(value: &TaskAttemptId) -> Generation {
    value
        .as_str()
        .rsplit_once("-attempt-")
        .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
        .map(Generation::new)
        .unwrap_or_else(|| Generation::new(1))
}

fn state_name(state: MaterializationJobState) -> &'static str {
    match state {
        MaterializationJobState::Queued => "queued",
        MaterializationJobState::Planning => "planning",
        MaterializationJobState::WaitingForSources => "waiting_for_sources",
        MaterializationJobState::Materializing => "materializing",
        MaterializationJobState::Verifying => "verifying",
        MaterializationJobState::Complete => "complete",
        MaterializationJobState::Stalled => "stalled",
        MaterializationJobState::Failed => "failed",
        MaterializationJobState::Cancelled => "cancelled",
    }
}

fn coverage_state_name(state: CoverageState) -> &'static str {
    match state {
        CoverageState::Partial => "partial",
        CoverageState::Complete => "complete",
        CoverageState::Retiring => "retiring",
        CoverageState::Deleted => "deleted",
    }
}

/// Availability has a deliberately smaller public state set than the durable coverage view.
/// Retiring/deleted summaries must never leak through the availability contract, where they are
/// represented as unavailable instead.
fn availability_coverage_state_name(state: CoverageState) -> &'static str {
    match state {
        CoverageState::Partial => "partial",
        CoverageState::Complete => "complete",
        CoverageState::Retiring | CoverageState::Deleted => "unavailable",
    }
}

fn availability_name(status: AvailabilityStatus) -> &'static str {
    match status {
        AvailabilityStatus::Available => "available",
        AvailabilityStatus::Degraded => "degraded",
        AvailabilityStatus::Unavailable => "unavailable",
        AvailabilityStatus::Unknown => "unknown",
    }
}

/// Computes source-serving availability from the distinct object IDs that have a healthy route.
///
/// A namespace can have multiple verified Placement rows for one object (for example, copies on
/// different Volumes or historical rows that survived a retry).  Counting rows therefore makes
/// a single routed object look like a complete source set.  The planner and this read path both
/// operate on object IDs, so the status must use the deduplicated set as well.
fn source_serving_status(
    object_count: u64,
    has_placements: bool,
    _objects_with_copy: u64,
    served_object_ids: &BTreeSet<ObjectId>,
) -> AvailabilityStatus {
    let served_count = served_object_ids.len() as u64;
    if object_count == 0 {
        AvailabilityStatus::Available
    } else if !has_placements {
        AvailabilityStatus::Unavailable
    } else if served_count >= object_count {
        AvailabilityStatus::Available
    } else if served_count == 0 {
        AvailabilityStatus::Unavailable
    } else {
        AvailabilityStatus::Degraded
    }
}

/// Evaluates durability for every object in the Commit ObjectSet.
///
/// Durability is an object-level invariant: a complete Volume is only one possible way to cover
/// a Commit and says nothing about the requested replica count or failure-domain spread.  Each
/// object's verified placements are therefore checked independently with the namespace policy.
fn objects_satisfy_durability_policy(
    policy: &DurabilityPolicy,
    object_set: &NamespaceObjectSet,
    placements: &[ObjectPlacement],
) -> bool {
    object_set.objects.iter().all(|object| {
        policy.satisfied_by(placements.iter().filter(|placement| {
            placement.tenant_id == object_set.tenant_id
                && placement.object_namespace_id == object_set.object_namespace_id
                && placement.matches_ref(object)
        }))
    })
}

fn durability_status(
    content_presence: AvailabilityStatus,
    policy: &DurabilityPolicy,
    object_set: &NamespaceObjectSet,
    placements: &[ObjectPlacement],
) -> &'static str {
    if content_presence == AvailabilityStatus::Unavailable {
        "unavailable"
    } else if objects_satisfy_durability_policy(policy, object_set, placements) {
        "satisfied"
    } else {
        "under_replicated"
    }
}

fn readiness_name(readiness: ViewReadiness) -> &'static str {
    match readiness {
        ViewReadiness::Ready => "ready",
        ViewReadiness::NotReady => "not_ready",
    }
}

fn issue(
    code: &str,
    message: impl Into<String>,
    retryable: bool,
    now: UnixMillis,
) -> crate::dto::ResourceIssueSummary {
    crate::dto::ResourceIssueSummary {
        code: code.to_owned(),
        message: message.into(),
        retryable,
        occurred_at_unix_ms: Some(now.to_string()),
    }
}

fn view(job: &MaterializationJob, object_set: &NamespaceObjectSet) -> MaterializationView {
    MaterializationView {
        materialization_id: job.materialization_id.to_string(),
        tenant_id: job.key.tenant_id.to_string(),
        artifact_id: Some(job.artifact_id.to_string()),
        object_namespace_id: job.key.object_namespace_id.to_string(),
        commit_id: job.key.commit_id.to_string(),
        target_storage_volume_id: job.key.target_storage_volume_id.to_string(),
        purpose: job.key.purpose,
        plan_revision: job.plan_revision.to_string(),
        coverage_goal: job.key.coverage_goal,
        state: state_name(job.state).to_owned(),
        object_set_digest: object_set.object_set_digest.to_string(),
        verified_objects: job.verified_object_count.to_string(),
        total_objects: job.object_count.to_string(),
        verified_bytes: job.verified_bytes.to_string(),
        total_bytes: job.total_bytes.to_string(),
        missing_objects: job.missing_object_count.to_string(),
        missing_bytes: job.missing_bytes.to_string(),
        source_count: job.source_count.to_string(),
        issue: job.issue.as_ref().map(|message| {
            issue(
                "MATERIALIZATION_ISSUE",
                message.clone(),
                !job.state.terminal(),
                job.updated_at_unix_ms,
            )
        }),
    }
}

fn coverage_view(coverage: &VolumeCommitCoverage) -> VolumeCommitCoverageView {
    VolumeCommitCoverageView {
        object_namespace_id: coverage.object_namespace_id.to_string(),
        commit_id: coverage.commit_id.to_string(),
        storage_volume_id: coverage.storage_volume_id.to_string(),
        placement_generation: coverage.placement_generation.to_string(),
        object_set_digest: coverage.object_set_digest.to_string(),
        total_objects: coverage.object_count.to_string(),
        verified_objects: coverage.verified_object_count.to_string(),
        total_bytes: coverage.total_bytes.to_string(),
        verified_bytes: coverage.verified_bytes.to_string(),
        missing_objects: (coverage
            .object_count
            .get()
            .saturating_sub(coverage.verified_object_count.get()))
        .to_string(),
        missing_bytes: (coverage
            .total_bytes
            .get()
            .saturating_sub(coverage.verified_bytes.get()))
        .to_string(),
        state: coverage_state_name(coverage.state).to_owned(),
    }
}

#[derive(Debug, Clone)]
struct RoutedPlacement {
    placement: ObjectPlacement,
    source: MaterializationSource,
}

/// A planner source group is a single fenced transport route, not merely a Volume. A Volume can
/// have more than one live owner over time (or multiple route generations while reconnecting).
/// The per-object Placement identity is carried in the signed manifest, so objects on one route
/// can share a Batch without losing their individual durable evidence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceRouteKey {
    storage_volume_id: StorageVolumeId,
    placement_generation: neoengram_domain::protocol::PlacementGeneration,
    agent_id: AgentId,
    edge_cluster_id: neoengram_domain::protocol::EdgeClusterId,
    gateway_pool_id: neoengram_domain::protocol::GatewayPoolId,
    session_generation: neoengram_domain::protocol::SessionGeneration,
    mount_generation: neoengram_domain::protocol::MountGeneration,
    route_generation: neoengram_domain::protocol::RouteGeneration,
}

impl SourceRouteKey {
    fn from_source(source: &MaterializationSource) -> Option<Self> {
        Some(Self {
            storage_volume_id: source.storage_volume_id.clone()?,
            placement_generation: source.placement_generation,
            agent_id: source.agent_id.clone(),
            edge_cluster_id: source.edge_cluster_id.clone(),
            gateway_pool_id: source.gateway_pool_id.clone(),
            session_generation: source.session_generation,
            mount_generation: source.mount_generation,
            route_generation: source.route_generation,
        })
    }

    fn batch_suffix(&self) -> String {
        // Keep the public identifier bounded even when deployment IDs use their maximum wire
        // length. The full route tuple is still carried and signed in MaterializationSource.
        let digest = blake3::hash(
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}",
                self.storage_volume_id,
                self.placement_generation,
                self.agent_id,
                self.edge_cluster_id,
                self.gateway_pool_id,
                self.session_generation,
                self.mount_generation,
                self.route_generation,
            )
            .as_bytes(),
        );
        digest.to_hex()[..24].to_owned()
    }
}

impl CatalogService {
    fn legacy_object_set(object_set: &NamespaceObjectSet) -> Result<ObjectSet, Error> {
        ObjectSet::new(
            object_set
                .objects
                .iter()
                .map(|object| {
                    CommitObject::new(
                        object.object_id,
                        object.size.get(),
                        object.encoding,
                        object.ordinal.get(),
                    )
                })
                .collect(),
        )
        .map_err(|error| invalid_request(format!("commit object set: {error}")))
    }

    async fn materialization_object_set(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
        commit_digest: ContentDigest,
        namespace: &ObjectNamespaceId,
    ) -> Result<NamespaceObjectSet, Error> {
        let precommits = self.precommits.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_unavailable",
                "PRECOMMIT_UNAVAILABLE",
                "Commit authority is not configured",
                true,
            )
        })?;
        let commit = precommits
            .get_commit(
                tenant_id,
                project_id,
                artifact_id,
                CommitId::from_digest(commit_digest),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit"))?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let object_set = repository
            .get_commit_object_set(tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        if commit.object_set_digest != object_set.object_set.object_set_digest {
            return Err(application_error(
                ErrorCategory::Conflict,
                "commit_object_set_mismatch",
                "COMMIT_OBJECT_SET_MISMATCH",
                "Commit authority and Placement authority disagree on the ObjectSet digest",
                false,
            ));
        }
        super::placement::namespace_object_set(tenant_id, namespace, commit_digest, &object_set)
    }

    /// Collects every namespace-scoped Placement for a Commit.  The authority API is deliberately
    /// object keyed, so this loop is also the boundary that prevents one object namespace from
    /// leaking into another.
    async fn v2_placements(
        &self,
        tenant_id: &TenantId,
        namespace: &ObjectNamespaceId,
        object_set: &NamespaceObjectSet,
    ) -> Result<Vec<ObjectPlacement>, Error> {
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let mut placements = Vec::new();
        for object in &object_set.objects {
            for placement in repository
                .object_placements_v2(tenant_id, namespace, &object.object_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .filter(|placement| {
                    placement.readable()
                        && placement.tenant_id == *tenant_id
                        && placement.object_namespace_id == *namespace
                        && placement.matches_ref(object)
                })
            {
                let unhealthy = repository
                    .latest_placement_health(
                        tenant_id,
                        namespace,
                        &placement.placement_id,
                        placement.placement_generation,
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some_and(|observation| {
                        matches!(
                            observation.state,
                            neoengram_domain::protocol::materialization::PlacementHealthState::Missing
                                | neoengram_domain::protocol::materialization::PlacementHealthState::Corrupt
                        )
                    });
                if !unhealthy {
                    placements.push(placement);
                }
            }
        }
        placements.sort_by_key(|placement| {
            (
                placement.storage_volume_id.clone(),
                placement.placement_generation,
                placement.placement_id.clone(),
            )
        });
        Ok(placements)
    }

    async fn target_coverage(
        &self,
        tenant_id: &TenantId,
        namespace: &ObjectNamespaceId,
        commit_id: CommitId,
        volume_id: &StorageVolumeId,
        object_set: &NamespaceObjectSet,
        placements: &[ObjectPlacement],
    ) -> Result<VolumeCommitCoverage, Error> {
        // The current owner fence is authoritative for an empty target and for targets whose
        // previous owner left verified objects behind.  Falling back to the newest observed
        // placement keeps availability queries useful when the route is temporarily unavailable,
        // but a healthy route must never regress to generation 1.
        let observed_generation = placements
            .iter()
            .filter(|placement| placement.storage_volume_id.as_ref() == Some(volume_id))
            .map(|placement| placement.placement_generation)
            .max()
            .unwrap_or_else(|| PlacementGeneration::new(1));
        let generation = self
            .route_for_volume(tenant_id, volume_id, "target")
            .await
            .map(|route| route.placement_generation)
            .unwrap_or(observed_generation);
        let legacy = Self::legacy_object_set(object_set)?;
        VolumeCommitCoverage::from_placements(
            tenant_id.clone(),
            namespace.clone(),
            commit_id,
            volume_id.clone(),
            generation,
            &legacy,
            placements,
        )
        .map_err(|error| invalid_request(format!("coverage: {error}")))
    }

    async fn route_for_volume(
        &self,
        tenant_id: &TenantId,
        volume_id: &StorageVolumeId,
        role: &str,
    ) -> Option<super::placement::ReadyReplicationRoute> {
        let volume = self
            .repository
            .get_storage_volume(tenant_id, volume_id)
            .await
            .ok()
            .flatten()?;
        self.ready_materialization_route(tenant_id, &volume, role)
            .await
            .ok()
    }

    async fn routed_candidates(
        &self,
        tenant_id: &TenantId,
        target_volume_id: Option<&StorageVolumeId>,
        placements: &[ObjectPlacement],
    ) -> BTreeMap<SourceRouteKey, Vec<RoutedPlacement>> {
        let mut grouped = BTreeMap::<SourceRouteKey, Vec<RoutedPlacement>>::new();
        for placement in placements {
            let Some(volume_id) = placement.storage_volume_id.clone() else {
                continue;
            };
            if target_volume_id.is_some_and(|target| volume_id == *target) {
                continue;
            }
            let Some(route) = self.route_for_volume(tenant_id, &volume_id, "source").await else {
                continue;
            };
            // Placement generation is the Volume-owner fence. A live route on the same Volume
            // must not make evidence from a previous owner generation readable: that Agent may no
            // longer control the bytes (or the marker may have been replaced).
            if placement.placement_generation != route.placement_generation {
                continue;
            }
            let source = MaterializationSource {
                placement_id: placement.placement_id.clone(),
                tenant_id: placement.tenant_id.clone(),
                object_namespace_id: placement.object_namespace_id.clone(),
                storage_volume_id: Some(volume_id.clone()),
                archive_id: placement.archive_id.clone(),
                agent_id: route.agent_id,
                edge_cluster_id: route.edge_cluster_id,
                gateway_pool_id: route.gateway_pool_id,
                placement_generation: placement.placement_generation,
                session_generation: route.session_generation,
                mount_generation: route.mount_generation,
                route_generation: route.route_generation,
            };
            let Some(route_key) = SourceRouteKey::from_source(&source) else {
                continue;
            };
            grouped.entry(route_key).or_default().push(RoutedPlacement {
                source,
                placement: placement.clone(),
            });
        }
        for candidates in grouped.values_mut() {
            candidates.sort_by_key(|candidate| {
                (
                    candidate.placement.object_id,
                    candidate.placement.placement_id.clone(),
                )
            });
        }
        grouped
    }

    /// Builds the two protection leases for every object in a planned batch.  The source lease
    /// uses the exact Placement chosen for that object; a batch may contain several placements on
    /// the same source Volume. Lease IDs are deterministic, so reconnect/replay does not create a
    /// second GC root. The leases are returned as a value so initial plan creation can publish
    /// them atomically with the parent Job and child rows.
    fn build_batch_leases(
        job: &MaterializationJob,
        batch: &MaterializationBatch,
        tasks: &[MaterializationObject],
        placements: &[ObjectPlacement],
    ) -> Result<(Vec<ObjectReadLease>, Vec<StagingLease>), Error> {
        let mut read_leases = Vec::new();
        let mut staging_leases = Vec::new();
        for task in tasks.iter().filter(|task| {
            task.current_batch_id.as_ref() == Some(&batch.batch_id) && !task.complete()
        }) {
            let source_ids = task
                .primary_source
                .iter()
                .chain(task.fallback_sources.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            if source_ids.is_empty() {
                return Err(invalid_request(format!(
                    "materialization object {} has no selected source",
                    task.object.object_id
                )));
            }
            // Protect every source selected by Central for this attempt.  A primary route can
            // fail after planning, and failover must be able to use a fallback without another
            // lease acquisition round.  Otherwise GC could reclaim the fallback placement.
            for source_id in source_ids {
                let source = placements
                    .iter()
                    .find(|placement| {
                        placement.placement_id == source_id
                            && placement.tenant_id == job.key.tenant_id
                            && placement.object_namespace_id == job.key.object_namespace_id
                            && placement.matches_ref(&task.object)
                            && placement.readable()
                    })
                    .ok_or_else(|| {
                        invalid_request(format!(
                            "selected source Placement {} for object {} is unavailable",
                            source_id, task.object.object_id
                        ))
                    })?;
                let read_lease_id = object_read_lease_id(
                    &job.materialization_id,
                    &batch.batch_id,
                    batch.plan_revision,
                    batch.batch_attempt,
                    &job.key.object_namespace_id,
                    task.object.object_id,
                    &source.placement_id,
                )
                .map_err(|error| invalid_request(format!("object read lease ID: {error}")))?;
                read_leases.push(ObjectReadLease {
                    lease_id: read_lease_id,
                    materialization_id: job.materialization_id.clone(),
                    batch_id: batch.batch_id.clone(),
                    plan_revision: batch.plan_revision,
                    tenant_id: job.key.tenant_id.clone(),
                    object_namespace_id: job.key.object_namespace_id.clone(),
                    object_id: task.object.object_id,
                    placement_id: source.placement_id.clone(),
                    placement_generation: source.placement_generation,
                    expires_at_unix_ms: batch.deadline_unix_ms,
                    state:
                        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
                });
            }

            let staging_lease_id = staging_lease_id(
                &job.materialization_id,
                batch.plan_revision,
                &job.key.object_namespace_id,
                task.object.object_id,
            )
            .map_err(|error| invalid_request(format!("staging lease ID: {error}")))?;
            staging_leases.push(StagingLease {
                lease_id: staging_lease_id,
                materialization_id: job.materialization_id.clone(),
                plan_revision: batch.plan_revision,
                tenant_id: job.key.tenant_id.clone(),
                object_namespace_id: job.key.object_namespace_id.clone(),
                object_id: task.object.object_id,
                target_storage_volume_id: batch.target.storage_volume_id.clone(),
                target_placement_generation: batch.target.placement_generation,
                staging_key: task.staging_key.clone(),
                expires_at_unix_ms: batch.deadline_unix_ms,
                state:
                    neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
            });
        }
        Ok((read_leases, staging_leases))
    }

    /// Releases all protection leases owned by one batch.  Release is deliberately idempotent:
    /// a receipt, failure report, retry and cancellation can race without reviving or deleting a
    /// newer plan's lease.
    async fn release_batch_leases(
        repository: &dyn PlacementRepository,
        job: &MaterializationJob,
        batch: &MaterializationBatch,
        tasks: &[MaterializationObject],
    ) -> Result<(), Error> {
        for object_id in &batch.object_ids {
            let Some(task) = tasks.iter().find(|task| {
                task.object.object_namespace_id == job.key.object_namespace_id
                    && task.object.object_id == *object_id
            }) else {
                continue;
            };
            let source_ids = task
                .primary_source
                .iter()
                .chain(task.fallback_sources.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            for source_id in source_ids {
                let lease_id = object_read_lease_id(
                    &job.materialization_id,
                    &batch.batch_id,
                    batch.plan_revision,
                    batch.batch_attempt,
                    &job.key.object_namespace_id,
                    *object_id,
                    &source_id,
                )
                .map_err(|error| invalid_request(format!("object read lease ID: {error}")))?;
                repository
                    .release_object_read_lease(
                        &job.key.tenant_id,
                        &job.key.object_namespace_id,
                        &lease_id,
                    )
                    .await
                    .map_err(map_central_error)?;
            }
            let lease_id = staging_lease_id(
                &job.materialization_id,
                batch.plan_revision,
                &job.key.object_namespace_id,
                *object_id,
            )
            .map_err(|error| invalid_request(format!("staging lease ID: {error}")))?;
            repository
                .release_staging_lease(&job.key.tenant_id, &job.key.object_namespace_id, &lease_id)
                .await
                .map_err(map_central_error)?;
        }
        Ok(())
    }

    /// Returns the deterministic subset of missing objects needed to satisfy a target goal.
    /// Complete coverage keeps every missing object in Commit order. Threshold goals first use
    /// currently routable objects, then source-backed objects whose route is waiting, and finally
    /// source-less objects only when the threshold cannot otherwise be met. This lets a later
    /// available object satisfy a threshold without incorrectly failing on an unavailable prefix,
    /// while preserving a deterministic Commit-ordinal tie-break within each source tier.
    fn required_missing_refs(
        object_set: &NamespaceObjectSet,
        target_present: &BTreeSet<ObjectId>,
        goal: CoverageGoal,
        verified_bytes: u64,
        preferred_source_ids: &BTreeSet<ObjectId>,
        fallback_source_ids: &BTreeSet<ObjectId>,
    ) -> Vec<ObjectRef> {
        let total_objects = object_set.object_count() as u64;
        let total_bytes = object_set
            .objects
            .iter()
            .map(|object| object.size.get())
            .fold(0_u64, u64::saturating_add);
        let verified_objects = target_present.len() as u64;
        if goal.satisfied_by(verified_objects, verified_bytes, total_objects, total_bytes) {
            return Vec::new();
        }
        let additional_objects = match goal {
            CoverageGoal::Complete => total_objects.saturating_sub(verified_objects),
            CoverageGoal::ObjectCount(required) => required
                .get()
                .saturating_sub(verified_objects)
                .min(total_objects.saturating_sub(verified_objects)),
            CoverageGoal::ByteCount(_) => u64::MAX,
        };
        let additional_bytes = match goal {
            CoverageGoal::ByteCount(required) => required.get().saturating_sub(verified_bytes),
            _ => 0,
        };
        // Threshold goals do not require a particular Commit ordinal. Prefer objects with a
        // currently routable source, then fall back to objects that have a Verified Placement but
        // no route yet (so the Job can wait for that route), and only then include source-less
        // objects when the threshold cannot otherwise be met. This prevents an unavailable early
        // object from making a satisfiable ObjectCount/ByteCount request fail prematurely.
        let mut candidates = Vec::with_capacity(object_set.objects.len());
        let mut seen = BTreeSet::new();
        if matches!(goal, CoverageGoal::Complete) {
            candidates.extend(
                object_set
                    .objects
                    .iter()
                    .filter(|object| !target_present.contains(&object.object_id))
                    .cloned(),
            );
        } else {
            for source_ids in [preferred_source_ids, fallback_source_ids] {
                for object in &object_set.objects {
                    if target_present.contains(&object.object_id)
                        || !source_ids.contains(&object.object_id)
                        || !seen.insert(object.object_id)
                    {
                        continue;
                    }
                    candidates.push(object.clone());
                }
            }
            for object in &object_set.objects {
                if !target_present.contains(&object.object_id) && seen.insert(object.object_id) {
                    candidates.push(object.clone());
                }
            }
        }
        let mut selected = Vec::new();
        let mut bytes = 0_u64;
        for object in candidates {
            let count_satisfied = match goal {
                CoverageGoal::ByteCount(_) => false,
                _ => selected.len() as u64 >= additional_objects,
            };
            let bytes_satisfied = match goal {
                CoverageGoal::ByteCount(_) => bytes >= additional_bytes,
                _ => false,
            };
            if count_satisfied || bytes_satisfied {
                break;
            }
            bytes = bytes.saturating_add(object.size.get());
            selected.push(object.clone());
        }
        selected
    }

    /// Deterministic greedy set-cover: each round selects the healthy source covering the most
    /// remaining objects, then assigns those objects to that source. Ties are lexical by Volume
    /// and Placement ID, making retries produce the same plan revision and batch partition.
    #[allow(clippy::type_complexity)]
    fn choose_sources(
        object_set: &NamespaceObjectSet,
        grouped: &BTreeMap<SourceRouteKey, Vec<RoutedPlacement>>,
        required_object_ids: &BTreeSet<ObjectId>,
    ) -> (
        BTreeMap<SourceRouteKey, Vec<(ObjectRef, RoutedPlacement)>>,
        BTreeSet<ObjectId>,
    ) {
        let mut refs = object_set
            .objects
            .iter()
            .filter(|object| required_object_ids.contains(&object.object_id))
            .map(Clone::clone)
            .collect::<Vec<_>>();
        refs.sort_by_key(|object| object.ordinal);
        let mut remaining = refs
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let mut selected = BTreeMap::<SourceRouteKey, Vec<(ObjectRef, RoutedPlacement)>>::new();
        while !remaining.is_empty() {
            let mut best: Option<(&SourceRouteKey, usize)> = None;
            for (route_key, candidates) in grouped {
                // A route can have historical generations or duplicate placement rows for the
                // same object. Set-cover scores distinct remaining object IDs, otherwise one
                // duplicated placement can make an otherwise smaller source win the plan.
                let count = candidates
                    .iter()
                    .filter(|candidate| remaining.contains(&candidate.placement.object_id))
                    .map(|candidate| candidate.placement.object_id)
                    .collect::<BTreeSet<_>>()
                    .len();
                if count == 0 {
                    continue;
                }
                if best.is_none_or(|(best_route, best_count)| {
                    count > best_count || (count == best_count && route_key < best_route)
                }) {
                    best = Some((route_key, count));
                }
            }
            let Some((route_key, _)) = best else { break };
            let candidates = grouped
                .get(route_key)
                .expect("best source route is present");
            let eligible = refs
                .iter()
                .filter(|object| remaining.contains(&object.object_id))
                .cloned()
                .collect::<Vec<_>>();
            for object in eligible {
                let Some(candidate) = candidates
                    .iter()
                    .filter(|candidate| candidate.placement.object_id == object.object_id)
                    .min_by_key(|candidate| {
                        (
                            candidate.placement.placement_generation,
                            candidate.placement.placement_id.clone(),
                        )
                    })
                else {
                    continue;
                };
                remaining.remove(&object.object_id);
                selected
                    .entry(route_key.clone())
                    .or_default()
                    .push((object, candidate.clone()));
            }
        }
        (selected, remaining)
    }

    /// Rebuilds one retry plan from the current namespace-scoped Placement evidence. Existing
    /// object tasks are supplied by the caller so their durable staging offsets can be carried
    /// into the next plan revision; source/route selection is always recomputed.
    async fn retry_plan(
        &self,
        current: &MaterializationJob,
        old_objects: &[MaterializationObject],
        object_set: &NamespaceObjectSet,
        plan_revision: Generation,
        now: UnixMillis,
    ) -> Result<
        (
            MaterializationJob,
            Vec<MaterializationObject>,
            Vec<MaterializationBatch>,
            VolumeCommitCoverage,
        ),
        Error,
    > {
        let tenant_id = &current.key.tenant_id;
        let namespace = &current.key.object_namespace_id;
        let target_volume_id = &current.key.target_storage_volume_id;
        let placements = self.v2_placements(tenant_id, namespace, object_set).await?;
        let coverage = self
            .target_coverage(
                tenant_id,
                namespace,
                current.key.commit_id,
                target_volume_id,
                object_set,
                &placements,
            )
            .await?;
        let target_present = object_set
            .objects
            .iter()
            .filter(|object| {
                placements.iter().any(|placement| {
                    placement.storage_volume_id.as_ref() == Some(target_volume_id)
                        && placement.placement_generation == coverage.placement_generation
                        && placement.readable()
                        && placement.matches_ref(object)
                })
            })
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let total_objects = object_set.objects.len() as u64;
        let total_bytes = object_set
            .total_bytes()
            .map_err(|error| invalid_request(format!("object total size: {error}")))?;
        let verified_objects = target_present.len() as u64;
        let verified_bytes = object_set
            .objects
            .iter()
            .filter(|object| target_present.contains(&object.object_id))
            .map(|object| object.size.get())
            .sum::<u64>();
        let missing_objects = total_objects.saturating_sub(verified_objects);
        let missing_bytes = total_bytes.saturating_sub(verified_bytes);
        current
            .key
            .coverage_goal
            .validate_against_totals(total_objects, total_bytes)
            .map_err(|error| invalid_request(format!("coverage_goal: {error}")))?;
        let routed = self
            .routed_candidates(tenant_id, Some(target_volume_id), &placements)
            .await;
        let routed_object_ids = routed
            .values()
            .flat_map(|candidates| {
                candidates
                    .iter()
                    .map(|candidate| candidate.placement.object_id)
            })
            .collect::<BTreeSet<_>>();
        let verified_source_ids = object_set
            .objects
            .iter()
            .filter(|object| {
                placements.iter().any(|placement| {
                    placement.storage_volume_id.as_ref() != Some(target_volume_id)
                        && placement.readable()
                        && placement.matches_ref(object)
                })
            })
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let required_refs = Self::required_missing_refs(
            object_set,
            &target_present,
            current.key.coverage_goal,
            verified_bytes,
            &routed_object_ids,
            &verified_source_ids,
        );
        let required_object_ids = required_refs
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let (selected, unresolved) =
            Self::choose_sources(object_set, &routed, &required_object_ids);
        let no_verified_source = required_refs.iter().any(|object| {
            !placements.iter().any(|placement| {
                placement.storage_volume_id.as_ref() != Some(target_volume_id)
                    && placement.readable()
                    && placement.matches_ref(object)
            })
        });
        let target_route = self
            .route_for_volume(tenant_id, target_volume_id, "target")
            .await;
        let selected_all_missing =
            unresolved.is_empty() && selected.values().flatten().count() == required_refs.len();
        let goal_satisfied = current.key.coverage_goal.satisfied_by(
            verified_objects,
            verified_bytes,
            total_objects,
            total_bytes,
        );
        let state = if goal_satisfied {
            MaterializationJobState::Complete
        } else if current.state == MaterializationJobState::Complete {
            // A health-triggered retry reopens a previously completed Job through its explicit
            // planning phase. Batches may already be queued; the next retry can advance this
            // phase once source or target routes become available.
            MaterializationJobState::Planning
        } else if no_verified_source {
            MaterializationJobState::Failed
        } else if target_route.is_none() || !selected_all_missing {
            MaterializationJobState::WaitingForSources
        } else {
            MaterializationJobState::Materializing
        };
        let issue_text = if state == MaterializationJobState::Failed {
            Some("one or more Commit objects have no Verified source Placement".to_owned())
        } else if state == MaterializationJobState::WaitingForSources {
            Some(if target_route.is_none() {
                "target StorageVolume has no healthy Agent/Gateway route".to_owned()
            } else {
                "Verified source Placement exists but no healthy route is available".to_owned()
            })
        } else {
            None
        };
        let deadline = now
            .get()
            .checked_add(MATERIALIZATION_PLAN_DEADLINE_MS)
            .ok_or_else(|| invalid_request("materialization deadline overflowed"))?;
        let mut next_job = current.clone();
        next_job.plan_revision = plan_revision;
        next_job.state = state;
        next_job.object_count = DecimalU64::new(total_objects);
        next_job.total_bytes = DecimalU64::new(total_bytes);
        next_job.verified_object_count = DecimalU64::new(verified_objects);
        next_job.verified_bytes = DecimalU64::new(verified_bytes);
        next_job.missing_object_count = DecimalU64::new(missing_objects);
        next_job.missing_bytes = DecimalU64::new(missing_bytes);
        next_job.source_count = DecimalU64::new(selected.len() as u64);
        next_job.updated_at_unix_ms = now;
        next_job.deadline_unix_ms = UnixMillis::new(deadline);
        next_job.issue = issue_text;

        let old_by_id = old_objects
            .iter()
            .map(|object| (object.object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        let routed_placement_ids = routed
            .values()
            .flat_map(|candidates| {
                candidates
                    .iter()
                    .map(|candidate| candidate.placement.placement_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut object_tasks = Vec::with_capacity(object_set.objects.len());
        for object in &object_set.objects {
            let previous = old_by_id.get(&object.object_id).copied();
            let mut task = MaterializationObject::new(
                current.materialization_id.clone(),
                object.clone(),
                plan_revision,
            );
            // A target Placement that failed a later integrity scrub is no longer a valid
            // checkpoint. Its published object may have been removed while the stable staging
            // key was retained, so restart that object from byte zero; ordinary in-flight tasks
            // keep their durable checkpoint across source/route failover.
            task.confirmed_offset = previous
                .filter(|object| {
                    !object.complete() || target_present.contains(&object.object.object_id)
                })
                .map(|object| object.confirmed_offset.get().min(object.object.size.get()))
                .map(DecimalU64::new)
                .unwrap_or_else(|| DecimalU64::new(0));
            task.attempt = Generation::new(
                previous
                    .map(|object| object.attempt.get().saturating_add(1))
                    .unwrap_or(1),
            );
            if target_present.contains(&object.object_id) {
                task.state = MaterializationObjectState::AlreadyPresent;
                task.confirmed_offset = object.size;
            } else if let Some((_, candidate)) = selected
                .values()
                .flatten()
                .find(|(reference, _)| reference.object_id == object.object_id)
            {
                task.state = MaterializationObjectState::Reserved;
                task.primary_source = Some(candidate.placement.placement_id.clone());
                task.fallback_sources = placements
                    .iter()
                    .filter(|placement| {
                        placement.storage_volume_id.as_ref() != Some(target_volume_id)
                            && placement.readable()
                            && placement.matches_ref(object)
                            && routed_placement_ids.contains(&placement.placement_id)
                            && placement.placement_id != candidate.placement.placement_id
                    })
                    .map(|placement| placement.placement_id.clone())
                    .collect();
            }
            object_tasks.push(task);
        }

        let mut batches = Vec::new();
        if !goal_satisfied {
            if let Some(target_route) = target_route {
                let target = MaterializationTarget {
                    tenant_id: tenant_id.clone(),
                    object_namespace_id: namespace.clone(),
                    storage_volume_id: target_volume_id.clone(),
                    agent_id: target_route.agent_id,
                    edge_cluster_id: target_route.edge_cluster_id,
                    gateway_pool_id: target_route.gateway_pool_id,
                    placement_generation: target_route.placement_generation,
                    session_generation: target_route.session_generation,
                    mount_generation: target_route.mount_generation,
                    route_generation: target_route.route_generation,
                };
                for (source_route, entries) in &selected {
                    let source = entries
                        .first()
                        .map(|(_, candidate)| candidate.source.clone())
                        .expect("selected source has at least one object");
                    let refs = entries
                        .iter()
                        .map(|(reference, _)| reference.clone())
                        .collect::<Vec<_>>();
                    let source_bindings = entries
                        .iter()
                        .map(|(reference, candidate)| MaterializationManifestSource {
                            object_id: reference.object_id,
                            placement_id: candidate.placement.placement_id.clone(),
                            placement_generation: candidate.placement.placement_generation,
                        })
                        .collect::<Vec<_>>();
                    // Object attempts advance when a plan is rebuilt so checkpoints and
                    // receipts from the previous plan cannot be accepted by the new one. A
                    // Batch has one attempt fence for all of its objects; using the highest
                    // task attempt keeps every receipt inside its one-step CAS window even when
                    // tasks failed independently before this replan.
                    let batch_attempt = refs
                        .iter()
                        .filter_map(|reference| {
                            object_tasks
                                .iter()
                                .find(|task| task.object.object_id == reference.object_id)
                                .map(|task| task.attempt)
                        })
                        .max()
                        .unwrap_or_else(|| Generation::new(1));
                    // A Batch carries one attempt fence for all of its objects.  Replanning can
                    // leave tasks at different per-object attempts (for example, one object may
                    // have failed before another was retried), but the target Agent validates
                    // every task against the Batch attempt.  Advance the lower tasks to the
                    // Batch fence instead of issuing an assignment that can never be consumed.
                    for task in &mut object_tasks {
                        if refs
                            .iter()
                            .any(|reference| reference.object_id == task.object.object_id)
                            && task.attempt < batch_attempt
                        {
                            task.attempt = batch_attempt;
                        }
                    }
                    let batch_id = MaterializationBatchId::new(format!(
                        "batch-{}-r{}-{}",
                        current.materialization_id,
                        plan_revision,
                        source_route.batch_suffix()
                    ))
                    .map_err(|error| invalid_request(format!("batch_id: {error}")))?;
                    let (manifest, _pages) = BatchManifest::paginate_with_sources(
                        current.materialization_id.clone(),
                        batch_id.clone(),
                        plan_revision,
                        batch_attempt,
                        namespace.clone(),
                        refs.clone(),
                        source_bindings,
                        4096,
                    )
                    .map_err(|error| invalid_request(format!("batch manifest: {error}")))?;
                    let bytes = refs
                        .iter()
                        .try_fold(0_u64, |total, reference| {
                            total.checked_add(reference.size.get()).ok_or(())
                        })
                        .map_err(|_| invalid_request("batch byte count overflowed"))?;
                    let mut object_ids = refs
                        .iter()
                        .map(|reference| reference.object_id)
                        .collect::<Vec<_>>();
                    object_ids.sort_unstable();
                    batches.push(MaterializationBatch {
                        batch_id: batch_id.clone(),
                        materialization_id: current.materialization_id.clone(),
                        plan_revision,
                        batch_attempt,
                        source,
                        target: target.clone(),
                        manifest_digest: manifest.manifest_digest,
                        object_count: DecimalU64::new(object_ids.len() as u64),
                        object_ids,
                        total_bytes: DecimalU64::new(bytes),
                        state: MaterializationBatchState::Queued,
                        max_bytes: DecimalU64::new(bytes),
                        deadline_unix_ms: UnixMillis::new(deadline),
                    });
                    for task in &mut object_tasks {
                        if refs
                            .iter()
                            .any(|reference| reference.object_id == task.object.object_id)
                        {
                            task.current_batch_id = Some(batch_id.clone());
                        }
                    }
                }
            }
        }
        Ok((next_job, object_tasks, batches, coverage))
    }

    pub async fn materialize_commit(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateCommitMaterializationRequest,
    ) -> Result<CreateCommitMaterializationResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let project_id = neoengram_domain::protocol::ProjectId::new(request.project_id)
            .map_err(|error| invalid_request(format!("project_id: {error}")))?;
        let artifact_id = ArtifactId::new(request.artifact_id)
            .map_err(|error| invalid_request(format!("artifact_id: {error}")))?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume_id =
            parse_volume(request.target_storage_volume_id, "target_storage_volume_id")?;
        let purpose = request.purpose;
        let repair_observation_digest = request
            .repair_observation_digest
            .as_deref()
            .map(|value| {
                value.parse::<ContentDigest>().map_err(|_| {
                    invalid_request("repair_observation_digest must be a 64-character digest")
                })
            })
            .transpose()?;
        let repair_target_placement_generation = request
            .target_placement_generation
            .as_deref()
            .map(|value| {
                parse_generation(value.to_owned(), "target_placement_generation")
                    .map(|generation| PlacementGeneration::new(generation.get()))
            })
            .transpose()?;
        match purpose {
            TaskPurpose::Copy
                if repair_observation_digest.is_some()
                    || repair_target_placement_generation.is_some() =>
            {
                return Err(invalid_request(
                    "copy materialization cannot carry a repair fence",
                ));
            }
            TaskPurpose::Repair
                if repair_observation_digest.is_some()
                    == repair_target_placement_generation.is_some() =>
            {
                return Err(invalid_request(
                    "repair requires exactly one repair_observation_digest or target_placement_generation",
                ));
            }
            _ => {}
        }
        let goal = request.coverage_goal.unwrap_or(CoverageGoal::Complete);
        goal.validate()
            .map_err(|error| invalid_request(format!("coverage_goal: {error}")))?;
        let key = MaterializationJobKey {
            tenant_id: tenant_id.clone(),
            object_namespace_id: namespace.clone(),
            commit_id: CommitId::from_digest(commit_digest),
            target_storage_volume_id: target_volume_id.clone(),
            coverage_goal: goal,
            purpose,
            repair_observation_digest,
            repair_target_placement_generation,
        };
        let operation_request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let materialization_id = materialization_id_for_request(
            &key,
            purpose,
            request.repair_observation_digest.as_deref(),
            request.target_placement_generation.as_deref(),
        )?;
        let (mut task, task_replayed) = self
            .begin_operation_task(
                TaskIntent::CommitMaterialize,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(namespace.clone()),
                    commit_id: Some(CommitId::from_digest(commit_digest)),
                    workspace_id: None,

                    snapshot_id: None,
                    storage_volume_id: Some(target_volume_id.clone()),
                },
                operation_request_id,
                &task_request,
                identity,
                Some("materialization"),
                Some(materialization_id.as_str()),
            )
            .await?;
        self.operation_result(
            &task,
            identity,
            self.link_operation_resource(
                &task,
                TaskResourceKind::Materialization,
                materialization_id.to_string(),
                TaskResourceRole::Primary,
            )
            .await,
        )
        .await?;
        let repository = self
            .operation_result(
                &task,
                identity,
                self.placement.as_ref().ok_or_else(|| {
                    application_error(
                        ErrorCategory::Unavailable,
                        "placement_authority_unavailable",
                        "PLACEMENT_AUTHORITY_UNAVAILABLE",
                        "placement authority is not configured",
                        true,
                    )
                }),
            )
            .await?;
        let object_set = self
            .operation_result(
                &task,
                identity,
                self.materialization_object_set(
                    &tenant_id,
                    &project_id,
                    &artifact_id,
                    commit_digest,
                    &namespace,
                )
                .await,
            )
            .await?;
        let volume = self
            .operation_result(
                &task,
                identity,
                self.repository
                    .get_storage_volume(&tenant_id, &target_volume_id)
                    .await
                    .map_err(map_central_error)
                    .and_then(|record| record.ok_or_else(|| not_found("storage volume"))),
            )
            .await?;
        if !volume.lifecycle.is_active() || volume.state != crate::StorageVolumeState::Ready {
            let error = application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "target StorageVolume is not ready",
                true,
            );
            self.fail_operation_task(&task, identity, &error).await;
            return Err(error);
        }
        if matches!(volume.access_mode, crate::StorageAccessMode::ReadOnlyMany) {
            let error = application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_writable",
                "STORAGE_VOLUME_NOT_WRITABLE",
                "target StorageVolume is read-only",
                false,
            );
            self.fail_operation_task(&task, identity, &error).await;
            return Err(error);
        }
        let existing = self
            .operation_result(
                &task,
                identity,
                repository
                    .get_materialization_by_key(&key)
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        if let Some(existing) = existing {
            let task_execution_reused = task.as_ref().is_some_and(|value| value.execution_reused);
            let task_matches_detail = task
                .as_ref()
                .is_some_and(|value| value.task_id == existing.operation_task_id.to_string());
            let execution_reused = task_execution_reused || !task_matches_detail;
            return Ok(CreateCommitMaterializationResponse {
                materialization: view(&existing, &object_set),
                request_replayed: (task_replayed || task.is_none() || task_matches_detail)
                    && !execution_reused,
                execution_reused,
                task,
            });
        }
        let placements = self
            .operation_result(
                &task,
                identity,
                self.v2_placements(&tenant_id, &namespace, &object_set)
                    .await,
            )
            .await?;
        let coverage = self
            .operation_result(
                &task,
                identity,
                self.target_coverage(
                    &tenant_id,
                    &namespace,
                    CommitId::from_digest(commit_digest),
                    &target_volume_id,
                    &object_set,
                    &placements,
                )
                .await,
            )
            .await?;
        let now = self.clock.now();
        let deadline = self
            .operation_result(
                &task,
                identity,
                now.get()
                    .checked_add(MATERIALIZATION_PLAN_DEADLINE_MS)
                    .ok_or_else(|| invalid_request("materialization deadline overflowed")),
            )
            .await?;
        let id = materialization_id;
        let total_bytes = self
            .operation_result(
                &task,
                identity,
                object_set
                    .total_bytes()
                    .map_err(|error| invalid_request(format!("object total size: {error}"))),
            )
            .await?;
        let total_objects = object_set.object_count() as u64;
        self.operation_result(
            &task,
            identity,
            goal.validate_against_totals(total_objects, total_bytes)
                .map_err(|error| invalid_request(format!("coverage_goal: {error}"))),
        )
        .await?;
        let target_present = object_set
            .objects
            .iter()
            .filter(|object| {
                placements.iter().any(|placement| {
                    placement.storage_volume_id.as_ref() == Some(&target_volume_id)
                        && placement.placement_generation == coverage.placement_generation
                        && placement.readable()
                        && placement.matches_ref(object)
                })
            })
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let verified_objects = target_present.len() as u64;
        let verified_bytes = object_set
            .objects
            .iter()
            .filter(|object| target_present.contains(&object.object_id))
            .map(|object| object.size.get())
            .sum::<u64>();
        let missing_objects = total_objects.saturating_sub(verified_objects);
        let missing_bytes = total_bytes.saturating_sub(verified_bytes);
        let routed = self
            .routed_candidates(&tenant_id, Some(&target_volume_id), &placements)
            .await;
        let routed_object_ids = routed
            .values()
            .flat_map(|candidates| {
                candidates
                    .iter()
                    .map(|candidate| candidate.placement.object_id)
            })
            .collect::<BTreeSet<_>>();
        let verified_source_ids = object_set
            .objects
            .iter()
            .filter(|object| {
                placements.iter().any(|placement| {
                    placement.storage_volume_id.as_ref() != Some(&target_volume_id)
                        && placement.readable()
                        && placement.matches_ref(object)
                })
            })
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let required_refs = Self::required_missing_refs(
            &object_set,
            &target_present,
            goal,
            verified_bytes,
            &routed_object_ids,
            &verified_source_ids,
        );
        let required_object_ids = required_refs
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let (selected, unresolved) =
            Self::choose_sources(&object_set, &routed, &required_object_ids);
        let no_verified_source = required_refs.iter().any(|object| {
            !placements.iter().any(|placement| {
                placement.storage_volume_id.as_ref() != Some(&target_volume_id)
                    && placement.readable()
                    && placement.matches_ref(object)
            })
        });
        let target_route = self
            .route_for_volume(&tenant_id, &target_volume_id, "target")
            .await;
        let selected_all_missing =
            unresolved.is_empty() && selected.values().flatten().count() == required_refs.len();
        let goal_satisfied =
            goal.satisfied_by(verified_objects, verified_bytes, total_objects, total_bytes);
        if !goal_satisfied && missing_bytes > 0 {
            if let Some(capacity) = &self.storage_availability {
                let available = self
                    .operation_result(
                        &task,
                        identity,
                        capacity
                            .current_available_bytes(&tenant_id, &target_volume_id)
                            .await
                            .map_err(map_central_error),
                    )
                    .await?;
                if let Some(available_bytes) = available {
                    let required_bytes = self
                        .operation_result(
                            &task,
                            identity,
                            materialization_capacity_requirement(
                                required_refs
                                    .iter()
                                    .map(|object| object.size.get())
                                    .try_fold(0_u64, |total, size| total.checked_add(size))
                                    .ok_or_else(|| {
                                        invalid_request(
                                            "materialization capacity requirement overflow",
                                        )
                                    })?,
                                volume.copy_reserve_bytes.get(),
                            ),
                        )
                        .await?;
                    if available_bytes < required_bytes {
                        let error = application_error(
                            ErrorCategory::ResourceExhausted,
                            "materialization_capacity_insufficient",
                            "MATERIALIZATION_CAPACITY_INSUFFICIENT",
                            format!(
                                "target StorageVolume requires {required_bytes} available bytes including staging and concurrency reserve, but reports {available_bytes}"
                            ),
                            true,
                        );
                        self.fail_operation_task(&task, identity, &error).await;
                        return Err(error);
                    }
                }
            }
        }
        let state = if goal_satisfied {
            MaterializationJobState::Complete
        } else if no_verified_source {
            MaterializationJobState::Failed
        } else if target_route.is_none() || !selected_all_missing {
            MaterializationJobState::WaitingForSources
        } else {
            MaterializationJobState::Materializing
        };
        let mut issue_text = None;
        if state == MaterializationJobState::Failed {
            issue_text =
                Some("one or more Commit objects have no Verified source Placement".to_owned());
        }
        if state == MaterializationJobState::WaitingForSources {
            issue_text = Some(if target_route.is_none() {
                "target StorageVolume has no healthy Agent/Gateway route".to_owned()
            } else {
                "Verified source Placement exists but no healthy route is available".to_owned()
            });
        }
        let source_count = selected.len() as u64;
        let (operation_task_id, task_attempt_id) = task
            .as_ref()
            .map(|value| {
                let task_id = TaskId::new(value.task_id.clone())
                    .map_err(|error| invalid_request(format!("task.task_id: {error}")))?;
                let attempt_id =
                    TaskAttemptId::new(format!("{}-attempt-{}", task_id, value.attempt)).map_err(
                        |error| invalid_request(format!("task.task_attempt_id: {error}")),
                    )?;
                Ok::<_, Error>((task_id, attempt_id))
            })
            .transpose()?
            .unwrap_or_else(|| {
                // Standalone in-memory compositions may omit the task repository. Keep the
                // materialization contract complete with a deterministic synthetic identity; the
                // production runtime always supplies the coordinator-backed task identity above.
                let task_id = TaskId::new(format!("task-materialization-{}", id))
                    .expect("materialization ID yields a valid task ID");
                let attempt_id = TaskAttemptId::new(format!("{}-attempt-1", task_id))
                    .expect("derived task ID yields a valid attempt ID");
                (task_id, attempt_id)
            });
        let job = MaterializationJob {
            materialization_id: id,
            operation_task_id,
            task_attempt_id,
            key: key.clone(),
            artifact_id: artifact_id.clone(),
            state,
            plan_revision: Generation::new(1),
            object_count: DecimalU64::new(total_objects),
            total_bytes: DecimalU64::new(total_bytes),
            verified_object_count: DecimalU64::new(verified_objects),
            verified_bytes: DecimalU64::new(verified_bytes),
            missing_object_count: DecimalU64::new(missing_objects),
            missing_bytes: DecimalU64::new(missing_bytes),
            source_count: DecimalU64::new(source_count),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            deadline_unix_ms: UnixMillis::new(deadline),
            issue: issue_text,
        };
        let mut objects_by_id = BTreeMap::<ObjectId, MaterializationObject>::new();
        let routed_placement_ids = routed
            .values()
            .flat_map(|candidates| {
                candidates
                    .iter()
                    .map(|candidate| candidate.placement.placement_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut planned_batches = Vec::new();
        let mut planned_objects = Vec::new();
        for object in &object_set.objects {
            let reference = object.clone();
            let mut task = MaterializationObject::new(
                job.materialization_id.clone(),
                reference,
                job.plan_revision,
            );
            if target_present.contains(&object.object_id) {
                task.state = MaterializationObjectState::AlreadyPresent;
                task.confirmed_offset = object.size;
            } else if let Some((_, candidate)) = selected
                .values()
                .flatten()
                .find(|(reference, _)| reference.object_id == object.object_id)
            {
                task.primary_source = Some(candidate.placement.placement_id.clone());
                task.fallback_sources = placements
                    .iter()
                    .filter(|placement| {
                        placement.storage_volume_id.as_ref() != Some(&target_volume_id)
                            && placement.readable()
                            && placement.matches_ref(object)
                            && routed_placement_ids.contains(&placement.placement_id)
                            && placement.placement_id != candidate.placement.placement_id
                    })
                    .map(|placement| placement.placement_id.clone())
                    .collect();
                task.state = MaterializationObjectState::Reserved;
            }
            objects_by_id.insert(object.object_id, task);
        }
        // Build one manifest and batch per selected source. A target route is required before a
        // batch can be dispatched; jobs without it remain WaitingForSources with durable objects.
        if !goal_satisfied && !selected.is_empty() {
            if let Some(target_route) = target_route {
                let target_generation = target_route.placement_generation;
                let target = MaterializationTarget {
                    tenant_id: tenant_id.clone(),
                    object_namespace_id: namespace.clone(),
                    storage_volume_id: target_volume_id.clone(),
                    agent_id: target_route.agent_id,
                    edge_cluster_id: target_route.edge_cluster_id,
                    gateway_pool_id: target_route.gateway_pool_id,
                    placement_generation: target_generation,
                    session_generation: target_route.session_generation,
                    mount_generation: target_route.mount_generation,
                    route_generation: target_route.route_generation,
                };
                for (source_route, entries) in &selected {
                    let source = entries
                        .first()
                        .map(|(_, candidate)| candidate.source.clone())
                        .expect("selected source has at least one object");
                    let refs = entries
                        .iter()
                        .map(|(reference, _)| reference.clone())
                        .collect::<Vec<_>>();
                    let source_bindings = entries
                        .iter()
                        .map(|(reference, candidate)| MaterializationManifestSource {
                            object_id: reference.object_id,
                            placement_id: candidate.placement.placement_id.clone(),
                            placement_generation: candidate.placement.placement_generation,
                        })
                        .collect::<Vec<_>>();
                    let batch_id = MaterializationBatchId::new(format!(
                        "batch-{}-{}",
                        job.materialization_id,
                        source_route.batch_suffix()
                    ))
                    .map_err(|error| invalid_request(format!("batch_id: {error}")))?;
                    let (manifest, _pages) = BatchManifest::paginate_with_sources(
                        job.materialization_id.clone(),
                        batch_id.clone(),
                        job.plan_revision,
                        Generation::new(1),
                        namespace.clone(),
                        refs.clone(),
                        source_bindings,
                        4096,
                    )
                    .map_err(|error| invalid_request(format!("batch manifest: {error}")))?;
                    let bytes = refs
                        .iter()
                        .try_fold(0_u64, |total, reference| {
                            total.checked_add(reference.size.get()).ok_or(())
                        })
                        .map_err(|_| invalid_request("batch byte count overflowed"))?;
                    let mut object_ids = refs
                        .iter()
                        .map(|reference| reference.object_id)
                        .collect::<Vec<_>>();
                    object_ids.sort_unstable();
                    let batch = MaterializationBatch {
                        batch_id: batch_id.clone(),
                        materialization_id: job.materialization_id.clone(),
                        plan_revision: job.plan_revision,
                        batch_attempt: Generation::new(1),
                        source,
                        target: target.clone(),
                        manifest_digest: manifest.manifest_digest,
                        object_count: DecimalU64::new(object_ids.len() as u64),
                        object_ids,
                        total_bytes: DecimalU64::new(bytes),
                        state: MaterializationBatchState::Queued,
                        max_bytes: DecimalU64::new(bytes),
                        deadline_unix_ms: UnixMillis::new(deadline),
                    };
                    planned_batches.push(batch.clone());
                    for reference in refs {
                        let mut task = objects_by_id
                            .remove(&reference.object_id)
                            .expect("every manifest object has a task");
                        task.current_batch_id = Some(batch_id.clone());
                        planned_objects.push(task);
                    }
                }
            }
        }
        planned_objects.extend(objects_by_id.into_values());
        let mut object_read_leases = Vec::new();
        let mut staging_leases = Vec::new();
        if !planned_batches.is_empty() {
            for batch in &planned_batches {
                let (reads, staging) = self
                    .operation_result(
                        &task,
                        identity,
                        Self::build_batch_leases(&job, batch, &planned_objects, &placements),
                    )
                    .await?;
                object_read_leases.extend(reads);
                staging_leases.extend(staging);
            }
        }
        let outcome = self
            .operation_result(
                &task,
                identity,
                repository
                    .insert_materialization_plan(MaterializationPlan {
                        job: job.clone(),
                        batches: planned_batches,
                        objects: planned_objects,
                        object_read_leases,
                        staging_leases,
                        coverage,
                    })
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        let (stored, replayed) = match outcome {
            MaterializationPlanInsertOutcome::Inserted(stored) => (stored, false),
            MaterializationPlanInsertOutcome::Existing(stored) => (stored, true),
        };
        let task_execution_reused = task.as_ref().is_some_and(|value| value.execution_reused);
        let task_matches_detail = task
            .as_ref()
            .is_some_and(|value| value.task_id == stored.operation_task_id.to_string());
        // A detail replay is an execution replay when it resolves to another canonical task;
        // only an exact request identity is reported as request_replayed.
        let execution_reused = task_execution_reused || (replayed && !task_matches_detail);
        if execution_reused {
            if let Some(value) = task.as_mut() {
                value.execution_reused = true;
            }
        }
        let previous_task = task.clone();
        task = if stored.state == MaterializationJobState::Complete {
            self.operation_result(
                &previous_task,
                identity,
                self.complete_operation_task(task, identity).await,
            )
            .await?
        } else {
            self.operation_result(
                &previous_task,
                identity,
                self.transition_operation_task(
                    task,
                    TaskState::Running,
                    identity,
                    Some("materialization plan accepted".to_owned()),
                )
                .await,
            )
            .await?
        };
        Ok(CreateCommitMaterializationResponse {
            materialization: view(&stored, &object_set),
            request_replayed: (replayed || task_replayed) && !execution_reused,
            execution_reused,
            task,
        })
    }

    /// Issues the short-lived Central capability for one persisted materialization batch. This is
    /// an internal data-plane boundary; the ticket is never exposed as a user resource.
    pub async fn issue_materialization_batch_ticket(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &ObjectNamespaceId,
        materialization_id: &MaterializationId,
        batch_id: &MaterializationBatchId,
    ) -> Result<SignedMaterializationBatchTicket, Error> {
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let job = repository
            .get_materialization(tenant_id, object_namespace_id, materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        let batch = repository
            .list_materialization_batches(
                tenant_id,
                &job.key.object_namespace_id,
                materialization_id,
            )
            .await
            .map_err(map_central_error)?
            .into_iter()
            .find(|batch| &batch.batch_id == batch_id)
            .ok_or_else(|| not_found("materialization batch"))?;
        if batch.plan_revision != job.plan_revision {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_plan_changed",
                "MATERIALIZATION_PLAN_CHANGED",
                "materialization batch belongs to an obsolete plan revision",
                true,
            ));
        }
        if matches!(
            batch.state,
            MaterializationBatchState::Succeeded | MaterializationBatchState::Failed
        ) {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_batch_not_active",
                "MATERIALIZATION_BATCH_NOT_ACTIVE",
                "a terminal materialization batch cannot receive a ticket",
                false,
            ));
        }
        batch
            .validate()
            .map_err(|error| invalid_request(format!("materialization batch: {error}")))?;
        let now = self.clock.now();
        if batch.deadline_unix_ms.get() <= now.get() {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_deadline_expired",
                "MATERIALIZATION_DEADLINE_EXPIRED",
                "materialization batch deadline has expired",
                true,
            ));
        }
        let target_route = self
            .route_for_volume(tenant_id, &batch.target.storage_volume_id, "target")
            .await
            .ok_or_else(|| {
                application_error(
                    ErrorCategory::Unavailable,
                    "materialization_target_route_unavailable",
                    "MATERIALIZATION_TARGET_ROUTE_UNAVAILABLE",
                    "target StorageVolume has no healthy Agent/Gateway route",
                    true,
                )
            })?;
        if target_route.agent_id != batch.target.agent_id
            || target_route.edge_cluster_id != batch.target.edge_cluster_id
            || target_route.gateway_pool_id != batch.target.gateway_pool_id
            || target_route.placement_generation != batch.target.placement_generation
            || target_route.session_generation != batch.target.session_generation
            || target_route.mount_generation != batch.target.mount_generation
            || target_route.route_generation != batch.target.route_generation
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_target_route_changed",
                "MATERIALIZATION_TARGET_ROUTE_CHANGED",
                "target route fence changed since the batch was planned",
                true,
            ));
        }
        if let Some(source_volume_id) = &batch.source.storage_volume_id {
            let source_route = self
                .route_for_volume(tenant_id, source_volume_id, "source")
                .await
                .ok_or_else(|| {
                    application_error(
                        ErrorCategory::Unavailable,
                        "materialization_source_route_unavailable",
                        "MATERIALIZATION_SOURCE_ROUTE_UNAVAILABLE",
                        "source StorageVolume has no healthy Agent/Gateway route",
                        true,
                    )
                })?;
            if source_route.agent_id != batch.source.agent_id
                || source_route.edge_cluster_id != batch.source.edge_cluster_id
                || source_route.gateway_pool_id != batch.source.gateway_pool_id
                || source_route.placement_generation != batch.source.placement_generation
                || source_route.session_generation != batch.source.session_generation
                || source_route.mount_generation != batch.source.mount_generation
                || source_route.route_generation != batch.source.route_generation
            {
                return Err(application_error(
                    ErrorCategory::Conflict,
                    "materialization_source_route_changed",
                    "MATERIALIZATION_SOURCE_ROUTE_CHANGED",
                    "source route fence changed since the batch was planned",
                    true,
                ));
            }
        }
        let ticket_seed = format!(
            "{}:{}:{}:{}:{}",
            materialization_id,
            batch_id,
            batch.plan_revision,
            batch.batch_attempt,
            batch.manifest_digest
        );
        let ticket_digest = blake3::hash(ticket_seed.as_bytes());
        let ticket_id = ObjectTicketId::new(format!("ticket-{}", &ticket_digest.to_hex()[..32]))
            .map_err(|error| invalid_request(format!("ticket_id: {error}")))?;
        let remaining_ms = batch.deadline_unix_ms.get().saturating_sub(now.get());
        let ttl_ms = remaining_ms.min(super::MAX_CENTRAL_COMMAND_TTL_MS);
        let ticket = MaterializationBatchTicket {
            ticket_id,
            operation_task_id: job.operation_task_id.clone(),
            task_attempt_id: job.task_attempt_id.clone(),
            task_attempt: task_attempt_generation(&job.task_attempt_id),
            stage_key: "transfer".to_owned(),
            stage_attempt: batch.batch_attempt,
            materialization_id: batch.materialization_id.clone(),
            batch_id: batch.batch_id.clone(),
            plan_revision: batch.plan_revision,
            batch_attempt: batch.batch_attempt,
            tenant_id: tenant_id.clone(),
            artifact_id: job.artifact_id,
            object_namespace_id: job.key.object_namespace_id,
            commit_id: job.key.commit_id,
            manifest_digest: batch.manifest_digest,
            source: batch.source,
            target: batch.target,
            max_bytes: batch.max_bytes,
            deadline_unix_ms: UnixMillis::new(now.get().saturating_add(ttl_ms)),
            capability:
                neoengram_domain::protocol::materialization::COMMIT_MATERIALIZATION_CAPABILITY_V2
                    .to_owned(),
        };
        let keyring = self.replication_ticket_keyring.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "materialization_ticket_signing_unavailable",
                "MATERIALIZATION_TICKET_SIGNING_UNAVAILABLE",
                "Central materialization ticket signing is not configured",
                true,
            )
        })?;
        keyring
            .sign_materialization_batch_ticket(ticket, now, ttl_ms)
            .await
            .map_err(|error| {
                application_error(
                    ErrorCategory::Unavailable,
                    "materialization_ticket_signing_failed",
                    "MATERIALIZATION_TICKET_SIGNING_FAILED",
                    error.to_string(),
                    true,
                )
            })
    }

    pub async fn query_commit_materialization(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitMaterializationRequest,
    ) -> Result<QueryCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let job = repository
            .get_materialization(&tenant_id, &namespace, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &job.key.commit_id.digest())
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set = super::placement::namespace_object_set(
            &tenant_id,
            &job.key.object_namespace_id,
            job.key.commit_id.digest(),
            &stored,
        )?;
        Ok(QueryCommitMaterializationResponse {
            materialization: view(&job, &object_set),
        })
    }

    pub async fn query_commit_materialization_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitMaterializationListRequest,
    ) -> Result<QueryCommitMaterializationListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target = request
            .target_storage_volume_id
            .map(|value| parse_volume(value, "target_storage_volume_id"))
            .transpose()?;
        let page_size = usize::from(
            request
                .page_size
                .unwrap_or(MAX_MATERIALIZATION_PAGE_SIZE as u16),
        );
        if page_size == 0 || page_size > MAX_MATERIALIZATION_PAGE_SIZE {
            return Err(invalid_request("page_size must be between 1 and 100"));
        }
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set =
            super::placement::namespace_object_set(&tenant_id, &namespace, commit_digest, &stored)?;
        let mut jobs = repository
            .list_materializations(&tenant_id, &namespace, &commit_digest, target.as_ref())
            .await
            .map_err(map_central_error)?;
        jobs.sort_by_key(|job| job.materialization_id.clone());
        let offset = request
            .cursor
            .as_deref()
            .map(|cursor| {
                cursor
                    .parse::<usize>()
                    .map_err(|_| invalid_request("cursor is invalid"))
            })
            .transpose()?
            .unwrap_or(0);
        if offset > jobs.len() {
            return Err(invalid_request("cursor is out of range"));
        }
        let end = (offset + page_size).min(jobs.len());
        let next_cursor = (end < jobs.len()).then(|| end.to_string());
        Ok(QueryCommitMaterializationListResponse {
            materializations: jobs[offset..end]
                .iter()
                .map(|job| view(job, &object_set))
                .collect(),
            next_cursor,
        })
    }

    pub async fn retry_commit_materialization(
        &self,
        identity: &AuthenticatedIdentity,
        request: RetryCommitMaterializationRequest,
    ) -> Result<RetryCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let expected = parse_generation(request.expected_plan_revision, "expected_plan_revision")?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let current = repository
            .get_materialization(&tenant_id, &namespace, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        if current.plan_revision != expected {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_plan_changed",
                "MATERIALIZATION_PLAN_CHANGED",
                "materialization plan revision changed",
                true,
            ));
        }
        let stored_object_set = repository
            .get_commit_object_set(&tenant_id, &current.key.commit_id.digest())
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set = super::placement::namespace_object_set(
            &tenant_id,
            &current.key.object_namespace_id,
            current.key.commit_id.digest(),
            &stored_object_set,
        )?;
        if current.state == MaterializationJobState::Complete {
            // Completion is a derived claim over current Placement health, not a permanent
            // promise. A later integrity scrub may exclude an object that was valid when this
            // Job completed; only replay while the freshly recomputed Coverage still satisfies
            // the requested goal. Otherwise fall through to the normal replan path so healthy
            // copies can refill the target while preserving durable staging checkpoints.
            let placements = self
                .v2_placements(&tenant_id, &current.key.object_namespace_id, &object_set)
                .await?;
            let coverage = self
                .target_coverage(
                    &tenant_id,
                    &current.key.object_namespace_id,
                    current.key.commit_id,
                    &current.key.target_storage_volume_id,
                    &object_set,
                    &placements,
                )
                .await?;
            let goal_satisfied = current.key.coverage_goal.satisfied_by(
                coverage.verified_object_count.get(),
                coverage.verified_bytes.get(),
                coverage.object_count.get(),
                coverage.total_bytes.get(),
            );
            if goal_satisfied {
                return Ok(RetryCommitMaterializationResponse {
                    materialization: view(&current, &object_set),
                    // The retry request is a fresh request identity. A complete, still-satisfied
                    // plan is therefore a semantic no-op rather than an exact request replay.
                    request_replayed: false,
                    execution_reused: true,
                });
            }
        }
        let now = self.clock.now();
        let next_revision = Generation::new(
            expected
                .get()
                .checked_add(1)
                .ok_or_else(|| invalid_request("materialization plan revision is exhausted"))?,
        );
        let old_objects = repository
            .list_materialization_objects(
                &tenant_id,
                &current.key.object_namespace_id,
                &materialization_id,
            )
            .await
            .map_err(map_central_error)?;
        let (mut planned_job, object_tasks, batches, coverage) = self
            .retry_plan(&current, &old_objects, &object_set, next_revision, now)
            .await?;
        // The operation task is the retry authority.  Its Attempt is advanced before this
        // planner is invoked by the unified task API; bind the replacement Job to that Attempt so
        // old assignments/reports remain fenced while the new route plan is delivered.  Focused
        // materialization tests without a task repository keep the original Job identity.
        if let Some(coordinator) = &self.task_coordinator {
            if let Some(operation_task) = coordinator
                .repository()
                .get(&tenant_id, &current.operation_task_id)
                .await
                .map_err(map_central_error)?
            {
                planned_job.task_attempt_id = TaskAttemptId::new(format!(
                    "{}-attempt-{}",
                    operation_task.task_id, operation_task.attempt
                ))
                .map_err(|error| invalid_request(format!("task_attempt_id: {error}")))?;
            }
        }
        let placements = self
            .v2_placements(&tenant_id, &current.key.object_namespace_id, &object_set)
            .await?;
        let mut object_read_leases = Vec::new();
        let mut staging_leases = Vec::new();
        for batch in &batches {
            let (reads, staging) =
                Self::build_batch_leases(&planned_job, batch, &object_tasks, &placements)?;
            object_read_leases.extend(reads);
            staging_leases.extend(staging);
        }
        let outcome = repository
            .replace_materialization_plan(MaterializationPlanReplacement {
                expected_plan_revision: expected,
                plan: MaterializationPlan {
                    job: planned_job,
                    batches,
                    objects: object_tasks,
                    object_read_leases,
                    staging_leases,
                    coverage,
                },
            })
            .await
            .map_err(map_central_error)?;
        let (stored_job, replayed) = match outcome {
            MaterializationPlanInsertOutcome::Inserted(stored) => (stored, false),
            MaterializationPlanInsertOutcome::Existing(stored) => (stored, true),
        };
        Ok(RetryCommitMaterializationResponse {
            materialization: view(&stored_job, &object_set),
            request_replayed: replayed,
            execution_reused: false,
        })
    }

    /// Replans a Commit materialization on behalf of the unified task retry endpoint.  The task
    /// repository has already advanced the Attempt when this method is called, so the normal
    /// materialization planner can bind the replacement Job/Tickets to that new identity without
    /// exposing a second public retry API.
    pub(crate) async fn retry_materialization_for_task(
        &self,
        identity: &AuthenticatedIdentity,
        task: &OperationTask,
    ) -> Result<RetryCommitMaterializationResponse, Error> {
        let namespace = task
            .object_namespace_id
            .clone()
            .ok_or_else(|| invalid_request("materialization task has no object namespace"))?;
        let materialization_id = task
            .detail_id
            .as_deref()
            .ok_or_else(|| invalid_request("materialization task has no detail_id"))?
            .to_owned();
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let materialization_id = parse_materialization_id(materialization_id)?;
        let current = repository
            .get_materialization(&task.tenant_id, &namespace, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        if current.operation_task_id != task.task_id {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_task_mismatch",
                "MATERIALIZATION_TASK_MISMATCH",
                "materialization is owned by a different operation task",
                false,
            ));
        }
        self.retry_commit_materialization(
            identity,
            RetryCommitMaterializationRequest {
                tenant_id: task.tenant_id.to_string(),
                object_namespace_id: namespace.to_string(),
                materialization_id: materialization_id.to_string(),
                expected_plan_revision: current.plan_revision.to_string(),
                request_id: format!("{}-retry-{}", task.request_id, task.attempt),
            },
        )
        .await
    }

    pub async fn cancel_commit_materialization(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelCommitMaterializationRequest,
    ) -> Result<CancelCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let expected = parse_generation(request.expected_plan_revision, "expected_plan_revision")?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let current = repository
            .get_materialization(&tenant_id, &namespace, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        if current.plan_revision != expected {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_plan_changed",
                "MATERIALIZATION_PLAN_CHANGED",
                "materialization plan revision changed",
                true,
            ));
        }
        if current.state.terminal() {
            let stored_object_set = repository
                .get_commit_object_set(&tenant_id, &current.key.commit_id.digest())
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| not_found("commit object set"))?;
            let object_set = super::placement::namespace_object_set(
                &tenant_id,
                &current.key.object_namespace_id,
                current.key.commit_id.digest(),
                &stored_object_set,
            )?;
            return Ok(CancelCommitMaterializationResponse {
                materialization: view(&current, &object_set),
            });
        }
        let active_objects = repository
            .list_materialization_objects(
                &tenant_id,
                &current.key.object_namespace_id,
                &materialization_id,
            )
            .await
            .map_err(map_central_error)?;
        let active_batches = repository
            .list_materialization_batches(
                &tenant_id,
                &current.key.object_namespace_id,
                &materialization_id,
            )
            .await
            .map_err(map_central_error)?;
        // Drop source/staging protection and fence every child before the parent is cancelled.
        // This is the cancellation linearization boundary: a reconnect cannot continue an old
        // batch after the parent has become terminal.
        for batch in &active_batches {
            Self::release_batch_leases(repository.as_ref(), &current, batch, &active_objects)
                .await?;
            if !matches!(
                batch.state,
                MaterializationBatchState::Succeeded | MaterializationBatchState::Failed
            ) {
                let mut failed = batch.clone();
                failed.state = MaterializationBatchState::Failed;
                repository
                    .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                        tenant_id: tenant_id.clone(),
                        object_namespace_id: current.key.object_namespace_id.clone(),
                        materialization_id: materialization_id.clone(),
                        batch_id: batch.batch_id.clone(),
                        expected_plan_revision: batch.plan_revision,
                        expected_batch_attempt: batch.batch_attempt,
                        batch: failed,
                    })
                    .await
                    .map_err(map_central_error)?;
            }
        }
        let mut next = current.clone();
        next.plan_revision = Generation::new(
            expected
                .get()
                .checked_add(1)
                .ok_or_else(|| invalid_request("materialization plan revision is exhausted"))?,
        );
        next.state = MaterializationJobState::Cancelled;
        next.updated_at_unix_ms = self.clock.now();
        let stored = repository
            .replace_materialization(&tenant_id, &materialization_id, expected, next)
            .await
            .map_err(map_central_error)?;
        let stored_object_set = repository
            .get_commit_object_set(&tenant_id, &stored.key.commit_id.digest())
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set = super::placement::namespace_object_set(
            &tenant_id,
            &stored.key.object_namespace_id,
            stored.key.commit_id.digest(),
            &stored_object_set,
        )?;
        Ok(CancelCommitMaterializationResponse {
            materialization: view(&stored, &object_set),
        })
    }

    pub async fn query_commit_coverage(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitCoverageRequest,
    ) -> Result<QueryCommitCoverageResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target = request
            .storage_volume_id
            .map(|value| parse_volume(value, "storage_volume_id"))
            .transpose()?;
        let page_size = usize::from(
            request
                .page_size
                .unwrap_or(MAX_MATERIALIZATION_PAGE_SIZE as u16),
        );
        if page_size == 0 || page_size > MAX_MATERIALIZATION_PAGE_SIZE {
            return Err(invalid_request("page_size must be between 1 and 100"));
        }
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set =
            super::placement::namespace_object_set(&tenant_id, &namespace, commit_digest, &stored)?;
        let placements = self
            .v2_placements(&tenant_id, &namespace, &object_set)
            .await?;
        let mut coverage = Vec::new();
        if let Some(target_volume) = target.as_ref() {
            // A requested target is a first-class query even before its first object is
            // published.  Derive the summary from the current owner generation so an empty
            // target is reported as `partial 0/N`, rather than disappearing from the response.
            // This also keeps stale placements behind a replaced owner generation from being
            // presented as current coverage.
            let summary = self
                .target_coverage(
                    &tenant_id,
                    &namespace,
                    CommitId::from_digest(commit_digest),
                    target_volume,
                    &object_set,
                    &placements,
                )
                .await?;
            // Coverage is a cacheable derived projection. A failed cache write must not make
            // object evidence disappear from the read path, but it is surfaced as an authority
            // error because future readers would otherwise observe divergent summaries.
            repository
                .upsert_volume_commit_coverage(summary.clone())
                .await
                .map_err(map_central_error)?;
            coverage.push(summary);
        } else {
            let mut grouped =
                BTreeMap::<(StorageVolumeId, PlacementGeneration), Vec<ObjectPlacement>>::new();
            for placement in placements {
                let Some(volume) = placement.storage_volume_id.clone() else {
                    continue;
                };
                grouped
                    .entry((volume, placement.placement_generation))
                    .or_default()
                    .push(placement);
            }
            for ((volume, generation), placements) in grouped {
                let summary = VolumeCommitCoverage::from_placements(
                    tenant_id.clone(),
                    namespace.clone(),
                    CommitId::from_digest(commit_digest),
                    volume,
                    generation,
                    &Self::legacy_object_set(&object_set)?,
                    &placements,
                )
                .map_err(|error| invalid_request(format!("coverage: {error}")))?;
                // Coverage is a cacheable derived projection. A failed cache write must not make
                // object evidence disappear from the read path, but it is surfaced as an
                // authority error because future readers would otherwise observe divergent
                // summaries.
                repository
                    .upsert_volume_commit_coverage(summary.clone())
                    .await
                    .map_err(map_central_error)?;
                coverage.push(summary);
            }
        }
        coverage.sort_by_key(|summary| {
            (
                summary.storage_volume_id.clone(),
                summary.placement_generation,
            )
        });
        let offset = request
            .cursor
            .as_deref()
            .map(|cursor| {
                cursor
                    .parse::<usize>()
                    .map_err(|_| invalid_request("cursor is invalid"))
            })
            .transpose()?
            .unwrap_or(0);
        if offset > coverage.len() {
            return Err(invalid_request("cursor is out of range"));
        }
        let end = (offset + page_size).min(coverage.len());
        Ok(QueryCommitCoverageResponse {
            coverage: coverage[offset..end].iter().map(coverage_view).collect(),
            next_cursor: (end < coverage.len()).then(|| end.to_string()),
        })
    }

    pub async fn query_commit_availability_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitAvailabilityV2Request,
    ) -> Result<QueryCommitAvailabilityV2Response, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target = request
            .target_storage_volume_id
            .map(|value| parse_volume(value, "target_storage_volume_id"))
            .transpose()?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let object_set =
            super::placement::namespace_object_set(&tenant_id, &namespace, commit_digest, &stored)?;
        let placements = self
            .v2_placements(&tenant_id, &namespace, &object_set)
            .await?;
        let mut missing = Vec::new();
        let mut source_volume_ids = BTreeSet::new();
        let mut objects_with_copy = 0_u64;
        for object in &object_set.objects {
            let copies = placements
                .iter()
                .filter(|placement| placement.object_id == object.object_id && placement.readable())
                .collect::<Vec<_>>();
            for copy in &copies {
                if let Some(volume) = &copy.storage_volume_id {
                    source_volume_ids.insert(volume.to_string());
                }
            }
            if copies.is_empty() {
                if missing.len() < MAX_MATERIALIZATION_PAGE_SIZE {
                    missing.push(MissingObjectView {
                        object_id: object.object_id.to_string(),
                        size: object.size.to_string(),
                        encoding: format!("{:?}", object.encoding).to_lowercase(),
                    });
                }
            } else {
                objects_with_copy += 1;
            }
        }
        let object_count = object_set.object_count() as u64;
        let content_presence = if object_count == 0 || objects_with_copy == object_count {
            AvailabilityStatus::Available
        } else if objects_with_copy == 0 {
            AvailabilityStatus::Unavailable
        } else {
            AvailabilityStatus::Degraded
        };
        let routed = if objects_with_copy == 0 {
            BTreeMap::new()
        } else {
            self.routed_candidates(&tenant_id, target.as_ref(), &placements)
                .await
        };
        let served_object_ids = routed
            .values()
            .flatten()
            .map(|candidate| candidate.placement.object_id)
            .collect::<BTreeSet<_>>();
        let source_serving = source_serving_status(
            object_count,
            !placements.is_empty(),
            objects_with_copy,
            &served_object_ids,
        );
        let mut target_coverage = "not_requested".to_owned();
        let mut view_readiness = ViewReadiness::NotReady;
        let mut complete_volume_ids = BTreeSet::<StorageVolumeId>::new();
        if let Some(target_volume) = target {
            let summary = self
                .target_coverage(
                    &tenant_id,
                    &namespace,
                    CommitId::from_digest(commit_digest),
                    &target_volume,
                    &object_set,
                    &placements,
                )
                .await?;
            target_coverage = availability_coverage_state_name(summary.state).to_owned();
            if summary.state == CoverageState::Complete {
                complete_volume_ids.insert(target_volume.clone());
                if self
                    .route_for_volume(&tenant_id, &target_volume, "target")
                    .await
                    .is_some()
                {
                    view_readiness = ViewReadiness::Ready;
                }
            }
        } else {
            let mut seen = BTreeSet::<(StorageVolumeId, PlacementGeneration)>::new();
            for placement in &placements {
                if let Some(volume) = &placement.storage_volume_id {
                    seen.insert((volume.clone(), placement.placement_generation));
                }
            }
            for (volume, generation) in seen {
                let summary = VolumeCommitCoverage::from_placements(
                    tenant_id.clone(),
                    namespace.clone(),
                    CommitId::from_digest(commit_digest),
                    volume,
                    generation,
                    &Self::legacy_object_set(&object_set)?,
                    &placements,
                )
                .map_err(|error| invalid_request(format!("coverage: {error}")))?;
                if summary.state == CoverageState::Complete {
                    complete_volume_ids.insert(summary.storage_volume_id.clone());
                }
            }
        }
        let complete_volume_count = complete_volume_ids.len() as u64;
        let policy = self
            .durability_policies
            .get(&(tenant_id.clone(), namespace.clone()))
            .unwrap_or(&self.default_durability_policy);
        let durability = durability_status(content_presence, policy, &object_set, &placements);
        Ok(QueryCommitAvailabilityV2Response {
            availability: CommitAvailabilityV2View {
                object_namespace_id: namespace.to_string(),
                commit_id: CommitId::from_digest(commit_digest).to_string(),
                object_count: object_count.to_string(),
                content_presence: availability_name(content_presence).to_owned(),
                source_serving: availability_name(source_serving).to_owned(),
                durability: durability.to_owned(),
                target_coverage,
                view_readiness: readiness_name(view_readiness).to_owned(),
                complete_volume_count: complete_volume_count.to_string(),
                missing_objects: missing,
                verified_storage_volume_ids: source_volume_ids
                    .into_iter()
                    .take(MAX_MATERIALIZATION_PAGE_SIZE)
                    .collect(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{durability_status, source_serving_status, CatalogService};
    use neoengram_domain::core::{CommitId, ObjectId};
    use neoengram_domain::protocol::materialization::{
        CoverageGoal, DurabilityPolicy, NamespaceObjectSet, ObjectPlacement, ObjectPlacementState,
    };
    use neoengram_domain::protocol::{
        CommitObject, DecimalU64, ObjectEncoding, ObjectNamespaceId, PlacementGeneration,
        PlacementId, StorageVolumeId, TenantId,
    };
    use std::collections::BTreeSet;

    fn object_set() -> NamespaceObjectSet {
        let tenant_id = TenantId::new("tenant-availability-test").unwrap();
        let namespace = ObjectNamespaceId::new("namespace-availability-test").unwrap();
        let object_set = neoengram_domain::protocol::ObjectSet::new(vec![
            CommitObject::new(ObjectId::from_bytes([1; 32]), 4, ObjectEncoding::Raw, 0),
            CommitObject::new(ObjectId::from_bytes([2; 32]), 6, ObjectEncoding::Raw, 1),
        ])
        .unwrap();
        NamespaceObjectSet::from_object_set(
            tenant_id,
            namespace,
            CommitId::from_bytes([3; 32]),
            &object_set,
        )
        .unwrap()
    }

    fn placement(
        object_id: ObjectId,
        placement_id: &str,
        volume_id: &str,
        failure_domain: &str,
    ) -> ObjectPlacement {
        let object_size = if object_id == ObjectId::from_bytes([1; 32]) {
            4
        } else {
            6
        };
        ObjectPlacement {
            placement_id: PlacementId::new(placement_id).unwrap(),
            tenant_id: TenantId::new("tenant-availability-test").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("namespace-availability-test").unwrap(),
            object_id,
            size: DecimalU64::new(object_size),
            encoding: ObjectEncoding::Raw,
            verified_digest: object_id.digest(),
            storage_volume_id: Some(StorageVolumeId::new(volume_id).unwrap()),
            archive_id: None,
            placement_generation: PlacementGeneration::new(1),
            state: ObjectPlacementState::Verified,
            failure_domain: failure_domain.to_owned(),
        }
    }

    #[test]
    fn source_serving_uses_distinct_object_ids() {
        let object_one = ObjectId::from_bytes([1; 32]);
        let object_two = ObjectId::from_bytes([2; 32]);
        let mut served = BTreeSet::new();
        served.insert(object_one);
        served.insert(object_one);
        assert_eq!(
            source_serving_status(2, true, 2, &served),
            neoengram_domain::protocol::materialization::AvailabilityStatus::Degraded
        );
        // A Commit with one missing object must stay degraded even when every currently
        // present copy has a healthy route. Source serving is evaluated against the full
        // ObjectSet, not merely the subset for which a Placement happens to exist.
        assert_eq!(
            source_serving_status(2, true, 1, &BTreeSet::from([object_one])),
            neoengram_domain::protocol::materialization::AvailabilityStatus::Degraded
        );
        served.insert(object_two);
        assert_eq!(
            source_serving_status(2, true, 2, &served),
            neoengram_domain::protocol::materialization::AvailabilityStatus::Available
        );
        assert_eq!(
            source_serving_status(2, false, 2, &served),
            neoengram_domain::protocol::materialization::AvailabilityStatus::Unavailable
        );
        assert_eq!(
            source_serving_status(0, false, 0, &BTreeSet::new()),
            neoengram_domain::protocol::materialization::AvailabilityStatus::Available
        );
    }

    #[test]
    fn threshold_planning_prefers_later_source_backed_objects() {
        let object_set = object_set();
        let target_present = BTreeSet::new();
        let later_object = ObjectId::from_bytes([2; 32]);
        let preferred_sources = BTreeSet::from([later_object]);
        let no_fallback_sources = BTreeSet::new();

        // The first Commit object has no source, but the later object can satisfy a one-object
        // threshold. The planner must select the latter instead of reporting a false no-source
        // failure for the prefix.
        let selected = CatalogService::required_missing_refs(
            &object_set,
            &target_present,
            CoverageGoal::ObjectCount(DecimalU64::new(1)),
            0,
            &preferred_sources,
            &no_fallback_sources,
        );
        assert_eq!(
            selected
                .iter()
                .map(|object| object.object_id)
                .collect::<Vec<_>>(),
            vec![later_object]
        );

        // The same source-aware ordering applies to a byte threshold. The six-byte later object
        // satisfies the requested bytes without pulling the unavailable four-byte prefix in.
        let selected = CatalogService::required_missing_refs(
            &object_set,
            &target_present,
            CoverageGoal::ByteCount(DecimalU64::new(6)),
            0,
            &preferred_sources,
            &no_fallback_sources,
        );
        assert_eq!(
            selected
                .iter()
                .map(|object| object.object_id)
                .collect::<Vec<_>>(),
            vec![later_object]
        );

        // If the threshold cannot be met by source-backed objects, the planner appends the
        // source-less prefix so the caller can report a permanent missing-object failure rather
        // than leaving the job in an endless waiting state.
        let selected = CatalogService::required_missing_refs(
            &object_set,
            &target_present,
            CoverageGoal::ObjectCount(DecimalU64::new(2)),
            0,
            &preferred_sources,
            &no_fallback_sources,
        );
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].object_id, later_object);
        assert_eq!(selected[1].object_id, ObjectId::from_bytes([1; 32]));
    }

    #[test]
    fn durability_requires_policy_replicas_and_failure_domains_per_object() {
        let object_set = object_set();
        let object_one = ObjectId::from_bytes([1; 32]);
        let object_two = ObjectId::from_bytes([2; 32]);
        let mut placements = vec![
            placement(object_one, "placement-one-a", "volume-a", "domain-a"),
            placement(object_one, "placement-one-b", "volume-b", "domain-a"),
            placement(object_two, "placement-two-a", "volume-a", "domain-a"),
            placement(object_two, "placement-two-b", "volume-b", "domain-b"),
        ];
        let policy = DurabilityPolicy::new(2, 2);
        assert_eq!(
            durability_status(
                neoengram_domain::protocol::materialization::AvailabilityStatus::Available,
                &policy,
                &object_set,
                &placements,
            ),
            "under_replicated"
        );

        placements[1].failure_domain = "domain-b".to_owned();
        assert_eq!(
            durability_status(
                neoengram_domain::protocol::materialization::AvailabilityStatus::Available,
                &policy,
                &object_set,
                &placements,
            ),
            "satisfied"
        );
    }
}
