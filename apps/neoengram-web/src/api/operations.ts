import { apiClient } from './client';
import { toApiProblem } from './problem';
import type {
  ApiVersionResponse,
  ApproveStorageEnrollmentRequest,
  ApproveStorageEnrollmentResponse,
  CancelPreCommitRequest,
  CancelPreCommitResponse,
  CommitPlaygroundRequest,
  CommitPlaygroundResponse,
  CreateAddJobRequest,
  CreateAddJobResponse,
  CreateArtifactRequest,
  CreateArtifactResponse,
  CreatePlaygroundRequest,
  CreatePlaygroundResponse,
  CreateProjectRequest,
  CreateProjectResponse,
  CreateSnapshotRequest,
  CreateSnapshotResponse,
  CreateStorageVolumeRequest,
  CreateStorageVolumeResponse,
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
  FinalizeAddJobResponse,
  HealthResponse,
  CreateTenantRequest,
  CreateTenantResponse,
  QueryArtifactCommitGraphResponse,
  QueryArtifactCommitDiffResponse,
  QueryArtifactListRequest,
  QueryArtifactListResponse,
  QueryArtifactResponse,
  QueryGatewayPoolListRequest,
  QueryGatewayPoolListResponse,
  QueryPlaygroundListRequest,
  QueryPlaygroundListResponse,
  QueryPlaygroundChangeListRequest,
  QueryPlaygroundChangeListResponse,
  QueryPlaygroundDatasetProfileRequest,
  QueryPlaygroundDatasetProfileResponse,
  QueryPlaygroundFileListRequest,
  QueryPlaygroundFileListResponse,
  QueryPlaygroundFileMetadataRequest,
  QueryPlaygroundFileMetadataResponse,
  QueryPreCommitResponse,
  QueryPlaygroundResponse,
  QueryProjectListRequest,
  QueryProjectListResponse,
  QuerySnapshotListRequest,
  QuerySnapshotListResponse,
  QuerySnapshotActivityListRequest,
  QuerySnapshotActivityListResponse,
  QuerySnapshotDatasetProfileRequest,
  QuerySnapshotDatasetProfileResponse,
  QuerySnapshotFileListRequest,
  QuerySnapshotFileListResponse,
  QuerySnapshotResponse,
  QueryStorageVolumeListRequest,
  QueryStorageVolumeListResponse,
  QueryStorageVolumeResponse,
  QueryStorageEnrollmentListRequest,
  QueryStorageEnrollmentListResponse,
  QueryStorageEnrollmentResponse,
  QueryTenantListRequest,
  QueryTenantListResponse,
  QueryTenantResponse,
  QueryJobResponse,
  RestartPreCommitRequest,
  RestartPreCommitResponse,
  RejectStorageEnrollmentRequest,
  RejectStorageEnrollmentResponse,
  RetrySnapshotDeliveryRequest,
  RetrySnapshotDeliveryResponse,
  CreateSnapshotDeliveryRequest,
  CreateSnapshotDeliveryResponse,
  CreateCommitMaterializationRequest,
  CreateCommitMaterializationResponse,
  QueryCommitMaterializationRequest,
  QueryCommitMaterializationResponse,
  QueryCommitMaterializationListRequest,
  QueryCommitMaterializationListResponse,
  RetryCommitMaterializationRequest,
  RetryCommitMaterializationResponse,
  CancelCommitMaterializationRequest,
  CancelCommitMaterializationResponse,
  QueryCommitCoverageRequest,
  QueryCommitCoverageResponse,
  CreateCommitReplicationRequest,
  CreateCommitReplicationResponse,
  QueryCommitReplicationRequest,
  QueryCommitReplicationResponse,
  QueryCommitReplicationListRequest,
  QueryCommitReplicationListResponse,
  QueryCommitPlacementListRequest,
  QueryCommitPlacementListResponse,
  RetryCommitReplicationRequest,
  RetryCommitReplicationResponse,
  CancelCommitReplicationRequest,
  CancelCommitReplicationResponse,
  QueryCommitAvailabilityRequest,
  QueryCommitAvailabilityResponse,
  MaterializationView,
  VolumeCommitCoverageView,
  LegacyReplicationView,
  LegacyCommitPlacementView,
  CreateWorkspaceRequest,
  CreateWorkspaceResponse,
  QuerySnapshotDeliveryRequest,
  QuerySnapshotDeliveryResponse,
  QuerySnapshotDeliveryListRequest,
  QuerySnapshotDeliveryListResponse,
  DeleteSnapshotDeliveryRequest,
  DeleteSnapshotDeliveryResponse,
  StartPreCommitRequest,
  StartPreCommitResponse,
  CreateS3AccessPointRequest,
  CreateS3AccessPointResponse,
  QueryS3AccessPointListRequest,
  QueryS3AccessPointListResponse,
  QueryS3AccessPointResponse,
  QueryS3AccessPointRequest,
  UpdateS3AccessPointRequest,
  UpdateS3AccessPointResponse,
  CreateS3CredentialRequest,
  CreateS3CredentialResponse,
  QueryS3CredentialListRequest,
  QueryS3CredentialListResponse,
  RevokeS3CredentialRequest,
  QueryS3ObjectListRequest,
  QueryS3ObjectListResponse,
  CreateS3DownloadUrlRequest,
  CreateS3DownloadUrlResponse,
  CreateDeletionRequest,
  CreateRetentionHoldRequest,
  CreateRetentionHoldResponse,
  DeletionMutationResponse,
  QueryDeletionImpactRequest,
  QueryDeletionImpactResponse,
  QueryDeletionListRequest,
  QueryDeletionListResponse,
  QueryDeletionRequest,
  QueryDeletionResponse,
  ReleaseRetentionHoldRequest,
  ReleaseRetentionHoldResponse,
  UpdateDeletionRequest,
} from './types';

export interface ApiResult<T> {
  data: T;
  requestId: string;
}

function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): ApiResult<T> {
  if (result.data === undefined) throw toApiProblem(result.error, result.response);
  return {
    data: result.data,
    requestId: result.response.headers.get('X-Request-ID') ?? 'request-id-unavailable',
  };
}

export async function queryApiVersion(): Promise<ApiResult<ApiVersionResponse>> {
  return unwrap(await apiClient.POST('/api/system/version/query', { body: {} }));
}

export async function liveProbe(): Promise<ApiResult<HealthResponse>> {
  return unwrap(await apiClient.GET('/health/live'));
}

export async function readyProbe(): Promise<ApiResult<HealthResponse>> {
  return unwrap(await apiClient.GET('/health/ready'));
}

const versionHeader = { header: { 'NeoEngram-API-Version': '1' as const } };

function materializationRequest(
  request: CreateCommitReplicationRequest,
): CreateCommitMaterializationRequest {
  return {
    ...request,
    object_namespace_id: request.object_namespace_id ?? request.artifact_id,
    coverage_goal: request.coverage_goal ?? 'complete',
  };
}

function materializationListRequest(
  request: QueryCommitReplicationListRequest,
): QueryCommitMaterializationListRequest {
  const { artifact_id: _artifactId, object_namespace_id, ...wireRequest } = request;
  const objectNamespaceId = object_namespace_id ?? _artifactId;
  if (!objectNamespaceId) {
    throw new Error('object_namespace_id is required for v2 materialization queries');
  }
  return {
    ...wireRequest,
    object_namespace_id: objectNamespaceId,
  };
}

function coverageRequest(request: QueryCommitPlacementListRequest): QueryCommitCoverageRequest {
  const { artifact_id: _artifactId, object_namespace_id, ...wireRequest } = request;
  const objectNamespaceId = object_namespace_id ?? _artifactId;
  if (!objectNamespaceId) {
    throw new Error('object_namespace_id is required for v2 coverage queries');
  }
  return {
    ...wireRequest,
    object_namespace_id: objectNamespaceId,
  };
}

function toLegacyMaterialization(value: MaterializationView): LegacyReplicationView {
  const state: LegacyReplicationView['state'] =
    value.state === 'materializing'
      ? 'transferring'
      : value.state === 'complete'
        ? 'published'
        : value.state === 'waiting_for_sources' || value.state === 'stalled'
          ? 'planning'
          : value.state;
  const result: LegacyReplicationView = {
    replication_id: value.materialization_id,
    tenant_id: value.tenant_id,
    // v2 namespaces initially map one-to-one to Artifacts. Keep the local page model total even
    // when an older server omits the optional artifact field from its materialization view.
    artifact_id: value.artifact_id ?? value.object_namespace_id,
    commit_id: value.commit_id,
    target_storage_volume_id: value.target_storage_volume_id,
    attempt: value.plan_revision,
    state,
    object_set_digest: value.object_set_digest,
    completed_objects: value.verified_objects,
    total_objects: value.total_objects,
    completed_bytes: value.verified_bytes,
    total_bytes: value.total_bytes,
  };
  const routing = value as MaterializationView & {
    target_edge_cluster_id?: string;
    target_gateway_pool_id?: string;
  };
  if (routing.target_edge_cluster_id)
    result.target_edge_cluster_id = routing.target_edge_cluster_id;
  if (routing.target_gateway_pool_id)
    result.target_gateway_pool_id = routing.target_gateway_pool_id;
  if (value.issue) result.issue = value.issue;
  return result;
}

function toLegacyCoverage(value: VolumeCommitCoverageView): LegacyCommitPlacementView {
  const state: LegacyCommitPlacementView['state'] =
    value.state === 'complete' ? 'published' : value.state === 'partial' ? 'staged' : value.state;
  return {
    placement_set_id: `${value.object_namespace_id}:${value.commit_id}:${value.storage_volume_id}`,
    backend_id: value.storage_volume_id,
    commit_id: value.commit_id,
    storage_volume_id: value.storage_volume_id,
    object_set_digest: value.object_set_digest,
    object_count: value.total_objects,
    verified_object_count: value.verified_objects,
    placement_generation: value.placement_generation,
    state,
  };
}

export async function queryTenantList(
  request: QueryTenantListRequest = {},
): Promise<ApiResult<QueryTenantListResponse>> {
  return unwrap(
    await apiClient.POST('/api/tenant/list/query', { body: request, params: versionHeader }),
  );
}

export async function queryTenant(tenantId: string): Promise<ApiResult<QueryTenantResponse>> {
  return unwrap(
    await apiClient.POST('/api/tenant/query', {
      body: { tenant_id: tenantId },
      params: versionHeader,
    }),
  );
}

export async function createTenant(
  request: CreateTenantRequest,
): Promise<ApiResult<CreateTenantResponse>> {
  return unwrap(
    await apiClient.POST('/api/tenant/create', { body: request, params: versionHeader }),
  );
}

export async function queryGatewayPoolList(
  request: QueryGatewayPoolListRequest = {},
): Promise<ApiResult<QueryGatewayPoolListResponse>> {
  return unwrap(
    await apiClient.POST('/api/gateway/pool/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryStorageVolumeList(
  request: QueryStorageVolumeListRequest,
): Promise<ApiResult<QueryStorageVolumeListResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/volume/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryStorageVolume(
  tenantId: string,
  storageVolumeId: string,
): Promise<ApiResult<QueryStorageVolumeResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/volume/query', {
      body: { tenant_id: tenantId, storage_volume_id: storageVolumeId },
      params: versionHeader,
    }),
  );
}

export async function createStorageVolume(
  request: CreateStorageVolumeRequest,
): Promise<ApiResult<CreateStorageVolumeResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/volume/create', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createStorageEnrollmentToken(
  request: CreateStorageEnrollmentTokenRequest,
): Promise<ApiResult<CreateStorageEnrollmentTokenResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/enrollment/token/create', {
      body: request,
      params: versionHeader,
    }),
  ) as ApiResult<CreateStorageEnrollmentTokenResponse>;
}

export async function queryStorageEnrollmentList(
  request: QueryStorageEnrollmentListRequest,
): Promise<ApiResult<QueryStorageEnrollmentListResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/enrollment/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryStorageEnrollment(
  tenantId: string,
  storageEnrollmentId: string,
): Promise<ApiResult<QueryStorageEnrollmentResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/enrollment/query', {
      body: { tenant_id: tenantId, storage_enrollment_id: storageEnrollmentId },
      params: versionHeader,
    }),
  );
}

export async function approveStorageEnrollment(
  request: ApproveStorageEnrollmentRequest,
): Promise<ApiResult<ApproveStorageEnrollmentResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/enrollment/approve', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function rejectStorageEnrollment(
  request: RejectStorageEnrollmentRequest,
): Promise<ApiResult<RejectStorageEnrollmentResponse>> {
  return unwrap(
    await apiClient.POST('/api/storage/enrollment/reject', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryProjectList(
  request: QueryProjectListRequest,
): Promise<ApiResult<QueryProjectListResponse>> {
  return unwrap(
    await apiClient.POST('/api/project/list/query', { body: request, params: versionHeader }),
  );
}

export async function createProject(
  request: CreateProjectRequest,
): Promise<ApiResult<CreateProjectResponse>> {
  return unwrap(
    await apiClient.POST('/api/project/create', { body: request, params: versionHeader }),
  );
}

export async function queryArtifactList(
  request: QueryArtifactListRequest,
): Promise<ApiResult<QueryArtifactListResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/list/query', { body: request, params: versionHeader }),
  ) as ApiResult<QueryArtifactListResponse>;
}

export async function queryArtifact(
  tenantId: string,
  projectId: string,
  artifactId: string,
): Promise<ApiResult<QueryArtifactResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/query', {
      body: { tenant_id: tenantId, project_id: projectId, artifact_id: artifactId },
      params: versionHeader,
    }),
  ) as ApiResult<QueryArtifactResponse>;
}

export async function createArtifact(
  request: CreateArtifactRequest,
): Promise<ApiResult<CreateArtifactResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/create', { body: request, params: versionHeader }),
  ) as ApiResult<CreateArtifactResponse>;
}

export async function queryArtifactCommitGraph(
  tenantId: string,
  projectId: string,
  artifactId: string,
  cursor?: string,
): Promise<ApiResult<QueryArtifactCommitGraphResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/commit/graph/query', {
      body: {
        tenant_id: tenantId,
        project_id: projectId,
        artifact_id: artifactId,
        page_size: 50,
        ...(cursor ? { cursor } : {}),
      },
      params: versionHeader,
    }),
  );
}

export async function queryArtifactCommitDiff(
  tenantId: string,
  projectId: string,
  artifactId: string,
  commitId: string,
  baseCommitId?: string,
): Promise<ApiResult<QueryArtifactCommitDiffResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/commit/diff/query', {
      body: {
        tenant_id: tenantId,
        project_id: projectId,
        artifact_id: artifactId,
        commit_id: commitId,
        ...(baseCommitId ? { base_commit_id: baseCommitId } : {}),
      },
      params: versionHeader,
    }),
  );
}

export async function queryPlaygroundList(
  request: QueryPlaygroundListRequest,
): Promise<ApiResult<QueryPlaygroundListResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/list/query', { body: request, params: versionHeader }),
  ) as ApiResult<QueryPlaygroundListResponse>;
}

export async function queryPlayground(
  tenantId: string,
  projectId: string,
  artifactId: string,
  playgroundId: string,
): Promise<ApiResult<QueryPlaygroundResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/query', {
      body: {
        tenant_id: tenantId,
        project_id: projectId,
        artifact_id: artifactId,
        playground_id: playgroundId,
      },
      params: versionHeader,
    }),
  ) as ApiResult<QueryPlaygroundResponse>;
}

export async function createPlayground(
  request: CreatePlaygroundRequest,
): Promise<ApiResult<CreatePlaygroundResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/create', { body: request, params: versionHeader }),
  ) as ApiResult<CreatePlaygroundResponse>;
}

export async function startPlaygroundPreCommit(
  request: StartPreCommitRequest,
): Promise<ApiResult<StartPreCommitResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/precommit/start', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryPlaygroundPreCommit(
  tenantId: string,
  precommitId: string,
): Promise<ApiResult<QueryPreCommitResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/precommit/query', {
      body: { tenant_id: tenantId, precommit_id: precommitId },
      params: versionHeader,
    }),
  );
}

export async function restartPlaygroundPreCommit(
  request: RestartPreCommitRequest,
): Promise<ApiResult<RestartPreCommitResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/precommit/restart', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function cancelPlaygroundPreCommit(
  request: CancelPreCommitRequest,
): Promise<ApiResult<CancelPreCommitResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/precommit/cancel', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function commitPlayground(
  request: CommitPlaygroundRequest,
): Promise<ApiResult<CommitPlaygroundResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/commit/create', {
      body: request,
      params: versionHeader,
    }),
  ) as ApiResult<CommitPlaygroundResponse>;
}

export async function queryPlaygroundFileList(
  request: QueryPlaygroundFileListRequest,
): Promise<ApiResult<QueryPlaygroundFileListResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/file/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryPlaygroundChangeList(
  request: QueryPlaygroundChangeListRequest,
): Promise<ApiResult<QueryPlaygroundChangeListResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/change/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryPlaygroundFileMetadata(
  request: QueryPlaygroundFileMetadataRequest,
): Promise<ApiResult<QueryPlaygroundFileMetadataResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/file/metadata/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryPlaygroundDatasetProfile(
  request: QueryPlaygroundDatasetProfileRequest,
): Promise<ApiResult<QueryPlaygroundDatasetProfileResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/dataset/profile/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotList(
  request: QuerySnapshotListRequest,
): Promise<ApiResult<QuerySnapshotListResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/list/query', { body: request, params: versionHeader }),
  ) as ApiResult<QuerySnapshotListResponse>;
}

export async function querySnapshot(
  tenantId: string,
  snapshotId: string,
): Promise<ApiResult<QuerySnapshotResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/query', {
      body: { tenant_id: tenantId, snapshot_id: snapshotId },
      params: versionHeader,
    }),
  );
}

export async function createSnapshot(
  request: CreateSnapshotRequest,
): Promise<ApiResult<CreateSnapshotResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/create', { body: request, params: versionHeader }),
  ) as ApiResult<CreateSnapshotResponse>;
}

/** Start a multi-source object materialization on a target Volume. */
export async function materializeCommit(
  request: CreateCommitMaterializationRequest,
): Promise<ApiResult<CreateCommitMaterializationResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/materialize', { body: request, params: versionHeader }),
  );
}

export async function queryCommitMaterialization(
  request: QueryCommitMaterializationRequest,
): Promise<ApiResult<QueryCommitMaterializationResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/materialization/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryCommitMaterializationList(
  request: QueryCommitMaterializationListRequest,
): Promise<ApiResult<QueryCommitMaterializationListResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/materialization/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryCommitCoverage(
  request: QueryCommitCoverageRequest,
): Promise<ApiResult<QueryCommitCoverageResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/coverage/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function retryCommitMaterialization(
  request: RetryCommitMaterializationRequest,
): Promise<ApiResult<RetryCommitMaterializationResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/materialization/retry', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function cancelCommitMaterialization(
  request: CancelCommitMaterializationRequest,
): Promise<ApiResult<CancelCommitMaterializationResponse>> {
  return unwrap(
    await apiClient.POST('/api/commit/materialization/cancel', {
      body: request,
      params: versionHeader,
    }),
  );
}

// Legacy UI names remain local aliases while pages migrate to the Materialization terminology.
// They call only the v2 routes; the removed v1 paths are never emitted by the client.
export async function replicateCommit(
  request: CreateCommitReplicationRequest,
): Promise<ApiResult<CreateCommitReplicationResponse>> {
  const result = await materializeCommit(materializationRequest(request));
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: {
      replication: toLegacyMaterialization(payload.materialization),
      replayed: payload.replayed,
    },
  };
}

export async function queryCommitReplication(
  request: QueryCommitReplicationRequest,
): Promise<ApiResult<QueryCommitReplicationResponse>> {
  const result = await queryCommitMaterialization({
    materialization_id: request.replication_id,
    tenant_id: request.tenant_id,
    object_namespace_id: request.object_namespace_id,
  });
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: { replication: toLegacyMaterialization(payload.materialization) },
  };
}

export async function queryCommitReplicationList(
  request: QueryCommitReplicationListRequest,
): Promise<ApiResult<QueryCommitReplicationListResponse>> {
  const result = await queryCommitMaterializationList(materializationListRequest(request));
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: {
      replications: payload.materializations.map(toLegacyMaterialization),
      ...(payload.next_cursor === undefined ? {} : { next_cursor: payload.next_cursor }),
    },
  };
}

export async function queryCommitPlacementList(
  request: QueryCommitPlacementListRequest,
): Promise<ApiResult<QueryCommitPlacementListResponse>> {
  const result = await queryCommitCoverage(coverageRequest(request));
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: {
      placements: payload.coverage.map(toLegacyCoverage),
      ...(payload.next_cursor === undefined ? {} : { next_cursor: payload.next_cursor }),
    },
  };
}

export async function retryCommitReplication(
  request: RetryCommitReplicationRequest,
): Promise<ApiResult<RetryCommitReplicationResponse>> {
  const result = await retryCommitMaterialization({
    tenant_id: request.tenant_id,
    object_namespace_id: request.object_namespace_id,
    materialization_id: request.replication_id,
    expected_plan_revision: request.expected_attempt,
    request_id: request.request_id,
  });
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: {
      replication: toLegacyMaterialization(payload.materialization),
      replayed: payload.replayed,
    },
  };
}

export async function cancelCommitReplication(
  request: CancelCommitReplicationRequest,
): Promise<ApiResult<CancelCommitReplicationResponse>> {
  const result = await cancelCommitMaterialization({
    tenant_id: request.tenant_id,
    object_namespace_id: request.object_namespace_id,
    materialization_id: request.replication_id,
    expected_plan_revision: request.expected_attempt,
  });
  const payload = result.data;
  return {
    requestId: result.requestId,
    data: { replication: toLegacyMaterialization(payload.materialization) },
  };
}

export async function queryCommitAvailability(
  request: QueryCommitAvailabilityRequest,
): Promise<ApiResult<QueryCommitAvailabilityResponse>> {
  const objectNamespaceId = request.object_namespace_id ?? request.artifact_id;
  if (!objectNamespaceId) {
    throw new Error('object_namespace_id is required for v2 availability queries');
  }
  const result = unwrap(
    await apiClient.POST('/api/commit/availability/query', {
      body: {
        tenant_id: request.tenant_id,
        commit_id: request.commit_id,
        ...(request.target_storage_volume_id === undefined
          ? {}
          : { target_storage_volume_id: request.target_storage_volume_id }),
        ...(request.cursor === undefined ? {} : { cursor: request.cursor }),
        ...(request.page_size === undefined ? {} : { page_size: request.page_size }),
        object_namespace_id: objectNamespaceId,
      },
      params: versionHeader,
    }),
  );
  const availability = result.data.availability;
  return {
    requestId: result.requestId,
    data: {
      availability: {
        commit_id: availability.commit_id,
        data_health: availability.content_presence,
        verified_placements: availability.complete_volume_count,
        missing_objects: String(availability.missing_objects.length),
        verified_storage_volume_ids: availability.verified_storage_volume_ids,
      },
    },
  };
}

export async function createWorkspace(
  request: CreateWorkspaceRequest,
): Promise<ApiResult<CreateWorkspaceResponse>> {
  return unwrap(
    await apiClient.POST('/api/workspace/create', { body: request, params: versionHeader }),
  );
}

export async function retrySnapshotDelivery(
  request: RetrySnapshotDeliveryRequest,
): Promise<ApiResult<RetrySnapshotDeliveryResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/delivery/retry', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createSnapshotDelivery(
  request: CreateSnapshotDeliveryRequest,
): Promise<ApiResult<CreateSnapshotDeliveryResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/delivery/create', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotDelivery(
  request: QuerySnapshotDeliveryRequest,
): Promise<ApiResult<QuerySnapshotDeliveryResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/delivery/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotDeliveryList(
  request: QuerySnapshotDeliveryListRequest,
): Promise<ApiResult<QuerySnapshotDeliveryListResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/delivery/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function deleteSnapshotDelivery(
  request: DeleteSnapshotDeliveryRequest,
): Promise<ApiResult<DeleteSnapshotDeliveryResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/delivery/delete', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotFileList(
  request: QuerySnapshotFileListRequest,
): Promise<ApiResult<QuerySnapshotFileListResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/file/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotActivityList(
  request: QuerySnapshotActivityListRequest,
): Promise<ApiResult<QuerySnapshotActivityListResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/activity/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function querySnapshotDatasetProfile(
  request: QuerySnapshotDatasetProfileRequest,
): Promise<ApiResult<QuerySnapshotDatasetProfileResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/dataset/profile/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryS3AccessPointList(
  request: QueryS3AccessPointListRequest,
): Promise<ApiResult<QueryS3AccessPointListResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/access-point/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryS3AccessPoint(
  request: QueryS3AccessPointRequest,
): Promise<ApiResult<QueryS3AccessPointResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/access-point/query', { body: request, params: versionHeader }),
  );
}

export async function createS3AccessPoint(
  request: CreateS3AccessPointRequest,
): Promise<ApiResult<CreateS3AccessPointResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/access-point/create', { body: request, params: versionHeader }),
  );
}

export async function enableS3AccessPoint(
  request: UpdateS3AccessPointRequest,
): Promise<ApiResult<UpdateS3AccessPointResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/access-point/enable', { body: request, params: versionHeader }),
  );
}

export async function disableS3AccessPoint(
  request: UpdateS3AccessPointRequest,
): Promise<ApiResult<UpdateS3AccessPointResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/access-point/disable', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createS3Credential(
  request: CreateS3CredentialRequest,
): Promise<ApiResult<CreateS3CredentialResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/credential/create', { body: request, params: versionHeader }),
  );
}

export async function queryS3CredentialList(
  request: QueryS3CredentialListRequest,
): Promise<ApiResult<QueryS3CredentialListResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/credential/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function revokeS3Credential(
  request: RevokeS3CredentialRequest,
): Promise<ApiResult<QueryS3CredentialListResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/credential/revoke', { body: request, params: versionHeader }),
  );
}

export async function queryS3ObjectList(
  request: QueryS3ObjectListRequest,
): Promise<ApiResult<QueryS3ObjectListResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/object/list/query', { body: request, params: versionHeader }),
  );
}

export async function createS3DownloadUrl(
  request: CreateS3DownloadUrlRequest,
): Promise<ApiResult<CreateS3DownloadUrlResponse>> {
  return unwrap(
    await apiClient.POST('/api/s3/object/download-url/create', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryDeletionImpact(
  request: QueryDeletionImpactRequest,
): Promise<ApiResult<QueryDeletionImpactResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/impact/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createDeletion(
  request: CreateDeletionRequest,
): Promise<ApiResult<DeletionMutationResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/create', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryDeletion(
  request: QueryDeletionRequest,
): Promise<ApiResult<QueryDeletionResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function queryDeletionList(
  request: QueryDeletionListRequest,
): Promise<ApiResult<QueryDeletionListResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/list/query', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function restoreDeletion(
  request: UpdateDeletionRequest,
): Promise<ApiResult<DeletionMutationResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/restore', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function retryDeletion(
  request: UpdateDeletionRequest,
): Promise<ApiResult<DeletionMutationResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/deletion/retry', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createRetentionHold(
  request: CreateRetentionHoldRequest,
): Promise<ApiResult<CreateRetentionHoldResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/retention-hold/create', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function releaseRetentionHold(
  request: ReleaseRetentionHoldRequest,
): Promise<ApiResult<ReleaseRetentionHoldResponse>> {
  return unwrap(
    await apiClient.POST('/api/resource/retention-hold/release', {
      body: request,
      params: versionHeader,
    }),
  );
}

export async function createAddJob(
  request: CreateAddJobRequest,
): Promise<ApiResult<CreateAddJobResponse>> {
  return unwrap(
    await apiClient.POST('/api/job/add/create', {
      body: request,
      params: { header: { 'NeoEngram-API-Version': '1' } },
    }),
  );
}

export async function queryJob(
  tenantId: string,
  jobId: string,
): Promise<ApiResult<QueryJobResponse>> {
  return unwrap(
    await apiClient.POST('/api/job/query', {
      body: { tenant_id: tenantId, job_id: jobId },
      params: { header: { 'NeoEngram-API-Version': '1' } },
    }),
  );
}

export async function finalizeAddJob(
  tenantId: string,
  jobId: string,
): Promise<ApiResult<FinalizeAddJobResponse>> {
  return unwrap(
    await apiClient.POST('/api/job/add/finalize', {
      body: { tenant_id: tenantId, job_id: jobId },
      params: { header: { 'NeoEngram-API-Version': '1' } },
    }),
  );
}
