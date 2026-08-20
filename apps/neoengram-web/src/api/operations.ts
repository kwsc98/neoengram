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
