import { apiClient } from './client';
import { toApiProblem } from './problem';
import type {
  ApiVersionResponse,
  CommitPlaygroundRequest,
  CommitPlaygroundResponse,
  CreateAddJobRequest,
  CreateAddJobResponse,
  CreateArtifactRequest,
  CreateArtifactResponse,
  CreatePlaygroundRequest,
  CreatePlaygroundResponse,
  CreateSnapshotRequest,
  CreateSnapshotResponse,
  CreateStorageVolumeRequest,
  CreateStorageVolumeResponse,
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
  QueryPlaygroundResponse,
  QueryProjectListRequest,
  QueryProjectListResponse,
  QuerySnapshotListRequest,
  QuerySnapshotListResponse,
  QuerySnapshotResponse,
  QueryStorageVolumeListRequest,
  QueryStorageVolumeListResponse,
  QueryStorageVolumeResponse,
  QueryTenantListRequest,
  QueryTenantListResponse,
  QueryTenantResponse,
  QueryJobResponse,
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

export async function queryProjectList(
  request: QueryProjectListRequest,
): Promise<ApiResult<QueryProjectListResponse>> {
  return unwrap(
    await apiClient.POST('/api/project/list/query', { body: request, params: versionHeader }),
  );
}

export async function queryArtifactList(
  request: QueryArtifactListRequest,
): Promise<ApiResult<QueryArtifactListResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/list/query', { body: request, params: versionHeader }),
  );
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
  );
}

export async function createArtifact(
  request: CreateArtifactRequest,
): Promise<ApiResult<CreateArtifactResponse>> {
  return unwrap(
    await apiClient.POST('/api/artifact/create', { body: request, params: versionHeader }),
  );
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
  );
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
  );
}

export async function createPlayground(
  request: CreatePlaygroundRequest,
): Promise<ApiResult<CreatePlaygroundResponse>> {
  return unwrap(
    await apiClient.POST('/api/playground/create', { body: request, params: versionHeader }),
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
  );
}

export async function querySnapshotList(
  request: QuerySnapshotListRequest,
): Promise<ApiResult<QuerySnapshotListResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/list/query', { body: request, params: versionHeader }),
  );
}

export async function querySnapshot(
  tenantId: string,
  projectId: string,
  artifactId: string,
  commitId: string,
): Promise<ApiResult<QuerySnapshotResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/query', {
      body: {
        tenant_id: tenantId,
        project_id: projectId,
        artifact_id: artifactId,
        commit_id: commitId,
      },
      params: versionHeader,
    }),
  );
}

export async function createSnapshot(
  request: CreateSnapshotRequest,
): Promise<ApiResult<CreateSnapshotResponse>> {
  return unwrap(
    await apiClient.POST('/api/snapshot/create', { body: request, params: versionHeader }),
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
