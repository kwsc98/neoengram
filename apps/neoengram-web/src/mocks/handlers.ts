import { http, HttpResponse } from 'msw';

import type {
  ArtifactView,
  CancelPreCommitRequest,
  CancelPreCommitResponse,
  CommitPlaygroundRequest,
  CommitPlaygroundResponse,
  CommitDiffEntry,
  CommitNode,
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
  CreateTenantRequest,
  CreateTenantResponse,
  FinalizeAddJobResponse,
  JobView,
  PreCommitView,
  ProblemDetails,
  QueryArtifactListRequest,
  QueryArtifactListResponse,
  QueryArtifactCommitDiffResponse,
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
  RestartPreCommitRequest,
  RestartPreCommitResponse,
  RetrySnapshotDeliveryRequest,
  RetrySnapshotDeliveryResponse,
  StartPreCommitRequest,
  StartPreCommitResponse,
  QueryStorageVolumeListRequest,
  QueryStorageVolumeListResponse,
  QueryStorageVolumeResponse,
  QueryTenantListRequest,
  QueryTenantListResponse,
  QueryTenantResponse,
  QueryJobResponse,
  StorageVolumeView,
  TenantView,
} from '@/api/types';

import {
  artifacts,
  commitGraphs,
  playgrounds,
  projects,
  resourceKey,
  resetMockData,
  snapshots,
  storageVolumes,
  tenants,
} from './data';

interface StoredJob {
  request: CreateAddJobRequest;
  requestJson: string;
  job: JobView;
  queryCount: number;
  finalizedAt?: string;
}

interface PageRequest {
  cursor?: string;
  page_size?: number;
}

interface PageResult<T> {
  items: T[];
  next_cursor?: string;
}

const jobs = new Map<string, StoredJob>();
const tenantCreatePayloads = new Map<string, string>();
const storageVolumeCreatePayloads = new Map<string, string>();
const artifactCreatePayloads = new Map<string, string>();
const playgroundCreatePayloads = new Map<string, string>();
const playgroundReadyAt = new Map<string, number>();
const commitRequests = new Map<
  string,
  { requestJson: string; response: CommitPlaygroundResponse }
>();
const precommits = new Map<string, PreCommitView>();
const precommitMutationRequests = new Map<string, string>();
const snapshotCreateRequests = new Map<
  string,
  { requestJson: string; response: CreateSnapshotResponse }
>();
const snapshotRetryRequests = new Map<
  string,
  { requestJson: string; response: RetrySnapshotDeliveryResponse }
>();

const snapshotReadyAt = new Map<string, number>();

function requestId(request: Request): string {
  return request.headers.get('X-Request-ID') ?? `mock-fallback-${crypto.randomUUID()}`;
}

function headers(request: Request, contentType = 'application/json'): HeadersInit {
  return { 'Content-Type': contentType, 'X-Request-ID': requestId(request) };
}

function problem(
  request: Request,
  status: number,
  code: string,
  title: string,
  detail: string,
  options: Pick<ProblemDetails, 'retryable' | 'retry_after_ms' | 'violations'> = {
    retryable: false,
  },
): HttpResponse<ProblemDetails> {
  return HttpResponse.json(
    {
      type: `urn:neoengram:problem:${code.toLowerCase().replaceAll('_', '-')}`,
      title,
      status,
      detail,
      instance: new URL(request.url).pathname,
      code,
      request_id: requestId(request),
      retryable: options.retryable,
      ...(options.retry_after_ms ? { retry_after_ms: options.retry_after_ms } : {}),
      ...(options.violations ? { violations: options.violations } : {}),
    },
    { status, headers: headers(request, 'application/problem+json') },
  );
}

function authorize(request: Request): HttpResponse<ProblemDetails> | null {
  if (request.headers.get('NeoEngram-API-Version') !== '1') {
    return problem(
      request,
      422,
      'API_VERSION_UNSUPPORTED',
      'API version is unsupported',
      'NeoEngram-API-Version must be 1',
      {
        retryable: false,
        violations: [{ field: 'NeoEngram-API-Version', reason: 'unsupported value' }],
      },
    );
  }
  const authorization = request.headers.get('Authorization');
  if (!['Bearer mock-access-token', 'Bearer mock-reader-token'].includes(authorization ?? '')) {
    return problem(
      request,
      401,
      'AUTHENTICATION_REQUIRED',
      'Authentication required',
      'A valid Bearer token is required',
    );
  }
  return null;
}

function isAdmin(request: Request): boolean {
  return request.headers.get('Authorization') === 'Bearer mock-access-token';
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([name, child]) => `${JSON.stringify(name)}:${stableJson(child)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function fingerprint(value: unknown): string {
  let hash = 2166136261;
  for (const character of stableJson(value)) {
    hash ^= character.codePointAt(0) ?? 0;
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function paginate<T>(
  request: Request,
  resource: string,
  filters: unknown,
  values: T[],
  page: PageRequest,
): PageResult<T> | HttpResponse<ProblemDetails> {
  const pageSize = page.page_size ?? 50;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 100) {
    return problem(
      request,
      422,
      'PROTOCOL_INVALID',
      'Request validation failed',
      'Invalid page_size',
      {
        retryable: false,
        violations: [{ field: 'page_size', reason: 'must be an integer between 1 and 100' }],
      },
    );
  }

  const expectedFingerprint = fingerprint(filters);
  let offset = 0;
  if (page.cursor) {
    const [prefix, cursorResource, rawOffset, cursorFingerprint] = page.cursor.split(':');
    offset = Number(rawOffset);
    if (
      prefix !== 'mock' ||
      cursorResource !== resource ||
      cursorFingerprint !== expectedFingerprint ||
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      offset > values.length
    ) {
      return problem(
        request,
        409,
        'CURSOR_INVALID',
        'Cursor is invalid',
        'The cursor is invalid or does not match the current query',
      );
    }
  }

  const items = values.slice(offset, offset + pageSize);
  const nextOffset = offset + items.length;
  return {
    items,
    ...(nextOffset < values.length
      ? { next_cursor: `mock:${resource}:${nextOffset}:${expectedFingerprint}` }
      : {}),
  };
}

function unavailable(request: Request, tenantId: string): HttpResponse<ProblemDetails> | null {
  if (tenantId !== 'tenant-unavailable') return null;
  return problem(
    request,
    503,
    'STORAGE_FAILURE',
    'Authority unavailable',
    'The authority store is temporarily unavailable',
    { retryable: true, retry_after_ms: '1000' },
  );
}

function requireTenant(request: Request, tenantId: string): HttpResponse<ProblemDetails> | null {
  const failed = unavailable(request, tenantId);
  if (failed) return failed;
  if (tenants.some((tenant) => tenant.tenant_id === tenantId)) return null;
  return problem(
    request,
    404,
    'TENANT_NOT_FOUND',
    'Tenant not found',
    'The requested Tenant was not found',
  );
}

function requireMutationAccess(
  request: Request,
  tenantId: string,
): HttpResponse<ProblemDetails> | null {
  const failed = requireTenant(request, tenantId);
  if (failed) return failed;
  if (isAdmin(request)) return null;
  return problem(
    request,
    403,
    'AUTHORIZATION_DENIED',
    'Authorization denied',
    'The principal is not authorized to mutate this resource',
  );
}

function resolveStorageVolume(
  request: Request,
  tenantId: string,
  storageVolumeId: string,
): StorageVolumeView | HttpResponse<ProblemDetails> {
  const storageVolume = storageVolumes.find(
    (item) => item.tenant_id === tenantId && item.storage_volume_id === storageVolumeId,
  );
  if (!storageVolume) {
    return problem(
      request,
      404,
      'STORAGE_VOLUME_NOT_FOUND',
      'StorageVolume not found',
      'The selected StorageVolume does not exist in this Tenant',
    );
  }
  if (storageVolume.state === 'unavailable') {
    return mutationConflict(
      request,
      'STORAGE_VOLUME_UNAVAILABLE',
      'The selected StorageVolume is unavailable for new resource placement',
    );
  }
  return storageVolume;
}

function mutationConflict(
  request: Request,
  code: string,
  detail: string,
): HttpResponse<ProblemDetails> {
  return problem(request, 409, code, 'Resource mutation conflict', detail);
}

function notFound(
  request: Request,
  resource: 'Artifact' | 'Playground' | 'Snapshot',
): HttpResponse<ProblemDetails> {
  return problem(
    request,
    404,
    `${resource.toUpperCase()}_NOT_FOUND`,
    `${resource} not found`,
    `The requested ${resource} was not found`,
  );
}

function validateCreate(
  request: Request,
  body: CreateAddJobRequest,
): HttpResponse<ProblemDetails> | null {
  const failed = unavailable(request, body.tenant_id);
  if (failed) return failed;
  if (!body.all && body.paths.length === 0) {
    return problem(
      request,
      422,
      'PROTOCOL_INVALID',
      'Request validation failed',
      'paths must be non-empty unless all is true',
      {
        retryable: false,
        violations: [{ field: 'paths', reason: 'must be non-empty unless all is true' }],
      },
    );
  }
  if (body.project_id === 'project-invalid') {
    return problem(
      request,
      422,
      'PROTOCOL_INVALID',
      'Request validation failed',
      'project_id is reserved by the mock validation scenario',
      {
        retryable: false,
        violations: [{ field: 'project_id', reason: 'mock validation rejection' }],
      },
    );
  }
  if (BigInt(body.deadline_unix_ms) <= BigInt(Date.now())) {
    return problem(
      request,
      408,
      'JOB_DEADLINE_EXCEEDED',
      'Job deadline exceeded',
      'The managed Add deadline has elapsed',
    );
  }
  return null;
}

function advance(stored: StoredJob): void {
  if (stored.job.state === 'succeeded') return;
  stored.queryCount += 1;
  if (stored.queryCount === 1) {
    stored.job.state = 'running';
    stored.job.resource_version = '4';
    stored.job.progress = {
      state: 'running',
      phase: 'hashing',
      files_completed: '320',
      bytes_completed: '4294967296',
    };
  } else if (stored.queryCount >= 2) {
    stored.job.state = 'prepared';
    stored.job.resource_version = '6';
    stored.job.progress = {
      state: 'prepared',
      phase: 'metadata_ready',
      files_completed: '864',
      bytes_completed: '12884901888',
    };
  }
}

function mockCommitChanges(target: CommitNode, base?: CommitNode): CommitDiffEntry[] {
  if (!base) {
    return [
      {
        change_type: 'added',
        path: 'dataset/index.json',
        new_size_bytes: '3072',
      },
      {
        change_type: 'added',
        path: `dataset/commits/${target.commit_id}.jsonl`,
        new_size_bytes: '5120',
      },
    ];
  }
  return [
    {
      change_type: 'modified',
      path: 'dataset/index.json',
      old_size_bytes: '2048',
      new_size_bytes: '3072',
    },
    {
      change_type: 'added',
      path: `dataset/commits/${target.commit_id}.jsonl`,
      new_size_bytes: '5120',
    },
    {
      change_type: 'deleted',
      path: `dataset/commits/${base.commit_id}.tmp`,
      old_size_bytes: '512',
    },
  ];
}

function diffSummary(changes: CommitDiffEntry[]) {
  let bytesAdded = 0n;
  let bytesRemoved = 0n;
  for (const change of changes) {
    const oldSize = BigInt(change.old_size_bytes ?? '0');
    const newSize = BigInt(change.new_size_bytes ?? '0');
    if (change.change_type === 'added') bytesAdded += newSize;
    if (change.change_type === 'deleted') bytesRemoved += oldSize;
    if (change.change_type === 'modified') {
      if (newSize >= oldSize) bytesAdded += newSize - oldSize;
      else bytesRemoved += oldSize - newSize;
    }
  }
  return {
    files_added: changes.filter((change) => change.change_type === 'added').length.toString(),
    files_modified: changes.filter((change) => change.change_type === 'modified').length.toString(),
    files_deleted: changes.filter((change) => change.change_type === 'deleted').length.toString(),
    files_renamed: changes.filter((change) => change.change_type === 'renamed').length.toString(),
    bytes_added: bytesAdded.toString(),
    bytes_removed: bytesRemoved.toString(),
  };
}

const mockLogicalFiles: QueryPlaygroundFileListResponse['items'] = [
  {
    path: 'dataset/night-rain/part-0042.parquet',
    entry_type: 'file',
    size_bytes: '19971597926',
    format: 'parquet',
    row_count: '12842731',
    updated_at_unix_ms: '1785167500000',
  },
  {
    path: 'dataset/index.json',
    entry_type: 'file',
    size_bytes: '3250586',
    format: 'json',
    row_count: '18554',
    updated_at_unix_ms: '1785167400000',
  },
  {
    path: 'labels/reviewed/night-v4.jsonl',
    entry_type: 'file',
    size_bytes: '1503238554',
    format: 'jsonl',
    row_count: '18409216',
    updated_at_unix_ms: '1785167300000',
  },
];

const mockPlaygroundChanges: QueryPlaygroundChangeListResponse['items'] = [
  {
    change_type: 'modified',
    path: 'dataset/index.json',
    old_size_bytes: '2936012',
    new_size_bytes: '3250586',
    format: 'json',
  },
  {
    change_type: 'added',
    path: 'dataset/night-rain/part-0042.parquet',
    new_size_bytes: '19971597926',
    format: 'parquet',
  },
  {
    change_type: 'renamed',
    path: 'labels/reviewed/night-v4.jsonl',
    previous_path: 'labels/reviewed/night-final.jsonl',
    old_size_bytes: '1503238554',
    new_size_bytes: '1503238554',
    format: 'jsonl',
  },
  {
    change_type: 'deleted',
    path: 'labels/drafts/night-v3.tmp',
    old_size_bytes: '650117120',
    format: 'binary',
  },
];

const mockDatasetProfile: QueryPlaygroundDatasetProfileResponse['profile'] = {
  state: 'ready',
  summary: {
    format_count: 4,
    logical_file_count: '18554',
    logical_size_bytes: '845571686400',
    row_count: '312847219',
    field_count: 4,
  },
  schema: {
    fields: [
      { name: 'image_id', data_type: 'string', nullable: false },
      { name: 'scene', data_type: 'string', nullable: false },
      { name: 'captured_at', data_type: 'timestamp', nullable: false },
      { name: 'quality_score', data_type: 'float32', nullable: true },
    ],
  },
  statistics: { row_count: '312847219', column_count: 4, null_value_count: '1842' },
  quality: { state: 'warning', checks_total: 8, checks_passed: 7, checks_failed: 0 },
  freshness: {
    observed_at_unix_ms: '1785167600000',
    source_updated_at_unix_ms: '1785167500000',
    age_seconds: '100',
  },
};

export const handlers = [
  http.post('*/api/system/version/query', ({ request }) =>
    HttpResponse.json(
      {
        api_versions: [1],
        agent_protocol_versions: [1],
        capabilities: ['managed_add', 'resource_browser', 'tenant_admin', 'sqlite_authority'],
      },
      { headers: headers(request) },
    ),
  ),
  http.get('*/health/live', ({ request }) =>
    HttpResponse.json({ status: 'ok' }, { headers: headers(request) }),
  ),
  http.get('*/health/ready', ({ request }) =>
    HttpResponse.json({ status: 'ok' }, { headers: headers(request) }),
  ),
  http.post('*/api/tenant/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryTenantListRequest;
    const search = body.query?.toLocaleLowerCase('zh-CN');
    const filtered = tenants.filter(
      (tenant) =>
        !search ||
        tenant.tenant_id.toLocaleLowerCase('zh-CN').includes(search) ||
        tenant.display_name.toLocaleLowerCase('zh-CN').includes(search),
    );
    const page = paginate(request, 'tenants', { query: body.query ?? '' }, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryTenantListResponse = {
      ...page,
      can_create_tenant: isAdmin(request),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/tenant/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as { tenant_id: string };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const response: QueryTenantResponse = {
      tenant: tenants.find((tenant) => tenant.tenant_id === body.tenant_id)!,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/tenant/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    if (!isAdmin(request)) {
      return problem(
        request,
        403,
        'AUTHORIZATION_DENIED',
        'Authorization denied',
        'Only a platform administrator can create a Tenant',
      );
    }
    const body = (await request.json()) as CreateTenantRequest;
    if (!body.tenant_id?.trim() || !body.display_name?.trim()) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'tenant_id and display_name are required',
        {
          retryable: false,
          violations: [
            { field: 'tenant_id', reason: 'must not be empty' },
            { field: 'display_name', reason: 'must not be empty' },
          ],
        },
      );
    }
    const requestJson = stableJson(body);
    const existing = tenants.find((tenant) => tenant.tenant_id === body.tenant_id);
    if (existing) {
      if (tenantCreatePayloads.get(body.tenant_id) !== requestJson) {
        return problem(
          request,
          409,
          'TENANT_ID_REUSED',
          'Tenant ID reused',
          'The Tenant ID already belongs to a different create request',
        );
      }
      const response: CreateTenantResponse = { tenant: existing, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const now = Date.now().toString();
    const tenant: TenantView = {
      tenant_id: body.tenant_id,
      display_name: body.display_name,
      ...(body.description ? { description: body.description } : {}),
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
      permissions: [
        'tenant.admin',
        'tenant.read',
        'storage.read',
        'storage.create',
        'artifact.read',
        'artifact.create',
        'playground.create',
        'snapshot.create',
        'commit.create',
        'job.create',
      ],
    };
    tenants.push(tenant);
    tenantCreatePayloads.set(body.tenant_id, requestJson);
    const response: CreateTenantResponse = { tenant, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/volume/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryStorageVolumeListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const search = body.query?.toLocaleLowerCase('zh-CN');
    const filtered = storageVolumes.filter(
      (storageVolume) =>
        storageVolume.tenant_id === body.tenant_id &&
        (!body.region || storageVolume.region === body.region) &&
        (!body.backend_type || storageVolume.backend_type === body.backend_type) &&
        (!search ||
          storageVolume.storage_volume_id.toLocaleLowerCase('zh-CN').includes(search) ||
          storageVolume.display_name.toLocaleLowerCase('zh-CN').includes(search)),
    );
    const filters = {
      tenant_id: body.tenant_id,
      region: body.region ?? '',
      backend_type: body.backend_type ?? '',
      query: body.query ?? '',
    };
    const page = paginate(request, 'storage-volumes', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryStorageVolumeListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/volume/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      storage_volume_id: string;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const storageVolume = storageVolumes.find(
      (item) =>
        item.tenant_id === body.tenant_id && item.storage_volume_id === body.storage_volume_id,
    );
    if (!storageVolume) {
      return problem(
        request,
        404,
        'STORAGE_VOLUME_NOT_FOUND',
        'StorageVolume not found',
        'The requested StorageVolume was not found',
      );
    }
    const response: QueryStorageVolumeResponse = { storage_volume: storageVolume };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/volume/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateStorageVolumeRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
    const region = /^[a-z0-9][a-z0-9-]{0,63}$/;
    const backendReferenceValid =
      (body.backend_type === 'pvc' &&
        Boolean(body.pvc_reference?.namespace && body.pvc_reference.claim_name) &&
        !body.nfs_reference) ||
      (body.backend_type === 'nfs' &&
        Boolean(body.nfs_reference?.server && body.nfs_reference.export_path?.startsWith('/')) &&
        !body.pvc_reference);
    if (
      !resourceId.test(body.storage_volume_id) ||
      !resourceId.test(body.edge_cluster_id) ||
      !body.display_name.trim() ||
      !region.test(body.region) ||
      !['read_write_many', 'read_write_once', 'read_only_many'].includes(body.access_mode) ||
      !backendReferenceValid
    ) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'StorageVolume identity, region and backend reference must be valid',
        {
          retryable: false,
          violations: [
            { field: 'storage_volume_id', reason: 'must be a valid resource ID' },
            { field: 'backend_type', reason: 'must match exactly one backend reference' },
          ],
        },
      );
    }

    const key = resourceKey(body.tenant_id, body.storage_volume_id);
    const requestJson = stableJson(body);
    const existing = storageVolumes.find(
      (item) =>
        item.tenant_id === body.tenant_id && item.storage_volume_id === body.storage_volume_id,
    );
    if (existing) {
      if (storageVolumeCreatePayloads.get(key) !== requestJson) {
        return mutationConflict(
          request,
          'STORAGE_VOLUME_ID_REUSED',
          'The StorageVolume ID already belongs to a different registration request',
        );
      }
      const response: CreateStorageVolumeResponse = {
        storage_volume: existing,
        replayed: true,
      };
      return HttpResponse.json(response, { headers: headers(request) });
    }

    const now = Date.now().toString();
    const storageVolume: StorageVolumeView = {
      tenant_id: body.tenant_id,
      storage_volume_id: body.storage_volume_id,
      display_name: body.display_name.trim(),
      edge_cluster_id: body.edge_cluster_id,
      region: body.region,
      backend_type: body.backend_type,
      access_mode: body.access_mode,
      ...(body.backend_type === 'pvc' && body.pvc_reference
        ? { pvc_reference: body.pvc_reference }
        : {}),
      state: 'ready',
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    storageVolumes.push(storageVolume);
    storageVolumeCreatePayloads.set(key, requestJson);
    const response: CreateStorageVolumeResponse = {
      storage_volume: storageVolume,
      replayed: false,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/project/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryProjectListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const search = body.query?.toLocaleLowerCase('zh-CN');
    const filtered = projects.filter(
      (project) =>
        project.tenant_id === body.tenant_id &&
        (!search ||
          project.project_id.toLocaleLowerCase('zh-CN').includes(search) ||
          project.display_name.toLocaleLowerCase('zh-CN').includes(search)),
    );
    const page = paginate(
      request,
      'projects',
      { tenant_id: body.tenant_id, query: body.query ?? '' },
      filtered,
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QueryProjectListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/artifact/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryArtifactListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const search = body.query?.toLocaleLowerCase('zh-CN');
    const filtered = artifacts.filter(
      (artifact) =>
        artifact.tenant_id === body.tenant_id &&
        (!body.project_id || artifact.project_id === body.project_id) &&
        (!search ||
          artifact.artifact_id.toLocaleLowerCase('zh-CN').includes(search) ||
          artifact.display_name.toLocaleLowerCase('zh-CN').includes(search)),
    );
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id ?? '',
      query: body.query ?? '',
    };
    const page = paginate(request, 'artifacts', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryArtifactListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/artifact/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      project_id: string;
      artifact_id: string;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const artifact = artifacts.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id,
    );
    if (!artifact) return notFound(request, 'Artifact');
    const response: QueryArtifactResponse = { artifact };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/artifact/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateArtifactRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    if (
      !projects.some(
        (project) => project.tenant_id === body.tenant_id && project.project_id === body.project_id,
      )
    ) {
      return problem(
        request,
        404,
        'PROJECT_NOT_FOUND',
        'Project not found',
        'The requested Project was not found',
      );
    }
    const initialization = body.initialization;
    let sourceCommit: CommitNode | undefined;
    if (initialization.mode === 'derived') {
      const sourceArtifact = artifacts.find(
        (artifact) =>
          artifact.tenant_id === body.tenant_id &&
          artifact.project_id === initialization.source_project_id &&
          artifact.artifact_id === initialization.source_artifact_id,
      );
      const sourceGraph = sourceArtifact
        ? commitGraphs.get(
            resourceKey(
              sourceArtifact.tenant_id,
              sourceArtifact.project_id,
              sourceArtifact.artifact_id,
            ),
          )
        : undefined;
      sourceCommit = sourceGraph?.nodes.find(
        (commit) => commit.commit_id === initialization.source_commit_id,
      );
      if (!sourceArtifact || !sourceCommit) {
        return problem(
          request,
          404,
          'SOURCE_COMMIT_NOT_FOUND',
          'Source Commit not found',
          'The selected source Artifact or Commit was not found in this Tenant',
        );
      }
    }

    const key = resourceKey(body.tenant_id, body.project_id, body.artifact_id);
    const requestJson = stableJson(body);
    const existing = artifacts.find(
      (artifact) =>
        artifact.tenant_id === body.tenant_id &&
        artifact.project_id === body.project_id &&
        artifact.artifact_id === body.artifact_id,
    );
    if (existing) {
      const priorRequest = artifactCreatePayloads.get(key);
      const equivalentExisting =
        existing.display_name === body.display_name.trim() &&
        existing.description === body.description?.trim() &&
        stableJson(existing.initialization) === stableJson(initialization);
      if (
        (priorRequest && priorRequest !== requestJson) ||
        (!priorRequest && !equivalentExisting)
      ) {
        return mutationConflict(
          request,
          'ARTIFACT_ID_REUSED',
          'The Artifact ID already belongs to a different create request',
        );
      }
      const response: CreateArtifactResponse = { artifact: existing, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }

    const now = Date.now().toString();
    const rootCommit: CommitNode | undefined =
      sourceCommit && initialization.mode === 'derived'
        ? {
            commit_id: `commit-root-${body.artifact_id}-${Date.now().toString(36)}`,
            message: `从 ${initialization.source_artifact_id} 派生`,
            description: `初始化来源 ${initialization.source_artifact_id}@${sourceCommit.commit_id}，后续版本历史独立演进。`,
            tag_names: [],
            created_at_unix_ms: now,
          }
        : undefined;
    const artifact: ArtifactView = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      display_name: body.display_name.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      initialization: structuredClone(initialization),
      ...(rootCommit ? { head_commit_id: rootCommit.commit_id } : {}),
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    artifacts.push(artifact);
    if (rootCommit) {
      commitGraphs.set(key, {
        graph_version: '1',
        head_commit_id: rootCommit.commit_id,
        nodes: [rootCommit],
      });
    } else {
      commitGraphs.set(key, { graph_version: '0', nodes: [] });
    }
    artifactCreatePayloads.set(key, requestJson);
    const response: CreateArtifactResponse = { artifact, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/artifact/commit/graph/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      project_id: string;
      artifact_id: string;
      cursor?: string;
      page_size?: number;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const graph = commitGraphs.get(resourceKey(body.tenant_id, body.project_id, body.artifact_id));
    if (!graph) return notFound(request, 'Artifact');
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      graph_version: graph.graph_version,
    };
    const page = paginate(request, 'commits', filters, graph.nodes, body);
    if (page instanceof HttpResponse) return page;
    return HttpResponse.json(
      {
        graph: {
          graph_version: graph.graph_version,
          ...(graph.head_commit_id ? { head_commit_id: graph.head_commit_id } : {}),
          nodes: page.items,
          ...(page.next_cursor ? { next_cursor: page.next_cursor } : {}),
        },
      },
      { headers: headers(request) },
    );
  }),
  http.post('*/api/artifact/commit/diff/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      project_id: string;
      artifact_id: string;
      commit_id: string;
      base_commit_id?: string;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const graph = commitGraphs.get(resourceKey(body.tenant_id, body.project_id, body.artifact_id));
    if (!graph) return notFound(request, 'Artifact');
    const target = graph.nodes.find((node) => node.commit_id === body.commit_id);
    if (!target) {
      return problem(
        request,
        404,
        'COMMIT_NOT_FOUND',
        'Commit not found',
        'The target Commit was not found in this Artifact',
      );
    }
    const baseCommitId = body.base_commit_id ?? target.parent_commit_id;
    if (baseCommitId === target.commit_id) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'base_commit_id must differ from commit_id',
        {
          retryable: false,
          violations: [{ field: 'base_commit_id', reason: 'must differ from commit_id' }],
        },
      );
    }
    const base = baseCommitId
      ? graph.nodes.find((node) => node.commit_id === baseCommitId)
      : undefined;
    if (baseCommitId && !base) {
      return problem(
        request,
        404,
        'COMMIT_NOT_FOUND',
        'Commit not found',
        'The base Commit was not found in this Artifact',
      );
    }
    const changes = mockCommitChanges(target, base);
    const response: QueryArtifactCommitDiffResponse = {
      diff: {
        ...(base ? { base_commit: base } : {}),
        target_commit: target,
        summary: diffSummary(changes),
        changes,
      },
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryPlaygroundListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    if (body.artifact_id && !body.project_id) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'project_id is required when artifact_id is provided',
        {
          retryable: false,
          violations: [{ field: 'project_id', reason: 'required with artifact_id' }],
        },
      );
    }
    const search = body.query?.toLocaleLowerCase('zh-CN');
    const filtered = playgrounds.filter(
      (playground) =>
        playground.tenant_id === body.tenant_id &&
        (!body.project_id || playground.project_id === body.project_id) &&
        (!body.artifact_id || playground.artifact_id === body.artifact_id) &&
        (!search ||
          playground.playground_id.toLocaleLowerCase('zh-CN').includes(search) ||
          playground.display_name.toLocaleLowerCase('zh-CN').includes(search)),
    );
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id ?? '',
      artifact_id: body.artifact_id ?? '',
      query: body.query ?? '',
    };
    const page = paginate(request, 'playgrounds', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryPlaygroundListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      project_id: string;
      artifact_id: string;
      playground_id: string;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    const playgroundKey = resourceKey(
      playground.tenant_id,
      playground.project_id,
      playground.artifact_id,
      playground.playground_id,
    );
    const readyAt = playgroundReadyAt.get(playgroundKey);
    if (playground.state === 'creating' && readyAt && Date.now() >= readyAt) {
      playground.state = 'ready';
      playground.updated_at_unix_ms = Date.now().toString();
      playgroundReadyAt.delete(playgroundKey);
    }
    const response: QueryPlaygroundResponse = { playground };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreatePlaygroundRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const artifactKey = resourceKey(body.tenant_id, body.project_id, body.artifact_id);
    const artifact = artifacts.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id,
    );
    const graph = commitGraphs.get(artifactKey);
    if (!artifact || !graph) return notFound(request, 'Artifact');
    const storageVolume = resolveStorageVolume(request, body.tenant_id, body.storage_volume_id);
    if (storageVolume instanceof HttpResponse) return storageVolume;
    if (
      body.base_commit_id &&
      !graph.nodes.some((node) => node.commit_id === body.base_commit_id)
    ) {
      return problem(
        request,
        404,
        'COMMIT_NOT_FOUND',
        'Commit not found',
        'The requested base Commit was not found',
      );
    }

    const key = resourceKey(body.tenant_id, body.project_id, body.artifact_id, body.playground_id);
    const requestJson = stableJson(body);
    const existing = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (existing) {
      const priorRequest = playgroundCreatePayloads.get(key);
      const equivalentExisting =
        existing.display_name === body.display_name.trim() &&
        existing.storage_volume_id === body.storage_volume_id &&
        existing.base_commit_id === body.base_commit_id;
      if (
        (priorRequest && priorRequest !== requestJson) ||
        (!priorRequest && !equivalentExisting)
      ) {
        return mutationConflict(
          request,
          'PLAYGROUND_ID_REUSED',
          'The Playground ID already belongs to a different create request',
        );
      }
      const response: CreatePlaygroundResponse = { playground: existing, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }

    const baseCommitId = body.base_commit_id ?? graph.head_commit_id;
    const now = Date.now().toString();
    const playground = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      playground_id: body.playground_id,
      storage_volume_id: storageVolume.storage_volume_id,
      region: storageVolume.region,
      display_name: body.display_name.trim(),
      ...(baseCommitId ? { base_commit_id: baseCommitId, head_commit_id: baseCommitId } : {}),
      index_version: { revision: '0', digest: '0'.repeat(64) },
      state: 'creating' as const,
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    playgrounds.push(playground);
    playgroundCreatePayloads.set(key, requestJson);
    playgroundReadyAt.set(key, Date.now() + 800);
    const response: CreatePlaygroundResponse = { playground, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/precommit/start', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as StartPreCommitRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    if (playground.state !== 'ready') {
      return mutationConflict(
        request,
        'PLAYGROUND_NOT_READY',
        'Only a Ready Playground can start Pre-commit',
      );
    }
    if (
      playground.index_version.revision !== body.expected_index_version.revision ||
      playground.index_version.digest !== body.expected_index_version.digest
    ) {
      return mutationConflict(
        request,
        'INDEX_VERSION_CONFLICT',
        'The expected IndexVersion no longer matches the Playground',
      );
    }

    const requestKey = resourceKey(body.tenant_id, 'precommit-start', body.precommit_request_id);
    const requestJson = stableJson(body);
    const priorRequest = precommitMutationRequests.get(requestKey);
    if (priorRequest && priorRequest !== requestJson) {
      return mutationConflict(
        request,
        'PRECOMMIT_REQUEST_ID_REUSED',
        'The Pre-commit request ID already belongs to a different request',
      );
    }
    if (priorRequest) {
      const existing = [...precommits.values()].find(
        (item) =>
          item.tenant_id === body.tenant_id &&
          item.precommit_request_id === body.precommit_request_id,
      )!;
      const response: StartPreCommitResponse = { precommit: existing, playground, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    if (playground.active_precommit_id) {
      return mutationConflict(
        request,
        'PRECOMMIT_ALREADY_ACTIVE',
        'The Playground already has an active Pre-commit',
      );
    }

    const now = Date.now().toString();
    const precommitId = `precommit-${fingerprint(body)}`;
    const precommit: PreCommitView = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      playground_id: body.playground_id,
      precommit_id: precommitId,
      precommit_request_id: body.precommit_request_id,
      attempt: 1,
      state: 'running',
      phase: 'queued',
      progress: { percent: 0, files_completed: '0', bytes_completed: '0' },
      checks: [],
      warnings: [],
      blockers: [],
      source_index_version: structuredClone(body.expected_index_version),
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    precommits.set(resourceKey(body.tenant_id, precommitId), precommit);
    precommitMutationRequests.set(requestKey, requestJson);
    playground.active_precommit_id = precommitId;
    playground.updated_at_unix_ms = now;
    const response: StartPreCommitResponse = { precommit, playground, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/precommit/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as { tenant_id: string; precommit_id: string };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const precommit = precommits.get(resourceKey(body.tenant_id, body.precommit_id));
    if (!precommit) {
      return problem(
        request,
        404,
        'PRECOMMIT_NOT_FOUND',
        'Pre-commit not found',
        'The requested Pre-commit was not found',
      );
    }
    if (precommit.state === 'running') {
      const now = Date.now().toString();
      if (precommit.precommit_request_id.includes('fail')) {
        precommit.state = 'abnormal';
        precommit.phase = 'idle';
        precommit.issue = {
          code: 'PRECOMMIT_VALIDATION_FAILED',
          message: '候选元数据未通过一致性校验。',
          retryable: true,
          occurred_at_unix_ms: now,
        };
        precommit.blockers = [
          { code: 'PRECOMMIT_VALIDATION_FAILED', message: '候选元数据未通过一致性校验。' },
        ];
      } else {
        precommit.state = 'ready';
        precommit.phase = 'idle';
        precommit.progress = {
          percent: 100,
          files_completed: '18554',
          files_total: '18554',
          bytes_completed: '845571686400',
          bytes_total: '845571686400',
        };
        precommit.checks = [
          { check_id: 'metadata-shape', status: 'passed', summary: 'Metadata 和逻辑路径检查通过' },
        ];
        precommit.candidate_index_version = structuredClone(precommit.source_index_version);
        precommit.diff_summary = {
          files_added: '2',
          files_modified: '1',
          files_deleted: '1',
          files_renamed: '1',
          bytes_added: '19971604070',
          bytes_removed: '650117120',
        };
      }
      precommit.updated_at_unix_ms = now;
    }
    const response: QueryPreCommitResponse = { precommit };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/precommit/restart', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as RestartPreCommitRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const precommit = precommits.get(resourceKey(body.tenant_id, body.precommit_id));
    if (!precommit) {
      return problem(
        request,
        404,
        'PRECOMMIT_NOT_FOUND',
        'Pre-commit not found',
        'The requested Pre-commit was not found',
      );
    }
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === precommit.tenant_id &&
        item.project_id === precommit.project_id &&
        item.artifact_id === precommit.artifact_id &&
        item.playground_id === precommit.playground_id,
    )!;
    const requestKey = resourceKey(body.tenant_id, 'precommit-restart', body.restart_request_id);
    const requestJson = stableJson(body);
    const priorRequest = precommitMutationRequests.get(requestKey);
    if (priorRequest && priorRequest !== requestJson) {
      return mutationConflict(
        request,
        'RESTART_REQUEST_ID_REUSED',
        'The restart request ID belongs to another request',
      );
    }
    if (priorRequest) {
      const response: RestartPreCommitResponse = { precommit, playground, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    if (!['abnormal', 'cancelled'].includes(precommit.state)) {
      return mutationConflict(
        request,
        'PRECOMMIT_INVALID_STATE',
        'This Pre-commit cannot be restarted',
      );
    }
    if (
      playground.index_version.revision !== body.expected_index_version.revision ||
      playground.index_version.digest !== body.expected_index_version.digest
    ) {
      return mutationConflict(
        request,
        'INDEX_VERSION_CONFLICT',
        'The expected IndexVersion no longer matches the Playground',
      );
    }
    const now = Date.now().toString();
    precommit.attempt += 1;
    precommit.state = 'running';
    precommit.phase = 'queued';
    precommit.progress = { percent: 0, files_completed: '0', bytes_completed: '0' };
    precommit.checks = [];
    precommit.warnings = [];
    precommit.blockers = [];
    delete precommit.issue;
    delete precommit.candidate_index_version;
    delete precommit.diff_summary;
    precommit.source_index_version = structuredClone(body.expected_index_version);
    precommit.updated_at_unix_ms = now;
    playground.active_precommit_id = precommit.precommit_id;
    playground.updated_at_unix_ms = now;
    precommitMutationRequests.set(requestKey, requestJson);
    const response: RestartPreCommitResponse = { precommit, playground, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/precommit/cancel', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CancelPreCommitRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const precommit = precommits.get(resourceKey(body.tenant_id, body.precommit_id));
    if (!precommit) {
      return problem(
        request,
        404,
        'PRECOMMIT_NOT_FOUND',
        'Pre-commit not found',
        'The requested Pre-commit was not found',
      );
    }
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === precommit.tenant_id &&
        item.project_id === precommit.project_id &&
        item.artifact_id === precommit.artifact_id &&
        item.playground_id === precommit.playground_id,
    )!;
    const requestKey = resourceKey(body.tenant_id, 'precommit-cancel', body.cancel_request_id);
    const requestJson = stableJson(body);
    const priorRequest = precommitMutationRequests.get(requestKey);
    if (priorRequest && priorRequest !== requestJson) {
      return mutationConflict(
        request,
        'CANCEL_REQUEST_ID_REUSED',
        'The cancel request ID belongs to another request',
      );
    }
    if (priorRequest) {
      const response: CancelPreCommitResponse = { precommit, playground, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    if (precommit.state === 'committed') {
      return mutationConflict(
        request,
        'PRECOMMIT_INVALID_STATE',
        'A committed Pre-commit cannot be cancelled',
      );
    }
    const now = Date.now().toString();
    precommit.state = 'cancelled';
    precommit.phase = 'idle';
    precommit.updated_at_unix_ms = now;
    if (playground.active_precommit_id === precommit.precommit_id) {
      delete playground.active_precommit_id;
    }
    playground.updated_at_unix_ms = now;
    precommitMutationRequests.set(requestKey, requestJson);
    const response: CancelPreCommitResponse = { precommit, playground, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/commit/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CommitPlaygroundRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;

    const requestKey = resourceKey(body.tenant_id, body.commit_request_id);
    const requestJson = stableJson(body);
    const previous = commitRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'COMMIT_REQUEST_ID_REUSED',
          'The Commit request ID already belongs to a different request',
        );
      }
      const response = structuredClone(previous.response);
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }

    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    if (playground.state !== 'ready') {
      return mutationConflict(
        request,
        'PLAYGROUND_NOT_READY',
        'The Playground is not ready to create a Commit',
      );
    }
    const precommit = precommits.get(resourceKey(body.tenant_id, body.precommit_id));
    if (
      !precommit ||
      precommit.project_id !== body.project_id ||
      precommit.artifact_id !== body.artifact_id ||
      precommit.playground_id !== body.playground_id
    ) {
      return problem(
        request,
        404,
        'PRECOMMIT_NOT_FOUND',
        'Pre-commit not found',
        'The requested Pre-commit was not found for this Playground',
      );
    }
    if (precommit.state !== 'ready' || !precommit.candidate_index_version) {
      return mutationConflict(
        request,
        'PRECOMMIT_NOT_READY',
        'Commit requires a Ready Pre-commit candidate',
      );
    }
    if (
      precommit.candidate_index_version.revision !==
        body.expected_candidate_index_version.revision ||
      precommit.candidate_index_version.digest !== body.expected_candidate_index_version.digest
    ) {
      return mutationConflict(
        request,
        'INDEX_VERSION_CONFLICT',
        'The expected candidate IndexVersion no longer matches the Pre-commit',
      );
    }
    if (!body.message.trim()) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'Commit message must not be empty',
        {
          retryable: false,
          violations: [{ field: 'message', reason: 'must not be empty' }],
        },
      );
    }
    const tagNames = (body.tag_names ?? []).map((tagName) => tagName.trim());
    const tagPattern = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/;
    if (
      tagNames.length > 20 ||
      new Set(tagNames).size !== tagNames.length ||
      tagNames.some((tagName) => !tagPattern.test(tagName) || tagName.startsWith('refs/'))
    ) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'Tag names must be unique valid names without a refs/ prefix',
        {
          retryable: false,
          violations: [{ field: 'tag_names', reason: 'contains an invalid or duplicate Tag name' }],
        },
      );
    }

    const artifactKey = resourceKey(body.tenant_id, body.project_id, body.artifact_id);
    const artifact = artifacts.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id,
    );
    const graph = commitGraphs.get(artifactKey);
    if (!artifact || !graph) return notFound(request, 'Artifact');
    const conflictingTag = tagNames.find((tagName) =>
      graph.nodes.some((node) => node.tag_names.includes(tagName)),
    );
    if (conflictingTag) {
      return mutationConflict(
        request,
        'TAG_ALREADY_EXISTS',
        `The Tag ${conflictingTag} already points to another Commit`,
      );
    }

    const now = Date.now().toString();
    const commit: CommitNode = {
      commit_id: `commit-${fingerprint({ request: body, at: now })}`,
      ...(playground.head_commit_id ? { parent_commit_id: playground.head_commit_id } : {}),
      message: body.message.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      tag_names: tagNames,
      created_at_unix_ms: now,
    };
    graph.nodes.unshift(commit);
    graph.head_commit_id = commit.commit_id;
    graph.graph_version = (BigInt(graph.graph_version) + 1n).toString();
    artifact.head_commit_id = commit.commit_id;
    artifact.resource_version = (BigInt(artifact.resource_version) + 1n).toString();
    artifact.updated_at_unix_ms = now;
    playground.head_commit_id = commit.commit_id;
    playground.updated_at_unix_ms = now;
    delete playground.active_precommit_id;
    precommit.state = 'committed';
    precommit.phase = 'idle';
    precommit.committed_commit_id = commit.commit_id;
    precommit.updated_at_unix_ms = now;

    const response: CommitPlaygroundResponse = {
      commit,
      playground,
      consumed_precommit: precommit,
      replayed: false,
    };
    commitRequests.set(requestKey, { requestJson, response: structuredClone(response) });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/file/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryPlaygroundFileListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    const filtered = mockLogicalFiles.filter(
      (item) =>
        (!body.path_prefix || item.path.startsWith(body.path_prefix)) &&
        (!body.format || item.format === body.format),
    );
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      playground_id: body.playground_id,
      path_prefix: body.path_prefix ?? '',
      format: body.format ?? '',
      index_version: playground.index_version,
    };
    const page = paginate(request, 'playground-files', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryPlaygroundFileListResponse = {
      index_version: playground.index_version,
      ...page,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/change/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryPlaygroundChangeListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    const precommit = body.precommit_id
      ? precommits.get(resourceKey(body.tenant_id, body.precommit_id))
      : undefined;
    if (
      body.precommit_id &&
      (!precommit ||
        precommit.project_id !== body.project_id ||
        precommit.artifact_id !== body.artifact_id ||
        precommit.playground_id !== body.playground_id)
    ) {
      return problem(
        request,
        404,
        'PRECOMMIT_NOT_FOUND',
        'Pre-commit not found',
        'The requested Pre-commit was not found for this Playground',
      );
    }
    const filtered = mockPlaygroundChanges.filter(
      (item) =>
        (!body.change_type || item.change_type === body.change_type) &&
        (!body.path_prefix || item.path.startsWith(body.path_prefix)),
    );
    const indexVersion = precommit?.candidate_index_version ?? playground.index_version;
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      playground_id: body.playground_id,
      precommit_id: body.precommit_id ?? '',
      change_type: body.change_type ?? '',
      path_prefix: body.path_prefix ?? '',
      index_version: indexVersion,
    };
    const page = paginate(request, 'playground-changes', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryPlaygroundChangeListResponse = {
      source: precommit ? 'precommit' : 'workspace',
      ...(precommit ? { precommit_id: precommit.precommit_id } : {}),
      index_version: indexVersion,
      summary: {
        files_added: '1',
        files_modified: '1',
        files_deleted: '1',
        files_renamed: '1',
        bytes_added: '19971912500',
        bytes_removed: '650117120',
      },
      ...page,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/file/metadata/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryPlaygroundFileMetadataRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    const file = mockLogicalFiles.find((item) => item.path === body.path);
    if (!file || file.entry_type !== 'file' || !file.size_bytes || !file.format) {
      return problem(
        request,
        404,
        'FILE_NOT_FOUND',
        'File not found',
        'The logical file was not found',
      );
    }
    const response: QueryPlaygroundFileMetadataResponse = {
      index_version: playground.index_version,
      metadata: {
        path: file.path,
        size_bytes: file.size_bytes,
        format: file.format,
        ...(file.row_count ? { row_count: file.row_count } : {}),
        media_type:
          file.format === 'parquet' ? 'application/vnd.apache.parquet' : 'application/json',
        ...(mockDatasetProfile.schema ? { schema: mockDatasetProfile.schema } : {}),
        ...(mockDatasetProfile.statistics ? { statistics: mockDatasetProfile.statistics } : {}),
        ...(mockDatasetProfile.quality ? { quality: mockDatasetProfile.quality } : {}),
        ...(mockDatasetProfile.freshness ? { freshness: mockDatasetProfile.freshness } : {}),
      },
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/playground/dataset/profile/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryPlaygroundDatasetProfileRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const playground = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (!playground) return notFound(request, 'Playground');
    const response: QueryPlaygroundDatasetProfileResponse = {
      index_version: playground.index_version,
      profile: mockDatasetProfile,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    if (body.artifact_id && !body.project_id) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'project_id is required when artifact_id is provided',
        {
          retryable: false,
          violations: [{ field: 'project_id', reason: 'required with artifact_id' }],
        },
      );
    }
    const filtered = snapshots.filter(
      (snapshot) =>
        snapshot.tenant_id === body.tenant_id &&
        (!body.project_id || snapshot.project_id === body.project_id) &&
        (!body.artifact_id || snapshot.artifact_id === body.artifact_id) &&
        (!body.commit_id || snapshot.commit_id === body.commit_id) &&
        (!body.region || snapshot.region === body.region) &&
        (!body.storage_volume_id || snapshot.storage_volume_id === body.storage_volume_id) &&
        (!body.state || snapshot.state === body.state),
    );
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id ?? '',
      artifact_id: body.artifact_id ?? '',
      commit_id: body.commit_id ?? '',
      region: body.region ?? '',
      storage_volume_id: body.storage_volume_id ?? '',
      state: body.state ?? '',
    };
    const page = paginate(request, 'snapshots', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QuerySnapshotListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as { tenant_id: string; snapshot_id: string };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    const readyAt = snapshotReadyAt.get(resourceKey(snapshot.tenant_id, snapshot.snapshot_id));
    if (snapshot.state === 'creating' && readyAt && Date.now() >= readyAt) {
      snapshot.state = 'ready';
      snapshot.phase = 'idle';
      snapshot.integrity = {
        state: 'verified',
        files_verified: snapshot.logical_file_count,
        bytes_verified: snapshot.logical_size_bytes,
        verified_at_unix_ms: Date.now().toString(),
      };
      snapshot.updated_at_unix_ms = Date.now().toString();
      snapshotReadyAt.delete(resourceKey(snapshot.tenant_id, snapshot.snapshot_id));
    }
    const response: QuerySnapshotResponse = { snapshot };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateSnapshotRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, body.snapshot_request_id);
    const requestJson = stableJson(body);
    const priorRequest = snapshotCreateRequests.get(requestKey);
    if (priorRequest) {
      if (priorRequest.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'SNAPSHOT_REQUEST_ID_REUSED',
          'The Snapshot request ID already belongs to a different request',
        );
      }
      const response = structuredClone(priorRequest.response);
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const existing = snapshots.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.commit_id === body.commit_id &&
        item.storage_volume_id === body.storage_volume_id,
    );
    if (existing) {
      const response: CreateSnapshotResponse = {
        snapshot: existing,
        replayed: false,
        placement_reused: true,
      };
      snapshotCreateRequests.set(requestKey, { requestJson, response: structuredClone(response) });
      return HttpResponse.json(response, { headers: headers(request) });
    }

    const storageVolume = resolveStorageVolume(request, body.tenant_id, body.storage_volume_id);
    if (storageVolume instanceof HttpResponse) return storageVolume;

    const graph = commitGraphs.get(resourceKey(body.tenant_id, body.project_id, body.artifact_id));
    const commit = graph?.nodes.find((node) => node.commit_id === body.commit_id);
    if (!commit) {
      return problem(
        request,
        404,
        'COMMIT_NOT_FOUND',
        'Commit not found',
        'The requested Commit was not found',
      );
    }
    const sameCommitSnapshot = snapshots.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.commit_id === body.commit_id,
    );
    const snapshot = {
      snapshot_id: `snap-${body.artifact_id}-${storageVolume.region}-${Date.now().toString(36)}`,
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      commit_id: commit.commit_id,
      storage_volume_id: storageVolume.storage_volume_id,
      region: storageVolume.region,
      message: commit.message,
      tag_names: [...commit.tag_names],
      state: 'creating' as const,
      phase: 'materializing' as const,
      integrity: { state: 'pending' as const, files_verified: '0', bytes_verified: '0' },
      logical_file_count: sameCommitSnapshot?.logical_file_count ?? '864',
      logical_size_bytes: sameCommitSnapshot?.logical_size_bytes ?? '12884901888',
      created_at_unix_ms: Date.now().toString(),
      updated_at_unix_ms: Date.now().toString(),
    };
    snapshots.unshift(snapshot);
    snapshotReadyAt.set(resourceKey(snapshot.tenant_id, snapshot.snapshot_id), Date.now() + 800);
    const response: CreateSnapshotResponse = {
      snapshot,
      replayed: false,
      placement_reused: false,
    };
    snapshotCreateRequests.set(requestKey, { requestJson, response: structuredClone(response) });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/retry', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as RetrySnapshotDeliveryRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, body.retry_request_id);
    const requestJson = stableJson(body);
    const priorRequest = snapshotRetryRequests.get(requestKey);
    if (priorRequest) {
      if (priorRequest.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'RETRY_REQUEST_ID_REUSED',
          'The retry request ID already belongs to a different request',
        );
      }
      const response = structuredClone(priorRequest.response);
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    if (snapshot.state !== 'abnormal') {
      return mutationConflict(
        request,
        'SNAPSHOT_INVALID_STATE',
        'Only an Abnormal Snapshot delivery can be retried',
      );
    }
    snapshot.state = 'creating';
    snapshot.phase = 'materializing';
    snapshot.integrity = { state: 'pending', files_verified: '0', bytes_verified: '0' };
    delete snapshot.issue;
    snapshot.updated_at_unix_ms = Date.now().toString();
    snapshotReadyAt.set(resourceKey(snapshot.tenant_id, snapshot.snapshot_id), Date.now() + 800);
    const response: RetrySnapshotDeliveryResponse = { snapshot, replayed: false };
    snapshotRetryRequests.set(requestKey, { requestJson, response: structuredClone(response) });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/file/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotFileListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    if (snapshot.state !== 'ready') {
      return mutationConflict(
        request,
        'SNAPSHOT_NOT_READY',
        'Snapshot files are available only when the Snapshot is Ready',
      );
    }
    const filtered = mockLogicalFiles.filter(
      (item) =>
        (!body.path_prefix || item.path.startsWith(body.path_prefix)) &&
        (!body.format || item.format === body.format),
    );
    const filters = {
      tenant_id: body.tenant_id,
      snapshot_id: body.snapshot_id,
      path_prefix: body.path_prefix ?? '',
      format: body.format ?? '',
    };
    const page = paginate(request, 'snapshot-files', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QuerySnapshotFileListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/activity/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotActivityListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    const items: QuerySnapshotActivityListResponse['items'] = [
      {
        activity_id: `activity-${snapshot.snapshot_id}-created`,
        activity_type: 'created' as const,
        summary: 'Snapshot 记录已创建并开始区域交付',
        phase: 'planning' as const,
        created_at_unix_ms: snapshot.created_at_unix_ms,
      },
      ...(snapshot.state === 'ready'
        ? [
            {
              activity_id: `activity-${snapshot.snapshot_id}-ready`,
              activity_type: 'ready' as const,
              summary: 'Snapshot 完整性校验通过并可读取',
              phase: 'idle' as const,
              created_at_unix_ms: snapshot.updated_at_unix_ms,
            },
          ]
        : []),
      ...(snapshot.state === 'abnormal' && snapshot.issue
        ? [
            {
              activity_id: `activity-${snapshot.snapshot_id}-failed`,
              activity_type: 'failed' as const,
              summary: snapshot.issue.message,
              phase: snapshot.phase,
              issue: snapshot.issue,
              created_at_unix_ms: snapshot.updated_at_unix_ms,
            },
          ]
        : []),
    ].sort((left, right) => right.created_at_unix_ms.localeCompare(left.created_at_unix_ms));
    const page = paginate(
      request,
      'snapshot-activities',
      { tenant_id: body.tenant_id, snapshot_id: body.snapshot_id },
      items,
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QuerySnapshotActivityListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/dataset/profile/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotDatasetProfileRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    const response: QuerySnapshotDatasetProfileResponse = { profile: mockDatasetProfile };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/job/add/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateAddJobRequest;
    const invalid = validateCreate(request, body);
    if (invalid) return invalid;
    const jobKey = resourceKey(body.tenant_id, body.job_id);
    const requestJson = stableJson(body);
    const existing = jobs.get(jobKey);
    if (existing) {
      if (existing.requestJson !== requestJson) {
        return problem(
          request,
          409,
          'JOB_ID_REUSED',
          'Job ID reused',
          'The Job ID already belongs to a different managed Add request',
        );
      }
      const response: CreateAddJobResponse = { job: existing.job, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const job: JobView = {
      operation: 'add',
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      playground_id: body.playground_id,
      job_id: body.job_id,
      state: 'queued',
      resource_version: '1',
      deadline_unix_ms: body.deadline_unix_ms,
    };
    jobs.set(jobKey, { request: body, requestJson, job, queryCount: 0 });
    const response: CreateAddJobResponse = { job, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/job/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as { tenant_id: string; job_id: string };
    const failed = unavailable(request, body.tenant_id);
    if (failed) return failed;
    const stored = jobs.get(resourceKey(body.tenant_id, body.job_id));
    if (!stored) {
      return problem(
        request,
        404,
        'JOB_NOT_FOUND',
        'Job not found',
        'The requested Job was not found',
      );
    }
    advance(stored);
    const response: QueryJobResponse = { job: stored.job };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/job/add/finalize', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as { tenant_id: string; job_id: string };
    const stored = jobs.get(resourceKey(body.tenant_id, body.job_id));
    if (!stored) {
      return problem(
        request,
        404,
        'JOB_NOT_FOUND',
        'Job not found',
        'The requested Job was not found',
      );
    }
    if (stored.job.state === 'succeeded' && stored.job.decision && stored.finalizedAt) {
      const replay: FinalizeAddJobResponse = {
        job: stored.job,
        decision: stored.job.decision,
        finalized_at_unix_ms: stored.finalizedAt,
        replayed: true,
      };
      return HttpResponse.json(replay, { headers: headers(request) });
    }
    if (stored.job.state !== 'prepared') {
      return problem(
        request,
        409,
        'JOB_INVALID_STATE',
        'Job state does not allow finalization',
        'Managed Add can only be finalized from Prepared or a stable terminal state',
      );
    }
    const revision = (BigInt(stored.request.expected_index_version.revision) + 1n).toString();
    const finalizedAt = Date.now().toString();
    const decision = {
      outcome: 'publish' as const,
      final_state: 'succeeded' as const,
      published_index_version: { revision, digest: 'e'.repeat(64) },
    };
    stored.job.state = 'succeeded';
    stored.job.resource_version = '7';
    stored.job.decision = decision;
    stored.job.finalized_at_unix_ms = finalizedAt;
    stored.finalizedAt = finalizedAt;
    const response: FinalizeAddJobResponse = {
      job: stored.job,
      decision,
      finalized_at_unix_ms: finalizedAt,
      replayed: false,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
];

export function resetMockJobs(): void {
  jobs.clear();
}

export function resetMockState(): void {
  jobs.clear();
  tenantCreatePayloads.clear();
  storageVolumeCreatePayloads.clear();
  artifactCreatePayloads.clear();
  playgroundCreatePayloads.clear();
  playgroundReadyAt.clear();
  commitRequests.clear();
  precommits.clear();
  precommitMutationRequests.clear();
  snapshotCreateRequests.clear();
  snapshotRetryRequests.clear();
  snapshotReadyAt.clear();
  resetMockData();
}
