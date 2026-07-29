import { http, HttpResponse } from 'msw';

import type {
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
  ProblemDetails,
  QueryArtifactListRequest,
  QueryArtifactListResponse,
  QueryArtifactCommitDiffResponse,
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
  StorageVolumeView,
  TenantView,
} from '@/api/types';
import { LOGICAL_ARTIFACT_STORAGE_COMPAT_ID } from '@/features/artifacts/prototype';

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
const commitRequests = new Map<
  string,
  { requestJson: string; response: CommitPlaygroundResponse }
>();

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
    let storageVolume: StorageVolumeView | undefined;
    if (body.storage_volume_id !== LOGICAL_ARTIFACT_STORAGE_COMPAT_ID) {
      const resolvedStorageVolume = resolveStorageVolume(
        request,
        body.tenant_id,
        body.storage_volume_id,
      );
      if (resolvedStorageVolume instanceof HttpResponse) return resolvedStorageVolume;
      storageVolume = resolvedStorageVolume;
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
        existing.storage_volume_id === (storageVolume?.storage_volume_id ?? '') &&
        existing.default_ref === (body.default_ref ?? 'refs/heads/main');
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
    const artifact = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      storage_volume_id: storageVolume?.storage_volume_id ?? '',
      region: storageVolume?.region ?? '',
      display_name: body.display_name.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      default_ref: body.default_ref ?? 'refs/heads/main',
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    artifacts.push(artifact);
    commitGraphs.set(key, { graph_version: '0', refs: [], nodes: [] });
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
          refs: graph.refs,
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

    const defaultHead = graph.refs.find(
      (refTip) => refTip.name === artifact.default_ref,
    )?.commit_id;
    const baseCommitId = body.base_commit_id ?? defaultHead;
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
      state: 'ready' as const,
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    playgrounds.push(playground);
    playgroundCreatePayloads.set(key, requestJson);
    const response: CreatePlaygroundResponse = { playground, replayed: false };
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
    const tagRefs = tagNames.map((tagName) => `refs/tags/${tagName}`);
    const conflictingTag = tagRefs.find((tagRef) =>
      graph.refs.some((refTip) => refTip.name === tagRef),
    );
    if (conflictingTag) {
      return mutationConflict(
        request,
        'TAG_ALREADY_EXISTS',
        `The Tag ${conflictingTag.replace('refs/tags/', '')} already points to another Commit`,
      );
    }

    const now = Date.now().toString();
    const defaultRef = artifact.default_ref;
    for (const node of graph.nodes) {
      node.ref_names = node.ref_names.filter((name) => name !== defaultRef);
    }
    const commit = {
      commit_id: `commit-${fingerprint({ request: body, at: now })}`,
      ...(playground.head_commit_id ? { parent_commit_id: playground.head_commit_id } : {}),
      message: body.message.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      ref_names: [defaultRef, ...tagRefs],
      created_at_unix_ms: now,
    };
    graph.nodes.unshift(commit);
    const refTip = graph.refs.find((candidate) => candidate.name === defaultRef);
    if (refTip) refTip.commit_id = commit.commit_id;
    else graph.refs.push({ name: defaultRef, commit_id: commit.commit_id });
    for (const tagRef of tagRefs) graph.refs.push({ name: tagRef, commit_id: commit.commit_id });
    graph.graph_version = (BigInt(graph.graph_version) + 1n).toString();
    artifact.resource_version = (BigInt(artifact.resource_version) + 1n).toString();
    artifact.updated_at_unix_ms = now;
    playground.head_commit_id = commit.commit_id;
    playground.updated_at_unix_ms = now;

    const response: CommitPlaygroundResponse = { commit, playground, replayed: false };
    commitRequests.set(requestKey, { requestJson, response: structuredClone(response) });
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
        (!body.artifact_id || snapshot.artifact_id === body.artifact_id),
    );
    const filters = {
      tenant_id: body.tenant_id,
      project_id: body.project_id ?? '',
      artifact_id: body.artifact_id ?? '',
    };
    const page = paginate(request, 'snapshots', filters, filtered, body);
    if (page instanceof HttpResponse) return page;
    const response: QuerySnapshotListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as {
      tenant_id: string;
      project_id: string;
      artifact_id: string;
      commit_id: string;
    };
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.commit_id === body.commit_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    const response: QuerySnapshotResponse = { snapshot };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateSnapshotRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const existing = snapshots.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.commit_id === body.commit_id,
    );
    if (existing) {
      if (existing.storage_volume_id !== body.storage_volume_id) {
        return mutationConflict(
          request,
          'SNAPSHOT_PLACEMENT_CONFLICT',
          'The Snapshot already exists on a different StorageVolume',
        );
      }
      const response: CreateSnapshotResponse = { snapshot: existing, replayed: true };
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
    const snapshot = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      commit_id: commit.commit_id,
      storage_volume_id: storageVolume.storage_volume_id,
      region: storageVolume.region,
      message: commit.message,
      ref_names: [...commit.ref_names],
      created_at_unix_ms: commit.created_at_unix_ms,
    };
    snapshots.unshift(snapshot);
    const response: CreateSnapshotResponse = { snapshot, replayed: false };
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
  commitRequests.clear();
  resetMockData();
}
