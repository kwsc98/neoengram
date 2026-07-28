import { http, HttpResponse } from 'msw';

import type {
  CreateAddJobRequest,
  CreateAddJobResponse,
  CreateTenantRequest,
  CreateTenantResponse,
  FinalizeAddJobResponse,
  JobView,
  ProblemDetails,
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
  QueryTenantListRequest,
  QueryTenantListResponse,
  QueryTenantResponse,
  QueryJobResponse,
  TenantView,
} from '@/api/types';

import {
  artifacts,
  commitGraphs,
  playgrounds,
  projects,
  resourceKey,
  snapshots,
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
      permissions: ['tenant.admin', 'tenant.read', 'artifact.read', 'job.create'],
    };
    tenants.push(tenant);
    tenantCreatePayloads.set(body.tenant_id, requestJson);
    const response: CreateTenantResponse = { tenant, replayed: false };
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
  tenants.splice(2);
}
