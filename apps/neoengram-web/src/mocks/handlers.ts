import { http, HttpResponse } from 'msw';

import { runtimeConfig } from '@/config';

import type {
  ApproveStorageEnrollmentRequest,
  ApproveStorageEnrollmentResponse,
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
  CreateProjectRequest,
  CreateProjectResponse,
  CreateSnapshotDeliveryRequest,
  CreateSnapshotDeliveryResponse,
  CreateSnapshotRequest,
  CreateSnapshotResponse,
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
  CreateStorageVolumeRequest,
  CreateStorageVolumeResponse,
  CreateTenantRequest,
  CreateTenantResponse,
  FinalizeAddJobResponse,
  JobView,
  DeleteSnapshotDeliveryRequest,
  DeleteSnapshotDeliveryResponse,
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
  QuerySnapshotDeliveryListRequest,
  QuerySnapshotDeliveryListResponse,
  QuerySnapshotDeliveryRequest,
  QuerySnapshotDeliveryResponse,
  QuerySnapshotFileListRequest,
  QuerySnapshotFileListResponse,
  QuerySnapshotResponse,
  QueryStorageEnrollmentListRequest,
  QueryStorageEnrollmentListResponse,
  QueryStorageEnrollmentRequest,
  QueryStorageEnrollmentResponse,
  RestartPreCommitRequest,
  RestartPreCommitResponse,
  RejectStorageEnrollmentRequest,
  RejectStorageEnrollmentResponse,
  RetrySnapshotDeliveryRequest,
  RetrySnapshotDeliveryResponse,
  SnapshotDeliveryView,
  CreateS3AccessPointRequest,
  CreateS3AccessPointResponse,
  QueryS3AccessPointListRequest,
  QueryS3AccessPointListResponse,
  QueryS3AccessPointRequest,
  QueryS3AccessPointResponse,
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
  DeletionImpactView,
  DeletionMutationResponse,
  DeletionOperationView,
  DeletionTargetView,
  QueryDeletionImpactRequest,
  QueryDeletionImpactResponse,
  QueryDeletionListRequest,
  QueryDeletionListResponse,
  QueryDeletionRequest,
  QueryDeletionResponse,
  ReleaseRetentionHoldRequest,
  ReleaseRetentionHoldResponse,
  ResourceRef,
  ResourceLifecycleView,
  RetentionHoldView,
  UpdateDeletionRequest,
  S3AccessPointView,
  S3CredentialView,
  S3ObjectEntryView,
  StartPreCommitRequest,
  StartPreCommitResponse,
  QueryStorageVolumeListRequest,
  QueryStorageVolumeListResponse,
  QueryStorageVolumeResponse,
  QueryTenantListRequest,
  QueryTenantListResponse,
  QueryTenantResponse,
  QueryJobResponse,
  StorageEnrollmentView,
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
  s3AccessPoints,
  s3Credentials,
  s3Objects,
  storageVolumes,
  tenants,
} from './data';
import { resolveMockS3Path } from './s3-files';

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

type StorageEnrollmentPermission =
  'storage.enrollment.create' | 'storage.enrollment.read' | 'storage.enrollment.review';

type StorageEnrollmentDescriptor = Pick<
  StorageEnrollmentView,
  | 'tenant_id'
  | 'storage_volume_id'
  | 'display_name'
  | 'edge_cluster_id'
  | 'region'
  | 'access_mode'
  | 'pvc_reference'
>;

interface PvcBinding {
  tenantId: string;
  storageVolumeId: string;
}

interface PvcOwner {
  tenantId: string;
  storageVolumeId: string;
  identityFingerprint: string;
}

const jobs = new Map<string, StoredJob>();
const tenantCreatePayloads = new Map<string, string>();
const projectCreatePayloads = new Map<string, string>();
const storageVolumeCreatePayloads = new Map<string, string>();
const storageEnrollmentTokenRequests = new Map<
  string,
  {
    requestJson: string;
    descriptor: StorageEnrollmentDescriptor;
    response: CreateStorageEnrollmentTokenResponse;
  }
>();
const storageEnrollmentDecisionRequests = new Map<
  string,
  | { kind: 'approve'; requestJson: string; response: ApproveStorageEnrollmentResponse }
  | { kind: 'reject'; requestJson: string; response: RejectStorageEnrollmentResponse }
>();
const storageEnrollments: StorageEnrollmentView[] = [];
const storageEnrollmentFrozenDescriptors = new Map<string, StorageEnrollmentDescriptor>();
const storageEnrollmentReviewAudit = new Map<string, string>();
const pvcBindings = new Map<string, PvcBinding>();
const activePvcOwners = new Map<string, PvcOwner>();
const artifactCreatePayloads = new Map<string, string>();
const playgroundQueryCounts = new Map<string, number>();
const commitRequests = new Map<
  string,
  { requestJson: string; response: CommitPlaygroundResponse }
>();
const precommits = new Map<string, PreCommitView>();
const precommitQueryCounts = new Map<string, number>();
const precommitSourceHeads = new Map<string, string | null>();
const precommitMutationRequests = new Map<string, string>();
const snapshotCreateRequests = new Map<
  string,
  { requestJson: string; response: CreateSnapshotResponse }
>();
const snapshotDeliveries: SnapshotDeliveryView[] = [];
const snapshotDeliveryCreateRequests = new Map<
  string,
  { requestJson: string; response: CreateSnapshotDeliveryResponse }
>();
const snapshotRetryRequests = new Map<
  string,
  { requestJson: string; response: RetrySnapshotDeliveryResponse }
>();
const snapshotDeliveryDeleteRequests = new Map<
  string,
  { requestJson: string; response: DeleteSnapshotDeliveryResponse }
>();
const snapshotQueryCounts = new Map<string, number>();
const s3AccessPointCreateRequests = new Map<
  string,
  { requestJson: string; response: CreateS3AccessPointResponse }
>();
const s3CredentialCreateRequests = new Map<
  string,
  { requestJson: string; response: CreateS3CredentialResponse }
>();
const deletionOperations: DeletionOperationView[] = [];
const deletionImpacts = new Map<string, DeletionImpactView>();
const deletionMutationRequests = new Map<string, { requestJson: string; response: unknown }>();
const retentionHolds: RetentionHoldView[] = [];

const READY_AFTER_QUERY_COUNT = 2;

function completesOnThisQuery(queryCounts: Map<string, number>, key: string): boolean {
  const currentCount = queryCounts.get(key);
  if (currentCount === undefined) return false;

  const nextCount = currentCount + 1;
  if (nextCount < READY_AFTER_QUERY_COUNT) {
    queryCounts.set(key, nextCount);
    return false;
  }

  queryCounts.delete(key);
  return true;
}

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

function contentDigest(value: unknown): string {
  const source = stableJson(value);
  let digest = '';
  for (let round = 0; round < 8; round += 1) {
    let hash = 2166136261 ^ round;
    for (const character of source) {
      hash ^= character.codePointAt(0) ?? 0;
      hash = Math.imul(hash, 16777619);
    }
    digest += (hash >>> 0).toString(16).padStart(8, '0');
  }
  return digest;
}

type LifecycleMockResource = {
  resource_version?: string;
  lifecycle?: ResourceLifecycleView;
};

function activeLifecycle(): ResourceLifecycleView {
  return { state: 'active', generation: '1' };
}

function resourceIdentity(resource: ResourceRef): string {
  switch (resource.type) {
    case 'storage_volume':
      return resourceKey(resource.type, resource.storage_volume_id);
    case 'artifact':
      return resourceKey(resource.type, resource.project_id, resource.artifact_id);
    case 'playground':
      return resourceKey(
        resource.type,
        resource.project_id,
        resource.artifact_id,
        resource.playground_id,
      );
    case 'snapshot':
      return resourceKey(resource.type, resource.snapshot_id);
  }
}

function findLifecycleResource(
  tenantId: string,
  resource: ResourceRef,
): LifecycleMockResource | undefined {
  switch (resource.type) {
    case 'storage_volume':
      return storageVolumes.find(
        (item) =>
          item.tenant_id === tenantId && item.storage_volume_id === resource.storage_volume_id,
      );
    case 'artifact':
      return artifacts.find(
        (item) =>
          item.tenant_id === tenantId &&
          item.project_id === resource.project_id &&
          item.artifact_id === resource.artifact_id,
      );
    case 'playground':
      return playgrounds.find(
        (item) =>
          item.tenant_id === tenantId &&
          item.project_id === resource.project_id &&
          item.artifact_id === resource.artifact_id &&
          item.playground_id === resource.playground_id,
      );
    case 'snapshot':
      return snapshots.find(
        (item) => item.tenant_id === tenantId && item.snapshot_id === resource.snapshot_id,
      );
  }
}

function isLifecycleActive(resource: unknown): boolean {
  return (
    (resource as LifecycleMockResource).lifecycle?.state !== 'pending_delete' &&
    (resource as LifecycleMockResource).lifecycle?.state !== 'deleting' &&
    (resource as LifecycleMockResource).lifecycle?.state !== 'restoring' &&
    (resource as LifecycleMockResource).lifecycle?.state !== 'deleted'
  );
}

function resourceTarget(tenantId: string, resource: ResourceRef): DeletionTargetView | undefined {
  const found = findLifecycleResource(tenantId, resource);
  if (!found?.resource_version) return undefined;
  return {
    resource,
    resource_version: found.resource_version,
    lifecycle_generation: found.lifecycle?.generation ?? '1',
    requires_agent_cleanup: resource.type !== 'artifact',
  };
}

function deletionTargets(
  tenantId: string,
  root: ResourceRef,
  cascade: boolean,
): DeletionTargetView[] {
  const refs: ResourceRef[] = [root];
  if (cascade && root.type === 'artifact') {
    refs.push(
      ...playgrounds
        .filter(
          (item) =>
            item.tenant_id === tenantId &&
            item.project_id === root.project_id &&
            item.artifact_id === root.artifact_id &&
            isLifecycleActive(item),
        )
        .map((item) => ({
          type: 'playground' as const,
          project_id: item.project_id,
          artifact_id: item.artifact_id,
          playground_id: item.playground_id,
        })),
      ...snapshots
        .filter(
          (item) =>
            item.tenant_id === tenantId &&
            item.project_id === root.project_id &&
            item.artifact_id === root.artifact_id &&
            isLifecycleActive(item),
        )
        .map((item) => ({ type: 'snapshot' as const, snapshot_id: item.snapshot_id })),
    );
  }
  if (cascade && root.type === 'storage_volume') {
    refs.push(
      ...playgrounds
        .filter(
          (item) =>
            item.tenant_id === tenantId &&
            item.storage_volume_id === root.storage_volume_id &&
            isLifecycleActive(item),
        )
        .map((item) => ({
          type: 'playground' as const,
          project_id: item.project_id,
          artifact_id: item.artifact_id,
          playground_id: item.playground_id,
        })),
      ...snapshots
        .filter(
          (item) =>
            item.tenant_id === tenantId &&
            item.storage_volume_id === root.storage_volume_id &&
            isLifecycleActive(item),
        )
        .map((item) => ({ type: 'snapshot' as const, snapshot_id: item.snapshot_id })),
    );
  }
  return [...new Map(refs.map((resource) => [resourceIdentity(resource), resource])).values()]
    .map((resource) => resourceTarget(tenantId, resource))
    .filter((target): target is DeletionTargetView => Boolean(target));
}

function setResourceLifecycle(
  tenantId: string,
  target: DeletionTargetView,
  state: 'active' | 'pending_delete' | 'restoring' | 'deleted',
  deletionId?: string,
  purgeAfter?: string,
): void {
  const resource = findLifecycleResource(tenantId, target.resource);
  if (!resource) return;
  const now = Date.now().toString();
  resource.resource_version = (BigInt(resource.resource_version ?? '1') + 1n).toString();
  const lifecycle: NonNullable<LifecycleMockResource['lifecycle']> = {
    state,
    generation: (BigInt(target.lifecycle_generation) + 1n).toString(),
  };
  if (state !== 'active') {
    if (deletionId) lifecycle.active_deletion_id = deletionId;
    lifecycle.delete_requested_at_unix_ms = now;
    if (purgeAfter) lifecycle.purge_after_unix_ms = purgeAfter;
  }
  resource.lifecycle = lifecycle;
}

function seedDeletionState(): void {
  deletionOperations.length = 0;
  deletionImpacts.clear();
  deletionMutationRequests.clear();
  retentionHolds.length = 0;
  const now = Date.now();
  deletionOperations.push({
    deletion_id: 'deletion-example-recoverable',
    tenant_id: 'tenant-a',
    root: { type: 'snapshot', snapshot_id: 'snapshot-archived-example' },
    state: 'recoverable',
    resource_version: '3',
    targets: [
      {
        resource: { type: 'snapshot', snapshot_id: 'snapshot-archived-example' },
        resource_version: '2',
        lifecycle_generation: '2',
        requires_agent_cleanup: true,
      },
    ],
    request_id: 'seed-deletion-request',
    request_digest: 'a'.repeat(64),
    impact_digest: 'b'.repeat(64),
    cascade: false,
    confirm_managed_data_erase: false,
    purge_after_unix_ms: (now + 5 * 24 * 60 * 60 * 1_000).toString(),
    created_at_unix_ms: (now - 2 * 24 * 60 * 60 * 1_000).toString(),
    updated_at_unix_ms: (now - 60_000).toString(),
    retry_count: '0',
  });
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

function requireEnrollmentPermission(
  request: Request,
  tenantId: string,
  permission: StorageEnrollmentPermission,
): HttpResponse<ProblemDetails> | null {
  const failed = requireTenant(request, tenantId);
  if (failed) return failed;
  const tenant = tenants.find((item) => item.tenant_id === tenantId);
  if (isAdmin(request) && tenant?.permissions.includes(permission)) return null;
  return problem(
    request,
    403,
    'AUTHORIZATION_DENIED',
    'Authorization denied',
    `The principal is missing ${permission} in this Tenant`,
  );
}

function storageEnrollmentNotFound(request: Request): HttpResponse<ProblemDetails> {
  return problem(
    request,
    404,
    'STORAGE_ENROLLMENT_NOT_FOUND',
    'Storage enrollment not found',
    'The requested Storage enrollment was not found',
  );
}

const RESOURCE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const REGION_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;
const KUBERNETES_NAMESPACE_PATTERN = /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$/;
const KUBERNETES_PVC_CLAIM_PATTERN =
  /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*$/;

function validPvcReference(reference: { namespace: string; claim_name: string }): boolean {
  return (
    reference.namespace.length <= 63 &&
    KUBERNETES_NAMESPACE_PATTERN.test(reference.namespace) &&
    reference.claim_name.length <= 253 &&
    KUBERNETES_PVC_CLAIM_PATTERN.test(reference.claim_name)
  );
}

function pvcIdentity(value: {
  edge_cluster_id: string;
  pvc_reference: { namespace: string; claim_name: string };
}): string {
  return resourceKey(
    value.edge_cluster_id,
    value.pvc_reference.namespace,
    value.pvc_reference.claim_name,
  );
}

function enrollmentDescriptor(value: StorageEnrollmentDescriptor): StorageEnrollmentDescriptor {
  return {
    tenant_id: value.tenant_id,
    storage_volume_id: value.storage_volume_id,
    display_name: value.display_name,
    edge_cluster_id: value.edge_cluster_id,
    region: value.region,
    access_mode: value.access_mode,
    pvc_reference: structuredClone(value.pvc_reference),
  };
}

function descriptorMatches(
  left: StorageEnrollmentDescriptor,
  right: StorageEnrollmentDescriptor,
): boolean {
  return stableJson(enrollmentDescriptor(left)) === stableJson(enrollmentDescriptor(right));
}

function volumeMatchesDescriptor(
  volume: StorageVolumeView,
  descriptor: StorageEnrollmentDescriptor,
): boolean {
  return (
    volume.backend_type === 'pvc' &&
    volume.tenant_id === descriptor.tenant_id &&
    volume.storage_volume_id === descriptor.storage_volume_id &&
    volume.display_name === descriptor.display_name &&
    volume.edge_cluster_id === descriptor.edge_cluster_id &&
    volume.region === descriptor.region &&
    volume.access_mode === descriptor.access_mode &&
    volume.pvc_reference?.namespace === descriptor.pvc_reference.namespace &&
    volume.pvc_reference?.claim_name === descriptor.pvc_reference.claim_name
  );
}

function rebuildPvcAuthorityState(): void {
  pvcBindings.clear();
  activePvcOwners.clear();
  for (const volume of storageVolumes) {
    if (volume.backend_type !== 'pvc' || !volume.pvc_reference) continue;
    const key = pvcIdentity({
      edge_cluster_id: volume.edge_cluster_id,
      pvc_reference: volume.pvc_reference,
    });
    pvcBindings.set(key, {
      tenantId: volume.tenant_id,
      storageVolumeId: volume.storage_volume_id,
    });
    if (volume.state === 'ready') {
      activePvcOwners.set(key, {
        tenantId: volume.tenant_id,
        storageVolumeId: volume.storage_volume_id,
        identityFingerprint: `seeded-owner:${volume.storage_volume_id}`,
      });
    }
  }
}

function expireStorageEnrollments(): void {
  const now = BigInt(Date.now());
  for (const enrollment of storageEnrollments) {
    if (enrollment.state !== 'pending_approval' || BigInt(enrollment.expires_at_unix_ms) > now) {
      continue;
    }
    enrollment.state = 'expired';
    enrollment.resource_version = (BigInt(enrollment.resource_version) + 1n).toString();
    enrollment.updated_at_unix_ms = now.toString();
  }
}

function seedStorageEnrollmentState(): void {
  const createdAt = Date.now() - 60_000;
  const expiresAt = createdAt + 24 * 60 * 60 * 1000;
  const createdAtUnixMs = createdAt.toString();
  const expiresAtUnixMs = expiresAt.toString();
  storageEnrollments.length = 0;
  storageEnrollmentFrozenDescriptors.clear();
  storageEnrollments.push(
    {
      tenant_id: 'tenant-a',
      storage_enrollment_id: 'storage-enrollment-review-01',
      storage_volume_id: 'volume-review-pvc',
      display_name: '待接入评测 PVC',
      edge_cluster_id: 'cluster-cn-east-1',
      region: 'cn-shanghai',
      access_mode: 'read_write_many',
      pvc_reference: { namespace: 'neoengram-data', claim_name: 'review-data' },
      registration_kind: 'initial',
      state: 'pending_approval',
      agent_version: '0.2.0',
      identity_fingerprint: 'a'.repeat(64),
      proof_of_possession_status: 'verified',
      probe: {
        descriptor_matches: true,
        observed_access_mode: 'read_write',
        observed_at_unix_ms: createdAtUnixMs,
      },
      resource_version: '1',
      created_at_unix_ms: createdAtUnixMs,
      expires_at_unix_ms: expiresAtUnixMs,
      updated_at_unix_ms: createdAtUnixMs,
    },
    {
      tenant_id: 'tenant-a',
      storage_enrollment_id: 'storage-enrollment-review-02',
      storage_volume_id: 'volume-shanghai-vision',
      display_name: '视觉数据 PVC',
      edge_cluster_id: 'cluster-cn-east-1',
      region: 'cn-shanghai',
      access_mode: 'read_write_many',
      pvc_reference: { namespace: 'neoengram-data', claim_name: 'vision-data' },
      registration_kind: 'replacement',
      state: 'pending_approval',
      agent_version: '0.2.0',
      identity_fingerprint: 'b'.repeat(64),
      proof_of_possession_status: 'verified',
      probe: {
        descriptor_matches: true,
        observed_access_mode: 'read_write',
        observed_at_unix_ms: createdAtUnixMs,
      },
      resource_version: '1',
      created_at_unix_ms: createdAtUnixMs,
      expires_at_unix_ms: expiresAtUnixMs,
      updated_at_unix_ms: createdAtUnixMs,
    },
    {
      tenant_id: 'tenant-b',
      storage_enrollment_id: 'storage-enrollment-tenant-b-01',
      storage_volume_id: 'volume-tenant-b-pending',
      display_name: 'Tenant B pending PVC',
      edge_cluster_id: 'cluster-cn-east-1',
      region: 'cn-shanghai',
      access_mode: 'read_write_once',
      pvc_reference: { namespace: 'neoengram-release', claim_name: 'pending-review' },
      registration_kind: 'initial',
      state: 'pending_approval',
      agent_version: '0.2.0',
      identity_fingerprint: 'c'.repeat(64),
      proof_of_possession_status: 'verified',
      probe: {
        descriptor_matches: true,
        observed_access_mode: 'read_write',
        observed_at_unix_ms: createdAtUnixMs,
      },
      resource_version: '1',
      created_at_unix_ms: createdAtUnixMs,
      expires_at_unix_ms: expiresAtUnixMs,
      updated_at_unix_ms: createdAtUnixMs,
    },
  );
  for (const enrollment of storageEnrollments) {
    storageEnrollmentFrozenDescriptors.set(
      resourceKey(enrollment.tenant_id, enrollment.storage_enrollment_id),
      enrollmentDescriptor(enrollment),
    );
  }
  rebuildPvcAuthorityState();
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
  if (storageVolume.state !== 'ready') {
    return mutationConflict(
      request,
      'STORAGE_VOLUME_UNAVAILABLE',
      'Only a Ready StorageVolume can accept new resource placement',
    );
  }
  return storageVolume;
}

function resolveSnapshotDelivery(
  tenantId: string,
  deliveryId: string,
): { delivery: SnapshotDeliveryView; snapshot: (typeof snapshots)[number] } | undefined {
  const delivery = snapshotDeliveries.find((item) => item.delivery_id === deliveryId);
  if (!delivery) return undefined;
  const snapshot = snapshots.find(
    (item) => item.tenant_id === tenantId && item.snapshot_id === delivery.snapshot_id,
  );
  return snapshot ? { delivery, snapshot } : undefined;
}

function seedSnapshotDeliveryState(): void {
  snapshotDeliveries.splice(0, snapshotDeliveries.length);
  const snapshot = snapshots.find(
    (item) => item.tenant_id === 'tenant-a' && item.snapshot_id === 'snap-road-main3-sha-01',
  );
  if (!snapshot) return;
  snapshotDeliveries.push({
    delivery_id: 'delivery-snapshot-main3-01',
    snapshot_id: snapshot.snapshot_id,
    commit_id: snapshot.commit_id,
    storage_volume_id: snapshot.storage_volume_id,
    mode: 'copy',
    target_relative_root: `snapshots/${snapshot.project_id}/${snapshot.artifact_id}/${snapshot.snapshot_id}/deliveries/delivery-snapshot-main3-01`,
    state: 'failed',
    source_index_digest: snapshot.commit_id,
    delivery_generation: '1',
    file_count: snapshot.logical_file_count,
    size_bytes: snapshot.logical_size_bytes,
    object_set_digest: snapshot.commit_id,
    resource_version: '1',
    issue: {
      code: 'COPY_INSUFFICIENT_SPACE',
      message: 'The target Volume does not have enough reserved space',
      retryable: true,
    },
    created_at_unix_ms: snapshot.created_at_unix_ms,
    updated_at_unix_ms: snapshot.updated_at_unix_ms,
  });
}

function seedPreCommitState(): void {
  const playground = playgrounds.find(
    (item) =>
      item.tenant_id === 'tenant-a' &&
      item.project_id === 'project-vision' &&
      item.artifact_id === 'quality-reports' &&
      item.playground_id === 'nightly-review',
  );
  if (!playground?.active_precommit_id) return;
  const key = resourceKey(playground.tenant_id, playground.active_precommit_id);
  precommits.set(key, {
    tenant_id: playground.tenant_id,
    project_id: playground.project_id,
    artifact_id: playground.artifact_id,
    playground_id: playground.playground_id,
    precommit_id: playground.active_precommit_id,
    precommit_request_id: 'precommit-nightly-seeded',
    data_layout: 'fast_cdc',
    attempt: 1,
    state: 'running',
    phase: 'hashing',
    progress: {
      percent: 52,
      files_completed: '9648',
      files_total: '18554',
      bytes_completed: '422785843200',
      bytes_total: '845571686400',
    },
    checks: [],
    warnings: [],
    blockers: [],
    source_index_version: structuredClone(playground.index_version),
    created_at_unix_ms: playground.updated_at_unix_ms,
    updated_at_unix_ms: playground.updated_at_unix_ms,
  });
  precommitQueryCounts.set(key, 2);
  precommitSourceHeads.set(key, playground.head_commit_id ?? null);
}

function mutationConflict(
  request: Request,
  code: string,
  detail: string,
): HttpResponse<ProblemDetails> {
  return problem(request, 409, code, 'Resource mutation conflict', detail);
}

function notFound(request: Request, resource: string): HttpResponse<ProblemDetails> {
  return problem(
    request,
    404,
    `${resource.toUpperCase().replaceAll(' ', '_')}_NOT_FOUND`,
    `${resource} not found`,
    `The requested ${resource} was not found`,
  );
}

function s3NotFound(request: Request, resource: string): HttpResponse<ProblemDetails> {
  return problem(
    request,
    404,
    `S3_${resource.toUpperCase()}_NOT_FOUND`,
    `${resource} not found`,
    `The requested S3 ${resource} was not found`,
  );
}

function resolveS3AccessPoint(
  request: Request,
  tenantId: string,
  accessPointId: string,
): S3AccessPointView | HttpResponse<ProblemDetails> {
  const accessPoint = s3AccessPoints.find(
    (item) => item.tenant_id === tenantId && item.access_point_id === accessPointId,
  );
  return accessPoint ?? s3NotFound(request, 'access point');
}

function s3ObjectEntries(
  entries: S3ObjectEntryView[],
  prefix = '',
  delimiter = '',
): S3ObjectEntryView[] {
  const matching = entries.filter(
    (entry) => entry.entry_type === 'object' && entry.key.startsWith(prefix),
  );
  if (!delimiter) return matching;
  const values: S3ObjectEntryView[] = [];
  const prefixes = new Set<string>();
  for (const entry of matching) {
    const remainder = entry.key.slice(prefix.length);
    const separator = remainder.indexOf(delimiter);
    if (separator === -1) {
      values.push(entry);
      continue;
    }
    const key = prefix + remainder.slice(0, separator + delimiter.length);
    if (prefixes.has(key)) continue;
    prefixes.add(key);
    values.push({ key, entry_type: 'prefix' });
  }
  return values.sort((left, right) => left.key.localeCompare(right.key));
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
  // In mock mode the Access Point endpoint defaults to the current Web origin. Keep the
  // object data plane separate from Central's `/api` listener so an anchor download cannot
  // accidentally turn into a Fusen service invocation.
  http.get('*/road-scenes-snapshot/*', ({ request }) => {
    const resolved = resolveMockS3Path(new URL(request.url).pathname);
    if (!resolved) {
      return new HttpResponse(
        '<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>',
        {
          status: 404,
          headers: { 'Content-Type': 'application/xml' },
        },
      );
    }
    if (!resolved.file) {
      return new HttpResponse(
        '<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>',
        {
          status: 404,
          headers: { 'Content-Type': 'application/xml' },
        },
      );
    }
    const file = resolved.file;
    const etag = file.etag === undefined ? {} : { ETag: file.etag };
    return new HttpResponse(file.body, {
      status: 200,
      headers: {
        'Content-Type': file.content_type,
        'Content-Length': String(new TextEncoder().encode(file.body).byteLength),
        'Content-Disposition': `attachment; filename="${file.key.split('/').pop() ?? 'download'}"`,
        ...etag,
        'Last-Modified': new Date(Number(file.last_modified_unix_ms)).toUTCString(),
      },
    });
  }),
  http.head('*/road-scenes-snapshot/*', ({ request }) => {
    const resolved = resolveMockS3Path(new URL(request.url).pathname);
    if (!resolved?.file) {
      return new HttpResponse(null, { status: 404 });
    }
    const file = resolved.file;
    const etag = file.etag === undefined ? {} : { ETag: file.etag };
    return new HttpResponse(null, {
      status: 200,
      headers: {
        'Content-Type': file.content_type,
        'Content-Length': String(new TextEncoder().encode(file.body).byteLength),
        ...etag,
        'Last-Modified': new Date(Number(file.last_modified_unix_ms)).toUTCString(),
      },
    });
  }),
  http.post('*/api/system/version/query', ({ request }) =>
    HttpResponse.json(
      {
        api_version: 1,
        agent_wire_version: 1,
        capabilities: [
          'managed_add',
          'artifact_catalog',
          'artifact_commit_graph',
          'playground_browser',
          'playground_materialize',
          'playground_precommit',
          'snapshot_materialize',
          'commit_layout_selection_v2',
          'snapshot_delivery_fuse_v2',
          'snapshot_delivery_copy_v2',
          'snapshot_delivery_hardlink_v2',
          's3_readonly_access_point',
          'resource_lifecycle_v1',
          'storage_enrollment',
          'tenant_admin',
          'sqlite_authority',
        ],
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
        'storage.enrollment.create',
        'storage.enrollment.read',
        'storage.enrollment.review',
        'artifact.read',
        'artifact.create',
        'project.read',
        'project.create',
        'playground.create',
        'snapshot.create',
        'job.create',
        'resource.lifecycle.read',
        'resource.lifecycle.manage',
        'retention.manage',
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
        isLifecycleActive(storageVolume) &&
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
    const backendReferenceValid =
      (body.backend_type === 'pvc' &&
        Boolean(body.pvc_reference && validPvcReference(body.pvc_reference)) &&
        !body.nfs_reference) ||
      (body.backend_type === 'nfs' &&
        Boolean(body.nfs_reference?.server && body.nfs_reference.export_path?.startsWith('/')) &&
        !body.pvc_reference);
    if (
      !RESOURCE_ID_PATTERN.test(body.storage_volume_id) ||
      !RESOURCE_ID_PATTERN.test(body.edge_cluster_id) ||
      !body.display_name.trim() ||
      body.display_name.length > 128 ||
      !REGION_PATTERN.test(body.region) ||
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

    if (body.backend_type === 'pvc' && body.pvc_reference) {
      const binding = pvcBindings.get(
        pvcIdentity({
          edge_cluster_id: body.edge_cluster_id,
          pvc_reference: body.pvc_reference,
        }),
      );
      if (binding) {
        return mutationConflict(
          request,
          'PVC_ALREADY_ENROLLED',
          'The PVC is already registered as another StorageVolume',
        );
      }
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
      allowed_delivery_modes: body.allowed_delivery_modes ?? ['fuse', 'copy'],
      hardlink_policy: body.hardlink_policy ?? 'disabled',
      max_whole_file_bytes: body.max_whole_file_bytes ?? '18446744073709551615',
      copy_reserve_bytes: body.copy_reserve_bytes ?? '0',
      ...(body.backend_type === 'pvc' && body.pvc_reference
        ? { pvc_reference: body.pvc_reference }
        : {}),
      state: 'unavailable',
      resource_version: '1',
      lifecycle: activeLifecycle(),
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    storageVolumes.push(storageVolume);
    if (storageVolume.backend_type === 'pvc' && storageVolume.pvc_reference) {
      pvcBindings.set(
        pvcIdentity({
          edge_cluster_id: storageVolume.edge_cluster_id,
          pvc_reference: storageVolume.pvc_reference,
        }),
        { tenantId: storageVolume.tenant_id, storageVolumeId: storageVolume.storage_volume_id },
      );
    }
    storageVolumeCreatePayloads.set(key, requestJson);
    const response: CreateStorageVolumeResponse = {
      storage_volume: storageVolume,
      replayed: false,
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/enrollment/token/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateStorageEnrollmentTokenRequest;
    const failed = requireEnrollmentPermission(
      request,
      body.tenant_id,
      'storage.enrollment.create',
    );
    if (failed) return failed;
    expireStorageEnrollments();
    if (
      !RESOURCE_ID_PATTERN.test(body.token_request_id) ||
      !RESOURCE_ID_PATTERN.test(body.storage_volume_id) ||
      !RESOURCE_ID_PATTERN.test(body.edge_cluster_id) ||
      !body.display_name.trim() ||
      body.display_name.length > 128 ||
      !REGION_PATTERN.test(body.region) ||
      !['read_write_many', 'read_write_once'].includes(body.access_mode) ||
      !body.pvc_reference ||
      !validPvcReference(body.pvc_reference)
    ) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'Storage enrollment identity and PVC descriptor must be valid',
        {
          retryable: false,
          violations: [
            { field: 'storage_volume_id', reason: 'must be a valid resource ID' },
            { field: 'pvc_reference', reason: 'namespace and claim_name are required' },
          ],
        },
      );
    }

    const requestKey = resourceKey(body.tenant_id, body.token_request_id);
    const requestJson = stableJson(body);
    const previous = storageEnrollmentTokenRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'STORAGE_ENROLLMENT_TOKEN_REQUEST_ID_REUSED',
          'The token request ID already belongs to another payload',
        );
      }
      return HttpResponse.json(
        { ...structuredClone(previous.response), replayed: true },
        { headers: headers(request) },
      );
    }

    const descriptor = enrollmentDescriptor(body);
    const pvcKey = pvcIdentity(descriptor);

    const existingVolume = storageVolumes.find(
      (item) =>
        item.tenant_id === body.tenant_id && item.storage_volume_id === body.storage_volume_id,
    );
    if (existingVolume && !volumeMatchesDescriptor(existingVolume, descriptor)) {
      return mutationConflict(
        request,
        'STORAGE_VOLUME_DESCRIPTOR_CONFLICT',
        'The existing StorageVolume descriptor does not match this enrollment',
      );
    }
    const pvcBinding = pvcBindings.get(pvcKey);
    if (
      pvcBinding &&
      (pvcBinding.tenantId !== body.tenant_id ||
        pvcBinding.storageVolumeId !== body.storage_volume_id)
    ) {
      return mutationConflict(
        request,
        'PVC_ALREADY_ENROLLED',
        'The PVC is already registered as another StorageVolume',
      );
    }
    if (
      storageEnrollments.some(
        (item) =>
          ((item.tenant_id === body.tenant_id &&
            item.storage_volume_id === body.storage_volume_id) ||
            pvcIdentity(item) === pvcKey) &&
          item.state === 'pending_approval',
      )
    ) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_ALREADY_PENDING',
        'This StorageVolume already has a pending enrollment request',
      );
    }

    const now = Date.now();
    const tokenId = `storage-enrollment-token-${crypto.randomUUID()}`;
    const response: CreateStorageEnrollmentTokenResponse = {
      token_id: tokenId,
      bootstrap_token: `ngenr_v1_${crypto.randomUUID().replaceAll('-', '')}`,
      volume_descriptor_digest: 'd'.repeat(64),
      expires_at_unix_ms: (now + 15 * 60 * 1000).toString(),
      replayed: false,
    };
    storageEnrollmentTokenRequests.set(requestKey, {
      requestJson,
      descriptor,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/enrollment/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryStorageEnrollmentListRequest;
    const failed = requireEnrollmentPermission(request, body.tenant_id, 'storage.enrollment.read');
    if (failed) return failed;
    expireStorageEnrollments();
    const normalizedQuery = body.query?.trim().toLocaleLowerCase('en-US') ?? '';
    const filtered = storageEnrollments.filter(
      (item) =>
        item.tenant_id === body.tenant_id &&
        (!body.state || item.state === body.state) &&
        (!body.registration_kind || item.registration_kind === body.registration_kind) &&
        (!normalizedQuery ||
          item.storage_volume_id.toLocaleLowerCase('en-US').includes(normalizedQuery) ||
          item.display_name.toLocaleLowerCase('en-US').includes(normalizedQuery)),
    );
    const page = paginate(
      request,
      'storage-enrollments',
      {
        tenant_id: body.tenant_id,
        state: body.state ?? '',
        registration_kind: body.registration_kind ?? '',
        query: normalizedQuery,
      },
      filtered,
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QueryStorageEnrollmentListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/enrollment/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryStorageEnrollmentRequest;
    const failed = requireEnrollmentPermission(request, body.tenant_id, 'storage.enrollment.read');
    if (failed) return failed;
    expireStorageEnrollments();
    const enrollment = storageEnrollments.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.storage_enrollment_id === body.storage_enrollment_id,
    );
    if (!enrollment) return storageEnrollmentNotFound(request);
    const response: QueryStorageEnrollmentResponse = { enrollment: structuredClone(enrollment) };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/enrollment/approve', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as ApproveStorageEnrollmentRequest;
    const failed = requireEnrollmentPermission(
      request,
      body.tenant_id,
      'storage.enrollment.review',
    );
    if (failed) return failed;
    expireStorageEnrollments();
    const enrollment = storageEnrollments.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.storage_enrollment_id === body.storage_enrollment_id,
    );
    if (!enrollment) return storageEnrollmentNotFound(request);
    const requestKey = resourceKey(body.tenant_id, body.approval_request_id);
    const requestJson = stableJson(body);
    const previous = storageEnrollmentDecisionRequests.get(requestKey);
    if (previous) {
      if (previous.kind !== 'approve' || previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'STORAGE_ENROLLMENT_DECISION_ID_REUSED',
          'The decision request ID already belongs to another review operation or payload',
        );
      }
      return HttpResponse.json(
        { ...structuredClone(previous.response), replayed: true },
        { headers: headers(request) },
      );
    }

    if (enrollment.state !== 'pending_approval') {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_STATE_CONFLICT',
        'The enrollment is no longer pending approval',
      );
    }
    if (enrollment.resource_version !== body.expected_resource_version) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_VERSION_CONFLICT',
        'The enrollment resource version changed before review',
      );
    }
    if (enrollment.registration_kind === 'replacement' && !body.confirm_replacement) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_REPLACEMENT_CONFIRMATION_REQUIRED',
        'Replacement enrollment approval requires explicit confirmation',
      );
    }
    if (
      !enrollment.probe.descriptor_matches ||
      enrollment.probe.observed_access_mode !== 'read_write'
    ) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_PROBE_FAILED',
        'The reported mount is not eligible for approval',
      );
    }

    const frozenDescriptor = storageEnrollmentFrozenDescriptors.get(
      resourceKey(enrollment.tenant_id, enrollment.storage_enrollment_id),
    );
    if (!frozenDescriptor || !descriptorMatches(enrollment, frozenDescriptor)) {
      return mutationConflict(
        request,
        'STORAGE_VOLUME_DESCRIPTOR_MISMATCH',
        'The enrollment no longer matches the descriptor frozen at bootstrap',
      );
    }

    const pvcKey = pvcIdentity(frozenDescriptor);
    const binding = pvcBindings.get(pvcKey);
    const owner = activePvcOwners.get(pvcKey);
    let storageVolume = storageVolumes.find(
      (item) =>
        item.tenant_id === enrollment.tenant_id &&
        item.storage_volume_id === enrollment.storage_volume_id,
    );
    if (
      binding &&
      (binding.tenantId !== enrollment.tenant_id ||
        binding.storageVolumeId !== enrollment.storage_volume_id)
    ) {
      return mutationConflict(
        request,
        'PVC_ALREADY_ENROLLED',
        'The PVC is already bound to another StorageVolume',
      );
    }
    if (
      storageEnrollments.some(
        (item) =>
          item.storage_enrollment_id !== enrollment.storage_enrollment_id &&
          ['pending_approval', 'approved', 'enrolled'].includes(item.state) &&
          pvcIdentity(item) === pvcKey,
      )
    ) {
      return mutationConflict(
        request,
        'PVC_ALREADY_ENROLLED',
        'The PVC already has another active enrollment',
      );
    }
    if (enrollment.registration_kind === 'initial') {
      const createsMissingVolume = !storageVolume && !binding && !owner;
      const bindsUnownedVolume =
        storageVolume?.state === 'unavailable' &&
        volumeMatchesDescriptor(storageVolume, frozenDescriptor) &&
        binding?.tenantId === enrollment.tenant_id &&
        binding.storageVolumeId === enrollment.storage_volume_id &&
        !owner;
      if (!createsMissingVolume && !bindsUnownedVolume) {
        return mutationConflict(
          request,
          'STORAGE_ENROLLMENT_INITIAL_CONFLICT',
          'Initial enrollment requires a missing Volume or an exact unavailable, unowned Volume binding',
        );
      }
    } else if (
      !storageVolume ||
      !volumeMatchesDescriptor(storageVolume, frozenDescriptor) ||
      !owner ||
      owner.tenantId !== enrollment.tenant_id ||
      owner.storageVolumeId !== enrollment.storage_volume_id
    ) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_REPLACEMENT_CONFLICT',
        'Replacement requires the exact existing Volume and its active owner',
      );
    }

    const now = Date.now().toString();
    if (!storageVolume) {
      storageVolume = {
        tenant_id: enrollment.tenant_id,
        storage_volume_id: enrollment.storage_volume_id,
        display_name: enrollment.display_name,
        edge_cluster_id: enrollment.edge_cluster_id,
        region: enrollment.region,
        backend_type: 'pvc',
        access_mode: enrollment.access_mode,
        allowed_delivery_modes: ['fuse', 'copy'],
        hardlink_policy: 'disabled',
        max_whole_file_bytes: '18446744073709551615',
        copy_reserve_bytes: '0',
        pvc_reference: structuredClone(enrollment.pvc_reference),
        state: 'unavailable',
        resource_version: '1',
        lifecycle: activeLifecycle(),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
      };
      storageVolumes.push(storageVolume);
    } else {
      storageVolume.state = 'unavailable';
      storageVolume.resource_version = (BigInt(storageVolume.resource_version) + 1n).toString();
      storageVolume.updated_at_unix_ms = now;
    }
    pvcBindings.set(pvcKey, {
      tenantId: enrollment.tenant_id,
      storageVolumeId: enrollment.storage_volume_id,
    });
    activePvcOwners.set(pvcKey, {
      tenantId: enrollment.tenant_id,
      storageVolumeId: enrollment.storage_volume_id,
      identityFingerprint: enrollment.identity_fingerprint,
    });
    enrollment.state = 'approved';
    enrollment.resource_version = (BigInt(enrollment.resource_version) + 1n).toString();
    enrollment.reviewed_at_unix_ms = now;
    enrollment.updated_at_unix_ms = now;
    const response: ApproveStorageEnrollmentResponse = {
      enrollment: structuredClone(enrollment),
      storage_volume: structuredClone(storageVolume),
      replayed: false,
    };
    storageEnrollmentDecisionRequests.set(requestKey, {
      kind: 'approve',
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/storage/enrollment/reject', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as RejectStorageEnrollmentRequest;
    const failed = requireEnrollmentPermission(
      request,
      body.tenant_id,
      'storage.enrollment.review',
    );
    if (failed) return failed;
    expireStorageEnrollments();
    const enrollment = storageEnrollments.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.storage_enrollment_id === body.storage_enrollment_id,
    );
    if (!enrollment) return storageEnrollmentNotFound(request);
    const requestKey = resourceKey(body.tenant_id, body.rejection_request_id);
    const requestJson = stableJson(body);
    const previous = storageEnrollmentDecisionRequests.get(requestKey);
    if (previous) {
      if (previous.kind !== 'reject' || previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'STORAGE_ENROLLMENT_DECISION_ID_REUSED',
          'The decision request ID already belongs to another review operation or payload',
        );
      }
      return HttpResponse.json(
        { ...structuredClone(previous.response), replayed: true },
        { headers: headers(request) },
      );
    }

    if (enrollment.state !== 'pending_approval') {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_STATE_CONFLICT',
        'The enrollment is no longer pending approval',
      );
    }
    if (enrollment.resource_version !== body.expected_resource_version) {
      return mutationConflict(
        request,
        'STORAGE_ENROLLMENT_VERSION_CONFLICT',
        'The enrollment resource version changed before review',
      );
    }

    const now = Date.now().toString();
    enrollment.state = 'rejected';
    enrollment.resource_version = (BigInt(enrollment.resource_version) + 1n).toString();
    enrollment.reviewed_at_unix_ms = now;
    const reviewReason = body.reason?.trim();
    if (reviewReason) {
      storageEnrollmentReviewAudit.set(
        resourceKey(enrollment.tenant_id, enrollment.storage_enrollment_id),
        reviewReason,
      );
    }
    enrollment.updated_at_unix_ms = now;
    const response: RejectStorageEnrollmentResponse = {
      enrollment: structuredClone(enrollment),
      replayed: false,
    };
    storageEnrollmentDecisionRequests.set(requestKey, {
      kind: 'reject',
      requestJson,
      response: structuredClone(response),
    });
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
  http.post('*/api/project/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateProjectRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    if (
      !RESOURCE_ID_PATTERN.test(body.project_id) ||
      !body.display_name?.trim() ||
      (body.description !== undefined && body.description.length > 2048)
    ) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'project_id and display_name must be valid and description must be at most 2048 characters',
      );
    }
    const key = resourceKey(body.tenant_id, body.project_id);
    const requestJson = stableJson(body);
    const existing = projects.find(
      (project) => project.tenant_id === body.tenant_id && project.project_id === body.project_id,
    );
    if (existing) {
      if (projectCreatePayloads.get(key) !== requestJson) {
        return mutationConflict(
          request,
          'PROJECT_ID_REUSED',
          'The Project ID already belongs to a different create request',
        );
      }
      const response: CreateProjectResponse = {
        project: structuredClone(existing),
        replayed: true,
      };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const now = Date.now().toString();
    const project = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      display_name: body.display_name.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    projects.push(project);
    projectCreatePayloads.set(key, requestJson);
    const response: CreateProjectResponse = { project, replayed: false };
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
        isLifecycleActive(artifact) &&
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
    const initialization = body.initialization;
    if (initialization.mode === 'derived') {
      return mutationConflict(
        request,
        'ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED',
        'derived Artifact initialization requires authoritative Commit validation',
      );
    }

    const createKey = resourceKey(body.tenant_id, body.artifact_id);
    const graphKey = resourceKey(body.tenant_id, body.project_id, body.artifact_id);
    const requestJson = stableJson(body);
    const existing = artifacts.find(
      (artifact) =>
        artifact.tenant_id === body.tenant_id && artifact.artifact_id === body.artifact_id,
    );
    if (existing) {
      const priorRequest = artifactCreatePayloads.get(createKey);
      const equivalentExisting =
        existing.project_id === body.project_id &&
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
    const artifact: ArtifactView = {
      tenant_id: body.tenant_id,
      project_id: body.project_id,
      artifact_id: body.artifact_id,
      display_name: body.display_name.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      initialization: structuredClone(initialization),
      resource_version: '1',
      lifecycle: activeLifecycle(),
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    artifacts.push(artifact);
    commitGraphs.set(graphKey, { graph_version: '0', nodes: [] });
    artifactCreatePayloads.set(createKey, requestJson);
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
        isLifecycleActive(playground) &&
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
    if (
      playground.state === 'creating' &&
      completesOnThisQuery(playgroundQueryCounts, playgroundKey)
    ) {
      playground.state = 'ready';
      playground.updated_at_unix_ms = Date.now().toString();
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

    const key = resourceKey(body.tenant_id, body.project_id, body.artifact_id, body.playground_id);
    const existing = playgrounds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.project_id === body.project_id &&
        item.artifact_id === body.artifact_id &&
        item.playground_id === body.playground_id,
    );
    if (existing) {
      const equivalentExisting =
        existing.display_name === body.display_name.trim() &&
        existing.storage_volume_id === body.storage_volume_id &&
        (!body.base_commit_id || existing.base_commit_id === body.base_commit_id);
      if (!equivalentExisting) {
        return mutationConflict(
          request,
          'PLAYGROUND_ID_REUSED',
          'The Playground ID already belongs to a different create request',
        );
      }
      const response: CreatePlaygroundResponse = { playground: existing, replayed: true };
      return HttpResponse.json(response, { headers: headers(request) });
    }
    if (
      body.base_commit_id &&
      !graph.nodes.some((commit) => commit.commit_id === body.base_commit_id)
    ) {
      return problem(
        request,
        404,
        'COMMIT_NOT_FOUND',
        'Commit not found',
        'The requested base Commit was not found in the selected Artifact',
      );
    }
    const storageVolume = resolveStorageVolume(request, body.tenant_id, body.storage_volume_id);
    if (storageVolume instanceof HttpResponse) return storageVolume;

    const baseCommitId = body.base_commit_id ?? artifact.head_commit_id;
    const indexVersion = baseCommitId
      ? { revision: graph.graph_version, digest: baseCommitId }
      : { revision: '0', digest: '0'.repeat(64) };
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
      index_version: indexVersion,
      state: 'creating' as const,
      storage_availability: 'ready' as const,
      resource_version: '1',
      lifecycle: activeLifecycle(),
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    playgrounds.push(playground);
    playgroundQueryCounts.set(key, 0);
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
    if (playground.state !== 'ready' || playground.storage_availability !== 'ready') {
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
      data_layout: body.data_layout,
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
    const precommitKey = resourceKey(body.tenant_id, precommitId);
    precommits.set(precommitKey, precommit);
    precommitQueryCounts.set(precommitKey, 0);
    precommitSourceHeads.set(precommitKey, playground.head_commit_id ?? null);
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
    const precommitKey = resourceKey(body.tenant_id, body.precommit_id);
    const precommit = precommits.get(precommitKey);
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
        const phases = [
          { phase: 'queued' as const, percent: 0, files: '0', bytes: '0' },
          { phase: 'scanning' as const, percent: 24, files: '3184', bytes: '128849018880' },
          { phase: 'hashing' as const, percent: 52, files: '9648', bytes: '422785843200' },
          { phase: 'uploading' as const, percent: 74, files: '18554', bytes: '625790156800' },
          { phase: 'validating' as const, percent: 91, files: '18554', bytes: '769658139624' },
        ];
        const nextCount = (precommitQueryCounts.get(precommitKey) ?? 0) + 1;
        precommitQueryCounts.set(precommitKey, nextCount);
        const next = phases[nextCount];
        if (next) {
          precommit.phase = next.phase;
          precommit.progress = {
            percent: next.percent,
            files_completed: next.files,
            files_total: '18554',
            bytes_completed: next.bytes,
            bytes_total: '845571686400',
          };
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
            {
              check_id: 'metadata-shape',
              status: 'passed',
              summary: 'Metadata 和逻辑路径检查通过',
            },
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
    const precommitKey = resourceKey(body.tenant_id, body.precommit_id);
    precommitQueryCounts.set(precommitKey, 0);
    precommitSourceHeads.set(precommitKey, playground.head_commit_id ?? null);
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
    const frozenHead = precommitSourceHeads.get(resourceKey(body.tenant_id, body.precommit_id));
    if (frozenHead === undefined || frozenHead !== (playground.head_commit_id ?? null)) {
      return mutationConflict(
        request,
        'HEAD_COMMIT_CONFLICT',
        'The Playground Head changed after this Pre-commit was started',
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
      commit_id: contentDigest({ request: body, at: now }),
      ...(playground.head_commit_id ? { parent_commit_id: playground.head_commit_id } : {}),
      message: body.message.trim(),
      ...(body.description?.trim() ? { description: body.description.trim() } : {}),
      tag_names: tagNames,
      data_layout: body.data_layout,
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
        isLifecycleActive(snapshot) &&
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
    const snapshotKey = resourceKey(snapshot.tenant_id, snapshot.snapshot_id);
    if (snapshot.state === 'creating' && completesOnThisQuery(snapshotQueryCounts, snapshotKey)) {
      snapshot.state = 'ready';
      snapshot.integrity = {
        state: 'verified',
        files_verified: snapshot.logical_file_count,
        bytes_verified: snapshot.logical_size_bytes,
        verified_at_unix_ms: Date.now().toString(),
      };
      snapshot.updated_at_unix_ms = Date.now().toString();
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
      data_layout: commit.data_layout,
      storage_volume_id: storageVolume.storage_volume_id,
      region: storageVolume.region,
      message: commit.message,
      tag_names: [...commit.tag_names],
      state: 'ready' as const,
      integrity: {
        state: 'verified' as const,
        files_verified: sameCommitSnapshot?.logical_file_count ?? '864',
        bytes_verified: sameCommitSnapshot?.logical_size_bytes ?? '12884901888',
        verified_at_unix_ms: Date.now().toString(),
      },
      resource_version: '1',
      lifecycle: activeLifecycle(),
      logical_file_count: sameCommitSnapshot?.logical_file_count ?? '864',
      logical_size_bytes: sameCommitSnapshot?.logical_size_bytes ?? '12884901888',
      created_at_unix_ms: Date.now().toString(),
      updated_at_unix_ms: Date.now().toString(),
    };
    snapshots.unshift(snapshot);
    snapshotQueryCounts.set(resourceKey(snapshot.tenant_id, snapshot.snapshot_id), 0);
    const response: CreateSnapshotResponse = {
      snapshot,
      replayed: false,
      placement_reused: false,
    };
    snapshotCreateRequests.set(requestKey, { requestJson, response: structuredClone(response) });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateSnapshotDeliveryRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, body.request_id);
    const requestJson = stableJson(body);
    const priorRequest = snapshotDeliveryCreateRequests.get(requestKey);
    if (priorRequest) {
      if (priorRequest.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'DELIVERY_REQUEST_ID_REUSED',
          'The delivery request ID already belongs to a different request',
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
    if (snapshot.state !== 'ready') {
      return mutationConflict(
        request,
        'SNAPSHOT_NOT_READY',
        'Snapshot deliveries require a Ready Snapshot',
      );
    }
    const storageVolume = storageVolumes.find(
      (item) =>
        item.tenant_id === body.tenant_id && item.storage_volume_id === snapshot.storage_volume_id,
    );
    if (!storageVolume) return notFound(request, 'StorageVolume');
    if (!storageVolume.allowed_delivery_modes.includes(body.mode)) {
      return mutationConflict(
        request,
        'DELIVERY_MODE_NOT_ALLOWED',
        'The StorageVolume policy does not allow this delivery mode',
      );
    }
    if (body.mode === 'hardlink' && snapshot.data_layout !== 'whole_file') {
      return mutationConflict(
        request,
        'HARDLINK_REQUIRES_WHOLE_FILE',
        'Hardlink delivery requires a WholeFile Commit',
      );
    }
    if (body.mode === 'hardlink' && storageVolume.hardlink_policy === 'disabled') {
      return mutationConflict(
        request,
        'HARDLINK_UNSAFE_VOLUME',
        'Hardlink delivery requires a sealed or trusted-local Volume policy',
      );
    }
    const now = Date.now().toString();
    const deliveryId = `delivery-${fingerprint(body)}`;
    const delivery: SnapshotDeliveryView = {
      delivery_id: deliveryId,
      snapshot_id: snapshot.snapshot_id,
      commit_id: snapshot.commit_id,
      storage_volume_id: snapshot.storage_volume_id,
      mode: body.mode,
      target_relative_root: `snapshots/${snapshot.project_id}/${snapshot.artifact_id}/${snapshot.snapshot_id}/deliveries/${deliveryId}`,
      state: 'requested',
      source_index_digest: snapshot.commit_id,
      delivery_generation: '1',
      file_count: snapshot.logical_file_count,
      size_bytes: snapshot.logical_size_bytes,
      object_set_digest: snapshot.commit_id,
      resource_version: '1',
      created_at_unix_ms: now,
      updated_at_unix_ms: now,
    };
    snapshotDeliveries.unshift(delivery);
    const response: CreateSnapshotDeliveryResponse = { delivery, replayed: false };
    snapshotDeliveryCreateRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotDeliveryRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const resolved = resolveSnapshotDelivery(body.tenant_id, body.delivery_id);
    if (!resolved) return notFound(request, 'Snapshot delivery');
    const response: QuerySnapshotDeliveryResponse = { delivery: resolved.delivery };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QuerySnapshotDeliveryListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return notFound(request, 'Snapshot');
    const page = paginate(
      request,
      'snapshot-deliveries',
      { tenant_id: body.tenant_id, snapshot_id: body.snapshot_id },
      snapshotDeliveries.filter((item) => item.snapshot_id === body.snapshot_id),
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QuerySnapshotDeliveryListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/retry', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as RetrySnapshotDeliveryRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, body.request_id);
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
    const resolved = resolveSnapshotDelivery(body.tenant_id, body.delivery_id);
    if (!resolved) return notFound(request, 'Snapshot delivery');
    if (resolved.delivery.state !== 'failed') {
      return mutationConflict(
        request,
        'DELIVERY_INVALID_STATE',
        'Only a failed Snapshot delivery can be retried',
      );
    }
    const now = Date.now().toString();
    resolved.delivery.state = 'requested';
    resolved.delivery.delivery_generation = (
      BigInt(resolved.delivery.delivery_generation) + 1n
    ).toString();
    resolved.delivery.resource_version = (
      BigInt(resolved.delivery.resource_version) + 1n
    ).toString();
    resolved.delivery.updated_at_unix_ms = now;
    delete resolved.delivery.issue;
    const response: RetrySnapshotDeliveryResponse = {
      delivery: resolved.delivery,
      replayed: false,
    };
    snapshotRetryRequests.set(requestKey, { requestJson, response: structuredClone(response) });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/snapshot/delivery/delete', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as DeleteSnapshotDeliveryRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, body.request_id);
    const requestJson = stableJson(body);
    const priorRequest = snapshotDeliveryDeleteRequests.get(requestKey);
    if (priorRequest) {
      if (priorRequest.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'DELIVERY_REQUEST_ID_REUSED',
          'The delivery request ID already belongs to a different request',
        );
      }
      const response = structuredClone(priorRequest.response);
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const resolved = resolveSnapshotDelivery(body.tenant_id, body.delivery_id);
    if (!resolved) return notFound(request, 'Snapshot delivery');
    resolved.delivery.state = 'deleting';
    resolved.delivery.resource_version = (
      BigInt(resolved.delivery.resource_version) + 1n
    ).toString();
    resolved.delivery.updated_at_unix_ms = Date.now().toString();
    const response: DeleteSnapshotDeliveryResponse = {
      delivery: resolved.delivery,
      replayed: false,
    };
    snapshotDeliveryDeleteRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
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
        created_at_unix_ms: snapshot.created_at_unix_ms,
      },
      ...(snapshot.state === 'ready'
        ? [
            {
              activity_id: `activity-${snapshot.snapshot_id}-ready`,
              activity_type: 'ready' as const,
              summary: 'Snapshot 完整性校验通过并可读取',
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
  http.post('*/api/s3/access-point/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryS3AccessPointListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const values = s3AccessPoints.filter((item) => item.tenant_id === body.tenant_id);
    const page = paginate(request, 's3-access-points', { tenant_id: body.tenant_id }, values, body);
    if (page instanceof HttpResponse) return page;
    const response: QueryS3AccessPointListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/access-point/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryS3AccessPointRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    const response: QueryS3AccessPointResponse = { access_point: accessPoint };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/access-point/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateS3AccessPointRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const snapshot = snapshots.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (!snapshot) return s3NotFound(request, 'snapshot');
    if (snapshot.state !== 'ready') {
      return mutationConflict(
        request,
        'SNAPSHOT_INVALID_STATE',
        'S3 access can only be enabled for a Ready Snapshot',
      );
    }
    if (
      !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/.test(
        body.bucket_name,
      )
    ) {
      return problem(
        request,
        422,
        'PROTOCOL_INVALID',
        'Request validation failed',
        'bucket_name must be a DNS-compatible bucket name',
      );
    }
    const requestJson = stableJson(body);
    const requestKey = resourceKey(body.tenant_id, body.request_id);
    const previous = s3AccessPointCreateRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'REQUEST_ID_REUSED',
          'The request ID belongs to another S3 request',
        );
      }
      const response = structuredClone(previous.response);
      response.replayed = true;
      delete response.secret_access_key;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const existing = s3AccessPoints.find(
      (item) => item.tenant_id === body.tenant_id && item.snapshot_id === body.snapshot_id,
    );
    if (existing) {
      if (existing.bucket_name !== body.bucket_name) {
        return mutationConflict(
          request,
          'SNAPSHOT_S3_ACCESS_POINT_EXISTS',
          'Snapshot already has a different S3 bucket',
        );
      }
      const response: CreateS3AccessPointResponse = {
        access_point: existing,
        access_key_id: 'NGS3ROADREADER01',
        credential_expires_at_unix_ms: '1790000000000',
        replayed: true,
      };
      s3AccessPointCreateRequests.set(requestKey, {
        requestJson,
        response: structuredClone(response),
      });
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const accessPoint: S3AccessPointView = {
      access_point_id: `ap-${body.snapshot_id}`,
      tenant_id: body.tenant_id,
      project_id: snapshot.project_id,
      artifact_id: snapshot.artifact_id,
      snapshot_id: snapshot.snapshot_id,
      commit_id: snapshot.commit_id,
      bucket_name: body.bucket_name,
      endpoint: runtimeConfig.s3Endpoint,
      region: snapshot.region,
      state: 'active',
      policy_generation: '1',
      created_at_unix_ms: Date.now().toString(),
      updated_at_unix_ms: Date.now().toString(),
    };
    s3AccessPoints.push(accessPoint);
    if (!s3Objects.has(accessPoint.access_point_id)) s3Objects.set(accessPoint.access_point_id, []);
    const accessKeyId = `NGS3${accessPoint.access_point_id.slice(-12).toUpperCase()}`;
    const secretAccessKey = `mock-secret-${crypto.randomUUID()}`;
    s3Credentials.push({
      credential_id: `cred-${crypto.randomUUID().slice(0, 8)}`,
      access_point_id: accessPoint.access_point_id,
      access_key_id: accessKeyId,
      state: 'active',
      expires_at_unix_ms: '1790000000000',
      created_at_unix_ms: Date.now().toString(),
    });
    const response: CreateS3AccessPointResponse = {
      access_point: accessPoint,
      access_key_id: accessKeyId,
      secret_access_key: secretAccessKey,
      credential_expires_at_unix_ms: '1790000000000',
      replayed: false,
    };
    s3AccessPointCreateRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/access-point/enable', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as UpdateS3AccessPointRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    accessPoint.state = 'active';
    accessPoint.policy_generation = (BigInt(accessPoint.policy_generation) + 1n).toString();
    accessPoint.updated_at_unix_ms = Date.now().toString();
    const response: UpdateS3AccessPointResponse = { access_point: accessPoint, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/access-point/disable', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as UpdateS3AccessPointRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    accessPoint.state = 'disabled';
    for (const credential of s3Credentials) {
      if (
        credential.access_point_id === accessPoint.access_point_id &&
        credential.state === 'active'
      ) {
        credential.state = 'revoked';
      }
    }
    accessPoint.policy_generation = (BigInt(accessPoint.policy_generation) + 1n).toString();
    accessPoint.updated_at_unix_ms = Date.now().toString();
    const response: UpdateS3AccessPointResponse = { access_point: accessPoint, replayed: false };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/credential/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryS3CredentialListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    const response: QueryS3CredentialListResponse = {
      items: s3Credentials.filter((item) => item.access_point_id === accessPoint.access_point_id),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/credential/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateS3CredentialRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    if (accessPoint.state !== 'active') {
      return mutationConflict(
        request,
        'S3_ACCESS_POINT_DISABLED',
        'Enable the S3 access point before issuing credentials',
      );
    }
    const requestJson = stableJson(body);
    const requestKey = resourceKey(body.tenant_id, body.request_id);
    const previous = s3CredentialCreateRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(
          request,
          'REQUEST_ID_REUSED',
          'The request ID belongs to another credential request',
        );
      }
      const response = structuredClone(previous.response);
      response.replayed = true;
      delete response.secret_access_key;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    if (
      s3Credentials.filter(
        (credential) =>
          credential.access_point_id === accessPoint.access_point_id &&
          credential.state === 'active',
      ).length >= 2
    ) {
      return mutationConflict(
        request,
        'S3_CREDENTIAL_LIMIT',
        'An Access Point can have at most two active credentials',
      );
    }
    const credential: S3CredentialView = {
      credential_id: `cred-${crypto.randomUUID().slice(0, 8)}`,
      access_point_id: accessPoint.access_point_id,
      access_key_id: `NGS3${crypto.randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`,
      state: 'active',
      expires_at_unix_ms: body.expires_at_unix_ms ?? '1790000000000',
      created_at_unix_ms: Date.now().toString(),
    };
    s3Credentials.push(credential);
    const response: CreateS3CredentialResponse = {
      credential,
      secret_access_key: `mock-secret-${crypto.randomUUID()}`,
      replayed: false,
    };
    s3CredentialCreateRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/credential/revoke', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as RevokeS3CredentialRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    const credential = s3Credentials.find(
      (item) =>
        item.access_point_id === accessPoint.access_point_id &&
        item.credential_id === body.credential_id,
    );
    if (!credential) return s3NotFound(request, 'credential');
    credential.state = 'revoked';
    const response: QueryS3CredentialListResponse = {
      items: s3Credentials.filter((item) => item.access_point_id === accessPoint.access_point_id),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/object/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryS3ObjectListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    if (accessPoint.state !== 'active') {
      return mutationConflict(
        request,
        'S3_ACCESS_POINT_DISABLED',
        'The S3 access point is disabled',
      );
    }
    const entries = s3ObjectEntries(
      s3Objects.get(accessPoint.access_point_id) ?? [],
      body.prefix,
      body.delimiter,
    );
    const page = paginate(
      request,
      's3-objects',
      {
        tenant_id: body.tenant_id,
        access_point_id: body.access_point_id,
        prefix: body.prefix ?? '',
        delimiter: body.delimiter ?? '',
      },
      entries,
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QueryS3ObjectListResponse = {
      ...page,
      common_prefixes: page.items
        .filter((entry) => entry.entry_type === 'prefix')
        .map((entry) => entry.key),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/s3/object/download-url/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateS3DownloadUrlRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const accessPoint = resolveS3AccessPoint(request, body.tenant_id, body.access_point_id);
    if (accessPoint instanceof HttpResponse) return accessPoint;
    const object = (s3Objects.get(accessPoint.access_point_id) ?? []).find(
      (entry) => entry.entry_type === 'object' && entry.key === body.key,
    );
    if (!object) return s3NotFound(request, 'object');
    const expiresSeconds = Math.min(Math.max(body.expires_seconds ?? 900, 60), 86_400);
    const expiresAt = Date.now() + expiresSeconds * 1000;
    const response: CreateS3DownloadUrlResponse = {
      url: `${accessPoint.endpoint}/${accessPoint.bucket_name}/${encodeURIComponent(body.key)}?expires=${expiresAt}`,
      expires_at_unix_ms: expiresAt.toString(),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/impact/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryDeletionImpactRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const root = findLifecycleResource(body.tenant_id, body.resource);
    if (!root || !isLifecycleActive(root)) return notFound(request, 'Resource');
    if (root.resource_version !== body.expected_resource_version) {
      return mutationConflict(
        request,
        'RESOURCE_VERSION_CONFLICT',
        'The resource changed before deletion impact was calculated',
      );
    }
    const targets = deletionTargets(body.tenant_id, body.resource, body.cascade);
    const dependentTargets = deletionTargets(body.tenant_id, body.resource, true).slice(1);
    const blockers = [] as DeletionImpactView['blockers'];
    if (
      ['artifact', 'storage_volume'].includes(body.resource.type) &&
      dependentTargets.length > 0 &&
      !body.cascade
    ) {
      blockers.push({
        code: 'CASCADE_REQUIRED',
        message: `该资源仍有 ${dependentTargets.length} 个依赖资源，必须显式确认级联处理`,
      });
    }
    if (body.resource.type === 'storage_volume' && !body.confirm_managed_data_erase) {
      blockers.push({
        code: 'MANAGED_DATA_ERASE_CONFIRMATION_REQUIRED',
        message: '删除 StorageVolume 必须确认清理 NeoEngram 受管目录',
      });
    }
    const targetSnapshots = targets
      .filter((target) => target.resource.type === 'snapshot')
      .map((target) =>
        snapshots.find(
          (snapshot) =>
            target.resource.type === 'snapshot' &&
            snapshot.snapshot_id === target.resource.snapshot_id,
        ),
      )
      .filter((snapshot): snapshot is NonNullable<typeof snapshot> => Boolean(snapshot));
    const targetPlaygrounds = targets
      .filter((target) => target.resource.type === 'playground')
      .map((target) =>
        playgrounds.find(
          (playground) =>
            target.resource.type === 'playground' &&
            playground.project_id === target.resource.project_id &&
            playground.artifact_id === target.resource.artifact_id &&
            playground.playground_id === target.resource.playground_id,
        ),
      )
      .filter((playground): playground is NonNullable<typeof playground> => Boolean(playground));
    const targetSnapshotIds = new Set(targetSnapshots.map((snapshot) => snapshot.snapshot_id));
    const targetAccessPointIds = new Set(
      s3AccessPoints
        .filter(
          (accessPoint) =>
            accessPoint.tenant_id === body.tenant_id &&
            targetSnapshotIds.has(accessPoint.snapshot_id),
        )
        .map((accessPoint) => accessPoint.access_point_id),
    );
    const activeCredentials = s3Credentials.filter(
      (credential) =>
        targetAccessPointIds.has(credential.access_point_id) && credential.state === 'active',
    ).length;
    const issuedAt = Date.now();
    const impact: DeletionImpactView = {
      tenant_id: body.tenant_id,
      root: body.resource,
      cascade: body.cascade,
      confirm_managed_data_erase: body.confirm_managed_data_erase,
      targets,
      active_job_count: targetPlaygrounds
        .filter((playground) => Boolean(playground.active_precommit_id))
        .length.toString(),
      active_s3_credential_count: activeCredentials.toString(),
      estimated_file_count: targetSnapshots
        .reduce((sum, snapshot) => sum + BigInt(snapshot.logical_file_count), 0n)
        .toString(),
      estimated_bytes: targetSnapshots
        .reduce((sum, snapshot) => sum + BigInt(snapshot.logical_size_bytes), 0n)
        .toString(),
      blockers,
      issued_at_unix_ms: issuedAt.toString(),
      expires_at_unix_ms: (issuedAt + 5 * 60_000).toString(),
    };
    const impactDigest = contentDigest(impact);
    deletionImpacts.set(impactDigest, structuredClone(impact));
    const response: QueryDeletionImpactResponse = { impact, impact_digest: impactDigest };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateDeletionRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, 'create', body.request_id);
    const requestJson = stableJson(body);
    const previous = deletionMutationRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(request, 'REQUEST_ID_REUSED', 'Request ID was reused');
      }
      const response = structuredClone(previous.response) as DeletionMutationResponse;
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const impact = deletionImpacts.get(body.impact_digest);
    if (
      !impact ||
      impact.tenant_id !== body.tenant_id ||
      stableJson(impact.root) !== stableJson(body.resource) ||
      impact.cascade !== body.cascade ||
      impact.confirm_managed_data_erase !== body.confirm_managed_data_erase
    ) {
      return mutationConflict(
        request,
        'DELETION_IMPACT_STALE',
        'The deletion impact no longer matches the request',
      );
    }
    if (Number(impact.expires_at_unix_ms) <= Date.now()) {
      return mutationConflict(
        request,
        'DELETION_IMPACT_EXPIRED',
        'The deletion impact has expired',
      );
    }
    if (impact.blockers.length) {
      return mutationConflict(request, 'DELETION_BLOCKED', impact.blockers[0]!.message);
    }
    const root = findLifecycleResource(body.tenant_id, body.resource);
    if (!root || !isLifecycleActive(root)) return notFound(request, 'Resource');
    if (root.resource_version !== body.expected_resource_version) {
      return mutationConflict(
        request,
        'RESOURCE_VERSION_CONFLICT',
        'The resource changed after impact confirmation',
      );
    }
    const now = Date.now();
    const deletionId = `deletion-${crypto.randomUUID()}`;
    const purgeAfter = (now + 7 * 24 * 60 * 60 * 1_000).toString();
    const deletion: DeletionOperationView = {
      deletion_id: deletionId,
      tenant_id: body.tenant_id,
      root: body.resource,
      state: 'recoverable',
      resource_version: '1',
      targets: impact.targets,
      request_id: body.request_id,
      request_digest: contentDigest(body),
      impact_digest: body.impact_digest,
      cascade: body.cascade,
      confirm_managed_data_erase: body.confirm_managed_data_erase,
      purge_after_unix_ms: purgeAfter,
      created_at_unix_ms: now.toString(),
      updated_at_unix_ms: now.toString(),
      retry_count: '0',
    };
    for (const target of deletion.targets) {
      setResourceLifecycle(body.tenant_id, target, 'pending_delete', deletionId, purgeAfter);
      if (target.resource.type !== 'snapshot') continue;
      for (const accessPoint of s3AccessPoints) {
        if (accessPoint.snapshot_id !== target.resource.snapshot_id) continue;
        accessPoint.state = 'disabled';
        accessPoint.policy_generation = (BigInt(accessPoint.policy_generation) + 1n).toString();
        accessPoint.updated_at_unix_ms = now.toString();
        for (const credential of s3Credentials) {
          if (credential.access_point_id === accessPoint.access_point_id)
            credential.state = 'revoked';
        }
      }
    }
    deletionOperations.unshift(deletion);
    const response: DeletionMutationResponse = { deletion, replayed: false };
    deletionMutationRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/list/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryDeletionListRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const allowedStates = new Set(body.states ?? []);
    const items = deletionOperations.filter(
      (deletion) =>
        deletion.tenant_id === body.tenant_id &&
        (allowedStates.size === 0 || allowedStates.has(deletion.state)),
    );
    const page = paginate(
      request,
      'resource-deletions',
      { tenant_id: body.tenant_id, states: [...allowedStates].sort() },
      items,
      body,
    );
    if (page instanceof HttpResponse) return page;
    const response: QueryDeletionListResponse = page;
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/query', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as QueryDeletionRequest;
    const failed = requireTenant(request, body.tenant_id);
    if (failed) return failed;
    const deletion = deletionOperations.find(
      (item) => item.tenant_id === body.tenant_id && item.deletion_id === body.deletion_id,
    );
    if (!deletion) return notFound(request, 'Deletion operation');
    const response: QueryDeletionResponse = {
      deletion,
      retention_holds: retentionHolds.filter(
        (hold) => hold.tenant_id === body.tenant_id && hold.deletion_id === body.deletion_id,
      ),
    };
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/restore', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as UpdateDeletionRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, 'restore', body.request_id);
    const requestJson = stableJson(body);
    const previous = deletionMutationRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(request, 'REQUEST_ID_REUSED', 'Request ID was reused');
      }
      const response = structuredClone(previous.response) as DeletionMutationResponse;
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const deletion = deletionOperations.find(
      (item) => item.tenant_id === body.tenant_id && item.deletion_id === body.deletion_id,
    );
    if (!deletion) return notFound(request, 'Deletion operation');
    if (deletion.resource_version !== body.expected_resource_version) {
      return mutationConflict(request, 'RESOURCE_VERSION_CONFLICT', 'Deletion task changed');
    }
    if (
      ['purging', 'finalizing', 'completed'].includes(deletion.state) ||
      Number(deletion.purge_after_unix_ms) <= Date.now()
    ) {
      return mutationConflict(
        request,
        'DELETION_NOT_RECOVERABLE',
        'The deletion task can no longer be restored',
      );
    }
    for (const target of deletion.targets) {
      setResourceLifecycle(body.tenant_id, target, 'active');
    }
    deletion.state = 'completed';
    deletion.completion = 'restored';
    deletion.resource_version = (BigInt(deletion.resource_version) + 1n).toString();
    deletion.updated_at_unix_ms = Date.now().toString();
    const response: DeletionMutationResponse = { deletion, replayed: false };
    deletionMutationRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/deletion/retry', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as UpdateDeletionRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, 'retry', body.request_id);
    const requestJson = stableJson(body);
    const previous = deletionMutationRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(request, 'REQUEST_ID_REUSED', 'Request ID was reused');
      }
      const response = structuredClone(previous.response) as DeletionMutationResponse;
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const deletion = deletionOperations.find(
      (item) => item.tenant_id === body.tenant_id && item.deletion_id === body.deletion_id,
    );
    if (!deletion) return notFound(request, 'Deletion operation');
    if (deletion.resource_version !== body.expected_resource_version) {
      return mutationConflict(request, 'RESOURCE_VERSION_CONFLICT', 'Deletion task changed');
    }
    if (!['blocked', 'failed'].includes(deletion.state)) {
      return mutationConflict(request, 'DELETION_NOT_RETRYABLE', 'Deletion task is not retryable');
    }
    deletion.state = 'recoverable';
    delete deletion.last_error;
    deletion.retry_count = (BigInt(deletion.retry_count) + 1n).toString();
    deletion.resource_version = (BigInt(deletion.resource_version) + 1n).toString();
    deletion.updated_at_unix_ms = Date.now().toString();
    const response: DeletionMutationResponse = { deletion, replayed: false };
    deletionMutationRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/retention-hold/create', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as CreateRetentionHoldRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, 'hold-create', body.request_id);
    const requestJson = stableJson(body);
    const previous = deletionMutationRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(request, 'REQUEST_ID_REUSED', 'Request ID was reused');
      }
      const response = structuredClone(previous.response) as CreateRetentionHoldResponse;
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const deletion = deletionOperations.find(
      (item) => item.tenant_id === body.tenant_id && item.deletion_id === body.deletion_id,
    );
    if (!deletion) return notFound(request, 'Deletion operation');
    if (!body.reason.trim()) {
      return problem(request, 422, 'PROTOCOL_INVALID', 'Reason is required', 'reason is required');
    }
    if (deletion.resource_version !== body.expected_resource_version) {
      return mutationConflict(request, 'RESOURCE_VERSION_CONFLICT', 'Deletion task changed');
    }
    const hold: RetentionHoldView = {
      retention_hold_id: `hold-${crypto.randomUUID()}`,
      tenant_id: body.tenant_id,
      deletion_id: body.deletion_id,
      reason: body.reason.trim(),
      state: 'active',
      ...(body.expires_at_unix_ms ? { expires_at_unix_ms: body.expires_at_unix_ms } : {}),
      created_at_unix_ms: Date.now().toString(),
    };
    retentionHolds.push(hold);
    deletion.resource_version = (BigInt(deletion.resource_version) + 1n).toString();
    deletion.updated_at_unix_ms = Date.now().toString();
    const response: CreateRetentionHoldResponse = {
      deletion,
      retention_hold: hold,
      replayed: false,
    };
    deletionMutationRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
    return HttpResponse.json(response, { headers: headers(request) });
  }),
  http.post('*/api/resource/retention-hold/release', async ({ request }) => {
    const denied = authorize(request);
    if (denied) return denied;
    const body = (await request.json()) as ReleaseRetentionHoldRequest;
    const failed = requireMutationAccess(request, body.tenant_id);
    if (failed) return failed;
    const requestKey = resourceKey(body.tenant_id, 'hold-release', body.request_id);
    const requestJson = stableJson(body);
    const previous = deletionMutationRequests.get(requestKey);
    if (previous) {
      if (previous.requestJson !== requestJson) {
        return mutationConflict(request, 'REQUEST_ID_REUSED', 'Request ID was reused');
      }
      const response = structuredClone(previous.response) as ReleaseRetentionHoldResponse;
      response.replayed = true;
      return HttpResponse.json(response, { headers: headers(request) });
    }
    const deletion = deletionOperations.find(
      (item) => item.tenant_id === body.tenant_id && item.deletion_id === body.deletion_id,
    );
    const hold = retentionHolds.find(
      (item) =>
        item.tenant_id === body.tenant_id &&
        item.deletion_id === body.deletion_id &&
        item.retention_hold_id === body.retention_hold_id,
    );
    if (!deletion || !hold) return notFound(request, 'Retention hold');
    if (deletion.resource_version !== body.expected_resource_version) {
      return mutationConflict(request, 'RESOURCE_VERSION_CONFLICT', 'Deletion task changed');
    }
    hold.state = 'released';
    hold.released_at_unix_ms = Date.now().toString();
    deletion.resource_version = (BigInt(deletion.resource_version) + 1n).toString();
    deletion.updated_at_unix_ms = Date.now().toString();
    const response: ReleaseRetentionHoldResponse = {
      deletion,
      retention_hold: hold,
      replayed: false,
    };
    deletionMutationRequests.set(requestKey, {
      requestJson,
      response: structuredClone(response),
    });
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
  projectCreatePayloads.clear();
  storageVolumeCreatePayloads.clear();
  storageEnrollmentTokenRequests.clear();
  storageEnrollmentDecisionRequests.clear();
  storageEnrollmentFrozenDescriptors.clear();
  storageEnrollmentReviewAudit.clear();
  pvcBindings.clear();
  activePvcOwners.clear();
  artifactCreatePayloads.clear();
  playgroundQueryCounts.clear();
  commitRequests.clear();
  precommits.clear();
  precommitQueryCounts.clear();
  precommitSourceHeads.clear();
  precommitMutationRequests.clear();
  snapshotCreateRequests.clear();
  snapshotDeliveries.splice(0, snapshotDeliveries.length);
  snapshotDeliveryCreateRequests.clear();
  snapshotRetryRequests.clear();
  snapshotDeliveryDeleteRequests.clear();
  snapshotQueryCounts.clear();
  s3AccessPointCreateRequests.clear();
  s3CredentialCreateRequests.clear();
  seedDeletionState();
  resetMockData();
  seedStorageEnrollmentState();
  seedPreCommitState();
  seedSnapshotDeliveryState();
}

seedStorageEnrollmentState();
seedPreCommitState();
seedSnapshotDeliveryState();
seedDeletionState();
