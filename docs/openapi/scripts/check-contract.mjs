import { readFileSync } from 'node:fs';

const bundleUrl = new URL('../../../target/openapi/neoengram-api.json', import.meta.url);
const document = JSON.parse(readFileSync(bundleUrl, 'utf8'));

function assert(condition, message) {
  if (!condition) {
    throw new Error(`OpenAPI contract check failed: ${message}`);
  }
}

function resolveRef(value) {
  if (!value?.$ref) {
    return value;
  }
  assert(value.$ref.startsWith('#/'), `external ref remains in bundle: ${value.$ref}`);
  return value.$ref
    .slice(2)
    .split('/')
    .map((part) => part.replaceAll('~1', '/').replaceAll('~0', '~'))
    .reduce((current, part) => current?.[part], document);
}

function sorted(values) {
  return [...values].sort();
}

function assertSameMembers(actual, expected, message) {
  assert(
    JSON.stringify(sorted(actual)) === JSON.stringify(sorted(expected)),
    `${message}: got [${sorted(actual).join(', ')}]`,
  );
}

const expectedOperations = {
  '/api/system/version/query': ['post', 'queryApiVersion'],
  '/api/tenant/list/query': ['post', 'queryTenantList'],
  '/api/tenant/query': ['post', 'queryTenant'],
  '/api/tenant/create': ['post', 'createTenant'],
  '/api/storage/volume/list/query': ['post', 'queryStorageVolumeList'],
  '/api/storage/volume/query': ['post', 'queryStorageVolume'],
  '/api/storage/volume/create': ['post', 'createStorageVolume'],
  '/api/project/list/query': ['post', 'queryProjectList'],
  '/api/artifact/list/query': ['post', 'queryArtifactList'],
  '/api/artifact/query': ['post', 'queryArtifact'],
  '/api/artifact/create': ['post', 'createArtifact'],
  '/api/artifact/commit/graph/query': ['post', 'queryArtifactCommitGraph'],
  '/api/artifact/commit/diff/query': ['post', 'queryArtifactCommitDiff'],
  '/api/playground/list/query': ['post', 'queryPlaygroundList'],
  '/api/playground/query': ['post', 'queryPlayground'],
  '/api/playground/create': ['post', 'createPlayground'],
  '/api/playground/commit/create': ['post', 'commitPlayground'],
  '/api/snapshot/list/query': ['post', 'querySnapshotList'],
  '/api/snapshot/query': ['post', 'querySnapshot'],
  '/api/snapshot/create': ['post', 'createSnapshot'],
  '/api/job/add/create': ['post', 'createAddJob'],
  '/api/job/query': ['post', 'queryJob'],
  '/api/job/add/finalize': ['post', 'finalizeAddJob'],
  '/health/live': ['get', 'liveProbe'],
  '/health/ready': ['get', 'readyProbe'],
};

assert(document.openapi === '3.1.0', 'OpenAPI version must be 3.1.0');
assertSameMembers(Object.keys(document.paths), Object.keys(expectedOperations), 'public path set changed');

const operationIds = [];
for (const [path, [method, operationId]] of Object.entries(expectedOperations)) {
  const operation = document.paths[path]?.[method];
  assert(operation, `${method.toUpperCase()} ${path} is missing`);
  assert(operation.operationId === operationId, `${method.toUpperCase()} ${path} has wrong operationId`);
  operationIds.push(operation.operationId);

  const requestId = (operation.parameters ?? [])
    .map(resolveRef)
    .find((parameter) => parameter.name === 'X-Request-ID');
  assert(requestId?.in === 'header' && requestId.required === false,
    `${operationId} does not accept an optional X-Request-ID`);

  if (path.startsWith('/api/')) {
    assert(!path.includes('/v1'), `${path} contains a path version`);
    assert(!path.includes('{'), `${path} contains a version or resource path variable`);
    assert(!path.includes(':'), `${path} uses a colon action`);
    assert(method === 'post', `${path} must use JSON RPC over POST`);
  }

  for (const [status, responseRef] of Object.entries(operation.responses)) {
    const response = resolveRef(responseRef);
    assert(response, `${operationId} response ${status} cannot be resolved`);
    assert(response.headers?.['X-Request-ID'], `${operationId} response ${status} omits X-Request-ID`);
    if (!status.startsWith('2')) {
      const problem = response.content?.['application/problem+json'];
      assert(problem, `${operationId} response ${status} is not RFC 9457 problem+json`);
      assert(resolveRef(problem.schema) === document.components.schemas.ProblemDetails,
        `${operationId} response ${status} does not use ProblemDetails`);
    }
  }
}

assert(!operationIds.some((id) => ['assignJob', 'expireAddJob', 'resumePublication'].includes(id)),
  'an internal authority operation is public');

for (const [path, [method, operationId]] of Object.entries(expectedOperations)) {
  if (!path.startsWith('/api/') || operationId === 'queryApiVersion') continue;
  const operation = document.paths[path][method];
  assert(operation.security?.some((entry) => Object.hasOwn(entry, 'BearerAuth')),
    `${operationId} does not require BearerAuth`);
  const version = operation.parameters.map(resolveRef)
    .find((parameter) => parameter.name === 'NeoEngram-API-Version');
  assert(version?.in === 'header' && version.required === true,
    `${operationId} lacks a required API version header`);
  assertSameMembers(version.schema?.enum ?? [], ['1'], `${operationId} accepts the wrong API versions`);
  const successMedia = resolveRef(operation.responses['200']).content?.['application/json'];
  assert(successMedia?.schema, `${operationId} has no JSON success DTO`);
}

const jobContracts = {
  createAddJob: {
    path: '/api/job/add/create',
    successSchema: '#/components/schemas/CreateAddJobResponse',
    statuses: ['200', '401', '403', '408', '409', '413', '422', '500', '503'],
    successExamples: ['created', 'replayed'],
  },
  queryJob: {
    path: '/api/job/query',
    successSchema: '#/components/schemas/QueryJobResponse',
    statuses: ['200', '401', '403', '404', '413', '422', '500', '503'],
    successExamples: ['current', 'repeated'],
  },
  finalizeAddJob: {
    path: '/api/job/add/finalize',
    successSchema: '#/components/schemas/FinalizeAddJobResponse',
    statuses: ['200', '401', '403', '404', '408', '409', '413', '422', '500', '503'],
    successExamples: ['published', 'replayed'],
  },
};

for (const [operationId, contract] of Object.entries(jobContracts)) {
  const operation = document.paths[contract.path].post;
  assert(operation.security?.some((entry) => Object.hasOwn(entry, 'BearerAuth')),
    `${operationId} does not require BearerAuth`);

  const parameters = operation.parameters.map(resolveRef);
  const version = parameters.find((parameter) => parameter.name === 'NeoEngram-API-Version');
  assert(version?.in === 'header' && version.required === true, `${operationId} lacks a required API version header`);
  assertSameMembers(version.schema?.enum ?? [], ['1'], `${operationId} accepts the wrong API versions`);

  assertSameMembers(Object.keys(operation.responses), contract.statuses, `${operationId} status mapping changed`);
  const successMedia = resolveRef(operation.responses['200']).content?.['application/json'];
  assert(successMedia?.schema?.$ref === contract.successSchema, `${operationId} returns the wrong success DTO`);
  assertSameMembers(Object.keys(successMedia.examples ?? {}), contract.successExamples,
    `${operationId} success/replay examples changed`);

  const hasFailureExample = Object.entries(operation.responses)
    .filter(([status]) => !status.startsWith('2'))
    .some(([, responseRef]) => {
      const media = resolveRef(responseRef).content?.['application/problem+json'];
      return Boolean(media?.example || Object.keys(media?.examples ?? {}).length);
    });
  assert(hasFailureExample, `${operationId} has no representative failure example`);
}

const versionQuery = document.paths['/api/system/version/query'].post;
assert(JSON.stringify(versionQuery.security) === '[]', 'version query must be unauthenticated');
assert(!(versionQuery.parameters ?? []).map(resolveRef)
  .some((parameter) => parameter.name === 'NeoEngram-API-Version'), 'version query must not require a version header');

const createRequest = document.components.schemas.CreateAddJobRequest;
assert(createRequest.additionalProperties === true, 'Add request must retain compatible extension fields');
assertSameMembers(createRequest.required, [
  'tenant_id',
  'project_id',
  'artifact_id',
  'playground_id',
  'job_id',
  'expected_index_version',
  'deadline_unix_ms',
  'paths',
  'all',
], 'Add request fields changed');
assert(!createRequest.properties.principal
  && !createRequest.properties.actor
  && !createRequest.properties.request_digest,
  'client request must not declare actor, principal, or request_digest');
assert(createRequest.properties.paths.maxItems === 4096, 'Add path limit must match neoengram-protocol');

const jobView = document.components.schemas.JobView;
assertSameMembers(Object.keys(jobView.properties), [
  'operation',
  'tenant_id',
  'project_id',
  'artifact_id',
  'playground_id',
  'job_id',
  'state',
  'resource_version',
  'deadline_unix_ms',
  'progress',
  'decision',
  'failure',
  'finalized_at_unix_ms',
], 'public JobView fields changed');

const forbiddenJobFields = [
  'accepted',
  'agent_id',
  'agent_mount_id',
  'artifact_placement_id',
  'assignment',
  'assignment_generation',
  'assignment_id',
  'assignment_target',
  'decision_generation',
  'edge_cluster_id',
  'fencing',
  'fencing_token',
  'finalized_ack',
  'generation',
  'index_delta',
  'lease',
  'manifest',
  'manifests',
  'mount_generation',
  'mutations',
  'owner_generation',
  'placement_generation',
  'prepared',
  'publication_candidate',
  'resume_publication',
  'storage_volume_id',
];
assertSameMembers(jobView.propertyNames?.not?.enum ?? [], forbiddenJobFields,
  'JobView internal-field denylist changed');

const canonicalU64 = document.components.schemas.CanonicalU64;
assert(canonicalU64.type === 'string' && canonicalU64.pattern, 'u64 values must be canonical decimal strings');
assert(document.components.schemas.ApiVersionResponse.properties.api_versions.items.type === 'integer',
  'small API versions must be JSON numbers');
assert(document.components.schemas.ApiVersionResponse.properties.agent_protocol_versions.items.type === 'integer',
  'small protocol versions must be JSON numbers');

const canonicalFields = [
  jobView.properties.resource_version,
  document.components.schemas.PublicJobProgress.properties.files_completed,
  document.components.schemas.PublicJobProgress.properties.bytes_completed,
  document.components.schemas.PublicJobProgress.properties.retry_after_ms,
  document.components.schemas.JobError.properties.retry_after_ms,
  document.components.schemas.ProblemDetails.properties.retry_after_ms,
  document.components.schemas.IndexVersion.properties.revision,
  document.components.schemas.CommitDiffSummary.properties.files_added,
  document.components.schemas.CommitDiffSummary.properties.files_modified,
  document.components.schemas.CommitDiffSummary.properties.files_deleted,
  document.components.schemas.CommitDiffSummary.properties.files_renamed,
  document.components.schemas.CommitDiffSummary.properties.bytes_added,
  document.components.schemas.CommitDiffSummary.properties.bytes_removed,
];
for (const schema of canonicalFields) {
  assert(resolveRef(schema) === canonicalU64, 'a public u64 field does not use CanonicalU64');
}

const problemRequired = document.components.schemas.ProblemDetails.required;
assertSameMembers(problemRequired, [
  'type',
  'title',
  'status',
  'detail',
  'instance',
  'code',
  'request_id',
  'retryable',
], 'ProblemDetails required fields changed');

for (const target of Object.values(document.components.schemas.PublicJobDecision.discriminator.mapping)) {
  assert(resolveRef({ $ref: target }), `unresolved Job decision discriminator target ${target}`);
}

const resourceContracts = {
  queryTenantList: ['QueryTenantListRequest', 'QueryTenantListResponse'],
  queryTenant: ['QueryTenantRequest', 'QueryTenantResponse'],
  createTenant: ['CreateTenantRequest', 'CreateTenantResponse'],
  queryStorageVolumeList: ['QueryStorageVolumeListRequest', 'QueryStorageVolumeListResponse'],
  queryStorageVolume: ['QueryStorageVolumeRequest', 'QueryStorageVolumeResponse'],
  createStorageVolume: ['CreateStorageVolumeRequest', 'CreateStorageVolumeResponse'],
  queryProjectList: ['QueryProjectListRequest', 'QueryProjectListResponse'],
  queryArtifactList: ['QueryArtifactListRequest', 'QueryArtifactListResponse'],
  queryArtifact: ['QueryArtifactRequest', 'QueryArtifactResponse'],
  createArtifact: ['CreateArtifactRequest', 'CreateArtifactResponse'],
  queryArtifactCommitGraph: ['QueryArtifactCommitGraphRequest', 'QueryArtifactCommitGraphResponse'],
  queryArtifactCommitDiff: ['QueryArtifactCommitDiffRequest', 'QueryArtifactCommitDiffResponse'],
  queryPlaygroundList: ['QueryPlaygroundListRequest', 'QueryPlaygroundListResponse'],
  queryPlayground: ['QueryPlaygroundRequest', 'QueryPlaygroundResponse'],
  createPlayground: ['CreatePlaygroundRequest', 'CreatePlaygroundResponse'],
  commitPlayground: ['CommitPlaygroundRequest', 'CommitPlaygroundResponse'],
  querySnapshotList: ['QuerySnapshotListRequest', 'QuerySnapshotListResponse'],
  querySnapshot: ['QuerySnapshotRequest', 'QuerySnapshotResponse'],
  createSnapshot: ['CreateSnapshotRequest', 'CreateSnapshotResponse'],
};

for (const [operationId, [requestName, responseName]] of Object.entries(resourceContracts)) {
  const [path, [method]] = Object.entries(expectedOperations)
    .find(([, [, candidate]]) => candidate === operationId);
  const operation = document.paths[path][method];
  const requestSchema = operation.requestBody.content['application/json'].schema;
  const successSchema = resolveRef(operation.responses['200']).content['application/json'].schema;
  assert(requestSchema.$ref === `#/components/schemas/${requestName}`,
    `${operationId} uses the wrong request DTO`);
  assert(successSchema.$ref === `#/components/schemas/${responseName}`,
    `${operationId} uses the wrong success DTO`);
}

const snapshotRequest = document.components.schemas.QuerySnapshotRequest;
assertSameMembers(snapshotRequest.required,
  ['tenant_id', 'project_id', 'artifact_id', 'commit_id'],
  'Snapshot identity must remain tenant/project/artifact/commit');
assert(!snapshotRequest.properties.snapshot_id, 'Snapshot must not introduce an independent snapshot_id');

const createSnapshotRequest = document.components.schemas.CreateSnapshotRequest;
assertSameMembers(createSnapshotRequest.required,
  ['tenant_id', 'project_id', 'artifact_id', 'commit_id', 'storage_volume_id'],
  'Snapshot create identity must remain tenant/project/artifact/commit');
assert(!createSnapshotRequest.properties.snapshot_id,
  'Snapshot create must not introduce an independent snapshot_id');

const commitRequest = document.components.schemas.CommitPlaygroundRequest;
assert(commitRequest.required.includes('commit_request_id'),
  'Playground Commit must have a stable mutation identity');
assert(commitRequest.required.includes('expected_index_version'),
  'Playground Commit must bind the expected IndexVersion');
assert(commitRequest.properties.description && commitRequest.properties.tag_names,
  'Playground Commit must accept a description and tag names');
assert(commitRequest.properties.tag_names.maxItems === 20,
  'Playground Commit tag limit changed');
assert(!commitRequest.properties.actor
  && !commitRequest.properties.principal
  && !commitRequest.properties.request_digest,
  'Playground Commit request must not declare actor, principal, or request_digest');

const publicResourceViews = [
  document.components.schemas.StorageVolumeView,
  document.components.schemas.ArtifactView,
  document.components.schemas.CommitNode,
  document.components.schemas.CommitDiffEntry,
  document.components.schemas.PlaygroundView,
  document.components.schemas.SnapshotView,
];
const forbiddenResourceFields = [
  'agent_id',
  'artifact_placement_id',
  'assignment',
  'chunk',
  'directory',
  'fencing_token',
  'lease',
  'manifest',
  'nfs_path',
  'object_location',
];
for (const view of publicResourceViews) {
  const fields = Object.keys(view.properties ?? {}).map((field) => field.toLowerCase());
  assert(!forbiddenResourceFields.some((field) => fields.includes(field)),
    'a public resource view exposes an internal storage or scheduling field');
}

const commitDiff = document.components.schemas.CommitDiffView;
assertSameMembers(commitDiff.required, ['target_commit', 'summary', 'changes'],
  'Commit diff required fields changed');
assert(!document.components.schemas.CommitDiffEntry.properties.manifest
  && !document.components.schemas.CommitDiffEntry.properties.digest
  && !document.components.schemas.CommitDiffEntry.properties.object_location,
  'Commit diff must not expose internal content identities or locations');

for (const schemaName of ['ArtifactView', 'PlaygroundView', 'SnapshotView']) {
  const view = document.components.schemas[schemaName];
  assert(view.required.includes('storage_volume_id') && view.required.includes('region'),
    `${schemaName} must expose its public storage placement`);
}

for (const schemaName of [
  'CreateArtifactRequest',
  'CreatePlaygroundRequest',
  'CreateSnapshotRequest',
]) {
  assert(document.components.schemas[schemaName].required.includes('storage_volume_id'),
    `${schemaName} must select a StorageVolume`);
}

const storageVolumeView = document.components.schemas.StorageVolumeView;
for (const forbidden of ['credentials', 'mount_path', 'nfs_reference', 'agent_id', 'fencing_token']) {
  assert(!storageVolumeView.properties[forbidden],
    `StorageVolumeView exposes forbidden field ${forbidden}`);
}

const createTenantRequest = document.components.schemas.CreateTenantRequest;
assert(createTenantRequest.additionalProperties === true,
  'Tenant create request must retain compatible extension fields');
assert(!createTenantRequest.properties.actor && !createTenantRequest.properties.principal,
  'Tenant create request must not declare actor or principal');

console.log('OpenAPI contract checks passed');
