import { readFileSync } from "node:fs";

const bundleUrl = new URL(
  "../../../target/openapi/neoengram-api.json",
  import.meta.url,
);
const document = JSON.parse(readFileSync(bundleUrl, "utf8"));

function assert(condition, message) {
  if (!condition) {
    throw new Error(`OpenAPI contract check failed: ${message}`);
  }
}

function resolveRef(value) {
  if (!value?.$ref) {
    return value;
  }
  assert(
    value.$ref.startsWith("#/"),
    `external ref remains in bundle: ${value.$ref}`,
  );
  return value.$ref
    .slice(2)
    .split("/")
    .map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"))
    .reduce((current, part) => current?.[part], document);
}

function sorted(values) {
  return [...values].sort();
}

function assertSameMembers(actual, expected, message) {
  assert(
    JSON.stringify(sorted(actual)) === JSON.stringify(sorted(expected)),
    `${message}: got [${sorted(actual).join(", ")}]`,
  );
}

function assertDescriptionIncludes(value, fragments, message) {
  const description = value?.description ?? "";
  for (const fragment of fragments) {
    assert(
      description.includes(fragment),
      `${message}: description omits ${fragment}`,
    );
  }
}

const expectedOperations = {
  "/api/system/version/query": ["post", "queryApiVersion"],
  "/api/tenant/list/query": ["post", "queryTenantList"],
  "/api/tenant/query": ["post", "queryTenant"],
  "/api/tenant/create": ["post", "createTenant"],
  "/api/storage/volume/list/query": ["post", "queryStorageVolumeList"],
  "/api/storage/volume/query": ["post", "queryStorageVolume"],
  "/api/storage/volume/create": ["post", "createStorageVolume"],
  "/api/storage/enrollment/token/create": [
    "post",
    "createStorageEnrollmentToken",
  ],
  "/api/storage/enrollment/list/query": ["post", "queryStorageEnrollmentList"],
  "/api/storage/enrollment/query": ["post", "queryStorageEnrollment"],
  "/api/storage/enrollment/approve": ["post", "approveStorageEnrollment"],
  "/api/storage/enrollment/reject": ["post", "rejectStorageEnrollment"],
  "/api/project/list/query": ["post", "queryProjectList"],
  "/api/artifact/list/query": ["post", "queryArtifactList"],
  "/api/artifact/query": ["post", "queryArtifact"],
  "/api/artifact/create": ["post", "createArtifact"],
  "/api/artifact/commit/graph/query": ["post", "queryArtifactCommitGraph"],
  "/api/artifact/commit/diff/query": ["post", "queryArtifactCommitDiff"],
  "/api/playground/list/query": ["post", "queryPlaygroundList"],
  "/api/playground/query": ["post", "queryPlayground"],
  "/api/playground/create": ["post", "createPlayground"],
  "/api/playground/precommit/start": ["post", "startPlaygroundPreCommit"],
  "/api/playground/precommit/query": ["post", "queryPlaygroundPreCommit"],
  "/api/playground/precommit/restart": ["post", "restartPlaygroundPreCommit"],
  "/api/playground/precommit/cancel": ["post", "cancelPlaygroundPreCommit"],
  "/api/playground/file/list/query": ["post", "queryPlaygroundFileList"],
  "/api/playground/change/list/query": ["post", "queryPlaygroundChangeList"],
  "/api/playground/file/metadata/query": [
    "post",
    "queryPlaygroundFileMetadata",
  ],
  "/api/playground/dataset/profile/query": [
    "post",
    "queryPlaygroundDatasetProfile",
  ],
  "/api/playground/commit/create": ["post", "commitPlayground"],
  "/api/snapshot/list/query": ["post", "querySnapshotList"],
  "/api/snapshot/query": ["post", "querySnapshot"],
  "/api/snapshot/create": ["post", "createSnapshot"],
  "/api/snapshot/delivery/retry": ["post", "retrySnapshotDelivery"],
  "/api/snapshot/file/list/query": ["post", "querySnapshotFileList"],
  "/api/snapshot/activity/list/query": ["post", "querySnapshotActivityList"],
  "/api/snapshot/dataset/profile/query": [
    "post",
    "querySnapshotDatasetProfile",
  ],
  "/api/job/add/create": ["post", "createAddJob"],
  "/api/job/query": ["post", "queryJob"],
  "/api/job/add/finalize": ["post", "finalizeAddJob"],
  "/health/live": ["get", "liveProbe"],
  "/health/ready": ["get", "readyProbe"],
};

assert(document.openapi === "3.1.0", "OpenAPI version must be 3.1.0");
assertSameMembers(
  Object.keys(document.paths),
  Object.keys(expectedOperations),
  "public path set changed",
);

const operationIds = [];
for (const [path, [method, operationId]] of Object.entries(
  expectedOperations,
)) {
  const operation = document.paths[path]?.[method];
  assert(operation, `${method.toUpperCase()} ${path} is missing`);
  assert(
    operation.operationId === operationId,
    `${method.toUpperCase()} ${path} has wrong operationId`,
  );
  operationIds.push(operation.operationId);

  const requestId = (operation.parameters ?? [])
    .map(resolveRef)
    .find((parameter) => parameter.name === "X-Request-ID");
  assert(
    requestId?.in === "header" && requestId.required === false,
    `${operationId} does not accept an optional X-Request-ID`,
  );

  if (path.startsWith("/api/")) {
    assert(!path.includes("/v1"), `${path} contains a path version`);
    assert(
      !path.includes("{"),
      `${path} contains a version or resource path variable`,
    );
    assert(!path.includes(":"), `${path} uses a colon action`);
    assert(method === "post", `${path} must use JSON RPC over POST`);
  }

  for (const [status, responseRef] of Object.entries(operation.responses)) {
    const response = resolveRef(responseRef);
    assert(response, `${operationId} response ${status} cannot be resolved`);
    assert(
      response.headers?.["X-Request-ID"],
      `${operationId} response ${status} omits X-Request-ID`,
    );
    if (!status.startsWith("2")) {
      const problem = response.content?.["application/problem+json"];
      assert(
        problem,
        `${operationId} response ${status} is not RFC 9457 problem+json`,
      );
      assert(
        resolveRef(problem.schema) ===
          document.components.schemas.ProblemDetails,
        `${operationId} response ${status} does not use ProblemDetails`,
      );
    }
  }
}

assert(
  !operationIds.some((id) =>
    ["assignJob", "expireAddJob", "resumePublication"].includes(id),
  ),
  "an internal authority operation is public",
);

for (const [path, [method, operationId]] of Object.entries(
  expectedOperations,
)) {
  if (!path.startsWith("/api/") || operationId === "queryApiVersion") continue;
  const operation = document.paths[path][method];
  assert(
    operation.security?.some((entry) => Object.hasOwn(entry, "BearerAuth")),
    `${operationId} does not require BearerAuth`,
  );
  const version = operation.parameters
    .map(resolveRef)
    .find((parameter) => parameter.name === "NeoEngram-API-Version");
  assert(
    version?.in === "header" && version.required === true,
    `${operationId} lacks a required API version header`,
  );
  assertSameMembers(
    version.schema?.enum ?? [],
    ["1"],
    `${operationId} accepts the wrong API versions`,
  );
  const successMedia = resolveRef(operation.responses["200"]).content?.[
    "application/json"
  ];
  assert(successMedia?.schema, `${operationId} has no JSON success DTO`);
}

const jobContracts = {
  createAddJob: {
    path: "/api/job/add/create",
    successSchema: "#/components/schemas/CreateAddJobResponse",
    statuses: [
      "200",
      "401",
      "403",
      "408",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
    successExamples: ["created", "replayed"],
  },
  queryJob: {
    path: "/api/job/query",
    successSchema: "#/components/schemas/QueryJobResponse",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
    successExamples: ["current", "repeated"],
  },
  finalizeAddJob: {
    path: "/api/job/add/finalize",
    successSchema: "#/components/schemas/FinalizeAddJobResponse",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "408",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
    successExamples: ["published", "replayed"],
  },
};

for (const [operationId, contract] of Object.entries(jobContracts)) {
  const operation = document.paths[contract.path].post;
  assert(
    operation.security?.some((entry) => Object.hasOwn(entry, "BearerAuth")),
    `${operationId} does not require BearerAuth`,
  );

  const parameters = operation.parameters.map(resolveRef);
  const version = parameters.find(
    (parameter) => parameter.name === "NeoEngram-API-Version",
  );
  assert(
    version?.in === "header" && version.required === true,
    `${operationId} lacks a required API version header`,
  );
  assertSameMembers(
    version.schema?.enum ?? [],
    ["1"],
    `${operationId} accepts the wrong API versions`,
  );

  assertSameMembers(
    Object.keys(operation.responses),
    contract.statuses,
    `${operationId} status mapping changed`,
  );
  const successMedia = resolveRef(operation.responses["200"]).content?.[
    "application/json"
  ];
  assert(
    successMedia?.schema?.$ref === contract.successSchema,
    `${operationId} returns the wrong success DTO`,
  );
  assertSameMembers(
    Object.keys(successMedia.examples ?? {}),
    contract.successExamples,
    `${operationId} success/replay examples changed`,
  );

  const hasFailureExample = Object.entries(operation.responses)
    .filter(([status]) => !status.startsWith("2"))
    .some(([, responseRef]) => {
      const media =
        resolveRef(responseRef).content?.["application/problem+json"];
      return Boolean(
        media?.example || Object.keys(media?.examples ?? {}).length,
      );
    });
  assert(
    hasFailureExample,
    `${operationId} has no representative failure example`,
  );
}

const versionQuery = document.paths["/api/system/version/query"].post;
assert(
  JSON.stringify(versionQuery.security) === "[]",
  "version query must be unauthenticated",
);
assert(
  !(versionQuery.parameters ?? [])
    .map(resolveRef)
    .some((parameter) => parameter.name === "NeoEngram-API-Version"),
  "version query must not require a version header",
);

for (const operationId of [
  "queryApiVersion",
  "createAddJob",
  "queryJob",
  "finalizeAddJob",
  "createStorageEnrollmentToken",
  "queryStorageEnrollmentList",
  "queryStorageEnrollment",
  "approveStorageEnrollment",
  "rejectStorageEnrollment",
  "liveProbe",
  "readyProbe",
]) {
  const [path, [method]] = Object.entries(expectedOperations).find(
    ([, [, candidate]]) => candidate === operationId,
  );
  const responses = document.paths[path][method].responses;
  assert(
    responses["429"]?.$ref === "#/components/responses/OverloadedProblem",
    `${operationId} does not declare the public 429 overload response`,
  );
  assert(
    responses["504"]?.$ref === "#/components/responses/RequestTimeoutProblem",
    `${operationId} does not declare the public 504 timeout response`,
  );
}

const createRequest = document.components.schemas.CreateAddJobRequest;
assert(
  createRequest.additionalProperties === true,
  "Add request must retain compatible extension fields",
);
assertSameMembers(
  createRequest.required,
  [
    "tenant_id",
    "project_id",
    "artifact_id",
    "playground_id",
    "job_id",
    "expected_index_version",
    "deadline_unix_ms",
    "paths",
    "all",
  ],
  "Add request fields changed",
);
assert(
  !createRequest.properties.principal &&
    !createRequest.properties.actor &&
    !createRequest.properties.request_digest,
  "client request must not declare actor, principal, or request_digest",
);
assert(
  createRequest.properties.paths.maxItems === 4096,
  "Add path limit must match neoengram-protocol",
);

const jobView = document.components.schemas.JobView;
assertSameMembers(
  Object.keys(jobView.properties),
  [
    "operation",
    "tenant_id",
    "project_id",
    "artifact_id",
    "playground_id",
    "job_id",
    "state",
    "resource_version",
    "deadline_unix_ms",
    "progress",
    "decision",
    "failure",
    "finalized_at_unix_ms",
  ],
  "public JobView fields changed",
);

const forbiddenJobFields = [
  "accepted",
  "agent_id",
  "agent_mount_id",
  "artifact_placement_id",
  "assignment",
  "assignment_generation",
  "assignment_id",
  "assignment_target",
  "decision_generation",
  "edge_cluster_id",
  "fencing",
  "fencing_token",
  "finalized_ack",
  "generation",
  "index_delta",
  "lease",
  "manifest",
  "manifests",
  "mount_generation",
  "mutations",
  "owner_generation",
  "placement_generation",
  "prepared",
  "publication_candidate",
  "resume_publication",
  "storage_volume_id",
];
assertSameMembers(
  jobView.propertyNames?.not?.enum ?? [],
  forbiddenJobFields,
  "JobView internal-field denylist changed",
);

const canonicalU64 = document.components.schemas.CanonicalU64;
assert(
  canonicalU64.type === "string" && canonicalU64.pattern,
  "u64 values must be canonical decimal strings",
);
assert(
  document.components.schemas.ApiVersionResponse.properties.api_versions.items
    .type === "integer",
  "small API versions must be JSON numbers",
);
assert(
  document.components.schemas.ApiVersionResponse.properties
    .agent_protocol_versions.items.type === "integer",
  "small protocol versions must be JSON numbers",
);

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
  assert(
    resolveRef(schema) === canonicalU64,
    "a public u64 field does not use CanonicalU64",
  );
}

const problemRequired = document.components.schemas.ProblemDetails.required;
assertSameMembers(
  problemRequired,
  [
    "type",
    "title",
    "status",
    "detail",
    "instance",
    "code",
    "request_id",
    "retryable",
  ],
  "ProblemDetails required fields changed",
);

for (const target of Object.values(
  document.components.schemas.PublicJobDecision.discriminator.mapping,
)) {
  assert(
    resolveRef({ $ref: target }),
    `unresolved Job decision discriminator target ${target}`,
  );
}

const resourceContracts = {
  queryTenantList: ["QueryTenantListRequest", "QueryTenantListResponse"],
  queryTenant: ["QueryTenantRequest", "QueryTenantResponse"],
  createTenant: ["CreateTenantRequest", "CreateTenantResponse"],
  queryStorageVolumeList: [
    "QueryStorageVolumeListRequest",
    "QueryStorageVolumeListResponse",
  ],
  queryStorageVolume: [
    "QueryStorageVolumeRequest",
    "QueryStorageVolumeResponse",
  ],
  createStorageVolume: [
    "CreateStorageVolumeRequest",
    "CreateStorageVolumeResponse",
  ],
  createStorageEnrollmentToken: [
    "CreateStorageEnrollmentTokenRequest",
    "CreateStorageEnrollmentTokenResponse",
  ],
  queryStorageEnrollmentList: [
    "QueryStorageEnrollmentListRequest",
    "QueryStorageEnrollmentListResponse",
  ],
  queryStorageEnrollment: [
    "QueryStorageEnrollmentRequest",
    "QueryStorageEnrollmentResponse",
  ],
  approveStorageEnrollment: [
    "ApproveStorageEnrollmentRequest",
    "ApproveStorageEnrollmentResponse",
  ],
  rejectStorageEnrollment: [
    "RejectStorageEnrollmentRequest",
    "RejectStorageEnrollmentResponse",
  ],
  queryProjectList: ["QueryProjectListRequest", "QueryProjectListResponse"],
  queryArtifactList: ["QueryArtifactListRequest", "QueryArtifactListResponse"],
  queryArtifact: ["QueryArtifactRequest", "QueryArtifactResponse"],
  createArtifact: ["CreateArtifactRequest", "CreateArtifactResponse"],
  queryArtifactCommitGraph: [
    "QueryArtifactCommitGraphRequest",
    "QueryArtifactCommitGraphResponse",
  ],
  queryArtifactCommitDiff: [
    "QueryArtifactCommitDiffRequest",
    "QueryArtifactCommitDiffResponse",
  ],
  queryPlaygroundList: [
    "QueryPlaygroundListRequest",
    "QueryPlaygroundListResponse",
  ],
  queryPlayground: ["QueryPlaygroundRequest", "QueryPlaygroundResponse"],
  createPlayground: ["CreatePlaygroundRequest", "CreatePlaygroundResponse"],
  startPlaygroundPreCommit: ["StartPreCommitRequest", "StartPreCommitResponse"],
  queryPlaygroundPreCommit: ["QueryPreCommitRequest", "QueryPreCommitResponse"],
  restartPlaygroundPreCommit: [
    "RestartPreCommitRequest",
    "RestartPreCommitResponse",
  ],
  cancelPlaygroundPreCommit: [
    "CancelPreCommitRequest",
    "CancelPreCommitResponse",
  ],
  queryPlaygroundFileList: [
    "QueryPlaygroundFileListRequest",
    "QueryPlaygroundFileListResponse",
  ],
  queryPlaygroundChangeList: [
    "QueryPlaygroundChangeListRequest",
    "QueryPlaygroundChangeListResponse",
  ],
  queryPlaygroundFileMetadata: [
    "QueryPlaygroundFileMetadataRequest",
    "QueryPlaygroundFileMetadataResponse",
  ],
  queryPlaygroundDatasetProfile: [
    "QueryPlaygroundDatasetProfileRequest",
    "QueryPlaygroundDatasetProfileResponse",
  ],
  commitPlayground: ["CommitPlaygroundRequest", "CommitPlaygroundResponse"],
  querySnapshotList: ["QuerySnapshotListRequest", "QuerySnapshotListResponse"],
  querySnapshot: ["QuerySnapshotRequest", "QuerySnapshotResponse"],
  createSnapshot: ["CreateSnapshotRequest", "CreateSnapshotResponse"],
  retrySnapshotDelivery: [
    "RetrySnapshotDeliveryRequest",
    "RetrySnapshotDeliveryResponse",
  ],
  querySnapshotFileList: [
    "QuerySnapshotFileListRequest",
    "QuerySnapshotFileListResponse",
  ],
  querySnapshotActivityList: [
    "QuerySnapshotActivityListRequest",
    "QuerySnapshotActivityListResponse",
  ],
  querySnapshotDatasetProfile: [
    "QuerySnapshotDatasetProfileRequest",
    "QuerySnapshotDatasetProfileResponse",
  ],
};

for (const [operationId, [requestName, responseName]] of Object.entries(
  resourceContracts,
)) {
  const [path, [method]] = Object.entries(expectedOperations).find(
    ([, [, candidate]]) => candidate === operationId,
  );
  const operation = document.paths[path][method];
  const requestSchema =
    operation.requestBody.content["application/json"].schema;
  const successSchema = resolveRef(operation.responses["200"]).content[
    "application/json"
  ].schema;
  assert(
    requestSchema.$ref === `#/components/schemas/${requestName}`,
    `${operationId} uses the wrong request DTO`,
  );
  assert(
    successSchema.$ref === `#/components/schemas/${responseName}`,
    `${operationId} uses the wrong success DTO`,
  );
}

const snapshotRequest = document.components.schemas.QuerySnapshotRequest;
assertSameMembers(
  snapshotRequest.required,
  ["tenant_id", "snapshot_id"],
  "Snapshot query identity must be tenant/snapshot",
);
assert(
  !snapshotRequest.properties.project_id &&
    !snapshotRequest.properties.artifact_id &&
    !snapshotRequest.properties.commit_id,
  "Snapshot query must not retain the old composite identity",
);

const createSnapshotRequest = document.components.schemas.CreateSnapshotRequest;
assertSameMembers(
  createSnapshotRequest.required,
  [
    "tenant_id",
    "project_id",
    "artifact_id",
    "commit_id",
    "storage_volume_id",
    "snapshot_request_id",
  ],
  "Snapshot create must bind Commit, Volume, and request identity",
);
assert(
  !createSnapshotRequest.properties.snapshot_id,
  "Snapshot ID must remain server-generated",
);
assert(
  !createSnapshotRequest.properties.region &&
    !createSnapshotRequest.properties.purpose &&
    !createSnapshotRequest.properties.retention_policy &&
    !createSnapshotRequest.properties.dataset_profile,
  "Snapshot create must not accept derived placement/profile or P1 product fields",
);
assertSameMembers(
  document.components.schemas.CreateSnapshotResponse.required,
  ["snapshot", "replayed", "placement_reused"],
  "Snapshot create replay signals changed",
);

const commitRequest = document.components.schemas.CommitPlaygroundRequest;
assert(
  commitRequest.required.includes("commit_request_id"),
  "Playground Commit must have a stable mutation identity",
);
assert(
  commitRequest.required.includes("precommit_id") &&
    commitRequest.required.includes("expected_candidate_index_version"),
  "Playground Commit must consume a Pre-commit candidate",
);
assert(
  commitRequest.properties.description && commitRequest.properties.tag_names,
  "Playground Commit must accept a description and tag names",
);
assert(
  commitRequest.properties.tag_names.maxItems === 20,
  "Playground Commit tag limit changed",
);
assert(
  !commitRequest.properties.actor &&
    !commitRequest.properties.principal &&
    !commitRequest.properties.request_digest &&
    !commitRequest.properties.source_head_commit_id &&
    !commitRequest.properties.expected_head_commit_id,
  "Playground Commit request must not declare identity internals or a client-supplied Head",
);
assert(
  document.components.schemas.CommitPlaygroundResponse.required.includes(
    "consumed_precommit",
  ),
  "Playground Commit response must return the consumed Pre-commit",
);

const artifactCreate = document.components.schemas.CreateArtifactRequest;
assert(
  !artifactCreate.properties.storage_volume_id &&
    !artifactCreate.properties.default_ref,
  "Artifact create must not select placement or a default Ref",
);
assert(
  artifactCreate.required.includes("initialization"),
  "Artifact create must declare initialization",
);
const initialization = document.components.schemas.ArtifactInitialization;
assert(
  initialization.discriminator?.propertyName === "mode" &&
    initialization.oneOf?.length === 2,
  "Artifact initialization must be a two-mode discriminated union",
);
assertSameMembers(
  document.components.schemas.DerivedArtifactInitialization.required,
  ["mode", "source_project_id", "source_artifact_id", "source_commit_id"],
  "Derived Artifact lineage scope changed",
);

const commitNode = document.components.schemas.CommitNode;
assert(
  commitNode.properties.tag_names && !commitNode.properties.ref_names,
  "public Commit nodes must expose Tags without Refs",
);
const commitGraph = document.components.schemas.CommitGraphView;
assert(
  commitGraph.properties.head_commit_id && !commitGraph.properties.refs,
  "public Commit graph must expose head Commit without Ref tips",
);

assertSameMembers(
  document.components.schemas.PlaygroundState.enum,
  ["creating", "ready", "abnormal"],
  "Playground states changed",
);
assertSameMembers(
  document.components.schemas.PreCommitState.enum,
  ["running", "ready", "abnormal", "cancelled", "committed"],
  "Pre-commit states changed",
);
assertSameMembers(
  document.components.schemas.PreCommitPhase.enum,
  ["queued", "scanning", "hashing", "uploading", "validating", "idle"],
  "Pre-commit phases changed",
);
assertDescriptionIncludes(
  document.components.schemas.PreCommitState,
  ["ready", "abnormal", "idle"],
  "Pre-commit state/terminal semantics are not documented",
);
assertDescriptionIncludes(
  document.components.schemas.PreCommitPhase,
  ["running", "idle", "ready"],
  "Pre-commit phase semantics are not documented",
);
assertDescriptionIncludes(
  document.components.schemas.PreCommitView,
  ["ready/idle", "abnormal/idle", "Blocked"],
  "Pre-commit ready/blocked mapping is not documented",
);
assertSameMembers(
  document.components.schemas.SnapshotState.enum,
  ["creating", "ready", "abnormal"],
  "Snapshot states changed",
);
assertSameMembers(
  document.components.schemas.SnapshotPhase.enum,
  ["planning", "materializing", "verifying", "idle"],
  "Snapshot phases changed",
);

const publicResourceViews = [
  document.components.schemas.StorageVolumeView,
  document.components.schemas.ArtifactView,
  document.components.schemas.CommitNode,
  document.components.schemas.CommitDiffEntry,
  document.components.schemas.PlaygroundView,
  document.components.schemas.PreCommitView,
  document.components.schemas.LogicalFileEntry,
  document.components.schemas.FileMetadataView,
  document.components.schemas.SnapshotView,
];
const forbiddenResourceFields = [
  "agent_id",
  "agent_mount_id",
  "artifact_placement_id",
  "assignment",
  "chunk",
  "content_digest",
  "directory",
  "file_digest",
  "fencing",
  "fencing_token",
  "lease",
  "manifest",
  "mount",
  "mount_path",
  "nfs_path",
  "object_count",
  "object_location",
  "physical_path",
  "credentials",
];
for (const view of publicResourceViews) {
  const fields = Object.keys(view.properties ?? {}).map((field) =>
    field.toLowerCase(),
  );
  assert(
    !forbiddenResourceFields.some((field) => fields.includes(field)),
    "a public resource view exposes an internal storage or scheduling field",
  );
}

const commitDiff = document.components.schemas.CommitDiffView;
assertSameMembers(
  commitDiff.required,
  ["target_commit", "summary", "changes"],
  "Commit diff required fields changed",
);
assert(
  !document.components.schemas.CommitDiffEntry.properties.manifest &&
    !document.components.schemas.CommitDiffEntry.properties.digest &&
    !document.components.schemas.CommitDiffEntry.properties.object_location,
  "Commit diff must not expose internal content identities or locations",
);

for (const schemaName of ["PlaygroundView", "SnapshotView"]) {
  const view = document.components.schemas[schemaName];
  assert(
    view.required.includes("storage_volume_id") &&
      view.required.includes("region"),
    `${schemaName} must expose its public storage placement`,
  );
}
assert(
  !document.components.schemas.ArtifactView.properties.storage_volume_id &&
    !document.components.schemas.ArtifactView.properties.region &&
    !document.components.schemas.ArtifactView.properties.default_ref,
  "ArtifactView must remain placement- and Ref-free",
);

for (const schemaName of ["CreatePlaygroundRequest", "CreateSnapshotRequest"]) {
  assert(
    document.components.schemas[schemaName].required.includes(
      "storage_volume_id",
    ),
    `${schemaName} must select a StorageVolume`,
  );
}

assertSameMembers(
  document.components.schemas.StorageVolumeState.enum,
  ["ready", "degraded", "unavailable"],
  "StorageVolume states changed",
);
assertDescriptionIncludes(
  document.components.schemas.StorageVolumeView,
  ["state=ready", "degraded", "unavailable", "禁止新放置"],
  "StorageVolume ready-only placement semantics are not documented",
);
const createStorageVolumeOperation =
  document.paths["/api/storage/volume/create"].post;
assertDescriptionIncludes(
  createStorageVolumeOperation,
  [
    "首次登记",
    "state=unavailable",
    "受信 Agent session",
    "健康挂载",
    "不得自动",
    "ready",
    "重放",
    "当前权威视图",
    "不改变或提升",
  ],
  "Direct StorageVolume registration must remain unavailable until Agent health is observed",
);
assertDescriptionIncludes(
  createStorageVolumeOperation.responses["200"],
  ["首次登记", "unavailable", "重放", "当前权威状态", "不触发状态提升"],
  "Direct StorageVolume registration response state is not documented",
);

const createPlaygroundRequest =
  document.components.schemas.CreatePlaygroundRequest;
assert(
  !createPlaygroundRequest.properties.region,
  "Playground create must derive Region from its selected Volume",
);
assertDescriptionIncludes(
  createPlaygroundRequest,
  ["state=ready", "Region"],
  "Playground ready-only placement semantics are not documented",
);
assertDescriptionIncludes(
  createSnapshotRequest,
  ["state=ready", "Region", "用途", "保留策略", "Dataset Profile"],
  "Snapshot P0 creation boundary is not documented",
);

const startPreCommit = document.paths["/api/playground/precommit/start"].post;
const restartPreCommit =
  document.paths["/api/playground/precommit/restart"].post;
const commitPlayground = document.paths["/api/playground/commit/create"].post;
assertDescriptionIncludes(
  startPreCommit,
  ["新的", "precommit_id", "内部冻结", "Head", "不得隐式"],
  "Pre-commit start/new-session semantics are not documented",
);
assertDescriptionIncludes(
  restartPreCommit,
  ["abnormal", "cancelled", "attempt + 1", "cancel", "start"],
  "Pre-commit restart/attempt semantics are not documented",
);
assertDescriptionIncludes(
  commitPlayground,
  ["state=ready, phase=idle", "blockers", "内部冻结", "Head", "CAS", "409"],
  "Commit candidate and internal Head CAS semantics are not documented",
);

for (const schemaName of [
  "StartPreCommitRequest",
  "RestartPreCommitRequest",
  "PreCommitView",
]) {
  const schema = document.components.schemas[schemaName];
  assert(
    !schema.properties.source_head_commit_id &&
      !schema.properties.expected_head_commit_id,
    `${schemaName} must not expose the internally frozen Head`,
  );
}

for (const operationId of ["createPlayground", "createSnapshot"]) {
  const [path, [method]] = Object.entries(expectedOperations).find(
    ([, [, candidate]]) => candidate === operationId,
  );
  assertDescriptionIncludes(
    document.paths[path][method],
    ["state=ready", "degraded", "unavailable", "409"],
    `${operationId} ready-only placement rejection is not documented`,
  );
}

assertDescriptionIncludes(
  document.components.schemas.DatasetProfileView,
  ["派生", "只读", "不是 Snapshot 创建输入"],
  "Dataset Profile read-only boundary is not documented",
);

const storageVolumeView = document.components.schemas.StorageVolumeView;
for (const forbidden of [
  "credentials",
  "mount_path",
  "nfs_reference",
  "agent_id",
  "fencing_token",
]) {
  assert(
    !storageVolumeView.properties[forbidden],
    `StorageVolumeView exposes forbidden field ${forbidden}`,
  );
}
assertSameMembers(
  Object.keys(storageVolumeView.properties),
  [
    "tenant_id",
    "storage_volume_id",
    "display_name",
    "edge_cluster_id",
    "region",
    "backend_type",
    "access_mode",
    "pvc_reference",
    "state",
    "resource_version",
    "created_at_unix_ms",
    "updated_at_unix_ms",
  ],
  "StorageVolumeView fields changed while adding enrollment",
);
assertSameMembers(
  storageVolumeView.required,
  [
    "tenant_id",
    "storage_volume_id",
    "display_name",
    "edge_cluster_id",
    "region",
    "backend_type",
    "access_mode",
    "state",
    "resource_version",
    "created_at_unix_ms",
    "updated_at_unix_ms",
  ],
  "StorageVolumeView required fields changed while adding enrollment",
);

const storageEnrollmentOperations = {
  createStorageEnrollmentToken: {
    path: "/api/storage/enrollment/token/create",
    permission: "storage.enrollment.create",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
  },
  queryStorageEnrollmentList: {
    path: "/api/storage/enrollment/list/query",
    permission: "storage.enrollment.read",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
  },
  queryStorageEnrollment: {
    path: "/api/storage/enrollment/query",
    permission: "storage.enrollment.read",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
  },
  approveStorageEnrollment: {
    path: "/api/storage/enrollment/approve",
    permission: "storage.enrollment.review",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
  },
  rejectStorageEnrollment: {
    path: "/api/storage/enrollment/reject",
    permission: "storage.enrollment.review",
    statuses: [
      "200",
      "401",
      "403",
      "404",
      "409",
      "413",
      "422",
      "429",
      "500",
      "503",
      "504",
    ],
  },
};

for (const [operationId, contract] of Object.entries(
  storageEnrollmentOperations,
)) {
  const operation = document.paths[contract.path].post;
  assertSameMembers(
    Object.keys(operation.responses),
    contract.statuses,
    `${operationId} status mapping changed`,
  );
  const permissionMentions =
    operation.description?.match(/storage\.enrollment\.[a-z.]+/g) ?? [];
  assertSameMembers(
    permissionMentions,
    [contract.permission],
    `${operationId} documents the wrong permission`,
  );
}

const storageEnrollmentConflictExamples =
  document.components.responses.StorageEnrollmentConflictProblem.content[
    "application/problem+json"
  ].examples;
assert(
  storageEnrollmentConflictExamples.staleResourceVersion.value.code ===
    "STORAGE_ENROLLMENT_VERSION_CONFLICT",
  "Storage enrollment stale resource version conflict code drifted",
);
assert(
  storageEnrollmentConflictExamples.tokenRequestIdentityReused.value.code ===
    "STORAGE_ENROLLMENT_TOKEN_REQUEST_ID_REUSED",
  "Storage enrollment token request identity conflict code drifted",
);
assert(
  storageEnrollmentConflictExamples.decisionRequestIdentityReused.value.code ===
    "STORAGE_ENROLLMENT_DECISION_ID_REUSED",
  "Storage enrollment decision request identity conflict code drifted",
);
assert(
  storageEnrollmentConflictExamples.replacementConfirmationRequired.value.code ===
    "STORAGE_ENROLLMENT_REPLACEMENT_CONFIRMATION_REQUIRED",
  "Storage enrollment replacement confirmation conflict code drifted",
);
assert(
  !document.components.responses.DeadlineProblem.description
    .toLowerCase()
    .includes("lease"),
  "Public Job deadline response must not expose assignment lease semantics",
);

const createEnrollmentTokenOperation =
  document.paths["/api/storage/enrollment/token/create"].post;
assertDescriptionIncludes(
  createEnrollmentTokenOperation,
  [
    "15 分钟",
    "成功消费一次",
    "无需预先登记 StorageVolume",
    "token_request_id",
    "不同 payload",
    "409",
  ],
  "Storage enrollment token lifetime, consumption, and idempotency semantics are incomplete",
);

const createEnrollmentTokenRequest =
  document.components.schemas.CreateStorageEnrollmentTokenRequest;
const createEnrollmentTokenRequestFields = [
  "tenant_id",
  "token_request_id",
  "storage_volume_id",
  "display_name",
  "edge_cluster_id",
  "region",
  "access_mode",
  "pvc_reference",
];
assertSameMembers(
  createEnrollmentTokenRequest.required,
  createEnrollmentTokenRequestFields,
  "Storage enrollment token request required fields changed",
);
assertSameMembers(
  Object.keys(createEnrollmentTokenRequest.properties),
  createEnrollmentTokenRequestFields,
  "Storage enrollment token request descriptor fields changed",
);
assert(
  !createEnrollmentTokenRequest.properties
    .expected_storage_volume_resource_version,
  "Storage enrollment token creation must not require a pre-registered Volume version",
);
assert(
  createEnrollmentTokenRequest.properties.access_mode.$ref ===
    "#/components/schemas/StorageEnrollmentAccessMode",
  "Storage enrollment token creation must use the writable enrollment access mode",
);

const createEnrollmentTokenResponse =
  document.components.schemas.CreateStorageEnrollmentTokenResponse;
const createEnrollmentTokenResponseFields = [
  "token_id",
  "bootstrap_token",
  "expires_at_unix_ms",
  "replayed",
];
assertSameMembers(
  createEnrollmentTokenResponse.required,
  createEnrollmentTokenResponseFields,
  "Storage enrollment token response required fields changed",
);
assertSameMembers(
  Object.keys(createEnrollmentTokenResponse.properties),
  createEnrollmentTokenResponseFields,
  "Storage enrollment token response fields changed",
);
assert(
  !createEnrollmentTokenResponse.properties.issued_at_unix_ms,
  "Storage enrollment token response must not expose an uncontracted issued timestamp",
);

const bootstrapToken =
  document.components.schemas.StorageEnrollmentBootstrapToken;
assert(
  bootstrapToken.type === "string" && bootstrapToken.readOnly === true,
  "bootstrap token must be an opaque response-only string schema",
);
const bootstrapTokenSchemaRef =
  "#/components/schemas/StorageEnrollmentBootstrapToken";
const bootstrapTokenRefOwners = Object.entries(document.components.schemas)
  .filter(([, schema]) =>
    JSON.stringify(schema).includes(bootstrapTokenSchemaRef),
  )
  .map(([name]) => name);
assertSameMembers(
  bootstrapTokenRefOwners,
  ["CreateStorageEnrollmentTokenResponse"],
  "raw bootstrap token is referenced outside its create success DTO",
);
const bootstrapTokenPropertyOwners = Object.entries(document.components.schemas)
  .filter(([, schema]) =>
    Object.hasOwn(schema.properties ?? {}, "bootstrap_token"),
  )
  .map(([name]) => name);
assertSameMembers(
  bootstrapTokenPropertyOwners,
  ["CreateStorageEnrollmentTokenResponse"],
  "bootstrap_token property appears outside its create success DTO",
);
assertDescriptionIncludes(
  createEnrollmentTokenResponse,
  ["只在本响应中返回", "相同 token_request_id", "相同 payload", "replayed"],
  "Storage enrollment token replay or exposure boundary is incomplete",
);

const tokenSuccessMedia = resolveRef(
  createEnrollmentTokenOperation.responses["200"],
).content["application/json"];
assertSameMembers(
  Object.keys(tokenSuccessMedia.examples ?? {}),
  ["created", "replayed"],
  "Storage enrollment token success/replay examples changed",
);
for (const example of Object.values(tokenSuccessMedia.examples)) {
  assertSameMembers(
    Object.keys(example.value),
    createEnrollmentTokenResponseFields,
    "Storage enrollment token example fields changed",
  );
}
assert(
  tokenSuccessMedia.examples.created.value.token_id ===
    tokenSuccessMedia.examples.replayed.value.token_id &&
    tokenSuccessMedia.examples.created.value.bootstrap_token ===
      tokenSuccessMedia.examples.replayed.value.bootstrap_token &&
    tokenSuccessMedia.examples.created.value.expires_at_unix_ms ===
      tokenSuccessMedia.examples.replayed.value.expires_at_unix_ms,
  "Storage enrollment token replay must return the original result",
);

const enrollmentListRequest =
  document.components.schemas.QueryStorageEnrollmentListRequest;
assertSameMembers(
  enrollmentListRequest.required,
  ["tenant_id"],
  "Storage enrollment list scope changed",
);
assertSameMembers(
  Object.keys(enrollmentListRequest.properties),
  ["tenant_id", "state", "registration_kind", "cursor", "page_size", "query"],
  "Storage enrollment list filters changed",
);
assert(
  document.components.schemas.QueryStorageEnrollmentListResponse.properties
    .items.items.$ref === "#/components/schemas/StorageEnrollmentView",
  "Storage enrollment list must return StorageEnrollmentView items",
);

const queryEnrollmentRequest =
  document.components.schemas.QueryStorageEnrollmentRequest;
assertSameMembers(
  queryEnrollmentRequest.required,
  ["tenant_id", "storage_enrollment_id"],
  "Storage enrollment query identity changed",
);
assertSameMembers(
  Object.keys(queryEnrollmentRequest.properties),
  ["tenant_id", "storage_enrollment_id"],
  "Storage enrollment query must not accept extra identity fields",
);

assertSameMembers(
  document.components.schemas.StorageEnrollmentRegistrationKind.enum,
  ["initial", "replacement"],
  "Storage enrollment registration kinds changed",
);
assertSameMembers(
  document.components.schemas.StorageEnrollmentState.enum,
  ["pending_approval", "approved", "enrolled", "rejected", "expired"],
  "Storage enrollment states changed",
);
assertSameMembers(
  document.components.schemas.StorageEnrollmentAccessMode.enum,
  ["read_write_many", "read_write_once"],
  "Storage enrollment must accept only writable PVC access modes",
);

const enrollmentProbe =
  document.components.schemas.StorageEnrollmentProbeSummary;
const enrollmentProbeFields = [
  "observed_access_mode",
  "descriptor_matches",
  "protocol_compatible",
  "observed_at_unix_ms",
];
assertSameMembers(
  enrollmentProbe.required,
  enrollmentProbeFields,
  "Storage enrollment probe required fields changed",
);
assertSameMembers(
  Object.keys(enrollmentProbe.properties),
  enrollmentProbeFields,
  "Storage enrollment probe leaks or omits fields",
);
assertSameMembers(
  enrollmentProbe.properties.observed_access_mode.enum,
  ["read_only", "read_write"],
  "Storage enrollment observed access modes changed",
);

const enrollmentView = document.components.schemas.StorageEnrollmentView;
const enrollmentViewRequiredFields = [
  "tenant_id",
  "storage_enrollment_id",
  "storage_volume_id",
  "display_name",
  "edge_cluster_id",
  "region",
  "access_mode",
  "pvc_reference",
  "registration_kind",
  "state",
  "agent_version",
  "identity_fingerprint",
  "proof_of_possession_status",
  "probe",
  "resource_version",
  "created_at_unix_ms",
  "updated_at_unix_ms",
  "expires_at_unix_ms",
];
assertSameMembers(
  enrollmentView.required,
  enrollmentViewRequiredFields,
  "StorageEnrollmentView required fields changed",
);
assertSameMembers(
  Object.keys(enrollmentView.properties),
  [...enrollmentViewRequiredFields, "reviewed_at_unix_ms"],
  "StorageEnrollmentView fields changed",
);
assert(
  enrollmentView.properties.access_mode.$ref ===
    "#/components/schemas/StorageEnrollmentAccessMode",
  "StorageEnrollmentView must use the writable enrollment access mode",
);
assert(
  enrollmentView.properties.proof_of_possession_status.$ref ===
    "#/components/schemas/StorageEnrollmentProofOfPossessionStatus",
  "StorageEnrollmentView must expose only the server-owned PoP verification status",
);
assertSameMembers(
  document.components.schemas.StorageEnrollmentProofOfPossessionStatus.enum,
  ["verified"],
  "Storage enrollment PoP status must not admit a client-asserted or unverified state",
);
assertDescriptionIncludes(
  enrollmentView,
  [
    "pending_approval",
    "24 小时",
    "expired",
    "approved",
    "unavailable",
    "enrolled",
  ],
  "Storage enrollment expiry or lifecycle semantics are incomplete",
);

const forbiddenEnrollmentFields = [
  "bootstrap_token",
  "token_key_id",
  "csr",
  "public_key",
  "public_key_spki",
  "proof_of_possession",
  "signature",
  "certificate",
  "private_key",
  "bootstrap_credential",
  "poll_credential",
  "pvc_uid",
  "csi_handle",
  "fsid",
  "device",
  "mount_path",
  "mount_options",
  "mount_fingerprint",
  "agent_id",
  "agent_mount_id",
  "compute_node_id",
  "session_generation",
  "certificate_generation",
  "credential_generation",
  "heartbeat",
  "jobs",
  "assignment",
  "tenant_assignment",
  "owner_generation",
  "lease",
  "fencing",
  "review_reason",
];
for (const schema of [enrollmentView, enrollmentProbe]) {
  const fields = Object.keys(schema.properties ?? {}).map((field) =>
    field.toLowerCase(),
  );
  assert(
    !forbiddenEnrollmentFields.some((field) => fields.includes(field)),
    "a public Storage enrollment DTO exposes an internal secret or authority field",
  );
}

const enrollmentListExample = resolveRef(
  document.paths["/api/storage/enrollment/list/query"].post.responses["200"],
).content["application/json"].example.items[0];
assert(
  enrollmentListExample.state === "pending_approval",
  "Storage enrollment list example must use pending_approval",
);
assert(
  BigInt(enrollmentListExample.expires_at_unix_ms) -
    BigInt(enrollmentListExample.created_at_unix_ms) ===
    86_400_000n,
  "Storage enrollment pending approval example must expire after 24 hours",
);
assertSameMembers(
  Object.keys(enrollmentListExample.probe),
  enrollmentProbeFields,
  "Storage enrollment list example probe fields changed",
);

const pvcReference = document.components.schemas.PvcReference;
assert(
  pvcReference.properties.namespace.$ref ===
    "#/components/schemas/KubernetesNamespaceName" &&
    pvcReference.properties.claim_name.$ref ===
      "#/components/schemas/KubernetesPvcClaimName",
  "PVC reference must use distinct Kubernetes Namespace and PVC claim name schemas",
);
const kubernetesNamespace = document.components.schemas.KubernetesNamespaceName;
const kubernetesPvcClaim = document.components.schemas.KubernetesPvcClaimName;
assert(
  kubernetesNamespace.maxLength === 63 &&
    new RegExp(kubernetesNamespace.pattern).test("neoengram-data") &&
    !new RegExp(kubernetesNamespace.pattern).test("neoengram.data") &&
    !new RegExp(kubernetesNamespace.pattern).test("a".repeat(64)),
  "Kubernetes Namespace must be a DNS-1123 label of at most 63 characters",
);
assert(
  kubernetesPvcClaim.maxLength === 253 &&
    new RegExp(kubernetesPvcClaim.pattern).test("dataset.claim") &&
    !new RegExp(kubernetesPvcClaim.pattern).test(`${"a".repeat(64)}.claim`),
  "Kubernetes PVC claim must be a DNS-1123 subdomain with label length limits",
);

const approveEnrollmentRequest =
  document.components.schemas.ApproveStorageEnrollmentRequest;
assertSameMembers(
  approveEnrollmentRequest.required,
  [
    "tenant_id",
    "storage_enrollment_id",
    "approval_request_id",
    "expected_resource_version",
    "confirm_replacement",
  ],
  "Storage enrollment approval CAS fields changed",
);
assertSameMembers(
  Object.keys(approveEnrollmentRequest.properties),
  approveEnrollmentRequest.required,
  "Storage enrollment approval accepts uncontracted fields",
);
assert(
  resolveRef(approveEnrollmentRequest.properties.expected_resource_version) ===
    canonicalU64,
  "Storage enrollment approval resource version must use CanonicalU64",
);
assert(
  approveEnrollmentRequest.properties.confirm_replacement.type === "boolean",
  "Storage enrollment replacement confirmation must be explicit",
);
assertDescriptionIncludes(
  approveEnrollmentRequest,
  [
    "expected_resource_version",
    "CAS",
    "replacement",
    "confirm_replacement=true",
    "幂等",
  ],
  "Storage enrollment approval CAS, replacement, or idempotency semantics are incomplete",
);

const approveEnrollmentResponse =
  document.components.schemas.ApproveStorageEnrollmentResponse;
assertSameMembers(
  approveEnrollmentResponse.required,
  ["enrollment", "storage_volume", "replayed"],
  "Storage enrollment approval response fields changed",
);
assert(
  approveEnrollmentResponse.properties.storage_volume.$ref ===
    "#/components/schemas/StorageVolumeView",
  "Storage enrollment approval must return StorageVolumeView",
);
assertDescriptionIncludes(
  approveEnrollmentResponse,
  ["StorageVolumeView", "approved", "unavailable", "enrolled", "ready"],
  "Storage enrollment approval result lifecycle is incomplete",
);
const approveEnrollmentOperation =
  document.paths["/api/storage/enrollment/approve"].post;
assertDescriptionIncludes(
  approveEnrollmentOperation,
  [
    "resource version CAS",
    "initial",
    "创建缺失",
    "精确绑定",
    "unavailable",
    "无活动 Owner",
    "replacement",
    "confirm_replacement=true",
    "state=unavailable",
    "共享 Tenant 级 decision",
  ],
  "Storage enrollment approval transaction or replacement semantics are incomplete",
);
const approveEnrollmentExample = resolveRef(
  approveEnrollmentOperation.responses["200"],
).content["application/json"].example;
assert(
  approveEnrollmentExample.enrollment.state === "approved" &&
    approveEnrollmentExample.storage_volume.state === "unavailable",
  "Storage enrollment approval example must return approved enrollment and unavailable Volume",
);

const rejectEnrollmentRequest =
  document.components.schemas.RejectStorageEnrollmentRequest;
assertSameMembers(
  rejectEnrollmentRequest.required,
  [
    "tenant_id",
    "storage_enrollment_id",
    "rejection_request_id",
    "expected_resource_version",
  ],
  "Storage enrollment rejection CAS fields changed",
);
assertSameMembers(
  Object.keys(rejectEnrollmentRequest.properties),
  [...rejectEnrollmentRequest.required, "reason"],
  "Storage enrollment rejection accepts uncontracted fields",
);
assert(
  resolveRef(rejectEnrollmentRequest.properties.expected_resource_version) ===
    canonicalU64,
  "Storage enrollment rejection resource version must use CanonicalU64",
);
assertDescriptionIncludes(
  rejectEnrollmentRequest,
  ["expected_resource_version", "CAS", "rejection_request_id", "幂等"],
  "Storage enrollment rejection CAS or idempotency semantics are incomplete",
);
const rejectEnrollmentOperation =
  document.paths["/api/storage/enrollment/reject"].post;
assertDescriptionIncludes(
  rejectEnrollmentOperation,
  ["reason", "审计", "不进入 StorageEnrollmentView", "公开响应"],
  "Storage enrollment rejection reason exposure boundary is incomplete",
);
const rejectEnrollmentExample = resolveRef(
  rejectEnrollmentOperation.responses["200"],
).content["application/json"].example;
assert(
  !Object.hasOwn(rejectEnrollmentExample.enrollment, "review_reason"),
  "Storage enrollment rejection response must not expose the private audit reason",
);

const createTenantRequest = document.components.schemas.CreateTenantRequest;
assert(
  createTenantRequest.additionalProperties === true,
  "Tenant create request must retain compatible extension fields",
);
assert(
  !createTenantRequest.properties.actor &&
    !createTenantRequest.properties.principal,
  "Tenant create request must not declare actor or principal",
);

console.log("OpenAPI contract checks passed");
