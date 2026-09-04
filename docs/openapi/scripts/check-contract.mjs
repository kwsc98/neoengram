import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url));
const actionRegistry = JSON.parse(
  execFileSync(
    "cargo",
    [
      "run",
      "--quiet",
      "--locked",
      "--offline",
      "-p",
      "neoengram-domain",
      "--example",
      "export_action_registry",
    ],
    {
      cwd: repositoryRoot,
      encoding: "utf8",
      maxBuffer: 4 * 1024 * 1024,
      stdio: ["ignore", "pipe", "inherit"],
    },
  ),
);

const bundleUrl = new URL(
  "../../../target/openapi/neoengram-api.json",
  import.meta.url,
);
const document = JSON.parse(readFileSync(bundleUrl, "utf8"));
const agentBundleUrl = new URL(
  "../../../target/openapi/neoengram-agent-api.json",
  import.meta.url,
);
const agentDocument = JSON.parse(readFileSync(agentBundleUrl, "utf8"));

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

function resolveAgentRef(value) {
  if (!value?.$ref) {
    return value;
  }
  assert(
    value.$ref.startsWith("#/"),
    `external Agent ref remains in bundle: ${value.$ref}`,
  );
  return value.$ref
    .slice(2)
    .split("/")
    .map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"))
    .reduce((current, part) => current?.[part], agentDocument);
}

function resolveAgentSchema(value) {
  const schema = resolveAgentRef(value);
  if (!schema?.allOf) return schema;
  return schema.allOf.reduce(
    (merged, part) => {
      const resolved = resolveAgentSchema(part) ?? {};
      const properties = { ...(merged.properties ?? {}) };
      for (const [name, property] of Object.entries(resolved.properties ?? {})) {
        if (
          !properties[name] ||
          (property && typeof property === "object" && Object.keys(property).length > 0)
        ) {
          properties[name] = property;
        }
      }
      return {
        ...merged,
        ...resolved,
        properties,
        required: [
          ...new Set([...(merged.required ?? []), ...(resolved.required ?? [])]),
        ],
      };
    },
    {},
  );
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

function findPermissiveObjects(value, path = [], found = []) {
  if (!value || typeof value !== "object") return found;
  if (value.additionalProperties === true) found.push(path.join("."));
  for (const [key, child] of Object.entries(value)) {
    findPermissiveObjects(child, [...path, key], found);
  }
  return found;
}

assert(
  findPermissiveObjects(document).length === 0,
  "public OpenAPI contains an object that accepts unknown fields",
);
assert(
  findPermissiveObjects(agentDocument).length === 0,
  "Agent OpenAPI contains an object that accepts unknown fields",
);

assertDescriptionIncludes(
  document.info,
  ["HTTP/2", "application/x-ndjson", "heartbeat", "MetadataBatch"],
  "Public API overview must describe the implemented Agent control channel",
);
assert(
  !document.info.description.includes("heartbeat 尚未实现") &&
    !document.info.description.includes("application/json-seq"),
  "Public API overview still describes the obsolete Agent transport state",
);

assert(actionRegistry.schema_version === 1, "action registry version changed");
const expectedOperations = Object.fromEntries(
  actionRegistry.public_openapi.map((route) => [
    route.path,
    [route.method.toLowerCase(), route.operation_id],
  ]),
);
assert(
  Object.keys(expectedOperations).length ===
    actionRegistry.public_openapi.length,
  "public action registry contains duplicate paths",
);
assertSameMembers(
  actionRegistry.central_routes
    .filter((route) => route.visibility === "public")
    .map((route) => `${route.method} ${route.path}`),
  actionRegistry.public_openapi
    .filter((route) => route.routed_by_central)
    .map((route) => `${route.method} ${route.path}`),
  "Central public route export differs from the OpenAPI registry",
);
for (const route of actionRegistry.central_routes.filter(
  (candidate) => candidate.visibility === "internal",
)) {
  assert(
    document.paths[route.path] === undefined,
    `internal Central route leaked into public OpenAPI: ${route.path}`,
  );
}

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
  const registryRoute = actionRegistry.public_openapi.find(
    (route) => route.path === path,
  );
  assert(
    (operation["x-neoengram-central-route"] !== false) ===
      registryRoute.routed_by_central,
    `${method.toUpperCase()} ${path} has wrong Central route availability`,
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
  new Set(operationIds).size === operationIds.length,
  "public OpenAPI operationId values are not unique",
);

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
  "createStorageEnrollmentToken",
  "queryStorageEnrollmentList",
  "queryStorageEnrollment",
  "approveStorageEnrollment",
  "completeStorageRecovery",
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

const canonicalU64 = document.components.schemas.CanonicalU64;
assert(
  canonicalU64.type === "string" && canonicalU64.pattern,
  "u64 values must be canonical decimal strings",
);
assert(
  document.components.schemas.ApiVersionResponse.properties.api_version.type ===
    "integer",
  "the current API version must be a JSON number",
);
assert(
  document.components.schemas.ApiVersionResponse.properties.agent_wire_version
    .type === "integer",
  "current agent wire version must be a JSON number",
);

const canonicalFields = [
  document.components.schemas.TaskView.properties.attempt,
  document.components.schemas.TaskProgressView.properties.completed,
  document.components.schemas.TaskProgressView.properties.completed_bytes,
  document.components.schemas.TaskView.properties.resource_version,
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
  completeStorageRecovery: [
    "CompleteStorageRecoveryRequest",
    "CompleteStorageRecoveryResponse",
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
  querySnapshotDelivery: [
    "QuerySnapshotDeliveryRequest",
    "QuerySnapshotDeliveryResponse",
  ],
  querySnapshotDeliveryList: [
    "QuerySnapshotDeliveryListRequest",
    "QuerySnapshotDeliveryListResponse",
  ],
  retrySnapshotDelivery: [
    "RetrySnapshotDeliveryRequest",
    "RetrySnapshotDeliveryResponse",
  ],
  deleteSnapshotDelivery: [
    "DeleteSnapshotDeliveryRequest",
    "DeleteSnapshotDeliveryResponse",
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
  createS3AccessPoint: [
    "CreateS3AccessPointRequest",
    "CreateS3AccessPointResponse",
  ],
  queryS3AccessPointList: [
    "QueryS3AccessPointListRequest",
    "QueryS3AccessPointListResponse",
  ],
  queryS3AccessPoint: [
    "QueryS3AccessPointRequest",
    "QueryS3AccessPointResponse",
  ],
  enableS3AccessPoint: [
    "UpdateS3AccessPointRequest",
    "UpdateS3AccessPointResponse",
  ],
  disableS3AccessPoint: [
    "UpdateS3AccessPointRequest",
    "UpdateS3AccessPointResponse",
  ],
  createS3Credential: [
    "CreateS3CredentialRequest",
    "CreateS3CredentialResponse",
  ],
  queryS3CredentialList: [
    "QueryS3CredentialListRequest",
    "QueryS3CredentialListResponse",
  ],
  revokeS3Credential: [
    "RevokeS3CredentialRequest",
    "QueryS3CredentialListResponse",
  ],
  queryS3ObjectList: ["QueryS3ObjectListRequest", "QueryS3ObjectListResponse"],
  createS3DownloadUrl: [
    "CreateS3DownloadUrlRequest",
    "CreateS3DownloadUrlResponse",
  ],
  queryResourceDeletionImpact: [
    "QueryDeletionImpactRequest",
    "QueryDeletionImpactResponse",
  ],
  createResourceDeletion: ["CreateDeletionRequest", "DeletionMutationResponse"],
  queryResourceDeletion: ["QueryDeletionRequest", "QueryDeletionResponse"],
  queryResourceDeletionList: [
    "QueryDeletionListRequest",
    "QueryDeletionListResponse",
  ],
  restoreResourceDeletion: [
    "UpdateDeletionRequest",
    "DeletionMutationResponse",
  ],
  retryResourceDeletion: ["UpdateDeletionRequest", "DeletionMutationResponse"],
  createResourceRetentionHold: [
    "CreateRetentionHoldRequest",
    "CreateRetentionHoldResponse",
  ],
  releaseResourceRetentionHold: [
    "ReleaseRetentionHoldRequest",
    "ReleaseRetentionHoldResponse",
  ],
  createGatewayPool: ["CreateGatewayPoolRequest", "GatewayPoolResponse"],
  queryGatewayPool: ["QueryGatewayPoolRequest", "GatewayPoolResponse"],
  queryGatewayPoolList: [
    "QueryGatewayPoolListRequest",
    "GatewayPoolListResponse",
  ],
  updateGatewayPool: ["UpdateGatewayPoolRequest", "GatewayPoolResponse"],
  drainGatewayPool: ["DrainGatewayPoolRequest", "GatewayPoolResponse"],
  createGatewayReplica: [
    "CreateGatewayReplicaRequest",
    "CreateGatewayReplicaResponse",
  ],
  queryGatewayReplicaList: [
    "QueryGatewayReplicaListRequest",
    "GatewayReplicaListResponse",
  ],
  drainGatewayReplica: [
    "MutateGatewayReplicaRequest",
    "GatewayReplicaResponse",
  ],
  revokeGatewayReplica: [
    "MutateGatewayReplicaRequest",
    "GatewayReplicaResponse",
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

assert(
  document.components.schemas.GatewayPoolId.$ref ===
    "#/components/schemas/ResourceId" &&
    document.components.schemas.GatewayReplicaId.$ref ===
      "#/components/schemas/ResourceId",
  "Gateway Pool and Replica IDs must remain explicit public schema aliases",
);
assertSameMembers(
  document.components.schemas.PermissionName.enum,
  [
    "task.read",
    "task.manage",
    "tenant.read",
    "tenant.create",
    "tenant.admin",
    "storage.read",
    "storage.create",
    "storage.enrollment.create",
    "storage.enrollment.read",
    "storage.enrollment.review",
    "artifact.read",
    "artifact.create",
    "artifact.commit.replicate",
    "project.read",
    "project.create",
    "playground.read",
    "playground.create",
    "snapshot.read",
    "snapshot.create",
    "s3.access.read",
    "s3.access.manage",
    "resource.lifecycle.read",
    "resource.lifecycle.manage",
    "retention.manage",
    "gateway.read",
    "gateway.manage",
  ],
  "public permission vocabulary changed",
);

const advertisedCapabilities = resolveRef(
  document.paths["/api/system/version/query"].post.responses["200"],
).content["application/json"].example.capabilities;
assert(
  [
    "commit_materialization_v2",
    "commit_layout_selection_v2",
    "snapshot_delivery_fuse_v2",
    "snapshot_delivery_copy_v2",
    "snapshot_delivery_hardlink_v2",
    "resource_lifecycle_v1",
  ].every((capability) => advertisedCapabilities.includes(capability)),
  "version query example must advertise the new Commit replication and SnapshotDelivery capabilities",
);

assertSameMembers(
  document.components.schemas.ResourceLifecycleState.enum,
  ["active", "pending_delete", "deleting", "restoring", "deleted"],
  "resource lifecycle states changed",
);
assertSameMembers(
  document.components.schemas.DeletionOperationState.enum,
  [
    "requested",
    "quiescing",
    "quarantining",
    "recoverable",
    "restoring",
    "purging",
    "finalizing",
    "completed",
    "blocked",
    "failed",
  ],
  "deletion Saga states changed",
);
assertSameMembers(
  document.components.schemas.DeletionCompletion.enum,
  ["restored", "purged"],
  "deletion completion states changed",
);
assertSameMembers(
  document.components.schemas.RetentionHoldState.enum,
  ["active", "released"],
  "Retention Hold states changed",
);

const resourceRef = document.components.schemas.ResourceRef;
assert(
  resourceRef.discriminator?.propertyName === "type" &&
    resourceRef.oneOf?.length === 4,
  "ResourceRef must remain a four-way tagged union",
);
for (const [schemaName, type, required] of [
  ["StorageVolumeResourceRef", "storage_volume", ["type", "storage_volume_id"]],
  ["ArtifactResourceRef", "artifact", ["type", "project_id", "artifact_id"]],
  [
    "PlaygroundResourceRef",
    "playground",
    ["type", "project_id", "artifact_id", "playground_id"],
  ],
  ["SnapshotResourceRef", "snapshot", ["type", "snapshot_id"]],
]) {
  const schema = document.components.schemas[schemaName];
  assert(
    schema.additionalProperties === false &&
      schema.properties.type.const === type,
    `${schemaName} lost its closed tagged-union boundary`,
  );
  assertSameMembers(schema.required, required, `${schemaName} scope changed`);
}

for (const schemaName of [
  "StorageVolumeView",
  "ArtifactView",
  "PlaygroundView",
  "SnapshotView",
]) {
  const schema = document.components.schemas[schemaName];
  assert(
    schema.required.includes("resource_version") &&
      schema.required.includes("lifecycle") &&
      schema.properties.resource_version.$ref ===
        "#/components/schemas/CanonicalU64" &&
      schema.properties.lifecycle.$ref ===
        "#/components/schemas/ResourceLifecycleView",
    `${schemaName} must expose the lifecycle CAS fence`,
  );
}

const lifecycleView = document.components.schemas.ResourceLifecycleView;
assertSameMembers(
  lifecycleView.required,
  ["state", "generation"],
  "ResourceLifecycleView required fence changed",
);
assert(
  lifecycleView.additionalProperties === false &&
    lifecycleView.properties.generation.$ref ===
      "#/components/schemas/PositiveCanonicalU64",
  "ResourceLifecycleView must remain closed with a positive generation",
);

assertSameMembers(
  document.components.schemas.CreateDeletionRequest.required,
  [
    "tenant_id",
    "resource",
    "cascade",
    "confirm_managed_data_erase",
    "expected_resource_version",
    "impact_digest",
    "request_id",
  ],
  "delete mutation confirmation boundary changed",
);
assertSameMembers(
  document.components.schemas.UpdateDeletionRequest.required,
  ["tenant_id", "deletion_id", "request_id", "expected_resource_version"],
  "delete update CAS boundary changed",
);
assertSameMembers(
  document.components.schemas.CreateRetentionHoldRequest.required,
  [
    "tenant_id",
    "deletion_id",
    "request_id",
    "expected_resource_version",
    "reason",
  ],
  "Retention Hold create boundary changed",
);
assertSameMembers(
  document.components.schemas.ReleaseRetentionHoldRequest.required,
  [
    "tenant_id",
    "deletion_id",
    "retention_hold_id",
    "request_id",
    "expected_resource_version",
  ],
  "Retention Hold release boundary changed",
);

for (const operationId of [
  "queryResourceDeletionImpact",
  "createResourceDeletion",
  "queryResourceDeletion",
  "queryResourceDeletionList",
  "restoreResourceDeletion",
  "retryResourceDeletion",
  "createResourceRetentionHold",
  "releaseResourceRetentionHold",
]) {
  const [path, [method]] = Object.entries(expectedOperations).find(
    ([, [, candidate]]) => candidate === operationId,
  );
  const operation = document.paths[path][method];
  assert(
    operation.tags.includes("ResourceLifecycle"),
    `${operationId} must use the ResourceLifecycle tag`,
  );
  assert(
    operation.responses["409"] || operationId.includes("queryResourceDeletion"),
    `${operationId} must expose lifecycle CAS conflicts`,
  );
}
assertDescriptionIncludes(
  document.components.schemas.GatewayEndpoint,
  [
    "canonical HTTPS origin",
    "loopback HTTP origin",
    "path",
    "query",
    "fragment",
  ],
  "Gateway endpoint must document its canonical origin boundary",
);
assert(
  document.components.schemas.CreateGatewayReplicaResponse.properties
    .activation_token.readOnly === true,
  "Gateway activation token must remain response-only",
);

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
    "target_edge_cluster_id",
    "target_storage_volume_id",
    "delivery_mode",
    "request_id",
  ],
  "Snapshot create must bind Commit and request identity",
);
assert(
  !createSnapshotRequest.properties.snapshot_id,
  "Snapshot ID must remain server-generated",
);
assertDescriptionIncludes(
  createSnapshotRequest,
  ["固定 Commit", "目标 EdgeCluster/StorageVolume", "不能创建 Artifact", "切换 Commit"],
  "Snapshot create must preserve Artifact/Commit authority",
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
  ["snapshot", "replayed"],
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
const contentDigest = document.components.schemas.ContentDigest;
const commitId = document.components.schemas.CommitId;
assert(
  commitId.$ref === "#/components/schemas/ContentDigest",
  "CommitId must alias the canonical ContentDigest schema",
);
for (const [schema, field] of [
  [
    document.components.schemas.DerivedArtifactInitialization.properties
      .source_commit_id,
    "DerivedArtifactInitialization.source_commit_id",
  ],
  [
    document.components.schemas.ArtifactView.properties.head_commit_id,
    "ArtifactView.head_commit_id",
  ],
  [
    document.components.schemas.CommitGraphView.properties.head_commit_id,
    "CommitGraphView.head_commit_id",
  ],
  [
    document.components.schemas.CommitNode.properties.commit_id,
    "CommitNode.commit_id",
  ],
  [
    document.components.schemas.CommitNode.properties.parent_commit_id,
    "CommitNode.parent_commit_id",
  ],
  [
    document.components.schemas.QueryArtifactCommitDiffRequest.properties
      .commit_id,
    "QueryArtifactCommitDiffRequest.commit_id",
  ],
  [
    document.components.schemas.QueryArtifactCommitDiffRequest.properties
      .base_commit_id,
    "QueryArtifactCommitDiffRequest.base_commit_id",
  ],
  [
    document.components.schemas.CreatePlaygroundRequest.properties
      .base_commit_id,
    "CreatePlaygroundRequest.base_commit_id",
  ],
  [
    document.components.schemas.PlaygroundView.properties.base_commit_id,
    "PlaygroundView.base_commit_id",
  ],
  [
    document.components.schemas.PlaygroundView.properties.head_commit_id,
    "PlaygroundView.head_commit_id",
  ],
  [
    document.components.schemas.PreCommitView.properties.committed_commit_id,
    "PreCommitView.committed_commit_id",
  ],
  [
    document.components.schemas.QuerySnapshotListRequest.properties.commit_id,
    "QuerySnapshotListRequest.commit_id",
  ],
  [
    document.components.schemas.CreateSnapshotRequest.properties.commit_id,
    "CreateSnapshotRequest.commit_id",
  ],
  [
    document.components.schemas.SnapshotView.properties.commit_id,
    "SnapshotView.commit_id",
  ],
]) {
  assert(
    schema.$ref === "#/components/schemas/CommitId",
    `${field} must use the shared CommitId schema`,
  );
}
assert(
  contentDigest.type === "string" &&
    contentDigest.minLength === 64 &&
    contentDigest.maxLength === 64 &&
    contentDigest.pattern === "^[0-9a-f]{64}$",
  "ContentDigest must remain canonical 64-character lowercase hexadecimal",
);

const commitIdentityFields = new Set([
  "commit_id",
  "head_commit_id",
  "base_commit_id",
  "parent_commit_id",
  "source_commit_id",
  "committed_commit_id",
]);
const canonicalCommitId = /^[0-9a-f]{64}$/;
function assertCanonicalCommitExamples(value, path = "document") {
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      assertCanonicalCommitExamples(item, `${path}[${index}]`),
    );
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [field, child] of Object.entries(value)) {
    const childPath = `${path}.${field}`;
    if (commitIdentityFields.has(field) && typeof child === "string") {
      assert(
        canonicalCommitId.test(child),
        `${childPath} must be a canonical CommitId example`,
      );
    }
    assertCanonicalCommitExamples(child, childPath);
  }
}
assertCanonicalCommitExamples(document.paths);

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
assert(
  document.components.schemas.SnapshotPhase === undefined,
  "SnapshotPhase must not remain in the new delivery protocol",
);
assert(
  document.components.schemas.SnapshotView.properties.phase === undefined,
  "SnapshotView must not expose delivery phase",
);
assert(
  document.components.schemas.SnapshotActivityView.properties.phase ===
    undefined,
  "Snapshot activity must not expose delivery phase",
);
assertSameMembers(
  document.components.schemas.SnapshotActivityView.properties.activity_type
    .enum,
  ["created", "status_changed", "ready", "failed"],
  "Snapshot activity must not contain SnapshotDelivery retry events",
);
assertDescriptionIncludes(
  document.components.schemas.SnapshotView,
  ["SnapshotDelivery", "目标物理交付", "不可变"],
  "Snapshot view must delegate materialization to SnapshotDelivery",
);

const deliveryQueryOperation =
  document.paths["/api/snapshot/delivery/query"].post;
assert(
  deliveryQueryOperation.description.includes("交付模式") &&
    !deliveryQueryOperation.description.includes("布局"),
  "SnapshotDelivery query must describe delivery mode rather than Commit layout",
);
const deliveryListResponses =
  document.paths["/api/snapshot/delivery/list/query"].post.responses;
assert(
  deliveryListResponses["404"]?.$ref ===
    "#/components/responses/ResourceNotFoundProblem" &&
    deliveryListResponses["409"]?.$ref ===
      "#/components/responses/CursorConflictProblem",
  "SnapshotDelivery list must expose missing Snapshot and cursor conflicts",
);
assert(
  document.components.schemas.QuerySnapshotDeliveryListResponse.properties.items.maxItems === 1,
  "SnapshotDelivery list must allow at most one Delivery per Snapshot",
);
const snapshotDeliveryView = document.components.schemas.SnapshotDeliveryView;
for (const field of ["source_index_digest", "object_set_digest"]) {
  assert(
    snapshotDeliveryView.properties[field].$ref ===
      "#/components/schemas/ContentDigest",
    `SnapshotDeliveryView.${field} must use ContentDigest`,
  );
}
assertDescriptionIncludes(
  document.paths["/api/snapshot/delivery/delete"].post,
  ["deleting", "200 只表示删除流程已持久化", "deleted"],
  "SnapshotDelivery delete must describe asynchronous completion",
);
const deliveryConflictCodes = Object.values(
  document.components.responses.MutationConflictProblem.content[
    "application/problem+json"
  ].examples,
).map((example) => example.value.code);
for (const code of [
  "HARDLINK_REQUIRES_WHOLE_FILE",
  "HARDLINK_CROSS_FILESYSTEM",
  "HARDLINK_UNSAFE_VOLUME",
  "HARDLINK_OBJECT_NOT_SEALED",
  "DELIVERY_TARGET_CONFLICT",
]) {
  assert(
    deliveryConflictCodes.includes(code),
    `SnapshotDelivery conflict examples omit ${code}`,
  );
}

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

const playgroundView = document.components.schemas.PlaygroundView;
assert(
  playgroundView.required.includes("storage_volume_id") &&
    playgroundView.required.includes("region"),
  "PlaygroundView must expose its public storage placement",
);
const snapshotView = document.components.schemas.SnapshotView;
assert(
  snapshotView.required.includes("data_health"),
  "SnapshotView must expose dynamic Commit data health",
);
assert(
  snapshotView.properties.delivery_id &&
    snapshotView.properties.edge_cluster_id &&
    snapshotView.properties.storage_volume_id &&
    snapshotView.properties.delivery_mode,
  "SnapshotView must expose its immutable delivery target",
);
assertSameMembers(
  snapshotView.required.filter((field) =>
    ["delivery_id", "edge_cluster_id", "storage_volume_id", "delivery_mode"].includes(field),
  ),
  ["delivery_id", "edge_cluster_id", "storage_volume_id", "delivery_mode"],
  "SnapshotView delivery target fields changed",
);
assertSameMembers(
  document.components.schemas.PlaygroundView.required.filter((field) =>
    ["tenant_id", "project_id", "artifact_id"].includes(field),
  ),
  ["tenant_id", "project_id", "artifact_id"],
  "Playground must retain its immutable Artifact source scope",
);
assertSameMembers(
  document.components.schemas.SnapshotView.required.filter((field) =>
    ["tenant_id", "project_id", "artifact_id", "commit_id"].includes(field),
  ),
  ["tenant_id", "project_id", "artifact_id", "commit_id"],
  "Snapshot must retain its immutable Artifact Commit source scope",
);
assertDescriptionIncludes(
  document.components.schemas.SnapshotView,
  ["不可变来源 scope", "不拥有或改写 Artifact 数据权威"],
  "Snapshot view must remain derived from Artifact authority",
);
assert(
  !document.components.schemas.ArtifactView.properties.storage_volume_id &&
    !document.components.schemas.ArtifactView.properties.region &&
    !document.components.schemas.ArtifactView.properties.default_ref,
  "ArtifactView must remain placement- and Ref-free",
);

assert(
  document.components.schemas.CreatePlaygroundRequest.required.includes(
    "storage_volume_id",
  ),
  "CreatePlaygroundRequest must select a StorageVolume",
);
assert(
  document.components.schemas.CreateSnapshotRequest.required.includes(
    "target_edge_cluster_id",
  ) &&
    document.components.schemas.CreateSnapshotRequest.required.includes(
      "target_storage_volume_id",
    ) &&
    document.components.schemas.CreateSnapshotRequest.required.includes("delivery_mode"),
  "CreateSnapshotRequest must select its immutable delivery target",
);

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
  [
    "固定 Commit",
    "不能创建 Artifact",
    "切换 Commit",
    "目标 EdgeCluster",
    "目标 Volume",
    "唯一 SnapshotDelivery",
  ],
  "Snapshot target-first creation boundary is not documented",
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

const [playgroundCreatePath, [playgroundCreateMethod]] = Object.entries(
  expectedOperations,
).find(([, [, candidate]]) => candidate === "createPlayground");
assertDescriptionIncludes(
  document.paths[playgroundCreatePath][playgroundCreateMethod],
  ["state=ready", "degraded", "unavailable", "409"],
  "createPlayground ready-only placement rejection is not documented",
);
const [snapshotCreatePath, [snapshotCreateMethod]] = Object.entries(
  expectedOperations,
).find(([, [, candidate]]) => candidate === "createSnapshot");
assertDescriptionIncludes(
  document.paths[snapshotCreatePath][snapshotCreateMethod],
  ["绑定一个目标 EdgeCluster", "StorageVolume", "唯一 SnapshotDelivery"],
  "createSnapshot target-first semantics are not documented",
);

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
    "allowed_delivery_modes",
    "hardlink_policy",
    "max_whole_file_bytes",
    "copy_reserve_bytes",
    "pvc_reference",
    "state",
    "resource_version",
    "lifecycle",
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
    "allowed_delivery_modes",
    "hardlink_policy",
    "max_whole_file_bytes",
    "copy_reserve_bytes",
    "state",
    "resource_version",
    "lifecycle",
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
  completeStorageRecovery: {
    path: "/api/storage/enrollment/recovery/complete",
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
  storageEnrollmentConflictExamples.replacementConfirmationRequired.value
    .code === "STORAGE_ENROLLMENT_REPLACEMENT_CONFIRMATION_REQUIRED",
  "Storage enrollment replacement confirmation conflict code drifted",
);
assert(
  document.paths["/api/task/retry"].post.description.includes("Attempt") &&
    document.paths["/api/task/cancel"].post.description.includes("审计"),
  "Task retry/cancel operations must document unified lifecycle semantics",
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
  "volume_descriptor_digest",
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
  [...createEnrollmentTokenResponseFields, "task"],
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

const completeRecoveryRequest =
  document.components.schemas.CompleteStorageRecoveryRequest;
assertSameMembers(
  completeRecoveryRequest.required,
  [
    "tenant_id",
    "storage_enrollment_id",
    "expected_resource_version",
    "owner_generation",
  ],
  "Storage recovery completion fence fields changed",
);
assertSameMembers(
  Object.keys(completeRecoveryRequest.properties),
  completeRecoveryRequest.required,
  "Storage recovery completion accepts uncontracted fields",
);
assert(
  resolveRef(completeRecoveryRequest.properties.expected_resource_version) ===
    canonicalU64 &&
    resolveRef(completeRecoveryRequest.properties.owner_generation) ===
      canonicalU64,
  "Storage recovery completion versions must use CanonicalU64",
);
assertDescriptionIncludes(
  document.paths["/api/storage/enrollment/recovery/complete"].post,
  ["Ready mount", "owner generation", "旧 generation", "409"],
  "Storage recovery completion does not document its Ready-owner fence",
);
const completeRecoveryResponse =
  document.components.schemas.CompleteStorageRecoveryResponse;
assertSameMembers(
  completeRecoveryResponse.required,
  ["enrollment", "storage_volume"],
  "Storage recovery completion response fields changed",
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
  createTenantRequest.additionalProperties === false,
  "Tenant create request must reject unknown fields",
);
assert(
  !createTenantRequest.properties.actor &&
    !createTenantRequest.properties.principal,
  "Tenant create request must not declare actor or principal",
);

const expectedAgentOperations = Object.fromEntries(
  actionRegistry.agent_actions.map((route) => [route.path, route.operation_id]),
);
assert(
  Object.keys(expectedAgentOperations).length ===
    actionRegistry.agent_actions.length,
  "Agent action registry contains duplicate paths",
);
assert(
  new Set(Object.values(expectedAgentOperations)).size ===
    actionRegistry.agent_actions.length,
  "Agent action registry contains duplicate operationId values",
);
assert(
  agentDocument.openapi === "3.1.0",
  "Agent OpenAPI version must be 3.1.0",
);
assertSameMembers(
  Object.keys(agentDocument.paths),
  Object.keys(expectedAgentOperations),
  "Agent action path set changed",
);

for (const [path, operationId] of Object.entries(expectedAgentOperations)) {
  assert(
    !path.includes("{"),
    `Agent action path uses a path parameter: ${path}`,
  );
  const pathItem = agentDocument.paths[path];
  const methods = Object.keys(pathItem).filter((key) =>
    [
      "get",
      "put",
      "post",
      "delete",
      "patch",
      "options",
      "head",
      "trace",
    ].includes(key),
  );
  assertSameMembers(
    methods,
    ["post"],
    `Agent action must be POST-only: ${path}`,
  );
  const operation = pathItem.post;
  assert(
    operation.operationId === operationId,
    `Agent operationId changed: ${path}`,
  );
  assert(
    Array.isArray(operation.security) && operation.security.length === 0,
    `Agent action must declare body-carried security: ${path}`,
  );
  assert(
    typeof operation["x-neoengram-body-security"] === "string",
    `Agent action omits its body security scheme: ${path}`,
  );
  const registryRoute = actionRegistry.agent_actions.find(
    (route) => route.path === path,
  );
  assert(
    registryRoute.method.toLowerCase() === methods[0],
    `Agent registry method changed: ${path}`,
  );
  assert(
    (path === "/agent/session/channel/open") ===
      (registryRoute.transport === "http2-ndjson"),
    `Agent action has wrong registry transport: ${path}`,
  );
  assert(
    operation.parameters === undefined && pathItem.parameters === undefined,
    `Agent action input must not use path, query, header, or cookie parameters: ${path}`,
  );
  const requestBody = resolveAgentRef(operation.requestBody);
  assert(
    requestBody?.required === true,
    `Agent action request body must be required: ${path}`,
  );
}

const channelOperation =
  agentDocument.paths["/agent/session/channel/open"].post;
const duplex = channelOperation["x-neoengram-http2-duplex"];
assert(
  duplex?.transport === "http2-full-duplex" &&
    duplex.primaryControlTransport === true &&
    duplex.mediaType === "application/x-ndjson" &&
    duplex.h2DataFrameBoundaries === "ignored" &&
    duplex.frameDelimiter === "LF" &&
    duplex.maxFrameBytes === 1_048_576 &&
    duplex.finalFrameRequiresDelimiter === true,
  "Agent primary control channel transport semantics changed",
);
assert(
  resolveAgentRef(channelOperation.requestBody).content[
    "application/x-ndjson"
  ] && channelOperation.responses["200"].content["application/x-ndjson"],
  "Agent control channel must stream NDJSON in both directions",
);

const agentDecimalU64 = resolveAgentRef(
  agentDocument.components.schemas.DecimalU64,
);
const agentPositiveDecimalU64 = resolveAgentRef(
  agentDocument.components.schemas.PositiveDecimalU64,
);
const decimalU64Pattern = new RegExp(agentDecimalU64.pattern);
const positiveDecimalU64Pattern = new RegExp(agentPositiveDecimalU64.pattern);
assert(
  agentDecimalU64.type === "string" &&
    agentDecimalU64.minLength === 1 &&
    agentDecimalU64.maxLength === 20 &&
    decimalU64Pattern.test("0") &&
    decimalU64Pattern.test("18446744073709551615") &&
    !decimalU64Pattern.test("18446744073709551616") &&
    !decimalU64Pattern.test("01"),
  "Agent DecimalU64 must be canonical and capped at u64::MAX",
);
assert(
  agentPositiveDecimalU64.type === "string" &&
    agentPositiveDecimalU64.minLength === 1 &&
    agentPositiveDecimalU64.maxLength === 20 &&
    !positiveDecimalU64Pattern.test("0") &&
    positiveDecimalU64Pattern.test("1") &&
    positiveDecimalU64Pattern.test("18446744073709551615") &&
    !positiveDecimalU64Pattern.test("18446744073709551616"),
  "Agent positive decimal u64 must reject zero and values above u64::MAX",
);

const agentProof = agentDocument.components.schemas.AgentRequestProof;
const ed25519PublicKeySpki = resolveAgentRef(
  agentProof.properties.public_key_spki,
);
const ed25519Signature = resolveAgentRef(agentProof.properties.signature);
for (const [schema, encodedLength, name] of [
  [ed25519PublicKeySpki, 59, "Ed25519 SPKI"],
  [ed25519Signature, 86, "Ed25519 signature"],
]) {
  const pattern = new RegExp(schema.pattern);
  assert(
    schema.type === "string" &&
      schema.minLength === encodedLength &&
      schema.maxLength === encodedLength &&
      schema.pattern === "^[A-Za-z0-9_-]+$" &&
      pattern.test("A".repeat(encodedLength)) &&
      !pattern.test(`${"A".repeat(encodedLength - 1)}=`),
    `${name} must use exact-length unpadded base64url`,
  );
}

const heartbeatPayload =
  agentDocument.components.schemas.AgentHeartbeatReportPayload;
const heartbeat = resolveAgentSchema(heartbeatPayload.properties.heartbeat);
assert(
  heartbeat.type === "object" &&
    heartbeat.required.includes("agent_id") &&
    heartbeat.required.includes("sequence") &&
    heartbeat.properties.agent_id &&
    heartbeat.properties.sequence,
  "Agent heartbeat must resolve to the concrete Rust DTO",
);
const mountReport = resolveAgentSchema(heartbeatPayload.properties.mount_report);
for (const field of ["installation_id", "boot_id", "session_generation"]) {
  assert(
    mountReport.required.includes(field) && mountReport.properties[field],
    `Agent mount report omits concrete Rust field ${field}`,
  );
}

const jobReportPayload =
  agentDocument.components.schemas.AgentJobReportCreatePayload;
const controlEnvelope = resolveAgentSchema(jobReportPayload.properties.report);
assert(
  controlEnvelope.title === "Envelope" &&
    controlEnvelope.type === "object" &&
    controlEnvelope.properties?.action &&
    controlEnvelope.properties?.body,
  "Agent job report must resolve to the strict action Envelope",
);
for (const field of [
  "wire_version",
  "action",
  "request_id",
  "trace_id",
  "session_generation",
  "deadline",
  "body",
]) {
  assert(
    controlEnvelope.required.includes(field) &&
      controlEnvelope.properties[field],
    `Envelope report omits concrete Rust field ${field}`,
  );
}

const channelAssignment = resolveAgentSchema(
  agentDocument.components.schemas.AgentChannelJobAssignment,
);
assertSameMembers(
  channelAssignment.required,
  ["assignment"],
  "Agent channel assignment wrapper changed",
);
const assignmentOperation = resolveAgentRef(
  channelAssignment.properties.assignment,
);
assert(
  assignmentOperation.oneOf?.length > 0,
  "Agent channel assignment must resolve to a concrete operation union",
);
for (const branch of assignmentOperation.oneOf) {
  assert(
    branch.required.includes("operation") &&
      branch.required.includes("input") &&
      branch.properties.operation &&
      resolveAgentRef(branch.properties.input)?.type === "object",
    "Agent assignment operation must require concrete operation and input fields",
  );
}

const channelDecision = resolveAgentSchema(
  agentDocument.components.schemas.AgentChannelJobDecision,
);
assert(
  channelDecision.required.includes("decision") &&
    channelDecision.required.includes("final_state"),
  "Agent channel decision must expose concrete decision and final_state fields",
);
const publishDecision = resolveAgentRef(channelDecision.properties.decision);
const finalJobState = resolveAgentRef(channelDecision.properties.final_state);
assert(
  publishDecision.oneOf?.length > 0 &&
    publishDecision.oneOf.every(
      (branch) =>
        branch.required.includes("outcome") &&
        typeof branch.properties.outcome?.const === "string",
    ),
  "Agent channel decision must resolve to the concrete publish decision union",
);
assert(
  finalJobState.type === "string" &&
    Array.isArray(finalJobState.enum) &&
    finalJobState.enum.length > 0,
  "Agent channel final_state must resolve to the Rust JobState enum",
);

console.log("Public and Agent OpenAPI contract checks passed");
