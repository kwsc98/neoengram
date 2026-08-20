import type { components } from './generated/openapi';

export type ApiVersionResponse = components['schemas']['ApiVersionResponse'];
export type CreateAddJobRequest = components['schemas']['CreateAddJobRequest'];
export type CreateAddJobResponse = components['schemas']['CreateAddJobResponse'];
export type QueryJobResponse = components['schemas']['QueryJobResponse'];
export type FinalizeAddJobResponse = components['schemas']['FinalizeAddJobResponse'];
export type JobView = components['schemas']['JobView'];
export type JobState = components['schemas']['JobState'];
export type ProblemDetails = components['schemas']['ProblemDetails'];
export type HealthResponse = components['schemas']['HealthResponse'];
export type QueryTenantListRequest = components['schemas']['QueryTenantListRequest'];
export type QueryTenantListResponse = components['schemas']['QueryTenantListResponse'];
export type QueryTenantResponse = components['schemas']['QueryTenantResponse'];
export type CreateTenantRequest = components['schemas']['CreateTenantRequest'];
export type CreateTenantResponse = components['schemas']['CreateTenantResponse'];
export type TenantView = components['schemas']['TenantView'];
export type QueryStorageVolumeListRequest = components['schemas']['QueryStorageVolumeListRequest'];
export type QueryStorageVolumeListResponse =
  components['schemas']['QueryStorageVolumeListResponse'];
export type QueryStorageVolumeResponse = components['schemas']['QueryStorageVolumeResponse'];
export type CreateStorageVolumeRequest = components['schemas']['CreateStorageVolumeRequest'];
export type CreateStorageVolumeResponse = components['schemas']['CreateStorageVolumeResponse'];
export type StorageVolumeView = components['schemas']['StorageVolumeView'];
export type StorageBackendType = components['schemas']['StorageBackendType'];
export type StorageAccessMode = components['schemas']['StorageAccessMode'];
export type CreateStorageEnrollmentTokenRequest =
  components['schemas']['CreateStorageEnrollmentTokenRequest'];
export type CreateStorageEnrollmentTokenResponse =
  components['schemas']['CreateStorageEnrollmentTokenResponse'] & {
    volume_descriptor_digest: string;
  };
export type QueryStorageEnrollmentListRequest =
  components['schemas']['QueryStorageEnrollmentListRequest'];
export type QueryStorageEnrollmentListResponse =
  components['schemas']['QueryStorageEnrollmentListResponse'];
export type QueryStorageEnrollmentRequest = components['schemas']['QueryStorageEnrollmentRequest'];
export type QueryStorageEnrollmentResponse =
  components['schemas']['QueryStorageEnrollmentResponse'];
export type ApproveStorageEnrollmentRequest =
  components['schemas']['ApproveStorageEnrollmentRequest'];
export type ApproveStorageEnrollmentResponse =
  components['schemas']['ApproveStorageEnrollmentResponse'];
export type RejectStorageEnrollmentRequest =
  components['schemas']['RejectStorageEnrollmentRequest'];
export type RejectStorageEnrollmentResponse =
  components['schemas']['RejectStorageEnrollmentResponse'];
export type StorageEnrollmentState = components['schemas']['StorageEnrollmentState'];
export type StorageEnrollmentAccessMode = components['schemas']['StorageEnrollmentAccessMode'];
export type StorageEnrollmentView = components['schemas']['StorageEnrollmentView'];
export type StorageEnrollmentProbeSummary = components['schemas']['StorageEnrollmentProbeSummary'];
export type QueryProjectListRequest = components['schemas']['QueryProjectListRequest'];
export type QueryProjectListResponse = components['schemas']['QueryProjectListResponse'];
export type CreateProjectRequest = components['schemas']['CreateProjectRequest'];
export type CreateProjectResponse = components['schemas']['CreateProjectResponse'];
export type ProjectView = components['schemas']['ProjectView'];
export type QueryArtifactListRequest = components['schemas']['QueryArtifactListRequest'];
export type QueryArtifactListResponse = components['schemas']['QueryArtifactListResponse'];
export type QueryArtifactResponse = components['schemas']['QueryArtifactResponse'];
export type ArtifactInitializationMode = components['schemas']['ArtifactInitialization']['mode'];
export type ArtifactInitialization = components['schemas']['ArtifactInitialization'];
export type CreateArtifactRequest = components['schemas']['CreateArtifactRequest'];
export type CreateArtifactResponse = components['schemas']['CreateArtifactResponse'];
export type ArtifactView = components['schemas']['ArtifactView'];
export type QueryArtifactCommitGraphResponse =
  components['schemas']['QueryArtifactCommitGraphResponse'];
export type CommitGraphView = components['schemas']['CommitGraphView'];
export type CommitNode = components['schemas']['CommitNode'];
export type QueryArtifactCommitDiffResponse =
  components['schemas']['QueryArtifactCommitDiffResponse'];
export type CommitDiffView = components['schemas']['CommitDiffView'];
export type CommitDiffEntry = components['schemas']['CommitDiffEntry'];
export type QueryPlaygroundListRequest = components['schemas']['QueryPlaygroundListRequest'];
export type QueryPlaygroundListResponse = Omit<
  components['schemas']['QueryPlaygroundListResponse'],
  'items'
> & { items: PlaygroundView[] };
export type QueryPlaygroundResponse = Omit<
  components['schemas']['QueryPlaygroundResponse'],
  'playground'
> & { playground: PlaygroundView };
export type CreatePlaygroundRequest = components['schemas']['CreatePlaygroundRequest'];
export type CreatePlaygroundResponse = Omit<
  components['schemas']['CreatePlaygroundResponse'],
  'playground'
> & { playground: PlaygroundView };
export type PlaygroundState = components['schemas']['PlaygroundState'];
export type PlaygroundStorageAvailability = components['schemas']['PlaygroundStorageAvailability'];
export type PlaygroundView = components['schemas']['PlaygroundView'];
export type StartPreCommitRequest = components['schemas']['StartPreCommitRequest'];
export type StartPreCommitResponse = components['schemas']['StartPreCommitResponse'];
export type QueryPreCommitRequest = components['schemas']['QueryPreCommitRequest'];
export type QueryPreCommitResponse = components['schemas']['QueryPreCommitResponse'];
export type RestartPreCommitRequest = components['schemas']['RestartPreCommitRequest'];
export type RestartPreCommitResponse = components['schemas']['RestartPreCommitResponse'];
export type CancelPreCommitRequest = components['schemas']['CancelPreCommitRequest'];
export type CancelPreCommitResponse = components['schemas']['CancelPreCommitResponse'];
export type PreCommitState = components['schemas']['PreCommitState'];
export type PreCommitPhase = components['schemas']['PreCommitPhase'];
export type PreCommitView = components['schemas']['PreCommitView'];
export type CommitPlaygroundRequest = components['schemas']['CommitPlaygroundRequest'];
export type CommitPlaygroundResponse = components['schemas']['CommitPlaygroundResponse'];
export type QueryPlaygroundFileListRequest =
  components['schemas']['QueryPlaygroundFileListRequest'];
export type QueryPlaygroundFileListResponse =
  components['schemas']['QueryPlaygroundFileListResponse'];
export type QueryPlaygroundChangeListRequest =
  components['schemas']['QueryPlaygroundChangeListRequest'];
export type QueryPlaygroundChangeListResponse =
  components['schemas']['QueryPlaygroundChangeListResponse'];
export type QueryPlaygroundFileMetadataRequest =
  components['schemas']['QueryPlaygroundFileMetadataRequest'];
export type QueryPlaygroundFileMetadataResponse =
  components['schemas']['QueryPlaygroundFileMetadataResponse'];
export type QueryPlaygroundDatasetProfileRequest =
  components['schemas']['QueryPlaygroundDatasetProfileRequest'];
export type QueryPlaygroundDatasetProfileResponse =
  components['schemas']['QueryPlaygroundDatasetProfileResponse'];
export type LogicalFileEntry = components['schemas']['LogicalFileEntry'];
export type PlaygroundChangeEntry = components['schemas']['PlaygroundChangeEntry'];
export type FileMetadataView = components['schemas']['FileMetadataView'];
export type DatasetProfileView = components['schemas']['DatasetProfileView'];
export type QuerySnapshotListRequest = components['schemas']['QuerySnapshotListRequest'];
export type QuerySnapshotListResponse = Omit<
  components['schemas']['QuerySnapshotListResponse'],
  'items'
> & { items: SnapshotView[] };
export type QuerySnapshotResponse = Omit<
  components['schemas']['QuerySnapshotResponse'],
  'snapshot'
> & { snapshot: SnapshotView };
export type CreateSnapshotRequest = components['schemas']['CreateSnapshotRequest'];
export type CreateSnapshotResponse = Omit<
  components['schemas']['CreateSnapshotResponse'],
  'snapshot'
> & { snapshot: SnapshotView };
export type RetrySnapshotDeliveryRequest = components['schemas']['RetrySnapshotDeliveryRequest'];
export type RetrySnapshotDeliveryResponse = components['schemas']['RetrySnapshotDeliveryResponse'];
export type QuerySnapshotFileListRequest = components['schemas']['QuerySnapshotFileListRequest'];
export type QuerySnapshotFileListResponse = components['schemas']['QuerySnapshotFileListResponse'];
export type QuerySnapshotActivityListRequest =
  components['schemas']['QuerySnapshotActivityListRequest'];
export type QuerySnapshotActivityListResponse =
  components['schemas']['QuerySnapshotActivityListResponse'];
export type QuerySnapshotDatasetProfileRequest =
  components['schemas']['QuerySnapshotDatasetProfileRequest'];
export type QuerySnapshotDatasetProfileResponse =
  components['schemas']['QuerySnapshotDatasetProfileResponse'];
export type SnapshotState = components['schemas']['SnapshotState'];
export type SnapshotIntegrityState = components['schemas']['SnapshotIntegritySummary']['state'];
export type SnapshotActivityType = components['schemas']['SnapshotActivityView']['activity_type'];
export type DatasetProfileState = components['schemas']['DatasetProfileState'];
export type SnapshotView = components['schemas']['SnapshotView'];
export type DataLayout = components['schemas']['DataLayout'];
export type SnapshotDeliveryMode = components['schemas']['SnapshotDeliveryMode'];
export type SnapshotDeliveryState = components['schemas']['SnapshotDeliveryState'];
export type SnapshotDeliveryView = components['schemas']['SnapshotDeliveryView'];
export type CreateSnapshotDeliveryRequest = components['schemas']['CreateSnapshotDeliveryRequest'];
export type CreateSnapshotDeliveryResponse =
  components['schemas']['CreateSnapshotDeliveryResponse'];
export type QuerySnapshotDeliveryRequest = components['schemas']['QuerySnapshotDeliveryRequest'];
export type QuerySnapshotDeliveryResponse = components['schemas']['QuerySnapshotDeliveryResponse'];
export type QuerySnapshotDeliveryListRequest =
  components['schemas']['QuerySnapshotDeliveryListRequest'];
export type QuerySnapshotDeliveryListResponse =
  components['schemas']['QuerySnapshotDeliveryListResponse'];
export type DeleteSnapshotDeliveryRequest = components['schemas']['DeleteSnapshotDeliveryRequest'];
export type DeleteSnapshotDeliveryResponse =
  components['schemas']['DeleteSnapshotDeliveryResponse'];

/** Read-only S3 response/request views; secret fields stay optional on idempotent replays. */
export type S3AccessPointState = 'active' | 'disabled';
export type S3CredentialState = 'active' | 'revoked' | 'expired';
export type S3ObjectEntryType = 'object' | 'prefix';

export interface S3AccessPointView {
  access_point_id: string;
  tenant_id: string;
  project_id: string;
  artifact_id: string;
  snapshot_id: string;
  commit_id: string;
  bucket_name: string;
  endpoint: string;
  region: string;
  state: S3AccessPointState;
  policy_generation: string;
  created_at_unix_ms: string;
  updated_at_unix_ms: string;
}

export interface S3CredentialView {
  credential_id: string;
  access_point_id: string;
  access_key_id: string;
  state: S3CredentialState;
  expires_at_unix_ms: string;
  created_at_unix_ms: string;
  last_used_at_unix_ms?: string;
}

export interface S3ObjectEntryView {
  key: string;
  entry_type: S3ObjectEntryType;
  size_bytes?: string;
  etag?: string;
  last_modified_unix_ms?: string;
}

export interface QueryS3AccessPointListRequest {
  tenant_id: string;
  cursor?: string;
  page_size?: number;
}

export interface QueryS3AccessPointListResponse {
  items: S3AccessPointView[];
  next_cursor?: string;
}

export interface QueryS3AccessPointRequest {
  tenant_id: string;
  access_point_id: string;
}

export interface QueryS3AccessPointResponse {
  access_point: S3AccessPointView;
}

export interface CreateS3AccessPointRequest {
  [key: string]: unknown;
  tenant_id: string;
  snapshot_id: string;
  bucket_name: string;
  request_id: string;
}

export interface CreateS3AccessPointResponse {
  access_point: S3AccessPointView;
  access_key_id: string;
  /** Returned only for the first successful execution, never for an idempotent replay. */
  secret_access_key?: string;
  credential_expires_at_unix_ms: string;
  replayed: boolean;
}

export interface UpdateS3AccessPointRequest {
  tenant_id: string;
  access_point_id: string;
  request_id: string;
}

export interface UpdateS3AccessPointResponse {
  access_point: S3AccessPointView;
  replayed: boolean;
}

export interface CreateS3CredentialRequest {
  tenant_id: string;
  access_point_id: string;
  request_id: string;
  expires_at_unix_ms?: string;
}

export interface CreateS3CredentialResponse {
  credential: S3CredentialView;
  /** Returned only for the first successful execution, never for an idempotent replay. */
  secret_access_key?: string;
  replayed: boolean;
}

export interface QueryS3CredentialListRequest {
  tenant_id: string;
  access_point_id: string;
}

export interface QueryS3CredentialListResponse {
  items: S3CredentialView[];
}

export interface RevokeS3CredentialRequest {
  tenant_id: string;
  access_point_id: string;
  credential_id: string;
  request_id: string;
}

export interface QueryS3ObjectListRequest {
  tenant_id: string;
  access_point_id: string;
  prefix?: string;
  delimiter?: '' | '/';
  cursor?: string;
  page_size?: number;
}

export interface QueryS3ObjectListResponse {
  items: S3ObjectEntryView[];
  next_cursor?: string;
  /** Optional S3-style directory entries when the service returns CommonPrefixes separately. */
  common_prefixes?: string[];
}

export interface CreateS3DownloadUrlRequest {
  tenant_id: string;
  access_point_id: string;
  key: string;
  expires_seconds?: number;
}

export interface CreateS3DownloadUrlResponse {
  url: string;
  expires_at_unix_ms: string;
}

/** Storage resource lifecycle v1 contracts. */
export type ResourceLifecycleState = components['schemas']['ResourceLifecycleState'];
export type ResourceLifecycleView = components['schemas']['ResourceLifecycleView'];
export type DeletionOperationState = components['schemas']['DeletionOperationState'];
export type DeletionCompletion = components['schemas']['DeletionCompletion'];
export type RetentionHoldState = components['schemas']['RetentionHoldState'];
export type ResourceRef = components['schemas']['ResourceRef'];
export type DeletionTargetView = components['schemas']['DeletionTargetView'];
export type DeletionBlockerView = components['schemas']['DeletionBlockerView'];
export type DeletionImpactView = components['schemas']['DeletionImpactView'];
export type DeletionOperationView = components['schemas']['DeletionOperationView'];
export type RetentionHoldView = components['schemas']['RetentionHoldView'];
export type QueryDeletionImpactRequest = components['schemas']['QueryDeletionImpactRequest'];
export type QueryDeletionImpactResponse = components['schemas']['QueryDeletionImpactResponse'];
export type CreateDeletionRequest = components['schemas']['CreateDeletionRequest'];
export type DeletionMutationResponse = components['schemas']['DeletionMutationResponse'];
export type QueryDeletionRequest = components['schemas']['QueryDeletionRequest'];
export type QueryDeletionResponse = components['schemas']['QueryDeletionResponse'];
export type QueryDeletionListRequest = components['schemas']['QueryDeletionListRequest'];
export type QueryDeletionListResponse = components['schemas']['QueryDeletionListResponse'];
export type UpdateDeletionRequest = components['schemas']['UpdateDeletionRequest'];
export type CreateRetentionHoldRequest = components['schemas']['CreateRetentionHoldRequest'];
export type CreateRetentionHoldResponse = components['schemas']['CreateRetentionHoldResponse'];
export type ReleaseRetentionHoldRequest = components['schemas']['ReleaseRetentionHoldRequest'];
export type ReleaseRetentionHoldResponse = components['schemas']['ReleaseRetentionHoldResponse'];
