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
export type QueryProjectListRequest = components['schemas']['QueryProjectListRequest'];
export type QueryProjectListResponse = components['schemas']['QueryProjectListResponse'];
export type ProjectSummary = components['schemas']['ProjectSummary'];
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
export type QueryPlaygroundListResponse = components['schemas']['QueryPlaygroundListResponse'];
export type QueryPlaygroundResponse = components['schemas']['QueryPlaygroundResponse'];
export type CreatePlaygroundRequest = components['schemas']['CreatePlaygroundRequest'];
export type CreatePlaygroundResponse = components['schemas']['CreatePlaygroundResponse'];
export type PlaygroundState = components['schemas']['PlaygroundState'];
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
export type QuerySnapshotListResponse = components['schemas']['QuerySnapshotListResponse'];
export type QuerySnapshotResponse = components['schemas']['QuerySnapshotResponse'];
export type CreateSnapshotRequest = components['schemas']['CreateSnapshotRequest'];
export type CreateSnapshotResponse = components['schemas']['CreateSnapshotResponse'];
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
export type SnapshotPhase = components['schemas']['SnapshotPhase'];
export type SnapshotView = components['schemas']['SnapshotView'];
