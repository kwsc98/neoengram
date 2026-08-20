use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        CancelPreCommitRequest, CancelPreCommitResponse, CommitPlaygroundRequest,
        CommitPlaygroundResponse, CreateArtifactRequest, CreateArtifactResponse,
        CreateDeletionRequest, CreatePlaygroundRequest, CreatePlaygroundResponse,
        CreateProjectRequest, CreateProjectResponse, CreateRetentionHoldRequest,
        CreateRetentionHoldResponse, CreateS3AccessPointRequest, CreateS3AccessPointResponse,
        CreateS3CredentialRequest, CreateS3CredentialResponse, CreateS3DownloadUrlRequest,
        CreateS3DownloadUrlResponse, CreateSnapshotDeliveryRequest, CreateSnapshotDeliveryResponse,
        CreateSnapshotRequest, CreateSnapshotResponse, CreateStorageVolumeRequest,
        CreateStorageVolumeResponse, CreateTenantRequest, CreateTenantResponse,
        DeleteSnapshotDeliveryRequest, DeleteSnapshotDeliveryResponse, DeletionMutationResponse,
        InternalS3AuthorizeRequest, InternalS3AuthorizeResponse, QueryArtifactCommitGraphRequest,
        QueryArtifactCommitGraphResponse, QueryArtifactListRequest, QueryArtifactListResponse,
        QueryArtifactRequest, QueryArtifactResponse, QueryDeletionImpactRequest,
        QueryDeletionImpactResponse, QueryDeletionListRequest, QueryDeletionListResponse,
        QueryDeletionRequest, QueryDeletionResponse, QueryPlaygroundChangeListRequest,
        QueryPlaygroundChangeListResponse, QueryPlaygroundDatasetProfileRequest,
        QueryPlaygroundDatasetProfileResponse, QueryPlaygroundFileListRequest,
        QueryPlaygroundFileListResponse, QueryPlaygroundFileMetadataRequest,
        QueryPlaygroundFileMetadataResponse, QueryPlaygroundListRequest,
        QueryPlaygroundListResponse, QueryPlaygroundRequest, QueryPlaygroundResponse,
        QueryPreCommitRequest, QueryPreCommitResponse, QueryProjectListRequest,
        QueryProjectListResponse, QueryS3AccessPointListRequest, QueryS3AccessPointListResponse,
        QueryS3AccessPointRequest, QueryS3AccessPointResponse, QueryS3CredentialListRequest,
        QueryS3CredentialListResponse, QueryS3ObjectListRequest, QueryS3ObjectListResponse,
        QuerySnapshotDeliveryListRequest, QuerySnapshotDeliveryListResponse,
        QuerySnapshotDeliveryRequest, QuerySnapshotDeliveryResponse, QuerySnapshotListRequest,
        QuerySnapshotListResponse, QuerySnapshotRequest, QuerySnapshotResponse,
        QueryStorageVolumeListRequest, QueryStorageVolumeListResponse, QueryStorageVolumeRequest,
        QueryStorageVolumeResponse, QueryTenantListRequest, QueryTenantListResponse,
        QueryTenantRequest, QueryTenantResponse, ReleaseRetentionHoldRequest,
        ReleaseRetentionHoldResponse, RestartPreCommitRequest, RestartPreCommitResponse,
        RetrySnapshotDeliveryRequest, RetrySnapshotDeliveryResponse, RevokeS3CredentialRequest,
        StartPreCommitRequest, StartPreCommitResponse, UpdateDeletionRequest,
        UpdateS3AccessPointRequest, UpdateS3AccessPointResponse,
    },
    service::CatalogService,
};

use super::authenticated_identity;

#[interface(name = "neoengram.tenant")]
pub trait TenantApi {
    #[fusen_rs::method(method = "POST", path = "/api/tenant/list/query")]
    async fn query_tenant_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTenantListRequest,
    ) -> Result<Response<QueryTenantListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/tenant/query")]
    async fn query_tenant(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTenantRequest,
    ) -> Result<Response<QueryTenantResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/tenant/create")]
    async fn create_tenant(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateTenantRequest,
    ) -> Result<Response<CreateTenantResponse>, Error>;
}

#[interface(name = "neoengram.project")]
pub trait ProjectApi {
    #[fusen_rs::method(method = "POST", path = "/api/project/list/query")]
    async fn query_project_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryProjectListRequest,
    ) -> Result<Response<QueryProjectListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/project/create")]
    async fn create_project(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateProjectRequest,
    ) -> Result<Response<CreateProjectResponse>, Error>;
}

#[interface(name = "neoengram.storage.volume")]
pub trait StorageVolumeApi {
    #[fusen_rs::method(method = "POST", path = "/api/storage/volume/list/query")]
    async fn query_storage_volume_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryStorageVolumeListRequest,
    ) -> Result<Response<QueryStorageVolumeListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/volume/query")]
    async fn query_storage_volume(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryStorageVolumeRequest,
    ) -> Result<Response<QueryStorageVolumeResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/volume/create")]
    async fn create_storage_volume(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateStorageVolumeRequest,
    ) -> Result<Response<CreateStorageVolumeResponse>, Error>;
}

#[interface(name = "neoengram.artifact")]
pub trait ArtifactApi {
    #[fusen_rs::method(method = "POST", path = "/api/artifact/list/query")]
    async fn query_artifact_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryArtifactListRequest,
    ) -> Result<Response<QueryArtifactListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/artifact/query")]
    async fn query_artifact(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryArtifactRequest,
    ) -> Result<Response<QueryArtifactResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/artifact/commit/graph/query")]
    async fn query_artifact_commit_graph(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryArtifactCommitGraphRequest,
    ) -> Result<Response<QueryArtifactCommitGraphResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/artifact/create")]
    async fn create_artifact(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateArtifactRequest,
    ) -> Result<Response<CreateArtifactResponse>, Error>;
}

#[interface(name = "neoengram.playground")]
pub trait PlaygroundApi {
    #[fusen_rs::method(method = "POST", path = "/api/playground/list/query")]
    async fn query_playground_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundListRequest,
    ) -> Result<Response<QueryPlaygroundListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/query")]
    async fn query_playground(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundRequest,
    ) -> Result<Response<QueryPlaygroundResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/create")]
    async fn create_playground(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreatePlaygroundRequest,
    ) -> Result<Response<CreatePlaygroundResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/precommit/start")]
    async fn start_playground_precommit(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: StartPreCommitRequest,
    ) -> Result<Response<StartPreCommitResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/precommit/query")]
    async fn query_playground_precommit(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPreCommitRequest,
    ) -> Result<Response<QueryPreCommitResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/precommit/restart")]
    async fn restart_playground_precommit(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: RestartPreCommitRequest,
    ) -> Result<Response<RestartPreCommitResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/precommit/cancel")]
    async fn cancel_playground_precommit(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CancelPreCommitRequest,
    ) -> Result<Response<CancelPreCommitResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/file/list/query")]
    async fn query_playground_file_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundFileListRequest,
    ) -> Result<Response<QueryPlaygroundFileListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/change/list/query")]
    async fn query_playground_change_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundChangeListRequest,
    ) -> Result<Response<QueryPlaygroundChangeListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/file/metadata/query")]
    async fn query_playground_file_metadata(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundFileMetadataRequest,
    ) -> Result<Response<QueryPlaygroundFileMetadataResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/dataset/profile/query")]
    async fn query_playground_dataset_profile(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryPlaygroundDatasetProfileRequest,
    ) -> Result<Response<QueryPlaygroundDatasetProfileResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/playground/commit/create")]
    async fn commit_playground(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CommitPlaygroundRequest,
    ) -> Result<Response<CommitPlaygroundResponse>, Error>;
}

#[interface(name = "neoengram.snapshot")]
pub trait SnapshotApi {
    #[fusen_rs::method(method = "POST", path = "/api/snapshot/list/query")]
    async fn query_snapshot_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QuerySnapshotListRequest,
    ) -> Result<Response<QuerySnapshotListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/query")]
    async fn query_snapshot(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QuerySnapshotRequest,
    ) -> Result<Response<QuerySnapshotResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/create")]
    async fn create_snapshot(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateSnapshotRequest,
    ) -> Result<Response<CreateSnapshotResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/delivery/create")]
    async fn create_snapshot_delivery(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateSnapshotDeliveryRequest,
    ) -> Result<Response<CreateSnapshotDeliveryResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/delivery/query")]
    async fn query_snapshot_delivery(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QuerySnapshotDeliveryRequest,
    ) -> Result<Response<QuerySnapshotDeliveryResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/delivery/list/query")]
    async fn query_snapshot_delivery_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QuerySnapshotDeliveryListRequest,
    ) -> Result<Response<QuerySnapshotDeliveryListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/delivery/retry")]
    async fn retry_snapshot_delivery(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: RetrySnapshotDeliveryRequest,
    ) -> Result<Response<RetrySnapshotDeliveryResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/snapshot/delivery/delete")]
    async fn delete_snapshot_delivery(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: DeleteSnapshotDeliveryRequest,
    ) -> Result<Response<DeleteSnapshotDeliveryResponse>, Error>;
}

#[interface(name = "neoengram.s3")]
pub trait S3Api {
    #[fusen_rs::method(method = "POST", path = "/api/s3/access-point/create")]
    async fn create_access_point(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateS3AccessPointRequest,
    ) -> Result<Response<CreateS3AccessPointResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/access-point/list/query")]
    async fn query_access_point_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryS3AccessPointListRequest,
    ) -> Result<Response<QueryS3AccessPointListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/access-point/query")]
    async fn query_access_point(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryS3AccessPointRequest,
    ) -> Result<Response<QueryS3AccessPointResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/access-point/enable")]
    async fn enable_access_point(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: UpdateS3AccessPointRequest,
    ) -> Result<Response<UpdateS3AccessPointResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/access-point/disable")]
    async fn disable_access_point(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: UpdateS3AccessPointRequest,
    ) -> Result<Response<UpdateS3AccessPointResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/credential/create")]
    async fn create_credential(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateS3CredentialRequest,
    ) -> Result<Response<CreateS3CredentialResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/credential/list/query")]
    async fn query_credential_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryS3CredentialListRequest,
    ) -> Result<Response<QueryS3CredentialListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/credential/revoke")]
    async fn revoke_credential(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: RevokeS3CredentialRequest,
    ) -> Result<Response<QueryS3CredentialListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/object/list/query")]
    async fn query_object_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryS3ObjectListRequest,
    ) -> Result<Response<QueryS3ObjectListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/s3/object/download-url/create")]
    async fn create_download_url(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateS3DownloadUrlRequest,
    ) -> Result<Response<CreateS3DownloadUrlResponse>, Error>;
}

/// Private Gateway workload API. The runtime's ingress policy must only route this endpoint from
/// the mTLS-authenticated Gateway upstream; browser identities never use this interface.
#[interface(name = "neoengram.s3.internal")]
pub trait S3AuthorizationApi {
    #[fusen_rs::method(method = "POST", path = "/internal/s3/authorize")]
    async fn authorize_s3_request(
        &self,
        #[param(body)] request: InternalS3AuthorizeRequest,
    ) -> Result<Response<InternalS3AuthorizeResponse>, Error>;
}

#[interface(name = "neoengram.resource.lifecycle")]
pub trait ResourceLifecycleApi {
    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/impact/query")]
    async fn query_deletion_impact(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryDeletionImpactRequest,
    ) -> Result<Response<QueryDeletionImpactResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/create")]
    async fn create_deletion(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/query")]
    async fn query_deletion(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryDeletionRequest,
    ) -> Result<Response<QueryDeletionResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/list/query")]
    async fn query_deletion_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryDeletionListRequest,
    ) -> Result<Response<QueryDeletionListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/restore")]
    async fn restore_deletion(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: UpdateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/deletion/retry")]
    async fn retry_deletion(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: UpdateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/retention-hold/create")]
    async fn create_retention_hold(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateRetentionHoldRequest,
    ) -> Result<Response<CreateRetentionHoldResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/resource/retention-hold/release")]
    async fn release_retention_hold(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: ReleaseRetentionHoldRequest,
    ) -> Result<Response<ReleaseRetentionHoldResponse>, Error>;
}

pub struct TenantController {
    service: Arc<CatalogService>,
}

impl TenantController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl TenantApi for TenantController {
    async fn query_tenant_list(
        &self,
        call: Call,
        request: QueryTenantListRequest,
    ) -> Result<Response<QueryTenantListResponse>, Error> {
        self.service
            .list_tenants(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_tenant(
        &self,
        call: Call,
        request: QueryTenantRequest,
    ) -> Result<Response<QueryTenantResponse>, Error> {
        self.service
            .query_tenant(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_tenant(
        &self,
        call: Call,
        request: CreateTenantRequest,
    ) -> Result<Response<CreateTenantResponse>, Error> {
        self.service
            .create_tenant(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct ProjectController {
    service: Arc<CatalogService>,
}

impl ProjectController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl ProjectApi for ProjectController {
    async fn query_project_list(
        &self,
        call: Call,
        request: QueryProjectListRequest,
    ) -> Result<Response<QueryProjectListResponse>, Error> {
        self.service
            .list_projects(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_project(
        &self,
        call: Call,
        request: CreateProjectRequest,
    ) -> Result<Response<CreateProjectResponse>, Error> {
        self.service
            .create_project(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct StorageVolumeController {
    service: Arc<CatalogService>,
}

impl StorageVolumeController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl StorageVolumeApi for StorageVolumeController {
    async fn query_storage_volume_list(
        &self,
        call: Call,
        request: QueryStorageVolumeListRequest,
    ) -> Result<Response<QueryStorageVolumeListResponse>, Error> {
        self.service
            .list_storage_volumes(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_storage_volume(
        &self,
        call: Call,
        request: QueryStorageVolumeRequest,
    ) -> Result<Response<QueryStorageVolumeResponse>, Error> {
        self.service
            .query_storage_volume(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_storage_volume(
        &self,
        call: Call,
        request: CreateStorageVolumeRequest,
    ) -> Result<Response<CreateStorageVolumeResponse>, Error> {
        self.service
            .create_storage_volume(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct ArtifactController {
    service: Arc<CatalogService>,
}

impl ArtifactController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl ArtifactApi for ArtifactController {
    async fn query_artifact_list(
        &self,
        call: Call,
        request: QueryArtifactListRequest,
    ) -> Result<Response<QueryArtifactListResponse>, Error> {
        self.service
            .list_artifacts(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_artifact(
        &self,
        call: Call,
        request: QueryArtifactRequest,
    ) -> Result<Response<QueryArtifactResponse>, Error> {
        self.service
            .query_artifact(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_artifact_commit_graph(
        &self,
        call: Call,
        request: QueryArtifactCommitGraphRequest,
    ) -> Result<Response<QueryArtifactCommitGraphResponse>, Error> {
        self.service
            .query_artifact_commit_graph(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_artifact(
        &self,
        call: Call,
        request: CreateArtifactRequest,
    ) -> Result<Response<CreateArtifactResponse>, Error> {
        self.service
            .create_artifact(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct PlaygroundController {
    service: Arc<CatalogService>,
}

impl PlaygroundController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl PlaygroundApi for PlaygroundController {
    async fn query_playground_list(
        &self,
        call: Call,
        request: QueryPlaygroundListRequest,
    ) -> Result<Response<QueryPlaygroundListResponse>, Error> {
        self.service
            .list_playgrounds(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground(
        &self,
        call: Call,
        request: QueryPlaygroundRequest,
    ) -> Result<Response<QueryPlaygroundResponse>, Error> {
        self.service
            .query_playground(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_playground(
        &self,
        call: Call,
        request: CreatePlaygroundRequest,
    ) -> Result<Response<CreatePlaygroundResponse>, Error> {
        self.service
            .create_playground(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn start_playground_precommit(
        &self,
        call: Call,
        request: StartPreCommitRequest,
    ) -> Result<Response<StartPreCommitResponse>, Error> {
        self.service
            .start_playground_precommit(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground_precommit(
        &self,
        call: Call,
        request: QueryPreCommitRequest,
    ) -> Result<Response<QueryPreCommitResponse>, Error> {
        self.service
            .query_playground_precommit(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn restart_playground_precommit(
        &self,
        call: Call,
        request: RestartPreCommitRequest,
    ) -> Result<Response<RestartPreCommitResponse>, Error> {
        self.service
            .restart_playground_precommit(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn cancel_playground_precommit(
        &self,
        call: Call,
        request: CancelPreCommitRequest,
    ) -> Result<Response<CancelPreCommitResponse>, Error> {
        self.service
            .cancel_playground_precommit(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground_file_list(
        &self,
        call: Call,
        request: QueryPlaygroundFileListRequest,
    ) -> Result<Response<QueryPlaygroundFileListResponse>, Error> {
        self.service
            .query_playground_file_list(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground_change_list(
        &self,
        call: Call,
        request: QueryPlaygroundChangeListRequest,
    ) -> Result<Response<QueryPlaygroundChangeListResponse>, Error> {
        self.service
            .query_playground_change_list(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground_file_metadata(
        &self,
        call: Call,
        request: QueryPlaygroundFileMetadataRequest,
    ) -> Result<Response<QueryPlaygroundFileMetadataResponse>, Error> {
        self.service
            .query_playground_file_metadata(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_playground_dataset_profile(
        &self,
        call: Call,
        request: QueryPlaygroundDatasetProfileRequest,
    ) -> Result<Response<QueryPlaygroundDatasetProfileResponse>, Error> {
        self.service
            .query_playground_dataset_profile(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn commit_playground(
        &self,
        call: Call,
        request: CommitPlaygroundRequest,
    ) -> Result<Response<CommitPlaygroundResponse>, Error> {
        self.service
            .commit_playground(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct SnapshotController {
    service: Arc<CatalogService>,
}

impl SnapshotController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl SnapshotApi for SnapshotController {
    async fn query_snapshot_list(
        &self,
        call: Call,
        request: QuerySnapshotListRequest,
    ) -> Result<Response<QuerySnapshotListResponse>, Error> {
        self.service
            .list_snapshots(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_snapshot(
        &self,
        call: Call,
        request: QuerySnapshotRequest,
    ) -> Result<Response<QuerySnapshotResponse>, Error> {
        self.service
            .query_snapshot(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_snapshot(
        &self,
        call: Call,
        request: CreateSnapshotRequest,
    ) -> Result<Response<CreateSnapshotResponse>, Error> {
        self.service
            .create_snapshot(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_snapshot_delivery(
        &self,
        call: Call,
        request: CreateSnapshotDeliveryRequest,
    ) -> Result<Response<CreateSnapshotDeliveryResponse>, Error> {
        self.service
            .create_snapshot_delivery(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_snapshot_delivery(
        &self,
        call: Call,
        request: QuerySnapshotDeliveryRequest,
    ) -> Result<Response<QuerySnapshotDeliveryResponse>, Error> {
        self.service
            .query_snapshot_delivery(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_snapshot_delivery_list(
        &self,
        call: Call,
        request: QuerySnapshotDeliveryListRequest,
    ) -> Result<Response<QuerySnapshotDeliveryListResponse>, Error> {
        self.service
            .list_snapshot_deliveries(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn retry_snapshot_delivery(
        &self,
        call: Call,
        request: RetrySnapshotDeliveryRequest,
    ) -> Result<Response<RetrySnapshotDeliveryResponse>, Error> {
        self.service
            .retry_snapshot_delivery(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn delete_snapshot_delivery(
        &self,
        call: Call,
        request: DeleteSnapshotDeliveryRequest,
    ) -> Result<Response<DeleteSnapshotDeliveryResponse>, Error> {
        self.service
            .delete_snapshot_delivery(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct ResourceLifecycleController {
    service: Arc<CatalogService>,
}

impl ResourceLifecycleController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl ResourceLifecycleApi for ResourceLifecycleController {
    async fn query_deletion_impact(
        &self,
        call: Call,
        request: QueryDeletionImpactRequest,
    ) -> Result<Response<QueryDeletionImpactResponse>, Error> {
        self.service
            .query_deletion_impact(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_deletion(
        &self,
        call: Call,
        request: CreateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error> {
        self.service
            .create_deletion(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_deletion(
        &self,
        call: Call,
        request: QueryDeletionRequest,
    ) -> Result<Response<QueryDeletionResponse>, Error> {
        self.service
            .query_deletion(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_deletion_list(
        &self,
        call: Call,
        request: QueryDeletionListRequest,
    ) -> Result<Response<QueryDeletionListResponse>, Error> {
        self.service
            .list_deletions(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn restore_deletion(
        &self,
        call: Call,
        request: UpdateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error> {
        self.service
            .restore_deletion(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn retry_deletion(
        &self,
        call: Call,
        request: UpdateDeletionRequest,
    ) -> Result<Response<DeletionMutationResponse>, Error> {
        self.service
            .retry_deletion(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_retention_hold(
        &self,
        call: Call,
        request: CreateRetentionHoldRequest,
    ) -> Result<Response<CreateRetentionHoldResponse>, Error> {
        self.service
            .create_retention_hold(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn release_retention_hold(
        &self,
        call: Call,
        request: ReleaseRetentionHoldRequest,
    ) -> Result<Response<ReleaseRetentionHoldResponse>, Error> {
        self.service
            .release_retention_hold(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

pub struct S3Controller {
    service: Arc<CatalogService>,
}

pub struct S3AuthorizationController {
    service: Arc<CatalogService>,
}

impl S3AuthorizationController {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl S3AuthorizationApi for S3AuthorizationController {
    async fn authorize_s3_request(
        &self,
        request: InternalS3AuthorizeRequest,
    ) -> Result<Response<InternalS3AuthorizeResponse>, Error> {
        self.service
            .authorize_s3_request(request.0)
            .await
            .map(InternalS3AuthorizeResponse)
            .map(Response::new)
    }
}

impl S3Controller {
    #[must_use]
    pub fn new(service: Arc<CatalogService>) -> Self {
        Self { service }
    }
}

impl S3Api for S3Controller {
    async fn create_access_point(
        &self,
        call: Call,
        request: CreateS3AccessPointRequest,
    ) -> Result<Response<CreateS3AccessPointResponse>, Error> {
        self.service
            .create_s3_access_point(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_access_point_list(
        &self,
        call: Call,
        request: QueryS3AccessPointListRequest,
    ) -> Result<Response<QueryS3AccessPointListResponse>, Error> {
        self.service
            .list_s3_access_points(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_access_point(
        &self,
        call: Call,
        request: QueryS3AccessPointRequest,
    ) -> Result<Response<QueryS3AccessPointResponse>, Error> {
        self.service
            .query_s3_access_point(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn enable_access_point(
        &self,
        call: Call,
        request: UpdateS3AccessPointRequest,
    ) -> Result<Response<UpdateS3AccessPointResponse>, Error> {
        self.service
            .enable_s3_access_point(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn disable_access_point(
        &self,
        call: Call,
        request: UpdateS3AccessPointRequest,
    ) -> Result<Response<UpdateS3AccessPointResponse>, Error> {
        self.service
            .disable_s3_access_point(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_credential(
        &self,
        call: Call,
        request: CreateS3CredentialRequest,
    ) -> Result<Response<CreateS3CredentialResponse>, Error> {
        self.service
            .create_s3_credential(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_credential_list(
        &self,
        call: Call,
        request: QueryS3CredentialListRequest,
    ) -> Result<Response<QueryS3CredentialListResponse>, Error> {
        self.service
            .list_s3_credentials(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn revoke_credential(
        &self,
        call: Call,
        request: RevokeS3CredentialRequest,
    ) -> Result<Response<QueryS3CredentialListResponse>, Error> {
        self.service
            .revoke_s3_credential(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_object_list(
        &self,
        call: Call,
        request: QueryS3ObjectListRequest,
    ) -> Result<Response<QueryS3ObjectListResponse>, Error> {
        self.service
            .list_s3_objects(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_download_url(
        &self,
        call: Call,
        request: CreateS3DownloadUrlRequest,
    ) -> Result<Response<CreateS3DownloadUrlResponse>, Error> {
        self.service
            .create_s3_download_url(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}
