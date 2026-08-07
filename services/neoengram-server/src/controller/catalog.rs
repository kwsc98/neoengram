use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        CancelPreCommitRequest, CancelPreCommitResponse, CommitPlaygroundRequest,
        CommitPlaygroundResponse, CreateArtifactRequest, CreateArtifactResponse,
        CreatePlaygroundRequest, CreatePlaygroundResponse, CreateSnapshotRequest,
        CreateSnapshotResponse, CreateStorageVolumeRequest, CreateStorageVolumeResponse,
        CreateTenantRequest, CreateTenantResponse, QueryArtifactCommitGraphRequest,
        QueryArtifactCommitGraphResponse, QueryArtifactListRequest, QueryArtifactListResponse,
        QueryArtifactRequest, QueryArtifactResponse, QueryPlaygroundChangeListRequest,
        QueryPlaygroundChangeListResponse, QueryPlaygroundDatasetProfileRequest,
        QueryPlaygroundDatasetProfileResponse, QueryPlaygroundFileListRequest,
        QueryPlaygroundFileListResponse, QueryPlaygroundFileMetadataRequest,
        QueryPlaygroundFileMetadataResponse, QueryPlaygroundListRequest,
        QueryPlaygroundListResponse, QueryPlaygroundRequest, QueryPlaygroundResponse,
        QueryPreCommitRequest, QueryPreCommitResponse, QuerySnapshotListRequest,
        QuerySnapshotListResponse, QuerySnapshotRequest, QuerySnapshotResponse,
        QueryStorageVolumeListRequest, QueryStorageVolumeListResponse, QueryStorageVolumeRequest,
        QueryStorageVolumeResponse, QueryTenantListRequest, QueryTenantListResponse,
        QueryTenantRequest, QueryTenantResponse, RestartPreCommitRequest, RestartPreCommitResponse,
        StartPreCommitRequest, StartPreCommitResponse,
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
}
