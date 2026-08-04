use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        CreatePlaygroundRequest, CreatePlaygroundResponse, CreateStorageVolumeRequest,
        CreateStorageVolumeResponse, CreateTenantRequest, CreateTenantResponse,
        QueryPlaygroundListRequest, QueryPlaygroundListResponse, QueryPlaygroundRequest,
        QueryPlaygroundResponse, QueryStorageVolumeListRequest, QueryStorageVolumeListResponse,
        QueryStorageVolumeRequest, QueryStorageVolumeResponse, QueryTenantListRequest,
        QueryTenantListResponse, QueryTenantRequest, QueryTenantResponse,
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
}
