use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        ApiVersionResponse, ApproveStorageEnrollmentRequest, ApproveStorageEnrollmentResponse,
        CreateAddJobRequest, CreateAddJobResponse, CreateStorageEnrollmentTokenRequest,
        CreateStorageEnrollmentTokenResponse, EmptyRequest, FinalizeAddJobRequest,
        FinalizeAddJobResponse, HealthStatus, QueryJobRequest, QueryJobResponse,
        QueryStorageEnrollmentListRequest, QueryStorageEnrollmentListResponse,
        QueryStorageEnrollmentRequest, QueryStorageEnrollmentResponse,
        RejectStorageEnrollmentRequest, RejectStorageEnrollmentResponse,
    },
    error::unauthenticated,
    identity::AuthenticatedIdentity,
    service::{EnrollmentService, HealthService, JobService, SystemService},
};

/// Public system and probe routes.
#[interface(name = "neoengram.system")]
pub trait SystemApi {
    #[fusen_rs::method(method = "POST", path = "/api/system/version/query")]
    async fn query_api_version(
        &self,
        #[param(body)] request: EmptyRequest,
    ) -> Result<Response<ApiVersionResponse>, Error>;

    #[fusen_rs::method(method = "GET", path = "/health/live")]
    async fn live_probe(&self) -> Result<Response<HealthStatus>, Error>;

    #[fusen_rs::method(method = "GET", path = "/health/ready")]
    async fn ready_probe(&self) -> Result<Response<HealthStatus>, Error>;
}

/// Public Managed Add routes.
#[interface(name = "neoengram.job")]
pub trait JobApi {
    #[fusen_rs::method(method = "POST", path = "/api/job/add/create")]
    async fn create_add_job(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateAddJobRequest,
    ) -> Result<Response<CreateAddJobResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/job/query")]
    async fn query_job(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryJobRequest,
    ) -> Result<Response<QueryJobResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/job/add/finalize")]
    async fn finalize_add_job(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: FinalizeAddJobRequest,
    ) -> Result<Response<FinalizeAddJobResponse>, Error>;
}

/// Public storage enrollment administration routes.
#[interface(name = "neoengram.storage.enrollment")]
pub trait StorageEnrollmentApi {
    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/token/create")]
    async fn create_storage_enrollment_token(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateStorageEnrollmentTokenRequest,
    ) -> Result<Response<CreateStorageEnrollmentTokenResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/list/query")]
    async fn query_storage_enrollment_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryStorageEnrollmentListRequest,
    ) -> Result<Response<QueryStorageEnrollmentListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/query")]
    async fn query_storage_enrollment(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryStorageEnrollmentRequest,
    ) -> Result<Response<QueryStorageEnrollmentResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/approve")]
    async fn approve_storage_enrollment(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: ApproveStorageEnrollmentRequest,
    ) -> Result<Response<ApproveStorageEnrollmentResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/reject")]
    async fn reject_storage_enrollment(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: RejectStorageEnrollmentRequest,
    ) -> Result<Response<RejectStorageEnrollmentResponse>, Error>;
}

/// System route implementation.
pub struct SystemController {
    system: Arc<SystemService>,
    health: Arc<HealthService>,
}

impl SystemController {
    pub fn new(system: Arc<SystemService>, health: Arc<HealthService>) -> Self {
        Self { system, health }
    }
}

impl SystemApi for SystemController {
    async fn query_api_version(
        &self,
        _request: EmptyRequest,
    ) -> Result<Response<ApiVersionResponse>, Error> {
        Ok(Response::new(self.system.query_api_version()))
    }

    async fn live_probe(&self) -> Result<Response<HealthStatus>, Error> {
        Ok(Response::new(self.health.live()))
    }

    async fn ready_probe(&self) -> Result<Response<HealthStatus>, Error> {
        self.health.ready().await.map(Response::new)
    }
}

/// Job route implementation.
pub struct JobController {
    service: Arc<JobService>,
}

impl JobController {
    pub fn new(service: Arc<JobService>) -> Self {
        Self { service }
    }
}

impl JobApi for JobController {
    async fn create_add_job(
        &self,
        call: Call,
        request: CreateAddJobRequest,
    ) -> Result<Response<CreateAddJobResponse>, Error> {
        let identity = authenticated_identity(&call)?;
        self.service
            .create_add_job(&identity, request)
            .await
            .map(Response::new)
    }

    async fn query_job(
        &self,
        call: Call,
        request: QueryJobRequest,
    ) -> Result<Response<QueryJobResponse>, Error> {
        let identity = authenticated_identity(&call)?;
        self.service
            .query_job(&identity, request)
            .await
            .map(Response::new)
    }

    async fn finalize_add_job(
        &self,
        call: Call,
        request: FinalizeAddJobRequest,
    ) -> Result<Response<FinalizeAddJobResponse>, Error> {
        let identity = authenticated_identity(&call)?;
        self.service
            .finalize_add_job(&identity, request)
            .await
            .map(Response::new)
    }
}

/// Storage enrollment route implementation.
pub struct StorageEnrollmentController {
    service: Arc<EnrollmentService>,
}

impl StorageEnrollmentController {
    pub fn new(service: Arc<EnrollmentService>) -> Self {
        Self { service }
    }
}

impl StorageEnrollmentApi for StorageEnrollmentController {
    async fn create_storage_enrollment_token(
        &self,
        call: Call,
        request: CreateStorageEnrollmentTokenRequest,
    ) -> Result<Response<CreateStorageEnrollmentTokenResponse>, Error> {
        self.service
            .create_token(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_storage_enrollment_list(
        &self,
        call: Call,
        request: QueryStorageEnrollmentListRequest,
    ) -> Result<Response<QueryStorageEnrollmentListResponse>, Error> {
        self.service
            .list(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_storage_enrollment(
        &self,
        call: Call,
        request: QueryStorageEnrollmentRequest,
    ) -> Result<Response<QueryStorageEnrollmentResponse>, Error> {
        self.service
            .query(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn approve_storage_enrollment(
        &self,
        call: Call,
        request: ApproveStorageEnrollmentRequest,
    ) -> Result<Response<ApproveStorageEnrollmentResponse>, Error> {
        self.service
            .approve(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn reject_storage_enrollment(
        &self,
        call: Call,
        request: RejectStorageEnrollmentRequest,
    ) -> Result<Response<RejectStorageEnrollmentResponse>, Error> {
        self.service
            .reject(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}

fn authenticated_identity(call: &Call) -> Result<AuthenticatedIdentity, Error> {
    call.extensions()
        .get::<AuthenticatedIdentity>()
        .cloned()
        .ok_or_else(|| {
            unauthenticated(
                "authentication_required",
                "a valid Bearer token is required",
            )
        })
}
