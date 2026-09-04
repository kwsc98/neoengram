use std::sync::Arc;

mod catalog;
pub use catalog::*;
mod gateway;
pub use gateway::*;
mod task;
pub use task::*;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        ApiVersionResponse, ApproveStorageEnrollmentRequest, ApproveStorageEnrollmentResponse,
        CompleteStorageRecoveryRequest, CompleteStorageRecoveryResponse,
        CreateStorageEnrollmentTokenRequest, CreateStorageEnrollmentTokenResponse, EmptyRequest,
        HealthStatus, QueryStorageEnrollmentListRequest, QueryStorageEnrollmentListResponse,
        QueryStorageEnrollmentRequest, QueryStorageEnrollmentResponse,
        RejectStorageEnrollmentRequest, RejectStorageEnrollmentResponse,
    },
    error::unauthenticated,
    identity::AuthenticatedIdentity,
    service::{EnrollmentService, HealthService, SystemService},
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

    #[fusen_rs::method(method = "POST", path = "/api/storage/enrollment/recovery/complete")]
    async fn complete_storage_recovery(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CompleteStorageRecoveryRequest,
    ) -> Result<Response<CompleteStorageRecoveryResponse>, Error>;

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

    async fn complete_storage_recovery(
        &self,
        call: Call,
        request: CompleteStorageRecoveryRequest,
    ) -> Result<Response<CompleteStorageRecoveryResponse>, Error> {
        self.service
            .complete_recovery(&authenticated_identity(&call)?, request)
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

pub(super) fn authenticated_identity(call: &Call) -> Result<AuthenticatedIdentity, Error> {
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

#[cfg(test)]
mod action_registry_tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn compiled_controller_routes_match_the_domain_registry() {
        let descriptors = [
            SystemApiClient::descriptor().unwrap(),
            StorageEnrollmentApiClient::descriptor().unwrap(),
            TenantApiClient::descriptor().unwrap(),
            ProjectApiClient::descriptor().unwrap(),
            StorageVolumeApiClient::descriptor().unwrap(),
            ArtifactApiClient::descriptor().unwrap(),
            PlaygroundApiClient::descriptor().unwrap(),
            SnapshotApiClient::descriptor().unwrap(),
            PlacementApiClient::descriptor().unwrap(),
            S3ApiClient::descriptor().unwrap(),
            S3AuthorizationApiClient::descriptor().unwrap(),
            ResourceLifecycleApiClient::descriptor().unwrap(),
            GatewayRegistryApiClient::descriptor().unwrap(),
            TaskApiClient::descriptor().unwrap(),
        ];
        let actual = descriptors
            .into_iter()
            .flat_map(fusen_rs::contract::ServiceDescriptor::methods)
            .map(|method| {
                let operation = method.http_operation();
                (
                    operation.method().as_str().to_owned(),
                    operation.path().to_owned(),
                )
            })
            .collect::<BTreeSet<_>>();
        let expected = neoengram_domain::protocol::central_action_registry()
            .map(|descriptor| (descriptor.method.to_owned(), descriptor.path.to_owned()))
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);
    }
}
