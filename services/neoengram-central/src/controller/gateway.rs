use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        ActivateGatewayReplicaRequest, CreateGatewayPoolRequest, CreateGatewayReplicaRequest,
        CreateGatewayReplicaResponse, DrainGatewayPoolRequest, GatewayPoolListResponse,
        GatewayPoolResponse, GatewayReplicaListResponse, GatewayReplicaResponse,
        MutateGatewayReplicaRequest, QueryGatewayPoolListRequest, QueryGatewayPoolRequest,
        QueryGatewayReplicaListRequest, UpdateGatewayPoolRequest,
    },
    service::GatewayRegistryService,
};

use super::authenticated_identity;

#[interface(name = "neoengram.gateway.registry")]
pub trait GatewayRegistryApi {
    #[fusen_rs::method(method = "POST", path = "/api/gateway/pool/create")]
    async fn create_gateway_pool(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/pool/query")]
    async fn query_gateway_pool(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/pool/list/query")]
    async fn query_gateway_pool_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryGatewayPoolListRequest,
    ) -> Result<Response<GatewayPoolListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/pool/update")]
    async fn update_gateway_pool(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: UpdateGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/pool/drain")]
    async fn drain_gateway_pool(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: DrainGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/replica/create")]
    async fn create_gateway_replica(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CreateGatewayReplicaRequest,
    ) -> Result<Response<CreateGatewayReplicaResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/replica/list/query")]
    async fn query_gateway_replica_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryGatewayReplicaListRequest,
    ) -> Result<Response<GatewayReplicaListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/replica/activate")]
    async fn activate_gateway_replica(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: ActivateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/replica/drain")]
    async fn drain_gateway_replica(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: MutateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/gateway/replica/revoke")]
    async fn revoke_gateway_replica(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: MutateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error>;
}

pub struct GatewayRegistryController {
    service: Arc<GatewayRegistryService>,
}

impl GatewayRegistryController {
    #[must_use]
    pub fn new(service: Arc<GatewayRegistryService>) -> Self {
        Self { service }
    }
}

impl GatewayRegistryApi for GatewayRegistryController {
    async fn create_gateway_pool(
        &self,
        call: Call,
        request: CreateGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error> {
        self.service
            .create_pool(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_gateway_pool(
        &self,
        call: Call,
        request: QueryGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error> {
        self.service
            .query_pool(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_gateway_pool_list(
        &self,
        call: Call,
        request: QueryGatewayPoolListRequest,
    ) -> Result<Response<GatewayPoolListResponse>, Error> {
        self.service
            .list_pools(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn update_gateway_pool(
        &self,
        call: Call,
        request: UpdateGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error> {
        self.service
            .update_pool(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn drain_gateway_pool(
        &self,
        call: Call,
        request: DrainGatewayPoolRequest,
    ) -> Result<Response<GatewayPoolResponse>, Error> {
        self.service
            .drain_pool(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn create_gateway_replica(
        &self,
        call: Call,
        request: CreateGatewayReplicaRequest,
    ) -> Result<Response<CreateGatewayReplicaResponse>, Error> {
        self.service
            .create_replica(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_gateway_replica_list(
        &self,
        call: Call,
        request: QueryGatewayReplicaListRequest,
    ) -> Result<Response<GatewayReplicaListResponse>, Error> {
        self.service
            .list_replicas(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn drain_gateway_replica(
        &self,
        call: Call,
        request: MutateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error> {
        self.service
            .drain_replica(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn activate_gateway_replica(
        &self,
        call: Call,
        request: ActivateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error> {
        self.service
            .activate_replica(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn revoke_gateway_replica(
        &self,
        call: Call,
        request: MutateGatewayReplicaRequest,
    ) -> Result<Response<GatewayReplicaResponse>, Error> {
        self.service
            .revoke_replica(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}
