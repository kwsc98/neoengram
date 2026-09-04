use std::sync::Arc;

use fusen_rs::{interface, Call, Error, Response};

use crate::{
    dto::{
        CancelTaskRequest, QueryTaskEventListRequest, QueryTaskEventListResponse,
        QueryTaskListRequest, QueryTaskListResponse, QueryTaskRequest, QueryTaskResponse,
        QueryTaskSummaryRequest, QueryTaskSummaryResponse, RetryTaskRequest, TaskMutationResponse,
    },
    service::TaskService,
};

use super::authenticated_identity;

/// Public query and lifecycle controls for all write-operation tasks.
#[interface(name = "neoengram.task")]
pub trait TaskApi {
    #[fusen_rs::method(method = "POST", path = "/api/task/list/query")]
    async fn query_task_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTaskListRequest,
    ) -> Result<Response<QueryTaskListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/task/query")]
    async fn query_task(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTaskRequest,
    ) -> Result<Response<QueryTaskResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/task/event/list/query")]
    async fn query_task_event_list(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTaskEventListRequest,
    ) -> Result<Response<QueryTaskEventListResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/task/summary/query")]
    async fn query_task_summary(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: QueryTaskSummaryRequest,
    ) -> Result<Response<QueryTaskSummaryResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/task/retry")]
    async fn retry_task(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: RetryTaskRequest,
    ) -> Result<Response<TaskMutationResponse>, Error>;

    #[fusen_rs::method(method = "POST", path = "/api/task/cancel")]
    async fn cancel_task(
        &self,
        #[param(context)] call: Call,
        #[param(body)] request: CancelTaskRequest,
    ) -> Result<Response<TaskMutationResponse>, Error>;
}

pub struct TaskController {
    service: Arc<TaskService>,
}

impl TaskController {
    #[must_use]
    pub fn new(service: Arc<TaskService>) -> Self {
        Self { service }
    }
}

impl TaskApi for TaskController {
    async fn query_task_list(
        &self,
        call: Call,
        request: QueryTaskListRequest,
    ) -> Result<Response<QueryTaskListResponse>, Error> {
        self.service
            .list_tasks(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_task(
        &self,
        call: Call,
        request: QueryTaskRequest,
    ) -> Result<Response<QueryTaskResponse>, Error> {
        self.service
            .query_task(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_task_event_list(
        &self,
        call: Call,
        request: QueryTaskEventListRequest,
    ) -> Result<Response<QueryTaskEventListResponse>, Error> {
        self.service
            .list_task_events(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn query_task_summary(
        &self,
        call: Call,
        request: QueryTaskSummaryRequest,
    ) -> Result<Response<QueryTaskSummaryResponse>, Error> {
        self.service
            .task_summary(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn retry_task(
        &self,
        call: Call,
        request: RetryTaskRequest,
    ) -> Result<Response<TaskMutationResponse>, Error> {
        self.service
            .retry_task(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }

    async fn cancel_task(
        &self,
        call: Call,
        request: CancelTaskRequest,
    ) -> Result<Response<TaskMutationResponse>, Error> {
        self.service
            .cancel_task(&authenticated_identity(&call)?, request)
            .await
            .map(Response::new)
    }
}
