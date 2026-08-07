use crate::dto::ApiVersionResponse;

/// Public API capability application service.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemService;

impl SystemService {
    pub fn query_api_version(&self) -> ApiVersionResponse {
        ApiVersionResponse {
            api_versions: vec![1],
            agent_protocol_versions: vec![1],
            capabilities: vec![
                "artifact_catalog".to_owned(),
                "artifact_commit_graph".to_owned(),
                "managed_add".to_owned(),
                "playground_browser".to_owned(),
                "playground_materialize".to_owned(),
                "playground_precommit".to_owned(),
                "snapshot_materialize".to_owned(),
                "sqlite_authority".to_owned(),
            ],
        }
    }
}
