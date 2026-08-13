use crate::dto::ApiVersionResponse;

/// Public API capability application service.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemService {
    storage_execution_enabled: bool,
}

impl SystemService {
    #[must_use]
    pub const fn new(storage_execution_enabled: bool) -> Self {
        Self {
            storage_execution_enabled,
        }
    }

    pub fn query_api_version(&self) -> ApiVersionResponse {
        let mut capabilities = vec![
            "artifact_catalog".to_owned(),
            "artifact_commit_graph".to_owned(),
            "managed_add".to_owned(),
            "playground_browser".to_owned(),
            "sqlite_authority".to_owned(),
        ];
        if self.storage_execution_enabled {
            capabilities.extend([
                "playground_materialize".to_owned(),
                "playground_precommit".to_owned(),
                "snapshot_materialize".to_owned(),
            ]);
        }
        ApiVersionResponse {
            api_versions: vec![1],
            agent_protocol_versions: vec![1],
            capabilities,
        }
    }
}
