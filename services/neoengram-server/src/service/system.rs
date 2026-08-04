use crate::dto::ApiVersionResponse;

/// Public API capability application service.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemService;

impl SystemService {
    pub fn query_api_version(&self) -> ApiVersionResponse {
        ApiVersionResponse {
            api_versions: vec![1],
            agent_protocol_versions: vec![1],
            capabilities: vec!["managed_add".to_owned(), "sqlite_authority".to_owned()],
        }
    }
}
