use crate::dto::ApiVersionResponse;

/// Public API capability application service.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemService {
    storage_execution_enabled: bool,
    s3_readonly_access_point_enabled: bool,
    resource_lifecycle_enabled: bool,
}

impl SystemService {
    #[must_use]
    pub const fn new(storage_execution_enabled: bool) -> Self {
        Self {
            storage_execution_enabled,
            s3_readonly_access_point_enabled: false,
            resource_lifecycle_enabled: false,
        }
    }

    #[must_use]
    pub const fn with_s3_readonly_access_point(mut self, enabled: bool) -> Self {
        self.s3_readonly_access_point_enabled = enabled;
        self
    }

    #[must_use]
    pub const fn with_resource_lifecycle(mut self, enabled: bool) -> Self {
        self.resource_lifecycle_enabled = enabled;
        self
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
                "commit_layout_selection_v2".to_owned(),
                "snapshot_delivery_fuse_v2".to_owned(),
                "snapshot_delivery_copy_v2".to_owned(),
                "snapshot_delivery_hardlink_v2".to_owned(),
            ]);
        }
        if self.storage_execution_enabled && self.s3_readonly_access_point_enabled {
            capabilities.push("s3_readonly_access_point".to_owned());
        }
        if self.resource_lifecycle_enabled {
            capabilities.push("resource_lifecycle_v1".to_owned());
        }
        ApiVersionResponse {
            api_version: 1,
            agent_wire_version: 1,
            capabilities,
        }
    }
}
