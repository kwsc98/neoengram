mod config;
mod history;
mod layout;
mod lock;
mod validation;
mod workspaces;

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::local::{
    metadata::{MetadataStore, WorkspaceId},
    objects::{ObjectStore, ObjectStoreKind},
};

pub(crate) use config::ChunkingPolicy;
pub(crate) use layout::{EXPORTS_DIR_NAME, MOUNTS_DIR_NAME, WORKSPACES_DIR_NAME};
pub(crate) use lock::RepositoryWriteLock;
pub(crate) use validation::is_neoengram_dir_name;

pub(crate) const NEOENGRAM_DIR_NAME: &str = ".neoengram";
pub(super) const DEFAULT_HEAD_REFERENCE: &str = "refs/heads/main";

pub(crate) fn is_managed_container_dir_name(name: &OsStr) -> bool {
    [WORKSPACES_DIR_NAME, MOUNTS_DIR_NAME, EXPORTS_DIR_NAME]
        .iter()
        .any(|reserved| name == *reserved)
}

#[derive(Debug, Clone)]
pub(crate) struct Repository {
    /// Container root that owns `.neoengram` and all shared immutable data.
    container_root: PathBuf,
    /// Active Workspace root. Existing worktree code deliberately uses this accessor so the
    /// storage/workspace split does not leak into every path operation.
    root: PathBuf,
    workspace_id: WorkspaceId,
    metadata_store: Arc<dyn MetadataStore>,
    object_store: Arc<dyn ObjectStore>,
    object_store_kind: ObjectStoreKind,
    chunking_policy: ChunkingPolicy,
}

impl Repository {
    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub(crate) fn container_root(&self) -> &Path {
        &self.container_root
    }

    pub(crate) fn chunking_policy(&self) -> ChunkingPolicy {
        self.chunking_policy
    }
}
