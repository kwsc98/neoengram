//! Workspace index and directory models used by the standalone metadata and worktree adapters.
//!
//! The reusable domain API exposes paged `FileRecord`/`IndexDelta` values and hierarchical
//! directories. Local filesystem boundaries consume directory file records directly.

use serde::{Deserialize, Serialize};

pub(crate) use neoengram_domain::core::{ChunkingStrategy, WORKSPACE_INDEX_FORMAT_VERSION};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Chunk {
    pub(crate) hash: String,
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspaceFileRecord {
    pub(crate) path: String,
    pub(crate) total_size: u64,
    pub(crate) chunking: ChunkingStrategy,
    pub(crate) chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspaceIndex {
    pub(crate) format_version: u32,
    pub(crate) files: Vec<WorkspaceFileRecord>,
}

impl Default for WorkspaceIndex {
    fn default() -> Self {
        Self {
            format_version: WORKSPACE_INDEX_FORMAT_VERSION,
            files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DirectoryEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DirectoryEntry {
    pub(crate) ordinal: u64,
    pub(crate) name: String,
    pub(crate) kind: DirectoryEntryKind,
    pub(crate) target_id: String,
    pub(crate) total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Commit {
    pub(crate) root_directory_id: String,
    pub(crate) parent: Option<String>,
    pub(crate) message: String,
    pub(crate) created_at_unix_ms: u64,
}
