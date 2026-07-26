#![allow(dead_code)]

use serde::{Deserialize, Serialize};

pub(crate) const INDEX_FORMAT_VERSION: u32 = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Chunk {
    pub(crate) hash: String,
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChunkingStrategy {
    #[default]
    FastCdc,
    WholeFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileNode {
    pub(crate) path: String,
    pub(crate) total_size: u64,
    pub(crate) chunking: ChunkingStrategy,
    pub(crate) chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Index {
    pub(crate) format_version: u32,
    pub(crate) files: Vec<FileNode>,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            format_version: INDEX_FORMAT_VERSION,
            files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Tree {
    pub(crate) files: Vec<FileNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Commit {
    pub(crate) root_directory_id: String,
    pub(crate) parent: Option<String>,
    pub(crate) message: String,
    pub(crate) created_at_unix_ms: u64,
}
