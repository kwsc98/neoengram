use serde::{Deserialize, Serialize};

/// The kind of an immutable direct directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectoryEntryKind {
    File,
    Directory,
}

/// A direct child of an immutable content-addressed directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    /// Zero-based position in the directory's canonical name ordering.
    pub ordinal: u64,
    /// A single normalized UTF-8 path component.
    pub name: String,
    /// Whether the target is a file Manifest or another Directory.
    pub kind: DirectoryEntryKind,
    /// Manifest ID for files and Directory ID for directories.
    pub target_id: String,
    /// Logical file size. Directories always store zero here.
    pub total_size: u64,
}
