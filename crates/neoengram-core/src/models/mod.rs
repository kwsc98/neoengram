//! Serializable NeoEngram domain models and repository format constants.

mod chunk;
mod commit;
mod directory;
mod file;
mod format;
mod index;

pub use chunk::{ChunkRef, ObjectSpec};
pub use commit::Commit;
pub use directory::{Directory, DirectoryEntry, DirectoryEntryKind, DirectoryEntryTarget};
pub use file::{ChunkingStrategy, FileRecord, Manifest};
pub use format::{INDEX_FORMAT_VERSION, MAX_INDEX_MUTATIONS_PER_PAGE};
pub use index::{
    validate_index_mutation_paths, validate_index_snapshot, IndexDelta, IndexDeltaPage,
    IndexMutation, IndexVersion,
};
