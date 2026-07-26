//! Environment-independent domain models and canonical encoders for NeoEngram.
//!
//! This crate owns portable logical paths, strongly typed content identities, immutable metadata
//! models, paged Index deltas, and their canonical BLAKE3 encodings. It deliberately contains no
//! filesystem, database, network, process, or terminal integration.

pub mod canonical;
mod error;
mod ids;
pub mod models;
mod path;

pub use error::{ValidationError, ValidationErrorKind, ValidationResult};
pub use ids::{CommitId, ContentDigest, DirectoryId, ManifestId, ObjectId};
pub use models::{
    validate_index_mutation_paths, validate_index_snapshot, ChunkRef, ChunkingStrategy, Commit,
    Directory, DirectoryEntry, DirectoryEntryKind, DirectoryEntryTarget, FileRecord, IndexDelta,
    IndexDeltaPage, IndexMutation, IndexVersion, Manifest, ObjectSpec, INDEX_FORMAT_VERSION,
    MAX_INDEX_MUTATIONS_PER_PAGE,
};
pub use path::{validate_path_set, LogicalPath, PathComponent};
