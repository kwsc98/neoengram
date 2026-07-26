//! Runtime-neutral execution contracts shared by standalone and managed NeoEngram runtimes.
//!
//! This crate deliberately has no command-line, process-environment, or physical-path discovery.
//! Callers provide every external capability through an explicit port.

mod add;
mod canonical;
mod commit;
mod error;
mod mutation;
mod ports;
mod prepare_add;
mod progress;

pub use add::{
    AddStatistics, ChunkingSelection, ManagedResource, PrepareAddRequest, PreparedAdd,
    PREPARED_ADD_HASH_DOMAIN,
};
pub use commit::{
    build_commit_graph, publish_commit, BuildCommitGraphRequest, BuildCommitGraphResult,
    CommitPublisher, DirectoryObject, PublishCommitRequest, PublishCommitResult,
};
pub use error::{EngineError, EngineResult, ErrorCode, RetryClass};
pub use mutation::{
    execute_mutation, finalize_mutation, ExecuteMutationRequest, ExecuteMutationResult,
    FinalizeMutationRequest, FinalizeMutationResult, JournalId, JournalPhase, JournalRecord,
    MutationAction, MutationApplication, MutationCheckpoint, MutationIdentity, MutationPlan,
    WorktreeReceipt, WorktreeReceiptStatus, MUTATION_PLAN_HASH_DOMAIN,
};
pub use neoengram_core::IndexMutation;
pub use ports::{
    Clock, DirectoryMetadata, FailureInjector, FailurePoint, FileFingerprint,
    ImmutableCatalogReader, IndexEntry, IndexSnapshotReader, IndexVersion, JournalStore,
    LockManager, ManifestMetadata, MutationGuard, NoopFailureInjector, ObjectMetadata, ObjectPage,
    ObjectPutOutcome, ObjectSpec, ObjectStore, Page, PageCursor, PageRequest, PathScope, Worktree,
    WorktreeEntry, WorktreeEntryKind, WorktreeScanRequest, MAX_PAGE_SIZE,
};
pub use prepare_add::prepare_add;
pub use progress::{NoopProgressSink, ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit};
