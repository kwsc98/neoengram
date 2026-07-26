//! Standalone NeoEngram application composition.
//!
//! This crate owns repository discovery, SQLite metadata, local filesystem transactions, and
//! FUSE integration. The command-line binary is an adapter over the typed command facade below.

mod app;
mod local;

/// Engine-compatible filesystem adapters available to Standalone composition roots.
///
/// Format-v8 commands still contain private compatibility views while they migrate onto these
/// paged ports; callers must not treat those private views as cross-crate contracts.
pub mod engine_adapters {
    pub use neoengram_fs::{
        FileJournalStore, LocalLockManager, LocalWorktree, LooseObjectStore, SystemClock,
        VerifiedRoot,
    };
}

static DIAGNOSTIC_SINK: std::sync::OnceLock<std::sync::Arc<dyn DiagnosticSink>> =
    std::sync::OnceLock::new();
static FAILURE_INJECTOR: std::sync::OnceLock<
    std::sync::Arc<dyn neoengram_engine::FailureInjector>,
> = std::sync::OnceLock::new();

/// Installs the process adapter used for asynchronous diagnostics and debug failure markers.
///
/// A process may install this sink once. Library callers that do not install one remain silent.
pub fn install_diagnostic_sink(sink: std::sync::Arc<dyn DiagnosticSink>) -> bool {
    DIAGNOSTIC_SINK.set(sink).is_ok()
}

/// Installs the execution fault injector used by deterministic debug adapters.
pub fn install_failure_injector(
    injector: std::sync::Arc<dyn neoengram_engine::FailureInjector>,
) -> bool {
    FAILURE_INJECTOR.set(injector).is_ok()
}

pub(crate) fn failure_injected(point: &'static str) -> bool {
    FAILURE_INJECTOR.get().is_some_and(|injector| {
        injector
            .check(neoengram_engine::FailurePoint::Named(point))
            .is_err()
    })
}

pub(crate) fn emit_diagnostic(message: impl Into<String>) {
    if let Some(sink) = DIAGNOSTIC_SINK.get() {
        sink.emit(OutputEvent {
            channel: OutputChannel::Stderr,
            message: message.into(),
        });
    }
}

/// Destination selected by an application result event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputChannel {
    /// Normal command output.
    Stdout,
    /// Non-fatal warnings and diagnostics.
    Stderr,
}

/// One terminal-neutral rendering event returned to an outer adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEvent {
    channel: OutputChannel,
    message: String,
}

impl OutputEvent {
    /// Returns the destination channel.
    #[must_use]
    pub const fn channel(&self) -> OutputChannel {
        self.channel
    }

    /// Returns the event's complete message without a trailing newline.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Observer for asynchronous diagnostics that must be visible before a command returns.
///
/// Execution progress uses [`neoengram_engine::ProgressSink`]. Keeping diagnostics separate
/// prevents Standalone from turning structured engine observations into terminal text.
pub trait DiagnosticSink: Send + Sync {
    /// Receives one terminal-neutral diagnostic event.
    fn emit(&self, event: OutputEvent);
}

/// Diagnostic sink used by library callers that do not need asynchronous messages.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopDiagnosticSink;

impl DiagnosticSink for NoopDiagnosticSink {
    fn emit(&self, _event: OutputEvent) {}
}

/// Classification shared by status and diff file changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileChangeKind {
    Added,
    Modified,
    Deleted,
}

/// One repository-relative path change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathChange {
    pub path: String,
    pub kind: FileChangeKind,
}

/// Current HEAD attachment reported by status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadPosition {
    Branch(String),
    Detached(neoengram_core::CommitId),
}

/// Structured status observation over HEAD, Index, and the worktree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResult {
    pub head: HeadPosition,
    pub staged: Vec<PathChange>,
    pub unstaged: Vec<PathChange>,
    pub untracked: Vec<String>,
}

impl StatusResult {
    /// Returns true when all three change sets are empty.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.staged.is_empty() && self.unstaged.is_empty() && self.untracked.is_empty()
    }
}

/// One side of a diff comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffEndpoint {
    Head,
    Index,
    Worktree,
    Commit {
        target: String,
        commit_id: neoengram_core::CommitId,
    },
}

/// Compact logical size and chunk count for one file version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    pub bytes: u64,
    pub chunks: u64,
}

/// Chunk reuse accounting for one changed path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkDelta {
    pub reused: u64,
    pub added: u64,
    pub added_bytes: u64,
    pub removed: u64,
}

/// One changed file in a structured diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub path: String,
    pub kind: FileChangeKind,
    pub before: Option<FileStats>,
    pub after: Option<FileStats>,
    pub chunks: ChunkDelta,
}

/// Aggregate counts for a complete diff.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiffSummary {
    pub added_files: u64,
    pub modified_files: u64,
    pub deleted_files: u64,
    pub added_bytes: u64,
    pub removed_bytes: u64,
    pub reused_chunks: u64,
    pub added_chunks: u64,
    pub removed_chunks: u64,
    pub estimated_added_bytes: u64,
}

/// Complete structured diff result. Rendering options remain outside the use case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffResult {
    pub left: DiffEndpoint,
    pub right: DiffEndpoint,
    pub entries: Vec<DiffEntry>,
    pub summary: DiffSummary,
}

/// A canonical Commit paired with its independently derived identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub id: neoengram_core::CommitId,
    pub commit: neoengram_core::Commit,
}

/// Ordered newest-first Commit history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogResult {
    pub entries: Vec<CommitRecord>,
}

/// File metadata exposed by show without leaking Standalone's materialized Tree view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowFileRecord {
    pub path: String,
    pub total_size: u64,
    pub chunk_count: u64,
    pub chunking: neoengram_core::ChunkingStrategy,
}

/// One immutable Commit and its flattened file summary for inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowResult {
    pub record: CommitRecord,
    pub files: Vec<ShowFileRecord>,
}

/// Successful repository integrity scan counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsckResult {
    pub refs: u64,
    pub commits: u64,
    pub directories: u64,
    pub objects: u64,
}

/// One file processed and staged by an Add operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedFile {
    /// Portable repository-relative path.
    pub path: neoengram_core::LogicalPath,
    /// Exact file size observed while chunking.
    pub total_size: u64,
    /// Number of chunks referenced by the staged Manifest.
    pub chunk_count: u64,
    /// Chunking strategy used for this file.
    pub chunking: neoengram_core::ChunkingStrategy,
}

/// Result returned after a Standalone Add has either published its Index CAS or made no changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddResult {
    /// Files processed during this invocation, in completion order.
    pub files: Vec<AddedFile>,
    /// Tracked paths removed from the selected `--all` scope.
    pub removed_files: u64,
    /// Current Index version observed after the operation.
    pub index_version: neoengram_core::IndexVersion,
    /// Whether this invocation published a new Index version.
    pub index_published: bool,
}

/// Result returned after a Commit graph and its authoritative HEAD/ref update are published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitResult {
    /// Canonical identifier of the published Commit.
    pub commit_id: neoengram_core::CommitId,
    /// Canonical identifier of the Commit's root Directory.
    pub root_directory_id: neoengram_core::DirectoryId,
    /// Number of files reachable from the root Directory.
    pub file_count: u64,
    /// Whether publication advanced a detached HEAD rather than a branch.
    pub detached: bool,
}

/// Result of one local mark-and-sweep object collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcResult {
    /// Whether the sweep was observational and left every object intact.
    pub dry_run: bool,
    /// Number of unreachable objects found by the mark phase.
    pub unreferenced_objects: u64,
    /// Total bytes occupied by unreachable objects.
    pub reclaimable_bytes: u64,
    /// Number of objects actually removed by the sweep.
    pub removed_objects: u64,
    /// Total bytes actually reclaimed by the sweep.
    pub reclaimed_bytes: u64,
}

/// HEAD position selected by a successful checkout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutHead {
    /// HEAD remains attached to the named local branch.
    Branch(String),
    /// HEAD points directly at an immutable Commit.
    Detached(neoengram_core::CommitId),
}

/// Result of atomically replacing the workspace Index, worktree, and HEAD position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutResult {
    /// Commit materialized by the checkout.
    pub commit_id: neoengram_core::CommitId,
    /// Number of files reachable from the checked-out Commit.
    pub file_count: u64,
    /// Number of tracked worktree files removed during replacement.
    pub removed_count: u64,
    /// Published HEAD position.
    pub head: CheckoutHead,
}

/// Result of removing tracked paths from the standalone Index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveResult {
    /// Canonical paths removed from the Index, in stable operation order.
    pub removed_paths: Vec<neoengram_core::LogicalPath>,
    /// Whether the worktree was deliberately left untouched.
    pub cached: bool,
}

/// Authority layer updated by a restore operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreTarget {
    /// Restore the selected Index entries from HEAD.
    Index,
    /// Restore selected worktree files from the Index.
    Worktree,
}

/// Result of restoring selected paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreResult {
    /// Canonical paths restored by the operation.
    pub paths: Vec<neoengram_core::LogicalPath>,
    /// Layer that received the restored state.
    pub target: RestoreTarget,
}

/// Final disposition of an interrupted checkout transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutRecoveryStatus {
    /// Checkout was replayed to its intended Commit.
    Recovered,
    /// Checkout was rolled back to its pre-transaction state.
    Aborted,
}

/// Final disposition shared by interrupted remove and restore transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationRecoveryStatus {
    /// The already-applied operation was durably completed.
    Completed,
    /// The operation was rolled back to its pre-transaction state.
    RolledBack,
}

/// Operation-specific result of scanning and resolving one interrupted transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// The repository had no unfinished transaction.
    Nothing,
    /// A checkout transaction was replayed or explicitly aborted.
    Checkout {
        /// Intended immutable checkout target.
        target: neoengram_core::CommitId,
        /// Durable recovery disposition.
        status: CheckoutRecoveryStatus,
    },
    /// A tracked-path removal transaction was completed or rolled back.
    Removal {
        /// Opaque local transaction identifier.
        transaction_id: String,
        /// Durable recovery disposition.
        status: MutationRecoveryStatus,
    },
    /// A worktree restore transaction was completed or rolled back.
    Restore {
        /// Opaque local transaction identifier.
        transaction_id: String,
        /// Durable recovery disposition.
        status: MutationRecoveryStatus,
    },
}

/// Result of resolving at most one interrupted standalone mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverResult {
    /// Durable recovery result.
    pub outcome: RecoveryOutcome,
}

/// Result of initializing or reopening a standalone repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitResult {
    /// Canonical path of the repository administration directory.
    pub repository_dir: std::path::PathBuf,
    /// Whether a format-v8 repository already existed at the requested location.
    pub reinitialized: bool,
}

/// Materialization method for a read-only export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportMode {
    /// Copy verified bytes into the export.
    Copy,
    /// Link verified whole-file objects without replacement.
    HardLink,
}

impl From<ExportMode> for crate::app::export::ExportMode {
    fn from(value: ExportMode) -> Self {
        match value {
            ExportMode::Copy => Self::Copy,
            ExportMode::HardLink => Self::HardLink,
        }
    }
}

/// Result of creating a standalone workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCreateResult {
    /// Registered workspace name.
    pub name: String,
    /// Absolute workspace root.
    pub root: std::path::PathBuf,
    /// Immutable Commit used to initialize the workspace.
    pub commit_id: neoengram_core::CommitId,
    /// Initial branch attachment or detached Commit position.
    pub head: HeadPosition,
}

/// One registered standalone workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSummary {
    /// Registered workspace name.
    pub name: String,
    /// Absolute workspace root.
    pub root: std::path::PathBuf,
    /// Current branch attachment or detached Commit position.
    pub head: HeadPosition,
    /// Commit on which the workspace Index is based, when one exists.
    pub base_commit_id: Option<neoengram_core::CommitId>,
    /// Whether this is the workspace from which the command was invoked.
    pub current: bool,
}

/// Result of listing all workspaces registered with a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceListResult {
    /// Workspaces in the metadata store's stable listing order.
    pub workspaces: Vec<WorkspaceSummary>,
}

/// Result of removing a standalone workspace registration and worktree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRemoveResult {
    /// Removed workspace name.
    pub name: String,
    /// Former absolute workspace root.
    pub root: std::path::PathBuf,
}

/// Result of publishing an immutable read-only snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportResult {
    /// Commit materialized into the snapshot.
    pub commit_id: neoengram_core::CommitId,
    /// Absolute destination published atomically by the export.
    pub destination: std::path::PathBuf,
    /// Number of files reachable in the exported Commit.
    pub file_count: u64,
    /// Materialization method used for the export.
    pub mode: ExportMode,
}

/// Result returned when a foreground read-only mount session finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountResult {
    /// Commit fixed for the lifetime of the mount.
    pub commit_id: neoengram_core::CommitId,
    /// Canonical mountpoint used by the FUSE session.
    pub mountpoint: std::path::PathBuf,
    /// Number of files reachable at the mounted root.
    pub file_count: u64,
    /// Total logical bytes reachable at the mounted root.
    pub total_size_bytes: u64,
    /// Verified Chunk cache capacity in bytes.
    pub cache_size_bytes: u64,
}

/// Result of unmounting a NeoEngram filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnmountResult {
    /// Absolute mountpoint passed to the platform unmount adapter.
    pub mountpoint: std::path::PathBuf,
}

/// Typed entry points used by the command-line adapter.
pub mod commands {
    use std::path::PathBuf;

    use anyhow::Result;
    use neoengram_core::ChunkingStrategy;

    pub use crate::{
        AddResult, AddedFile, CheckoutHead, CheckoutRecoveryStatus, CheckoutResult, ChunkDelta,
        CommitRecord, CommitResult, DiagnosticSink, DiffEndpoint, DiffEntry, DiffResult,
        DiffSummary, ExportMode, ExportResult, FileChangeKind, FileStats, FsckResult, GcResult,
        HeadPosition, InitResult, LogResult, MountResult, MutationRecoveryStatus,
        NoopDiagnosticSink, OutputChannel, OutputEvent, PathChange, RecoverResult, RecoveryOutcome,
        RemoveResult, RestoreResult, RestoreTarget, ShowFileRecord, ShowResult, StatusResult,
        UnmountResult, WorkspaceCreateResult, WorkspaceListResult, WorkspaceRemoveResult,
        WorkspaceSummary,
    };
    pub use neoengram_engine::{NoopProgressSink, ProgressEvent, ProgressSink};

    /// Repository-wide chunking policy selected at initialization.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RepositoryChunking {
        /// Every file uses FastCDC.
        FastCdc,
        /// Every non-empty file is a single object.
        WholeFile,
        /// Each file can select either strategy.
        Mixed,
    }

    impl From<RepositoryChunking> for crate::local::repository::ChunkingPolicy {
        fn from(value: RepositoryChunking) -> Self {
            match value {
                RepositoryChunking::FastCdc => Self::FastCdc,
                RepositoryChunking::WholeFile => Self::WholeFile,
                RepositoryChunking::Mixed => Self::Mixed,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct InitRequest {
        pub cwd: PathBuf,
        pub path: PathBuf,
        pub chunking: Option<RepositoryChunking>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WorkspaceCreateRequest {
        pub cwd: PathBuf,
        pub name: String,
        pub from: String,
        pub path: Option<PathBuf>,
        pub branch: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WorkspaceListRequest {
        pub cwd: PathBuf,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WorkspaceRemoveRequest {
        pub cwd: PathBuf,
        pub name: String,
        pub force: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct AddRequest {
        pub cwd: PathBuf,
        pub path: PathBuf,
        pub all: bool,
        pub chunking: Option<ChunkingStrategy>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RemoveRequest {
        pub cwd: PathBuf,
        pub path: PathBuf,
        pub cached: bool,
        pub force: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct StatusRequest {
        pub cwd: PathBuf,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DiffRequest {
        pub cwd: PathBuf,
        pub staged: bool,
        pub targets: Vec<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CommitRequest {
        pub cwd: PathBuf,
        pub message: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LogRequest {
        pub cwd: PathBuf,
        pub max_count: Option<usize>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ShowRequest {
        pub cwd: PathBuf,
        pub target: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CheckoutRequest {
        pub cwd: PathBuf,
        pub target: String,
        pub force: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ExportRequest {
        pub cwd: PathBuf,
        pub target: String,
        pub destination: PathBuf,
        pub mode: ExportMode,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RecoverRequest {
        pub cwd: PathBuf,
        pub abort: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RestoreRequest {
        pub cwd: PathBuf,
        pub paths: Vec<PathBuf>,
        pub staged: bool,
        pub force: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct GcRequest {
        pub cwd: PathBuf,
        pub dry_run: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct FsckRequest {
        pub cwd: PathBuf,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct MountRequest {
        pub cwd: PathBuf,
        pub target: String,
        pub mountpoint: PathBuf,
        pub cache_size_mib: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct UnmountRequest {
        pub cwd: PathBuf,
        pub mountpoint: PathBuf,
    }

    /// Initializes a standalone repository.
    pub async fn init(request: InitRequest) -> Result<InitResult> {
        crate::app::init::execute(request.cwd, request.path, request.chunking.map(Into::into)).await
    }

    /// Creates a standalone workspace.
    pub async fn workspace_create(
        request: WorkspaceCreateRequest,
    ) -> Result<WorkspaceCreateResult> {
        crate::app::workspace::create(
            request.cwd,
            request.name,
            request.from,
            request.path,
            request.branch,
        )
        .await
    }

    /// Lists standalone workspaces.
    pub async fn workspace_list(request: WorkspaceListRequest) -> Result<WorkspaceListResult> {
        crate::app::workspace::list(request.cwd).await
    }

    /// Removes a standalone workspace.
    pub async fn workspace_remove(
        request: WorkspaceRemoveRequest,
    ) -> Result<WorkspaceRemoveResult> {
        crate::app::workspace::remove(request.cwd, request.name, request.force).await
    }

    /// Stages files and optional deletions.
    pub async fn add(
        request: AddRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<AddResult> {
        if request.all {
            crate::app::add::execute_with_options(
                request.cwd,
                request.path,
                true,
                request.chunking,
                progress,
            )
            .await
        } else {
            crate::app::add::execute(request.cwd, request.path, request.chunking, progress).await
        }
    }

    /// Removes a tracked path.
    pub async fn remove(
        request: RemoveRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<RemoveResult> {
        crate::app::rm::execute(
            request.cwd,
            request.path,
            request.cached,
            request.force,
            progress,
        )
        .await
    }

    /// Computes standalone status.
    pub async fn status(request: StatusRequest) -> Result<StatusResult> {
        crate::app::status::execute(request.cwd).await
    }

    /// Computes a standalone diff.
    pub async fn diff(request: DiffRequest) -> Result<DiffResult> {
        crate::app::diff::execute(request.cwd, request.staged, request.targets).await
    }

    /// Publishes a standalone commit.
    pub async fn commit(
        request: CommitRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<CommitResult> {
        crate::app::commit::execute(request.cwd, request.message, progress).await
    }

    /// Lists standalone history.
    pub async fn log(request: LogRequest) -> Result<LogResult> {
        crate::app::log::execute(request.cwd, request.max_count).await
    }

    /// Shows a fixed commit.
    pub async fn show(request: ShowRequest) -> Result<ShowResult> {
        crate::app::show::execute(request.cwd, request.target).await
    }

    /// Checks out a branch or fixed commit.
    pub async fn checkout(
        request: CheckoutRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<CheckoutResult> {
        crate::app::checkout::execute(request.cwd, request.target, request.force, progress).await
    }

    /// Materializes a read-only export.
    pub async fn export(request: ExportRequest) -> Result<ExportResult> {
        crate::app::export::execute(
            request.cwd,
            request.target,
            request.destination,
            request.mode.into(),
        )
        .await
    }

    /// Recovers or aborts an interrupted transaction.
    pub async fn recover(
        request: RecoverRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<RecoverResult> {
        crate::app::recover::execute(request.cwd, request.abort, progress).await
    }

    /// Restores index or worktree state.
    pub async fn restore(
        request: RestoreRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<RestoreResult> {
        crate::app::restore::execute(
            request.cwd,
            request.paths,
            request.staged,
            request.force,
            progress,
        )
        .await
    }

    /// Collects unreachable local objects.
    pub async fn gc(request: GcRequest) -> Result<GcResult> {
        crate::app::gc::execute(request.cwd, request.dry_run).await
    }

    /// Verifies a standalone repository.
    pub async fn fsck(request: FsckRequest) -> Result<FsckResult> {
        crate::app::fsck::execute(request.cwd).await
    }

    /// Mounts a fixed commit read-only.
    pub async fn mount(
        request: MountRequest,
        progress: std::sync::Arc<dyn ProgressSink>,
    ) -> Result<MountResult> {
        crate::app::mount::execute(
            request.cwd,
            request.target,
            request.mountpoint,
            request.cache_size_mib,
            progress,
        )
        .await
    }

    /// Unmounts a NeoEngram filesystem.
    pub async fn unmount(request: UnmountRequest) -> Result<UnmountResult> {
        crate::app::mount::unmount(request.cwd, request.mountpoint).await
    }
}

#[cfg(test)]
mod progress_tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    use neoengram_engine::{
        EngineError, EngineResult, ErrorCode, ProgressEvent, ProgressPhase, ProgressSink,
    };

    use crate::commands::{
        self, AddRequest, CheckoutRequest, CommitRequest, DiffRequest, FsckRequest, GcRequest,
        InitRequest, LogRequest, NoopProgressSink, RecoverRequest, RemoveRequest, RestoreRequest,
        ShowRequest, StatusRequest,
    };
    use crate::{FileChangeKind, MutationRecoveryStatus, RecoveryOutcome};

    #[derive(Debug, Default)]
    struct RecordingProgress {
        events: Mutex<Vec<ProgressEvent>>,
    }

    impl ProgressSink for RecordingProgress {
        fn emit(&self, event: &ProgressEvent) -> EngineResult<()> {
            self.events
                .lock()
                .map_err(|_| EngineError::new(ErrorCode::Internal, "progress lock poisoned"))?
                .push(event.clone());
            Ok(())
        }
    }

    impl RecordingProgress {
        fn phases(&self) -> anyhow::Result<Vec<ProgressPhase>> {
            Ok(self
                .events
                .lock()
                .map_err(|_| anyhow::anyhow!("progress lock poisoned"))?
                .iter()
                .map(|event| event.phase)
                .collect())
        }

        fn clear(&self) -> anyhow::Result<()> {
            self.events
                .lock()
                .map_err(|_| anyhow::anyhow!("progress lock poisoned"))?
                .clear();
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FailOnceOnCompletedProgress {
        failed: AtomicBool,
    }

    impl ProgressSink for FailOnceOnCompletedProgress {
        fn emit(&self, event: &ProgressEvent) -> EngineResult<()> {
            if event.phase == ProgressPhase::Completed && !self.failed.swap(true, Ordering::SeqCst)
            {
                return Err(EngineError::new(
                    ErrorCode::InjectedFailure,
                    "test progress failure after mutation application",
                ));
            }
            Ok(())
        }
    }

    fn assert_mutation_progress(progress: &RecordingProgress) -> anyhow::Result<()> {
        assert_eq!(
            progress.phases()?,
            vec![
                ProgressPhase::Journaling,
                ProgressPhase::ApplyingMutation,
                ProgressPhase::Completed,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn add_and_commit_forward_structured_engine_progress() -> anyhow::Result<()> {
        let temporary = tempfile::tempdir()?;
        commands::init(InitRequest {
            cwd: temporary.path().to_path_buf(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        std::fs::write(temporary.path().join("model.bin"), b"payload")?;
        let progress = Arc::new(RecordingProgress::default());

        let added = commands::add(
            AddRequest {
                cwd: temporary.path().to_path_buf(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            progress.clone(),
        )
        .await?;
        assert_eq!(added.files.len(), 1);
        assert_eq!(added.files[0].path.as_str(), "model.bin");
        assert_eq!(added.files[0].total_size, 7);
        assert_eq!(added.files[0].chunk_count, 1);
        assert_eq!(added.removed_files, 0);
        assert!(added.index_published);

        let committed = commands::commit(
            CommitRequest {
                cwd: temporary.path().to_path_buf(),
                message: "snapshot".to_owned(),
            },
            progress.clone(),
        )
        .await?;
        assert_eq!(committed.file_count, 1);
        assert!(!committed.detached);

        let phases = progress
            .events
            .lock()
            .map_err(|_| anyhow::anyhow!("progress lock poisoned"))?
            .iter()
            .map(|event| event.phase)
            .collect::<Vec<_>>();
        assert!(phases.contains(&ProgressPhase::Scanning));
        assert!(phases.contains(&ProgressPhase::Chunking));
        assert!(phases.contains(&ProgressPhase::BuildingCandidate));
        assert!(phases.contains(&ProgressPhase::Prepared));
        assert_eq!(phases.last(), Some(&ProgressPhase::Completed));
        Ok(())
    }

    #[tokio::test]
    async fn worktree_mutations_forward_structured_engine_progress() -> anyhow::Result<()> {
        let temporary = tempfile::tempdir()?;
        let cwd = std::fs::canonicalize(temporary.path())?;
        commands::init(InitRequest {
            cwd: cwd.clone(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        std::fs::write(temporary.path().join("model.bin"), b"version one")?;
        commands::add(
            AddRequest {
                cwd: cwd.clone(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            Arc::new(NoopProgressSink),
        )
        .await?;
        let first = commands::commit(
            CommitRequest {
                cwd: cwd.clone(),
                message: "first".to_owned(),
            },
            Arc::new(NoopProgressSink),
        )
        .await?;

        std::fs::write(temporary.path().join("model.bin"), b"version two")?;
        commands::add(
            AddRequest {
                cwd: cwd.clone(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            Arc::new(NoopProgressSink),
        )
        .await?;
        commands::commit(
            CommitRequest {
                cwd: cwd.clone(),
                message: "second".to_owned(),
            },
            Arc::new(NoopProgressSink),
        )
        .await?;

        let progress = Arc::new(RecordingProgress::default());
        commands::checkout(
            CheckoutRequest {
                cwd: cwd.clone(),
                target: first.commit_id.to_string(),
                force: true,
            },
            progress.clone(),
        )
        .await?;
        assert_eq!(
            std::fs::read(temporary.path().join("model.bin"))?,
            b"version one"
        );
        assert_mutation_progress(progress.as_ref())?;

        progress.clear()?;
        std::fs::write(temporary.path().join("model.bin"), b"local edit")?;
        commands::restore(
            RestoreRequest {
                cwd: cwd.clone(),
                paths: vec!["model.bin".into()],
                staged: false,
                force: true,
            },
            progress.clone(),
        )
        .await?;
        assert_eq!(
            std::fs::read(temporary.path().join("model.bin"))?,
            b"version one"
        );
        assert_mutation_progress(progress.as_ref())?;

        progress.clear()?;
        commands::remove(
            RemoveRequest {
                cwd,
                path: "model.bin".into(),
                cached: false,
                force: true,
            },
            progress.clone(),
        )
        .await?;
        assert!(!temporary.path().join("model.bin").exists());
        assert_mutation_progress(progress.as_ref())?;
        Ok(())
    }

    #[tokio::test]
    async fn recover_forwards_structured_engine_progress() -> anyhow::Result<()> {
        let temporary = tempfile::tempdir()?;
        let cwd = std::fs::canonicalize(temporary.path())?;
        commands::init(InitRequest {
            cwd: cwd.clone(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        std::fs::write(temporary.path().join("model.bin"), b"published")?;
        commands::add(
            AddRequest {
                cwd: cwd.clone(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            Arc::new(NoopProgressSink),
        )
        .await?;
        commands::commit(
            CommitRequest {
                cwd: cwd.clone(),
                message: "snapshot".to_owned(),
            },
            Arc::new(NoopProgressSink),
        )
        .await?;
        std::fs::write(temporary.path().join("model.bin"), b"local edit")?;

        let interrupted = commands::restore(
            RestoreRequest {
                cwd: cwd.clone(),
                paths: vec!["model.bin".into()],
                staged: false,
                force: true,
            },
            Arc::new(FailOnceOnCompletedProgress::default()),
        )
        .await;
        let error = interrupted.expect_err("progress failure must interrupt restore finalization");
        assert!(format!("{error:#}").contains("test progress failure"));
        assert_eq!(
            std::fs::read(temporary.path().join("model.bin"))?,
            b"published"
        );

        let progress = Arc::new(RecordingProgress::default());
        let recovered =
            commands::recover(RecoverRequest { cwd, abort: false }, progress.clone()).await?;
        assert!(matches!(
            recovered.outcome,
            RecoveryOutcome::Restore {
                status: MutationRecoveryStatus::Completed,
                ..
            }
        ));
        assert_mutation_progress(progress.as_ref())?;
        Ok(())
    }

    #[tokio::test]
    async fn readonly_commands_return_domain_results() -> anyhow::Result<()> {
        let temporary = tempfile::tempdir()?;
        let cwd = temporary.path().to_path_buf();
        commands::init(InitRequest {
            cwd: cwd.clone(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        std::fs::write(temporary.path().join("model.bin"), b"payload")?;
        commands::add(
            AddRequest {
                cwd: cwd.clone(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            Arc::new(NoopProgressSink),
        )
        .await?;

        let staged = commands::status(StatusRequest { cwd: cwd.clone() }).await?;
        assert_eq!(staged.staged.len(), 1);
        assert_eq!(staged.staged[0].kind, FileChangeKind::Added);
        let committed = commands::commit(
            CommitRequest {
                cwd: cwd.clone(),
                message: "snapshot".to_owned(),
            },
            Arc::new(NoopProgressSink),
        )
        .await?;

        let status = commands::status(StatusRequest { cwd: cwd.clone() }).await?;
        assert!(status.is_clean());
        let diff = commands::diff(DiffRequest {
            cwd: cwd.clone(),
            staged: false,
            targets: Vec::new(),
        })
        .await?;
        assert!(diff.entries.is_empty());
        let log = commands::log(LogRequest {
            cwd: cwd.clone(),
            max_count: Some(1),
        })
        .await?;
        assert_eq!(log.entries.len(), 1);
        assert_eq!(log.entries[0].id, committed.commit_id);
        assert_eq!(
            log.entries[0].commit.root_directory_id,
            committed.root_directory_id
        );
        let show = commands::show(ShowRequest {
            cwd: cwd.clone(),
            target: "HEAD".to_owned(),
        })
        .await?;
        assert_eq!(show.record.id, log.entries[0].id);
        assert_eq!(show.files.len(), 1);
        assert_eq!(show.files[0].path, "model.bin");
        let fsck = commands::fsck(FsckRequest { cwd: cwd.clone() }).await?;
        assert_eq!(fsck.commits, 1);
        assert_eq!(fsck.refs, 1);
        assert_eq!(fsck.objects, 1);
        let gc = commands::gc(GcRequest { cwd, dry_run: true }).await?;
        assert!(gc.dry_run);
        assert_eq!(gc.unreferenced_objects, 0);
        assert_eq!(gc.reclaimable_bytes, 0);
        assert_eq!(gc.removed_objects, 0);
        assert_eq!(gc.reclaimed_bytes, 0);
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_result_tests {
    use std::sync::Arc;

    use crate::commands::{
        self, AddRequest, CommitRequest, ExportMode, ExportRequest, InitRequest, NoopProgressSink,
        WorkspaceCreateRequest, WorkspaceListRequest, WorkspaceRemoveRequest,
    };
    use crate::HeadPosition;

    #[tokio::test]
    async fn lifecycle_commands_return_typed_domain_data() -> anyhow::Result<()> {
        let temporary = tempfile::tempdir()?;
        let cwd = std::fs::canonicalize(temporary.path())?;

        let initialized = commands::init(InitRequest {
            cwd: cwd.clone(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        assert!(!initialized.reinitialized);
        assert!(initialized.repository_dir.is_absolute());
        assert!(initialized.repository_dir.is_dir());
        let reopened = commands::init(InitRequest {
            cwd: cwd.clone(),
            path: ".".into(),
            chunking: None,
        })
        .await?;
        assert!(reopened.reinitialized);
        assert_eq!(reopened.repository_dir, initialized.repository_dir);

        std::fs::write(temporary.path().join("model.bin"), b"payload")?;
        commands::add(
            AddRequest {
                cwd: cwd.clone(),
                path: "model.bin".into(),
                all: false,
                chunking: None,
            },
            Arc::new(NoopProgressSink),
        )
        .await?;
        let committed = commands::commit(
            CommitRequest {
                cwd: cwd.clone(),
                message: "lifecycle result".to_owned(),
            },
            Arc::new(NoopProgressSink),
        )
        .await?;

        let created = commands::workspace_create(WorkspaceCreateRequest {
            cwd: cwd.clone(),
            name: "experiment".to_owned(),
            from: "HEAD".to_owned(),
            path: None,
            branch: None,
        })
        .await?;
        assert_eq!(created.commit_id, committed.commit_id);
        assert_eq!(created.head, HeadPosition::Detached(committed.commit_id));
        assert!(created.root.is_absolute());
        assert!(created.root.is_dir());

        let listed = commands::workspace_list(WorkspaceListRequest { cwd: cwd.clone() }).await?;
        let experiment = listed
            .workspaces
            .iter()
            .find(|workspace| workspace.name == "experiment")
            .expect("created workspace must be listed");
        assert!(!experiment.current);
        assert_eq!(experiment.base_commit_id, Some(committed.commit_id));
        assert_eq!(experiment.head, HeadPosition::Detached(committed.commit_id));

        let exported = commands::export(ExportRequest {
            cwd: cwd.clone(),
            target: "HEAD".to_owned(),
            destination: "exports/snapshot".into(),
            mode: ExportMode::Copy,
        })
        .await?;
        assert_eq!(exported.commit_id, committed.commit_id);
        assert_eq!(exported.file_count, 1_u64);
        assert_eq!(exported.mode, ExportMode::Copy);
        assert!(exported.destination.is_absolute());
        assert!(exported.destination.is_dir());

        let removed = commands::workspace_remove(WorkspaceRemoveRequest {
            cwd,
            name: "experiment".to_owned(),
            force: false,
        })
        .await?;
        assert_eq!(removed.name, created.name);
        assert_eq!(removed.root, created.root);
        assert!(!removed.root.exists());
        Ok(())
    }
}
