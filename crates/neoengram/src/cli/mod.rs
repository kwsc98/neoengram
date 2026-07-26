use std::{io::IsTerminal, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use neoengram_core::ChunkingStrategy;
use neoengram_engine::{
    EngineError, EngineResult, ErrorCode, FailureInjector, FailurePoint, ProgressEvent,
    ProgressPhase, ProgressSink, ProgressUnit,
};
use neoengram_standalone::commands::{
    self, AddResult, CheckoutHead, CheckoutRecoveryStatus, CheckoutResult, CommitResult,
    DiagnosticSink, DiffEndpoint, DiffEntry, DiffResult, DiffSummary, ExportMode, ExportResult,
    FileChangeKind, FileStats, FsckResult, GcResult, HeadPosition, InitResult, LogResult,
    MountResult, MutationRecoveryStatus, OutputChannel, OutputEvent, PathChange, RecoverResult,
    RecoveryOutcome, RemoveResult, RepositoryChunking, RestoreResult, ShowResult, StatusResult,
    UnmountResult, WorkspaceCreateResult, WorkspaceListResult, WorkspaceRemoveResult,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommandResult {
    events: Vec<RenderEvent>,
}

impl CommandResult {
    fn stdout(message: impl Into<String>) -> Self {
        let mut result = Self::default();
        result.push_stdout(message);
        result
    }

    fn push_stdout(&mut self, message: impl Into<String>) {
        self.events.push(RenderEvent {
            channel: OutputChannel::Stdout,
            message: message.into(),
        });
    }

    fn push_stderr(&mut self, message: impl Into<String>) {
        self.events.push(RenderEvent {
            channel: OutputChannel::Stderr,
            message: message.into(),
        });
    }

    fn events(&self) -> &[RenderEvent] {
        &self.events
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderEvent {
    channel: OutputChannel,
    message: String,
}

impl RenderEvent {
    const fn channel(&self) -> OutputChannel {
        self.channel
    }

    fn message(&self) -> &str {
        &self.message
    }
}

/// 面向 AI 数据集和模型权重的内容寻址版本控制系统。
#[derive(Debug, Parser)]
#[command(name = "neoengram", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// 创建一个新的本地 NeoEngram 仓库。
    Init(InitArgs),
    /// 创建和查看共享同一对象仓库的可写 Workspace。
    Workspace(WorkspaceArgs),
    /// 将文件内容切块并写入本地对象库。
    Add(AddArgs),
    /// 从暂存区移除已跟踪路径；默认同时移除工作区内容。
    Rm(RmArgs),
    /// 显示暂存区、工作区和未跟踪文件的状态。
    Status,
    /// 比较工作区、index 或两个不可变 Commit 的文件与 Chunk 变化。
    Diff(DiffArgs),
    /// 将暂存区固化为一个只有单父节点的不可变提交。
    Commit(CommitArgs),
    /// 按时间倒序显示当前 HEAD 的单父提交历史。
    Log(LogArgs),
    /// 显示一个 Commit 及其完整文件清单。
    Show(ShowArgs),
    /// 切换到分支或 Commit，并恢复对应的工作区和 index。
    Checkout(CheckoutArgs),
    /// 将固定 Commit 物化为独立的只读目录。
    Export(ExportArgs),
    /// 完成或回滚一次被中断的工作区事务。
    Recover(RecoverArgs),
    /// 从 HEAD 恢复 index，或从 index 恢复工作区文件。
    Restore(RestoreArgs),
    /// 删除未被 index 或已保存 Tree 引用的 Chunk 对象。
    Gc(GcArgs),
    /// 验证本地 refs、历史、元数据和 Chunk 对象的完整性。
    Fsck,
    /// 把固定 Commit 作为只读 FUSE 文件系统挂载到空目录。
    Mount(MountArgs),
    /// 卸载 NeoEngram FUSE 文件系统。
    Unmount(UnmountArgs),
}

#[derive(Debug, Args)]
struct WorkspaceArgs {
    #[command(subcommand)]
    command: WorkspaceCommand,
}

#[derive(Debug, Subcommand)]
enum WorkspaceCommand {
    /// 从固定 Commit 创建一个独立 Index 的可写 Workspace。
    Create(WorkspaceCreateArgs),
    /// 列出仓库注册的所有 Workspace。
    List,
    /// 删除一个非默认 Workspace；默认保护未提交内容。
    Remove(WorkspaceRemoveArgs),
}

#[derive(Debug, Args)]
struct WorkspaceCreateArgs {
    #[arg(value_name = "NAME")]
    name: String,
    #[arg(long, value_name = "TARGET", default_value = "HEAD")]
    from: String,
    #[arg(long, value_name = "PATH")]
    path: Option<PathBuf>,
    /// 创建并独占一个新分支；省略时创建 Detached Workspace。
    #[arg(short, long, value_name = "BRANCH")]
    branch: Option<String>,
}

#[derive(Debug, Args)]
struct WorkspaceRemoveArgs {
    #[arg(value_name = "NAME")]
    name: String,
    #[arg(short, long)]
    force: bool,
}

#[derive(Debug, Args)]
struct InitArgs {
    /// 要初始化为 NeoEngram 仓库的目录。
    #[arg(value_name = "PATH", default_value = ".")]
    path: PathBuf,
    /// 固定仓库分块策略；mixed 仓库允许 add 按文件选择策略。
    #[arg(long, value_enum)]
    chunking: Option<RepositoryChunkingArg>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepositoryChunkingArg {
    #[value(name = "fastcdc")]
    FastCdc,
    #[value(name = "whole-file")]
    WholeFile,
    Mixed,
}

impl From<RepositoryChunkingArg> for RepositoryChunking {
    fn from(value: RepositoryChunkingArg) -> Self {
        match value {
            RepositoryChunkingArg::FastCdc => Self::FastCdc,
            RepositoryChunkingArg::WholeFile => Self::WholeFile,
            RepositoryChunkingArg::Mixed => Self::Mixed,
        }
    }
}

#[derive(Debug, Args)]
struct AddArgs {
    /// 要加入本地对象库的文件或目录。
    #[arg(value_name = "PATH", default_value = ".")]
    path: PathBuf,
    /// 让指定范围的 index 与工作区一致，包括暂存已删除路径。
    #[arg(short = 'A', long)]
    all: bool,
    /// mixed 仓库中覆盖目标文件的分块策略；固定策略仓库不接受此参数。
    #[arg(long, value_enum)]
    chunking: Option<ChunkingArg>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ChunkingArg {
    #[value(name = "fastcdc")]
    FastCdc,
    #[value(name = "whole-file")]
    WholeFile,
}

impl From<ChunkingArg> for ChunkingStrategy {
    fn from(value: ChunkingArg) -> Self {
        match value {
            ChunkingArg::FastCdc => Self::FastCdc,
            ChunkingArg::WholeFile => Self::WholeFile,
        }
    }
}

#[derive(Debug, Args)]
struct RmArgs {
    /// 要移除的已跟踪文件或目录范围。
    #[arg(value_name = "PATH")]
    path: PathBuf,
    /// 只从 index 移除，保留工作区内容。
    #[arg(long)]
    cached: bool,
    /// 明确丢弃工作区中未暂存的冲突内容。
    #[arg(short, long)]
    force: bool,
}

#[derive(Debug, Args)]
struct CommitArgs {
    /// 本次提交的说明。
    #[arg(short, long, value_name = "MESSAGE")]
    message: String,
}

#[derive(Debug, Args)]
struct DiffArgs {
    /// 比较 HEAD 与 index；省略时比较 index 与工作区。
    #[arg(long)]
    staged: bool,
    /// 只输出汇总统计。
    #[arg(long)]
    stat: bool,
    /// 一个 TARGET 表示 Commit 与工作区（或 --staged 时与 index）比较；两个 TARGET
    /// 表示 Commit 与 Commit 比较。
    #[arg(value_name = "TARGET", num_args = 0..=2)]
    targets: Vec<String>,
}

#[derive(Debug, Args)]
struct RestoreArgs {
    /// 从 HEAD 恢复 index，不修改工作区。
    #[arg(short = 'S', long)]
    staged: bool,
    /// 覆盖工作区中与 index 不一致的普通文件。
    #[arg(short, long)]
    force: bool,
    /// 要恢复的文件或目录范围。
    #[arg(value_name = "PATH", required = true, num_args = 1..)]
    paths: Vec<PathBuf>,
}

#[derive(Debug, Args)]
struct GcArgs {
    /// 只报告可回收对象，不删除任何内容。
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct LogArgs {
    /// 最多显示多少个 Commit。
    #[arg(short = 'n', long, value_name = "COUNT")]
    max_count: Option<usize>,
}

#[derive(Debug, Args)]
struct ShowArgs {
    /// 完整 Commit ID；省略时使用当前 HEAD。
    #[arg(value_name = "TARGET", default_value = "HEAD")]
    target: String,
}

#[derive(Debug, Args)]
struct CheckoutArgs {
    /// `main`、`HEAD` 或完整 Commit ID；Commit ID 会进入 detached HEAD。
    #[arg(value_name = "TARGET", default_value = "HEAD")]
    target: String,
    /// 丢弃暂存修改、已跟踪修改以及冲突的叶子文件。
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct ExportArgs {
    #[arg(value_name = "TARGET")]
    target: String,
    #[arg(value_name = "DIR")]
    destination: PathBuf,
    /// 导出物化方式；hardlink 仅支持同文件系统中的 WholeFile loose 对象。
    #[arg(long, value_enum, default_value_t = ExportModeArg::Copy)]
    mode: ExportModeArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportModeArg {
    Copy,
    Hardlink,
}

impl From<ExportModeArg> for ExportMode {
    fn from(value: ExportModeArg) -> Self {
        match value {
            ExportModeArg::Copy => Self::Copy,
            ExportModeArg::Hardlink => Self::HardLink,
        }
    }
}

#[derive(Debug, Args)]
struct RecoverArgs {
    /// 对 checkout：回滚到事务开始前，不继续切换。对 rm：仅在 index 已提交时拒绝 abort；
    /// index 尚未提交时无论是否指定此选项都会恢复备份并回到事务开始前。
    #[arg(long)]
    abort: bool,
}

#[derive(Debug, Args)]
struct MountArgs {
    /// `HEAD`、`main` 或完整 Commit ID；挂载后视图不再跟随引用移动。
    #[arg(value_name = "TARGET")]
    target: String,
    /// 已存在、为空且位于仓库之外的挂载目录。
    #[arg(value_name = "MOUNTPOINT")]
    mountpoint: PathBuf,
    /// 已验证 Chunk 的有界内存缓存大小。
    #[arg(long, value_name = "MIB", default_value_t = 512)]
    cache_size_mib: u64,
}

#[derive(Debug, Args)]
struct UnmountArgs {
    #[arg(value_name = "MOUNTPOINT")]
    mountpoint: PathBuf,
}

pub(crate) async fn run() -> Result<CommandResult> {
    let terminal = Arc::new(TerminalSink);
    let _ = neoengram_standalone::install_diagnostic_sink(terminal.clone());
    let _ = neoengram_standalone::install_failure_injector(Arc::new(
        EnvironmentFailureInjector::from_env(),
    ));
    let cwd = std::env::current_dir().context("无法确定当前工作目录")?;
    match Cli::parse().command {
        Command::Init(arguments) => commands::init(commands::InitRequest {
            cwd,
            path: arguments.path,
            chunking: arguments.chunking.map(Into::into),
        })
        .await
        .map(|result| render_init(&result)),
        Command::Workspace(arguments) => match arguments.command {
            WorkspaceCommand::Create(create) => {
                commands::workspace_create(commands::WorkspaceCreateRequest {
                    cwd,
                    name: create.name,
                    from: create.from,
                    path: create.path,
                    branch: create.branch,
                })
                .await
                .map(|result| render_workspace_create(&result))
            }
            WorkspaceCommand::List => {
                commands::workspace_list(commands::WorkspaceListRequest { cwd })
                    .await
                    .map(|result| render_workspace_list(&result))
            }
            WorkspaceCommand::Remove(remove) => {
                commands::workspace_remove(commands::WorkspaceRemoveRequest {
                    cwd,
                    name: remove.name,
                    force: remove.force,
                })
                .await
                .map(|result| render_workspace_remove(&result))
            }
        },
        Command::Add(arguments) => {
            let chunking = arguments.chunking.map(ChunkingStrategy::from);
            commands::add(
                commands::AddRequest {
                    cwd,
                    path: arguments.path,
                    all: arguments.all,
                    chunking,
                },
                terminal.clone(),
            )
            .await
            .map(|result| render_add(&result))
        }
        Command::Rm(arguments) => commands::remove(
            commands::RemoveRequest {
                cwd,
                path: arguments.path,
                cached: arguments.cached,
                force: arguments.force,
            },
            terminal.clone(),
        )
        .await
        .map(|result| render_remove(&result)),
        Command::Status => commands::status(commands::StatusRequest { cwd })
            .await
            .map(|result| render_status(&result)),
        Command::Diff(arguments) => {
            let stat_only = arguments.stat;
            commands::diff(commands::DiffRequest {
                cwd,
                staged: arguments.staged,
                targets: arguments.targets,
            })
            .await
            .map(|result| render_diff(&result, stat_only))
        }
        Command::Commit(arguments) => commands::commit(
            commands::CommitRequest {
                cwd,
                message: arguments.message,
            },
            terminal.clone(),
        )
        .await
        .map(|result| render_commit(&result)),
        Command::Log(arguments) => commands::log(commands::LogRequest {
            cwd,
            max_count: arguments.max_count,
        })
        .await
        .map(|result| render_log(&result)),
        Command::Show(arguments) => commands::show(commands::ShowRequest {
            cwd,
            target: arguments.target,
        })
        .await
        .map(|result| render_show(&result)),
        Command::Checkout(arguments) => commands::checkout(
            commands::CheckoutRequest {
                cwd,
                target: arguments.target,
                force: arguments.force,
            },
            terminal.clone(),
        )
        .await
        .map(|result| render_checkout(&result)),
        Command::Export(arguments) => commands::export(commands::ExportRequest {
            cwd,
            target: arguments.target,
            destination: arguments.destination,
            mode: arguments.mode.into(),
        })
        .await
        .map(|result| render_export(&result)),
        Command::Recover(arguments) => commands::recover(
            commands::RecoverRequest {
                cwd,
                abort: arguments.abort,
            },
            terminal.clone(),
        )
        .await
        .map(|result| render_recover(&result)),
        Command::Restore(arguments) => commands::restore(
            commands::RestoreRequest {
                cwd,
                paths: arguments.paths,
                staged: arguments.staged,
                force: arguments.force,
            },
            terminal.clone(),
        )
        .await
        .map(|result| render_restore(&result)),
        Command::Gc(arguments) => commands::gc(commands::GcRequest {
            cwd,
            dry_run: arguments.dry_run,
        })
        .await
        .map(|result| render_gc(&result)),
        Command::Fsck => commands::fsck(commands::FsckRequest { cwd })
            .await
            .map(|result| render_fsck(&result)),
        Command::Mount(arguments) => commands::mount(
            commands::MountRequest {
                cwd,
                target: arguments.target,
                mountpoint: arguments.mountpoint,
                cache_size_mib: arguments.cache_size_mib,
            },
            terminal,
        )
        .await
        .map(|result| render_mount(&result)),
        Command::Unmount(arguments) => commands::unmount(commands::UnmountRequest {
            cwd,
            mountpoint: arguments.mountpoint,
        })
        .await
        .map(|result| render_unmount(&result)),
    }
}

#[derive(Debug, Clone, Copy)]
struct TerminalSink;

#[derive(Debug)]
struct EnvironmentFailureInjector {
    crash_at: Option<String>,
    pause_at: Option<String>,
    fail_after_rename: Option<String>,
}

impl EnvironmentFailureInjector {
    fn from_env() -> Self {
        Self {
            crash_at: std::env::var("NEOENGRAM_TEST_CRASH_AT").ok(),
            pause_at: std::env::var("NEOENGRAM_TEST_PAUSE_AT").ok(),
            fail_after_rename: std::env::var("NEOENGRAM_TEST_FAIL_AFTER_RENAME").ok(),
        }
    }
}

impl FailureInjector for EnvironmentFailureInjector {
    fn check(&self, point: FailurePoint) -> EngineResult<()> {
        let FailurePoint::Named(name) = point else {
            return Ok(());
        };
        let rename_failure = name == "rename-after-publish-before-sync"
            && self.fail_after_rename.as_deref() == Some("before-sync");
        if self.crash_at.as_deref() == Some(name)
            || self.pause_at.as_deref() == Some(name)
            || rename_failure
        {
            Err(EngineError::new(
                ErrorCode::InjectedFailure,
                format!("debug failure point reached: {name}"),
            ))
        } else {
            Ok(())
        }
    }
}

impl DiagnosticSink for TerminalSink {
    fn emit(&self, event: OutputEvent) {
        render_event(event.channel(), event.message());
    }
}

impl ProgressSink for TerminalSink {
    fn emit(&self, event: &ProgressEvent) -> EngineResult<()> {
        // Progress is an interactive concern. Suppressing it for redirected stderr also keeps
        // command-result streams and the debug failure-marker protocol deterministic.
        if std::io::stderr().is_terminal() {
            eprintln!("{}", format_progress(event));
        }
        Ok(())
    }
}

pub(crate) fn render(result: &CommandResult) {
    for event in result.events() {
        render_event(event.channel(), event.message());
    }
}

fn render_init(init: &InitResult) -> CommandResult {
    let action = if init.reinitialized {
        "Reinitialized existing"
    } else {
        "Initialized empty"
    };
    CommandResult::stdout(format!(
        "{action} NeoEngram repository in {}",
        init.repository_dir.display()
    ))
}

fn render_workspace_create(workspace: &WorkspaceCreateResult) -> CommandResult {
    let position = match &workspace.head {
        HeadPosition::Branch(reference) => reference.clone(),
        HeadPosition::Detached(commit_id) => format!("detached {commit_id}"),
    };
    CommandResult::stdout(format!(
        "Created workspace {} at {} ({position})",
        workspace.name,
        workspace.root.display()
    ))
}

fn render_workspace_list(list: &WorkspaceListResult) -> CommandResult {
    let mut result = CommandResult::default();
    for workspace in &list.workspaces {
        let marker = if workspace.current { "*" } else { " " };
        let position = match &workspace.head {
            HeadPosition::Branch(reference) => reference.clone(),
            HeadPosition::Detached(commit_id) => format!("detached {commit_id}"),
        };
        result.push_stdout(format!(
            "{marker} {}\t{}\t{position}",
            workspace.name,
            workspace.root.display()
        ));
    }
    result
}

fn render_workspace_remove(workspace: &WorkspaceRemoveResult) -> CommandResult {
    CommandResult::stdout(format!("Removed workspace {}", workspace.name))
}

fn render_export(export: &ExportResult) -> CommandResult {
    let mut result = match export.mode {
        ExportMode::Copy => CommandResult::stdout(format!(
            "Exported read-only snapshot of {} ({} files) at {}",
            export.commit_id,
            export.file_count,
            export.destination.display()
        )),
        ExportMode::HardLink => CommandResult::stdout(format!(
            "Exported hard-linked read-only view of {} ({} files) at {}",
            export.commit_id,
            export.file_count,
            export.destination.display()
        )),
    };
    if export.mode == ExportMode::HardLink {
        result.push_stderr(
            "Warning: hard-linked files share content and permissions with repository objects; modifying the view corrupts every snapshot that references those objects.",
        );
    }
    result
}

fn render_mount(_mount: &MountResult) -> CommandResult {
    CommandResult::default()
}

fn render_unmount(unmount: &UnmountResult) -> CommandResult {
    CommandResult::stdout(format!("Unmounted {}", unmount.mountpoint.display()))
}

fn render_status(status: &StatusResult) -> CommandResult {
    let mut result = CommandResult::default();
    match &status.head {
        HeadPosition::Branch(branch) => result.push_stdout(format!("On branch {branch}")),
        HeadPosition::Detached(commit_id) => {
            result.push_stdout(format!("HEAD detached at {commit_id}"));
        }
    }
    if status.is_clean() {
        result.push_stdout("nothing to commit, working tree clean");
        return result;
    }

    render_status_changes(&mut result, "Changes to be committed:", &status.staged);
    render_status_changes(
        &mut result,
        "Changes not staged for commit:",
        &status.unstaged,
    );
    if !status.untracked.is_empty() {
        result.push_stdout("Untracked files:");
        for path in &status.untracked {
            result.push_stdout(format!("  {path}"));
        }
    }
    result
}

fn render_add(add: &AddResult) -> CommandResult {
    let mut result = CommandResult::default();
    for file in &add.files {
        result.push_stdout(format!(
            "Processed: {}, {} chunks created",
            file.path, file.chunk_count
        ));
    }
    result
}

fn render_remove(remove: &RemoveResult) -> CommandResult {
    let mut result = CommandResult::default();
    for path in &remove.removed_paths {
        result.push_stdout(format!("Removed: {path}"));
    }
    result
}

fn render_checkout(checkout: &CheckoutResult) -> CommandResult {
    let mut result = CommandResult::stdout(format!(
        "Checked out {} ({} files, {} removed)",
        checkout.commit_id, checkout.file_count, checkout.removed_count
    ));
    match &checkout.head {
        CheckoutHead::Branch(branch) => {
            result.push_stdout(format!("HEAD is now on branch {branch}"));
        }
        CheckoutHead::Detached(commit_id) => {
            result.push_stdout(format!("HEAD is now detached at {commit_id}"));
        }
    }
    result
}

fn render_restore(restore: &RestoreResult) -> CommandResult {
    let mut result = CommandResult::default();
    for path in &restore.paths {
        result.push_stdout(format!("Restored: {path}"));
    }
    result
}

fn render_recover(recover: &RecoverResult) -> CommandResult {
    let mut result = CommandResult::default();
    match &recover.outcome {
        RecoveryOutcome::Nothing => result.push_stdout("No unfinished transaction"),
        RecoveryOutcome::Checkout { target, status } => match status {
            CheckoutRecoveryStatus::Aborted => {
                result.push_stdout(format!("Aborted checkout transaction for {target}"));
            }
            CheckoutRecoveryStatus::Recovered => {
                result.push_stdout(format!("Recovered checkout transaction to {target}"));
                result.push_stdout(
                    "Note: recovery replayed checkout with --force semantics; \
                     conflicting local changes may have been discarded",
                );
            }
        },
        RecoveryOutcome::Removal {
            transaction_id,
            status,
        } => match status {
            MutationRecoveryStatus::Completed => {
                result.push_stdout(format!("Completed rm transaction {transaction_id}"));
            }
            MutationRecoveryStatus::RolledBack => {
                result.push_stdout(format!("Rolled back rm transaction {transaction_id}"));
                result.push_stdout("Re-run `neoengram rm` if you still want to remove those paths");
            }
        },
        RecoveryOutcome::Restore {
            transaction_id,
            status,
        } => match status {
            MutationRecoveryStatus::Completed => {
                result.push_stdout(format!("Completed restore transaction {transaction_id}"));
            }
            MutationRecoveryStatus::RolledBack => {
                result.push_stdout(format!("Rolled back restore transaction {transaction_id}"));
                result.push_stdout("Re-run `neoengram restore` to restore those paths");
            }
        },
    }
    result
}

fn render_commit(commit: &CommitResult) -> CommandResult {
    let mut result = CommandResult::default();
    result.push_stdout(format!("Committed {}", commit.commit_id));
    result.push_stdout(format!(
        "Directory: {} ({} files)",
        commit.root_directory_id, commit.file_count
    ));
    if commit.detached {
        result.push_stdout(format!("HEAD is now detached at {}", commit.commit_id));
    }
    result
}

fn render_gc(gc: &GcResult) -> CommandResult {
    if gc.dry_run {
        CommandResult::stdout(format!(
            "GC dry-run: {} unreferenced objects, {} bytes reclaimable",
            gc.unreferenced_objects, gc.reclaimable_bytes
        ))
    } else {
        CommandResult::stdout(format!(
            "GC complete: {} objects removed, {} bytes reclaimed",
            gc.removed_objects, gc.reclaimed_bytes
        ))
    }
}

fn render_status_changes(result: &mut CommandResult, title: &str, changes: &[PathChange]) {
    if changes.is_empty() {
        return;
    }
    result.push_stdout(title);
    for change in changes {
        let kind = match change.kind {
            FileChangeKind::Added => "added",
            FileChangeKind::Modified => "modified",
            FileChangeKind::Deleted => "deleted",
        };
        result.push_stdout(format!("  {kind}: {}", change.path));
    }
}

fn render_diff(diff: &DiffResult, stat_only: bool) -> CommandResult {
    let mut result = CommandResult::stdout(format!(
        "Comparing {} -> {}",
        diff_endpoint_label(&diff.left),
        diff_endpoint_label(&diff.right)
    ));
    if !stat_only {
        for entry in &diff.entries {
            result.push_stdout(render_diff_entry(entry));
        }
    }
    if diff.entries.is_empty() {
        result.push_stdout("no differences");
    }
    result.push_stdout(render_diff_summary(&diff.summary));
    result
}

fn diff_endpoint_label(endpoint: &DiffEndpoint) -> &str {
    match endpoint {
        DiffEndpoint::Head => "HEAD",
        DiffEndpoint::Index => "index",
        DiffEndpoint::Worktree => "worktree",
        DiffEndpoint::Commit { target, .. } => target,
    }
}

fn render_diff_entry(entry: &DiffEntry) -> String {
    let kind = match entry.kind {
        FileChangeKind::Added => "added",
        FileChangeKind::Modified => "modified",
        FileChangeKind::Deleted => "deleted",
    };
    format!(
        "{kind}: {} ({} -> {}; chunks reused {}, new {}, removed {}, +{} bytes)",
        entry.path,
        format_file_stats(entry.before),
        format_file_stats(entry.after),
        entry.chunks.reused,
        entry.chunks.added,
        entry.chunks.removed,
        entry.chunks.added_bytes
    )
}

fn format_file_stats(stats: Option<FileStats>) -> String {
    match stats {
        Some(stats) => format!("{} bytes/{} chunks", stats.bytes, stats.chunks),
        None => "<absent>".to_owned(),
    }
}

fn render_diff_summary(summary: &DiffSummary) -> String {
    format!(
        "Summary: {} added, {} modified, {} deleted; {} -> {} bytes; {} reused chunks, {} new chunks, {} removed chunks; estimated new storage {} bytes",
        summary.added_files,
        summary.modified_files,
        summary.deleted_files,
        summary.removed_bytes,
        summary.added_bytes,
        summary.reused_chunks,
        summary.added_chunks,
        summary.removed_chunks,
        summary.estimated_added_bytes
    )
}

fn render_log(log: &LogResult) -> CommandResult {
    let mut result = CommandResult::default();
    for (position, entry) in log.entries.iter().enumerate() {
        if position > 0 {
            result.push_stdout("");
        }
        result.push_stdout(format!("commit {}", entry.id));
        result.push_stdout(format!("Directory: {}", entry.commit.root_directory_id));
        result.push_stdout(format!("Time:   {}", entry.commit.created_at_unix_ms));
        result.push_stdout("");
        result.push_stdout(format!("    {}", entry.commit.message));
    }
    result
}

fn render_show(show: &ShowResult) -> CommandResult {
    let mut result = CommandResult::default();
    result.push_stdout(format!("commit {}", show.record.id));
    result.push_stdout(format!(
        "Directory: {}",
        show.record.commit.root_directory_id
    ));
    let parent = show
        .record
        .commit
        .parent
        .map_or_else(|| "<root>".to_owned(), |parent| parent.to_string());
    result.push_stdout(format!("Parent: {parent}"));
    result.push_stdout(format!("Time:   {}", show.record.commit.created_at_unix_ms));
    result.push_stdout("");
    result.push_stdout(format!("    {}", show.record.commit.message));
    result.push_stdout("");
    result.push_stdout(format!("Files ({}):", show.files.len()));
    for file in &show.files {
        let chunking = match file.chunking {
            ChunkingStrategy::FastCdc => "fastcdc",
            ChunkingStrategy::WholeFile => "whole-file",
        };
        result.push_stdout(format!(
            "{:>12} bytes  {:>6} chunks  {:>10}  {}",
            file.total_size, file.chunk_count, chunking, file.path
        ));
    }
    result
}

fn render_fsck(fsck: &FsckResult) -> CommandResult {
    CommandResult::stdout(format!(
        "Fsck OK: {} refs, {} commits, {} directories, {} objects",
        fsck.refs, fsck.commits, fsck.directories, fsck.objects
    ))
}

fn render_event(channel: OutputChannel, message: &str) {
    match channel {
        OutputChannel::Stdout => println!("{message}"),
        OutputChannel::Stderr => eprintln!("{message}"),
    }
}

fn format_progress(event: &ProgressEvent) -> String {
    let phase = match event.phase {
        ProgressPhase::Preparing => "Preparing",
        ProgressPhase::Scanning => "Scanning",
        ProgressPhase::Snapshotting => "Snapshotting",
        ProgressPhase::Chunking => "Chunking",
        ProgressPhase::PublishingObjects => "Publishing objects",
        ProgressPhase::BuildingCandidate => "Building candidate",
        ProgressPhase::Prepared => "Prepared",
        ProgressPhase::Journaling => "Journaling",
        ProgressPhase::ApplyingMutation => "Applying mutation",
        ProgressPhase::Verifying => "Verifying",
        ProgressPhase::Recovering => "Recovering",
        ProgressPhase::Completed => "Completed",
        _ => "Working",
    };
    let amount = event.total.map_or_else(
        || event.completed.to_string(),
        |total| format!("{}/{total}", event.completed),
    );
    let unit = match (event.unit, event.completed == 1 && event.total.is_none()) {
        (ProgressUnit::Items, true) => "item",
        (ProgressUnit::Items, false) => "items",
        (ProgressUnit::Files, true) => "file",
        (ProgressUnit::Files, false) => "files",
        (ProgressUnit::Objects, true) => "object",
        (ProgressUnit::Objects, false) => "objects",
        (ProgressUnit::Bytes, _) => "bytes",
        (ProgressUnit::Actions, true) => "action",
        (ProgressUnit::Actions, false) => "actions",
        _ => "units",
    };
    event.logical_path.as_ref().map_or_else(
        || format!("{phase}: {amount} {unit}"),
        |path| format!("{phase}: {amount} {unit} ({path})"),
    )
}

#[cfg(test)]
mod tests {
    use super::{format_progress, render_export};
    use neoengram_core::{CommitId, LogicalPath};
    use neoengram_engine::{ProgressEvent, ProgressPhase, ProgressUnit};
    use neoengram_standalone::commands::{ExportMode, ExportResult, OutputChannel};

    #[test]
    fn formats_engine_progress_only_at_the_cli_boundary() {
        let event = ProgressEvent::new(ProgressPhase::Chunking, ProgressUnit::Files, 2)
            .with_total(3)
            .at_path(
                "models/weights.bin"
                    .parse::<LogicalPath>()
                    .expect("valid path"),
            );

        assert_eq!(
            format_progress(&event),
            "Chunking: 2/3 files (models/weights.bin)"
        );
    }

    #[test]
    fn renders_hardlink_warning_only_at_the_cli_boundary() {
        let result = render_export(&ExportResult {
            commit_id: CommitId::from_bytes([7; 32]),
            destination: "/tmp/neoengram-export".into(),
            file_count: 3,
            mode: ExportMode::HardLink,
        });

        assert_eq!(result.events().len(), 2);
        assert_eq!(result.events()[0].channel(), OutputChannel::Stdout);
        assert!(result.events()[0]
            .message()
            .starts_with("Exported hard-linked read-only view"));
        assert_eq!(result.events()[1].channel(), OutputChannel::Stderr);
        assert_eq!(
            result.events()[1].message(),
            "Warning: hard-linked files share content and permissions with repository objects; modifying the view corrupts every snapshot that references those objects."
        );
    }
}
