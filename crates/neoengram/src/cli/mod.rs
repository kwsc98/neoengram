use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::app;

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
}

#[derive(Debug, Args)]
struct AddArgs {
    /// 要加入本地对象库的文件或目录。
    #[arg(value_name = "PATH", default_value = ".")]
    path: PathBuf,
    /// 让指定范围的 index 与工作区一致，包括暂存已删除路径。
    #[arg(short = 'A', long)]
    all: bool,
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

pub(crate) async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Init(arguments) => app::init::execute(arguments.path).await,
        Command::Workspace(arguments) => match arguments.command {
            WorkspaceCommand::Create(create) => {
                app::workspace::create(create.name, create.from, create.path, create.branch).await
            }
            WorkspaceCommand::List => app::workspace::list().await,
            WorkspaceCommand::Remove(remove) => {
                app::workspace::remove(remove.name, remove.force).await
            }
        },
        Command::Add(arguments) => {
            if arguments.all {
                app::add::execute_with_options(arguments.path, true).await
            } else {
                app::add::execute(arguments.path).await
            }
        }
        Command::Rm(arguments) => {
            app::rm::execute(arguments.path, arguments.cached, arguments.force).await
        }
        Command::Status => app::status::execute().await,
        Command::Diff(arguments) => {
            app::diff::execute(arguments.staged, arguments.targets, arguments.stat).await
        }
        Command::Commit(arguments) => app::commit::execute(arguments.message).await,
        Command::Log(arguments) => app::log::execute(arguments.max_count).await,
        Command::Show(arguments) => app::show::execute(arguments.target).await,
        Command::Checkout(arguments) => {
            app::checkout::execute(arguments.target, arguments.force).await
        }
        Command::Export(arguments) => {
            app::export::execute(arguments.target, arguments.destination).await
        }
        Command::Recover(arguments) => app::recover::execute(arguments.abort).await,
        Command::Restore(arguments) => {
            app::restore::execute(arguments.paths, arguments.staged, arguments.force).await
        }
        Command::Gc(arguments) => app::gc::execute(arguments.dry_run).await,
        Command::Fsck => app::fsck::execute().await,
        Command::Mount(arguments) => {
            app::mount::execute(
                arguments.target,
                arguments.mountpoint,
                arguments.cache_size_mib,
            )
            .await
        }
        Command::Unmount(arguments) => app::mount::unmount(arguments.mountpoint).await,
    }
}
