use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::{app, local::metadata::MetadataStoreKind};

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
    /// 将文件内容切块并写入本地对象库。
    Add(AddArgs),
    /// 从暂存区移除已跟踪路径；默认同时移除工作区内容。
    Rm(RmArgs),
    /// 显示暂存区、工作区和未跟踪文件的状态。
    Status,
    /// 将暂存区固化为一个只有单父节点的不可变提交。
    Commit(CommitArgs),
    /// 按时间倒序显示当前 HEAD 的单父提交历史。
    Log(LogArgs),
    /// 显示一个 Commit 及其完整文件清单。
    Show(ShowArgs),
    /// 切换到分支或 Commit，并恢复对应的工作区和 index。
    Checkout(CheckoutArgs),
    /// 完成或回滚一次被中断的工作区事务。
    Recover(RecoverArgs),
    /// 验证本地 refs、历史、元数据和 Chunk 对象的完整性。
    Fsck,
}

#[derive(Debug, Args)]
struct InitArgs {
    /// 要初始化为 NeoEngram 仓库的目录。
    #[arg(value_name = "PATH", default_value = ".")]
    path: PathBuf,
    /// 新仓库使用的元数据后端；省略时默认使用 SQLite。
    #[arg(long, value_enum, value_name = "BACKEND")]
    metadata_store: Option<MetadataStoreArgument>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetadataStoreArgument {
    Json,
    Sqlite,
}

impl From<MetadataStoreArgument> for MetadataStoreKind {
    fn from(value: MetadataStoreArgument) -> Self {
        match value {
            MetadataStoreArgument::Json => Self::Json,
            MetadataStoreArgument::Sqlite => Self::Sqlite,
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
struct RecoverArgs {
    /// 回滚到事务开始前的工作区和 index，不继续原操作。
    #[arg(long)]
    abort: bool,
}

pub(crate) async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Init(arguments) => {
            app::init::execute(arguments.path, arguments.metadata_store.map(Into::into)).await
        }
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
        Command::Commit(arguments) => app::commit::execute(arguments.message).await,
        Command::Log(arguments) => app::log::execute(arguments.max_count).await,
        Command::Show(arguments) => app::show::execute(arguments.target).await,
        Command::Checkout(arguments) => {
            app::checkout::execute(arguments.target, arguments.force).await
        }
        Command::Recover(arguments) => app::recover::execute(arguments.abort).await,
        Command::Fsck => app::fsck::execute().await,
    }
}
