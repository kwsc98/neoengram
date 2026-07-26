use std::path::PathBuf;
use std::sync::{mpsc, Arc};

use anyhow::{Context, Result};
use neoengram_core::CommitId;
use neoengram_engine::{ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit};

use crate::local::{mount, repository::Repository};
use crate::{MountResult, UnmountResult};

pub(crate) async fn execute(
    cwd: PathBuf,
    target: String,
    mountpoint: PathBuf,
    cache_size_mib: u64,
    progress: Arc<dyn ProgressSink>,
) -> Result<MountResult> {
    let cache_bytes = cache_size_mib
        .checked_mul(1024 * 1024)
        .context("FUSE Chunk 缓存大小溢出")?;
    let repository = Repository::discover(&cwd)?;
    let mountpoint = if mountpoint.is_absolute() {
        mountpoint
    } else {
        cwd.join(mountpoint)
    };
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let mut task = tokio::task::spawn_blocking(move || {
        let mounted =
            mount::ReadOnlyMount::prepare(repository, target.trim(), &mountpoint, cache_bytes)?;
        let result = MountResult {
            commit_id: mounted
                .commit_id()
                .parse::<CommitId>()
                .context("FUSE 挂载返回无效 Commit ID")?,
            mountpoint: mounted.mountpoint().to_path_buf(),
            file_count: mounted.root_file_count(),
            total_size_bytes: mounted.root_total_size(),
            cache_size_bytes: cache_bytes,
        };
        progress.emit(
            &ProgressEvent::new(ProgressPhase::Preparing, ProgressUnit::Actions, 1).with_total(1),
        )?;
        mount::mount_foreground(mounted, shutdown_rx)?;
        progress.emit(&ProgressEvent::new(
            ProgressPhase::Completed,
            ProgressUnit::Actions,
            1,
        ))?;
        Ok::<MountResult, anyhow::Error>(result)
    });
    let result = tokio::select! {
        result = &mut task => result.context("FUSE 挂载任务异常终止")?,
        signal = shutdown_signal() => {
            signal?;
            let _ = shutdown_tx.send(());
            task.await.context("FUSE 挂载任务异常终止")?
        }
    }?;
    Ok(result)
}

pub(crate) async fn unmount(cwd: PathBuf, mountpoint: PathBuf) -> Result<UnmountResult> {
    let mountpoint = if mountpoint.is_absolute() {
        mountpoint
    } else {
        cwd.join(mountpoint)
    };
    let display = mountpoint.clone();
    tokio::task::spawn_blocking(move || mount::unmount(&mountpoint))
        .await
        .context("FUSE 卸载任务异常终止")??;
    Ok(UnmountResult {
        mountpoint: display,
    })
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("无法监听 SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("无法监听 Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("无法监听 Ctrl-C")
    }
}
