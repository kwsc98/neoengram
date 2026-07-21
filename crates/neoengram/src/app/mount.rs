use std::path::PathBuf;
use std::sync::mpsc;

use anyhow::{Context, Result};

use crate::local::{mount, repository::Repository};

pub(crate) async fn execute(
    target: String,
    mountpoint: PathBuf,
    cache_size_mib: u64,
) -> Result<()> {
    let cache_bytes = cache_size_mib
        .checked_mul(1024 * 1024)
        .context("FUSE Chunk 缓存大小溢出")?;
    let current_dir = std::env::current_dir().context("无法确定当前目录")?;
    let repository = Repository::discover(&current_dir)?;
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let mut task = tokio::task::spawn_blocking(move || {
        let mounted =
            mount::ReadOnlyMount::prepare(repository, target.trim(), &mountpoint, cache_bytes)?;
        println!(
            "Mounting fixed snapshot {} at {} (cache {} MiB)",
            mounted.commit_id(),
            mounted.mountpoint().display(),
            cache_size_mib
        );
        mount::mount_foreground(mounted, shutdown_rx)
    });
    tokio::select! {
        result = &mut task => result.context("FUSE 挂载任务异常终止")?,
        signal = shutdown_signal() => {
            signal?;
            let _ = shutdown_tx.send(());
            task.await.context("FUSE 挂载任务异常终止")?
        }
    }
}

pub(crate) async fn unmount(mountpoint: PathBuf) -> Result<()> {
    let display = mountpoint.clone();
    tokio::task::spawn_blocking(move || mount::unmount(&mountpoint))
        .await
        .context("FUSE 卸载任务异常终止")??;
    println!("Unmounted {}", display.display());
    Ok(())
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
