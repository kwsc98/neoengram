use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};

use crate::repository::Repository;

pub(crate) async fn execute(path: PathBuf) -> Result<()> {
    let task_path = path.clone();
    let outcome = tokio::task::spawn_blocking(move || initialize(&task_path))
        .await
        .with_context(|| format!("仓库初始化任务异常终止: {}", path.display()))??;

    if outcome.reinitialized {
        println!(
            "Reinitialized existing Synapse repository in {}",
            outcome.synapse_dir.display()
        );
    } else {
        println!(
            "Initialized empty Synapse repository in {}",
            outcome.synapse_dir.display()
        );
    }
    Ok(())
}

struct InitOutcome {
    synapse_dir: PathBuf,
    reinitialized: bool,
}

fn initialize(path: &Path) -> Result<InitOutcome> {
    ensure_repository_root(path)?;
    let root =
        fs::canonicalize(path).with_context(|| format!("无法解析仓库目录: {}", path.display()))?;
    let synapse_dir = root.join(crate::repository::SYNAPSE_DIR_NAME);

    let reinitialized = match fs::symlink_metadata(&synapse_dir) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "Synapse 内部路径不是普通目录: {}",
                synapse_dir.display()
            );
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查仓库目录: {}", synapse_dir.display()));
        }
    };

    let repository = Repository::open_or_default(root)?;
    repository.initialize_layout()?;
    Ok(InitOutcome {
        synapse_dir,
        reinitialized,
    })
}

fn ensure_repository_root(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => bail!("初始化路径不是普通目录: {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path)
            .with_context(|| format!("无法创建仓库目录: {}", path.display())),
        Err(error) => Err(error).with_context(|| format!("无法访问初始化路径: {}", path.display())),
    }
}
