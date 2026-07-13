use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};

use crate::repository::Repository;

pub(crate) async fn execute(path: PathBuf) -> Result<()> {
    let task_path = path.clone();
    let outcome = tokio::task::spawn_blocking(move || initialize(&task_path))
        .await
        .with_context(|| format!("仓库初始化任务异常终止: {}", path.display()))??;

    if outcome.reinitialized {
        println!(
            "Reinitialized existing NeoEngram repository in {}",
            outcome.repository_dir.display()
        );
    } else {
        println!(
            "Initialized empty NeoEngram repository in {}",
            outcome.repository_dir.display()
        );
    }
    Ok(())
}

struct InitOutcome {
    repository_dir: PathBuf,
    reinitialized: bool,
}

fn initialize(path: &Path) -> Result<InitOutcome> {
    ensure_repository_root(path)?;
    let root =
        fs::canonicalize(path).with_context(|| format!("无法解析仓库目录: {}", path.display()))?;
    let repository = Repository::open_or_default(root)?;
    let repository_dir = repository.repository_dir();
    let reinitialized = repository_dir
        .try_exists()
        .with_context(|| format!("无法检查仓库目录: {}", repository_dir.display()))?;
    repository.initialize_layout()?;
    Ok(InitOutcome {
        repository_dir,
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
