use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use tempfile::Builder;

use crate::local::{
    fs::durable::{create_dir_all_durable, rename_noreplace_durable},
    repository::{ChunkingPolicy, Repository, NEOENGRAM_DIR_NAME},
    STORAGE_TEMP_FILE_PREFIX,
};

pub(crate) async fn execute(path: PathBuf, chunking: Option<ChunkingPolicy>) -> Result<()> {
    let task_path = path.clone();
    let outcome = tokio::task::spawn_blocking(move || initialize(&task_path, chunking))
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

fn initialize(path: &Path, chunking: Option<ChunkingPolicy>) -> Result<InitOutcome> {
    ensure_repository_root(path)?;
    let root =
        fs::canonicalize(path).with_context(|| format!("无法解析仓库目录: {}", path.display()))?;
    let repository_dir = root.join(NEOENGRAM_DIR_NAME);
    let reinitialized = match fs::symlink_metadata(&repository_dir) {
        Ok(_) => true,
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查仓库目录: {}", repository_dir.display()));
        }
    };
    if reinitialized {
        let repository = Repository::open_or_create(root)?;
        if let Some(requested) = chunking {
            ensure!(
                repository.chunking_policy() == requested,
                "已有仓库使用 {} 分块策略，不能改为 {}；请新建仓库",
                repository.chunking_policy().as_str(),
                requested.as_str()
            );
        }
        repository.initialize_layout()?;
    } else {
        initialize_new_repository(&root, chunking.unwrap_or_default())?;
    }
    Ok(InitOutcome {
        repository_dir,
        reinitialized,
    })
}

fn initialize_new_repository(root: &Path, chunking: ChunkingPolicy) -> Result<()> {
    let temporary = Builder::new()
        .prefix(STORAGE_TEMP_FILE_PREFIX)
        .tempdir_in(root)
        .with_context(|| format!("无法创建仓库初始化临时目录: {}", root.display()))?;
    let staging_root = temporary.path().to_path_buf();
    let repository = Repository::at_with_chunking(staging_root.clone(), chunking)?;
    repository.initialize_layout()?;
    super::test_crash_at("init-before-publish");

    let staged_repository_dir = repository.repository_dir();
    let repository_dir = root.join(NEOENGRAM_DIR_NAME);
    rename_noreplace_durable(&staged_repository_dir, &repository_dir).with_context(|| {
        format!(
            "无法发布新仓库目录 {} -> {}",
            staged_repository_dir.display(),
            repository_dir.display()
        )
    })?;

    let published = Repository::open_or_create(root.to_path_buf())?;
    published.initialize_layout()
}

fn ensure_repository_root(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => bail!("初始化路径不是普通目录: {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .context("无法确定当前目录以创建仓库路径")?
                    .join(path)
            };
            create_dir_all_durable(&absolute)
        }
        Err(error) => Err(error).with_context(|| format!("无法访问初始化路径: {}", path.display())),
    }
}
