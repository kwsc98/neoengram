use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Error, Result};
use neoengram_core::CommitId;

use crate::local::{
    fs::durable::{create_dir_all_durable, remove_dir_all_durable, rename_noreplace_durable},
    metadata::{HeadState, WorkspaceId, DEFAULT_WORKSPACE_ID},
    repository::Repository,
    worktree,
};
use crate::{
    HeadPosition, WorkspaceCreateResult, WorkspaceListResult, WorkspaceRemoveResult,
    WorkspaceSummary,
};

pub(crate) async fn create(
    current_dir: PathBuf,
    name: String,
    from: String,
    path: Option<PathBuf>,
    branch: Option<String>,
) -> Result<WorkspaceCreateResult> {
    let repository = Repository::discover(&current_dir)?;
    ensure!(
        repository.workspace_by_name(&name)?.is_none(),
        "Workspace 已存在: {name}"
    );
    repository.validate_logical_path(&format!("workspaces/{name}"))?;
    let commit_id = repository.resolve_target_id(&from)?;
    let typed_commit_id = commit_id
        .parse::<CommitId>()
        .context("Workspace TARGET 解析为无效 Commit ID")?;
    let branch = branch.map(|branch| {
        if branch.starts_with("refs/heads/") {
            branch
        } else {
            format!("refs/heads/{branch}")
        }
    });
    if let Some(reference) = &branch {
        repository.ensure_new_branch_available(reference)?;
    }
    let root = path.map_or_else(
        || repository.container_root().join("workspaces").join(&name),
        |path| {
            if path.is_absolute() {
                path
            } else {
                current_dir.join(path)
            }
        },
    );
    ensure!(
        !root.try_exists()?,
        "Workspace 目录已经存在: {}",
        root.display()
    );
    create_dir_all_durable(&root)
        .with_context(|| format!("无法创建 Workspace 目录: {}", root.display()))?;
    for directory in [
        root.join(".neoengram"),
        root.join(".neoengram/locks"),
        root.join(".neoengram/transactions"),
    ] {
        if let Err(error) = create_dir_all_durable(&directory) {
            let error = error.context(format!(
                "无法创建 Workspace 管理目录: {}",
                directory.display()
            ));
            return Err(rollback_failed_creation(&repository, None, &root, error));
        }
    }
    let registration = match repository.register_workspace(&name, &root, &commit_id) {
        Ok(registration) => registration,
        Err(error) => {
            return Err(rollback_failed_creation(&repository, None, &root, error));
        }
    };
    let result = (|| -> Result<HeadPosition> {
        repository.publish_external_workspace_link(&registration)?;
        let workspace = repository.open_workspace(registration.id)?;
        worktree::checkout_snapshot(&workspace, &commit_id, true)
            .context("创建 Workspace 时无法物化目标 Commit")?;
        if let Some(reference) = branch {
            repository.attach_workspace_to_branch(registration.id, &commit_id, &reference)?;
            Ok(HeadPosition::Branch(reference))
        } else {
            Ok(HeadPosition::Detached(typed_commit_id))
        }
    })();
    let head = match result {
        Ok(head) => head,
        Err(error) => {
            return Err(rollback_failed_creation(
                &repository,
                Some(registration.id),
                &registration.root,
                error,
            ));
        }
    };
    Ok(WorkspaceCreateResult {
        name,
        root: registration.root,
        commit_id: typed_commit_id,
        head,
    })
}

fn rollback_failed_creation(
    repository: &Repository,
    workspace_id: Option<WorkspaceId>,
    root: &Path,
    error: Error,
) -> Error {
    let mut cleanup_errors = Vec::new();
    if let Some(workspace_id) = workspace_id {
        match repository.remove_workspace_registration(workspace_id) {
            Ok(_) => {}
            Err(cleanup) => cleanup_errors.push(format!("无法注销 Workspace: {cleanup:#}")),
        }
    }
    match root.try_exists() {
        Ok(true) => {
            if let Err(cleanup) = remove_dir_all_durable(root) {
                cleanup_errors.push(format!(
                    "无法删除 Workspace 目录 {}: {cleanup:#}",
                    root.display()
                ));
            }
        }
        Ok(false) => {}
        Err(cleanup) => cleanup_errors.push(format!(
            "无法检查 Workspace 目录 {}: {cleanup}",
            root.display()
        )),
    }
    if cleanup_errors.is_empty() {
        error
    } else {
        error.context(format!(
            "创建失败后的清理未完整完成: {}",
            cleanup_errors.join("; ")
        ))
    }
}

pub(crate) async fn list(cwd: PathBuf) -> Result<WorkspaceListResult> {
    let repository = Repository::discover(&cwd)?;
    let mut workspaces = Vec::new();
    for workspace in repository.list_workspaces()? {
        let current = workspace.id == repository.workspace_id();
        let head = match workspace.head {
            HeadState::Attached { reference } => HeadPosition::Branch(reference),
            HeadState::Detached { commit_id } => HeadPosition::Detached(
                commit_id
                    .parse()
                    .context("Workspace 元数据包含无效 detached Commit ID")?,
            ),
        };
        let base_commit_id = workspace
            .base_commit_id
            .map(|commit_id| {
                commit_id
                    .parse()
                    .context("Workspace 元数据包含无效 base Commit ID")
            })
            .transpose()?;
        workspaces.push(WorkspaceSummary {
            name: workspace.name,
            root: workspace.root,
            head,
            base_commit_id,
            current,
        });
    }
    Ok(WorkspaceListResult { workspaces })
}

pub(crate) async fn remove(
    cwd: PathBuf,
    name: String,
    force: bool,
) -> Result<WorkspaceRemoveResult> {
    let repository = Repository::discover(&cwd)?;
    let registration = repository
        .workspace_by_name(&name)?
        .with_context(|| format!("Workspace 不存在: {name}"))?;
    ensure!(
        registration.id != DEFAULT_WORKSPACE_ID,
        "不能删除默认 Workspace"
    );
    ensure!(
        registration.id != repository.workspace_id(),
        "不能从 Workspace 自身内部删除它；请切换到另一个 Workspace"
    );
    let workspace = repository.open_workspace(registration.id)?;
    let _worktree_lock = workspace.acquire_worktree_lock()?;
    if !force {
        ensure!(
            super::status::is_workspace_clean(&workspace)?,
            "Workspace 包含未提交或未跟踪修改；使用 --force 明确丢弃"
        );
    }

    let parent = registration
        .root
        .parent()
        .context("Workspace 路径没有父目录")?;
    let reservation = tempfile::Builder::new()
        .prefix(".neoengram-workspace-remove-")
        .tempdir_in(parent)?;
    let tombstone = reservation.path().to_path_buf();
    reservation.close()?;
    rename_noreplace_durable(&registration.root, &tombstone)?;
    match repository.remove_workspace_registration(registration.id) {
        Ok(true) => {}
        Ok(false) => {
            rename_noreplace_durable(&tombstone, &registration.root)?;
            anyhow::bail!("Workspace 注册在删除期间消失: {name}");
        }
        Err(error) => {
            rename_noreplace_durable(&tombstone, &registration.root)?;
            return Err(error);
        }
    }
    remove_dir_all_durable(&tombstone).with_context(|| {
        format!(
            "Workspace 已注销，但目录清理失败，请手工检查 {}",
            tombstone.display()
        )
    })?;
    Ok(WorkspaceRemoveResult {
        name,
        root: registration.root,
    })
}
