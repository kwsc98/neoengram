use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::local::{
    fs::durable::{publish_json_if_absent, read_json},
    metadata::{
        open_workspace_metadata_store, HeadState, WorkspaceId, WorkspaceRecord,
        DEFAULT_WORKSPACE_ID,
    },
};

use super::{Repository, WORKSPACES_DIR_NAME};

const WORKSPACE_LINK_FILE: &str = "workspace.json";
const WORKSPACE_LINK_FORMAT: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceLink {
    format_version: u32,
    repository: PathBuf,
    repository_id: String,
    workspace_id: WorkspaceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRegistration {
    pub(crate) id: WorkspaceId,
    pub(crate) name: String,
    pub(crate) root: PathBuf,
    pub(crate) head: HeadState,
    pub(crate) base_commit_id: Option<String>,
}

impl Repository {
    pub(crate) fn list_workspaces(&self) -> Result<Vec<WorkspaceRegistration>> {
        self.metadata_store
            .list_workspaces()?
            .into_iter()
            .map(|record| self.resolve_workspace_record(record))
            .collect()
    }

    pub(crate) fn workspace_by_name(&self, name: &str) -> Result<Option<WorkspaceRegistration>> {
        self.metadata_store
            .get_workspace(name)?
            .map(|record| self.resolve_workspace_record(record))
            .transpose()
    }

    pub(crate) fn ensure_branch_available(&self, reference: &str) -> Result<()> {
        if let Some(owner) = self.list_workspaces()?.into_iter().find(|workspace| {
            workspace.id != self.workspace_id
                && matches!(
                    &workspace.head,
                    HeadState::Attached { reference: attached } if attached == reference
                )
        }) {
            anyhow::bail!(
                "分支 {reference} 已被 Workspace {} 占用: {}",
                owner.name,
                owner.root.display()
            );
        }
        Ok(())
    }

    pub(crate) fn ensure_new_branch_available(&self, reference: &str) -> Result<()> {
        HeadState::Attached {
            reference: reference.to_owned(),
        }
        .validate()?;
        self.ensure_branch_available(reference)?;
        ensure!(
            self.metadata_store.get_reference(reference)?.is_none(),
            "分支已存在，不能作为新 Workspace 分支创建: {reference}"
        );
        Ok(())
    }

    pub(crate) fn register_workspace(
        &self,
        name: &str,
        root: &Path,
        commit_id: &str,
    ) -> Result<WorkspaceRegistration> {
        let root = absolute_normalized(root)?;
        ensure!(
            root != self.repository_dir() && !root.starts_with(self.repository_dir()),
            "Workspace 不能位于仓库存储目录内部"
        );
        if root.starts_with(&self.container_root) {
            let managed_root = self.container_root.join(WORKSPACES_DIR_NAME);
            ensure!(
                root != managed_root && root.starts_with(&managed_root),
                "仓库内 Workspace 只能位于受管目录 {} 的后代: {}",
                managed_root.display(),
                root.display()
            );
        }
        ensure!(
            self.list_workspaces()?
                .iter()
                .filter(|workspace| workspace.id != DEFAULT_WORKSPACE_ID)
                .all(|workspace| {
                    workspace.root != root
                        && !workspace.root.starts_with(&root)
                        && !root.starts_with(&workspace.root)
                }),
            "Workspace 路径不能与现有 Workspace 相互包含: {}",
            root.display()
        );
        let root_path = if let Ok(relative) = root.strip_prefix(&self.container_root) {
            relative
                .to_str()
                .filter(|relative| !relative.is_empty())
                .context("Workspace 相对路径不是有效 UTF-8")?
                .to_owned()
        } else {
            root.to_str()
                .context("Workspace 绝对路径不是有效 UTF-8")?
                .to_owned()
        };
        let record = self
            .metadata_store
            .create_workspace(name, &root_path, commit_id)?;
        self.resolve_workspace_record(record)
    }

    pub(crate) fn attach_workspace_to_branch(
        &self,
        id: WorkspaceId,
        expected_commit_id: &str,
        reference: &str,
    ) -> Result<()> {
        self.metadata_store
            .attach_workspace_to_branch(id, expected_commit_id, reference)
    }

    pub(crate) fn open_workspace(&self, id: WorkspaceId) -> Result<Self> {
        let registration = self
            .list_workspaces()?
            .into_iter()
            .find(|workspace| workspace.id == id)
            .context("Workspace 注册信息不存在")?;
        let metadata_store = open_workspace_metadata_store(&self.metadata_dir(), id);
        metadata_store.validate_layout()?;
        Ok(Self {
            container_root: self.container_root.clone(),
            root: registration.root,
            workspace_id: id,
            metadata_store,
            object_store: self.object_store.clone(),
            object_store_kind: self.object_store_kind,
        })
    }

    pub(crate) fn remove_workspace_registration(&self, id: WorkspaceId) -> Result<bool> {
        self.metadata_store.remove_workspace(id)
    }

    pub(crate) fn publish_external_workspace_link(
        &self,
        registration: &WorkspaceRegistration,
    ) -> Result<()> {
        if registration.root.starts_with(&self.container_root) {
            return Ok(());
        }
        let path = registration
            .root
            .join(".neoengram")
            .join(WORKSPACE_LINK_FILE);
        publish_json_if_absent(
            &path,
            &WorkspaceLink {
                format_version: WORKSPACE_LINK_FORMAT,
                repository: self.container_root.clone(),
                repository_id: self.repository_id()?,
                workspace_id: registration.id,
            },
        )
        .map(|_| ())
    }

    pub(super) fn open_linked_workspace(workspace_root: &Path) -> Result<Option<Self>> {
        let link_path = workspace_root.join(".neoengram").join(WORKSPACE_LINK_FILE);
        if !link_path.try_exists()? {
            return Ok(None);
        }
        let link: WorkspaceLink = read_json(&link_path)
            .with_context(|| format!("无法读取 Workspace 指针: {}", link_path.display()))?;
        ensure!(
            link.format_version == WORKSPACE_LINK_FORMAT,
            "不支持的 Workspace 指针格式: {}",
            link.format_version
        );
        let repository_root = fs::canonicalize(&link.repository).with_context(|| {
            format!("Workspace 指向的仓库不存在: {}", link.repository.display())
        })?;
        let repository = Self::open(repository_root)?;
        repository.validate()?;
        ensure!(
            repository.repository_id()? == link.repository_id,
            "Workspace 指向的 repository_id 不匹配"
        );
        let workspace = repository.open_workspace(link.workspace_id)?;
        ensure!(
            workspace.root == fs::canonicalize(workspace_root)?,
            "Workspace 指针与仓库注册路径不一致"
        );
        Ok(Some(workspace))
    }

    pub(super) fn select_workspace_for_path(self, path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path)
            .with_context(|| format!("无法解析工作目录: {}", path.display()))?;
        let selected = self
            .list_workspaces()?
            .into_iter()
            .filter(|workspace| path == workspace.root || path.starts_with(&workspace.root))
            .max_by_key(|workspace| workspace.root.components().count());
        selected.map_or(Ok(self.clone()), |workspace| {
            self.open_workspace(workspace.id)
        })
    }

    fn resolve_workspace_record(&self, record: WorkspaceRecord) -> Result<WorkspaceRegistration> {
        let stored = PathBuf::from(&record.root_path);
        let root = if stored.is_absolute() {
            stored
        } else {
            self.container_root.join(&record.root_path)
        };
        let root = absolute_normalized(&root)?;
        Ok(WorkspaceRegistration {
            id: record.id,
            name: record.name,
            root,
            head: record.head,
            base_commit_id: record.base_commit_id,
        })
    }
}

fn absolute_normalized(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("无法确定当前目录")?
            .join(path)
    };
    if absolute.try_exists()? {
        fs::canonicalize(&absolute)
            .with_context(|| format!("无法解析 Workspace 路径: {}", absolute.display()))
    } else {
        let parent = absolute.parent().context("Workspace 路径没有父目录")?;
        let name = absolute.file_name().context("Workspace 路径没有名称")?;
        Ok(fs::canonicalize(parent)
            .with_context(|| format!("无法解析 Workspace 父目录: {}", parent.display()))?
            .join(name))
    }
}
