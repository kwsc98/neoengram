use std::{fs, path::Path};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::local::{fs::durable::read_json, metadata::MetadataStoreKind, objects::ObjectStoreKind};

pub(super) const CURRENT_FORMAT_VERSION: u32 = 4;

/// 保存在 NeoEngram 控制目录 `metadata/repository.json` 中的后端选择与仓库格式配置。
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryConfig {
    pub(super) format_version: u32,
    pub(super) metadata_store: MetadataStoreKind,
    pub(super) object_store: ObjectStoreKind,
}

impl RepositoryConfig {
    pub(super) const fn current(
        metadata_store: MetadataStoreKind,
        object_store: ObjectStoreKind,
    ) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            metadata_store,
            object_store,
        }
    }
}

pub(super) const fn metadata_store_kind_name(kind: MetadataStoreKind) -> &'static str {
    match kind {
        MetadataStoreKind::Json => "json",
        MetadataStoreKind::Sqlite => "sqlite",
    }
}

pub(super) fn read_repository_config(path: &Path) -> Result<RepositoryConfig> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("仓库配置文件不存在: {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "仓库配置路径不是普通文件: {}",
        path.display()
    );
    read_json(path)
}
