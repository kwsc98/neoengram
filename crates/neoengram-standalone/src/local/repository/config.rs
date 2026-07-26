use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::local::{fs::durable::read_json, model::ChunkingStrategy, objects::ObjectStoreKind};

pub(super) const CURRENT_FORMAT_VERSION: u32 = 8;

/// Immutable repository-wide policy controlling which file chunking strategies are accepted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ChunkingPolicy {
    #[serde(rename = "fastcdc")]
    #[default]
    FastCdc,
    #[serde(rename = "whole-file")]
    WholeFile,
    #[serde(rename = "mixed")]
    Mixed,
}

impl ChunkingPolicy {
    pub(crate) const fn fixed_strategy(self) -> Option<ChunkingStrategy> {
        match self {
            Self::FastCdc => Some(ChunkingStrategy::FastCdc),
            Self::WholeFile => Some(ChunkingStrategy::WholeFile),
            Self::Mixed => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FastCdc => "fastcdc",
            Self::WholeFile => "whole-file",
            Self::Mixed => "mixed",
        }
    }
}

/// 保存在 NeoEngram 控制目录 `metadata/repository.json` 中的后端选择与仓库格式配置。
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryConfig {
    pub(super) format_version: u32,
    pub(super) repository_id: String,
    pub(super) object_store: ObjectStoreKind,
    pub(super) chunking: ChunkingPolicy,
}

impl RepositoryConfig {
    pub(super) fn new(object_store: ObjectStoreKind, chunking: ChunkingPolicy) -> Result<Self> {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("系统时间早于 Unix Epoch")?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"neoengram-repository-id-v1");
        hasher.update(&[0]);
        hasher.update(&now.as_nanos().to_le_bytes());
        hasher.update(&std::process::id().to_le_bytes());
        hasher.update(&NONCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        let config = Self {
            format_version: CURRENT_FORMAT_VERSION,
            repository_id: hasher.finalize().to_hex().to_string(),
            object_store,
            chunking,
        };
        config.validate()?;
        Ok(config)
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == CURRENT_FORMAT_VERSION,
            "不支持的 NeoEngram 仓库格式版本 {}（当前支持 {}）",
            self.format_version,
            CURRENT_FORMAT_VERSION
        );
        ensure!(
            self.repository_id.len() == 64
                && self
                    .repository_id
                    .bytes()
                    .all(|byte| { byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase() }),
            "NeoEngram repository_id 无效"
        );
        Ok(())
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
    let value: serde_json::Value = read_json(path)?;
    let format_version = value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .context("仓库配置缺少有效的 format_version")?;
    ensure!(
        format_version == u64::from(CURRENT_FORMAT_VERSION),
        "不支持的 NeoEngram 仓库格式版本 {format_version}（当前支持 {CURRENT_FORMAT_VERSION}）"
    );
    let config: RepositoryConfig =
        serde_json::from_value(value).context("NeoEngram 仓库配置格式无效")?;
    config.validate()?;
    Ok(config)
}
