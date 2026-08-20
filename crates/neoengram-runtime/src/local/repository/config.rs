use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::local::{
    metadata::RepositoryConfigRecord,
    model::{ChunkingStrategy, WORKSPACE_INDEX_FORMAT_VERSION},
    objects::ObjectStoreKind,
};

/// Clean-slate repository identity. Existing `.neoengram` repositories with any other format are
/// intentionally rejected and must be initialized again.
pub(super) const CURRENT_FORMAT_VERSION: u32 = WORKSPACE_INDEX_FORMAT_VERSION;

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

/// Repository identity and immutable storage policy stored in metadata SQLite.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryConfig {
    pub(super) format_version: u32,
    pub(super) repository_id: String,
    pub(super) object_store: ObjectStoreKind,
    pub(super) chunking: ChunkingPolicy,
}

impl RepositoryConfig {
    pub(super) fn from_metadata(record: RepositoryConfigRecord) -> Result<Self> {
        let object_store = match record.object_store.as_str() {
            "loose" => ObjectStoreKind::Loose,
            other => anyhow::bail!("不支持的对象存储后端: {other}"),
        };
        let chunking = match record.chunking.as_str() {
            "fastcdc" => ChunkingPolicy::FastCdc,
            "whole-file" => ChunkingPolicy::WholeFile,
            "mixed" => ChunkingPolicy::Mixed,
            other => anyhow::bail!("不支持的分块策略: {other}"),
        };
        let config = Self {
            format_version: record.format_version,
            repository_id: record.repository_id,
            object_store,
            chunking,
        };
        config.validate()?;
        Ok(config)
    }

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
