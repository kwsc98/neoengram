use serde::{Deserialize, Serialize};

use crate::{
    canonical, CommitId, DirectoryId, ValidationError, ValidationErrorKind, ValidationResult,
};

/// 一个不可变提交。
///
/// Commit ID 存在文件名和分支引用中，不放进结构本身，从而避免自引用 Hash。单个
/// `Option<CommitId>` 父节点从数据模型上排除了 merge commit。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// Canonical identity of the root immutable Directory.
    pub root_directory_id: DirectoryId,
    /// 唯一父提交；仓库首个提交为 `None`。
    pub parent: Option<CommitId>,
    /// 用户提供的提交说明。
    pub message: String,
    /// 自 Unix Epoch 起的毫秒数。
    pub created_at_unix_ms: u64,
}

impl Commit {
    /// Constructs a Commit after validating its message.
    pub fn new(
        root_directory_id: DirectoryId,
        parent: Option<CommitId>,
        message: impl Into<String>,
        created_at_unix_ms: u64,
    ) -> ValidationResult<Self> {
        let commit = Self {
            root_directory_id,
            parent,
            message: message.into(),
            created_at_unix_ms,
        };
        commit.validate()?;
        Ok(commit)
    }

    /// Validates the environment-independent Commit invariants.
    pub fn validate(&self) -> ValidationResult<()> {
        if self.message.trim().is_empty() {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidCommit,
                "Commit message must not be empty or whitespace",
            ));
        }
        Ok(())
    }

    /// Computes the canonical `neoengram-commit-v3` identity.
    pub fn canonical_id(&self) -> ValidationResult<CommitId> {
        canonical::commit_id(self)
    }
}
