use serde::{Deserialize, Serialize};

/// 一个不可变提交。
///
/// Commit ID 存在文件名和分支引用中，不放进结构本身，从而避免自引用 Hash。单个
/// `Option<String>` 父节点从数据模型上排除了 merge commit。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// 本次提交引用的不可变 Tree ID。
    pub tree_hash: String,
    /// 唯一父提交；仓库首个提交为 `None`。
    pub parent: Option<String>,
    /// 用户提供的提交说明。
    pub message: String,
    /// 自 Unix Epoch 起的毫秒数。
    pub created_at_unix_ms: u64,
}
