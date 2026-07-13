use serde::{Deserialize, Serialize};

use super::Chunk;

/// 一个不可变文件快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileNode {
    /// 使用 `/` 分隔、相对于仓库根目录的 UTF-8 路径。
    pub path: String,
    /// 原始文件的总字节数。
    pub total_size: u64,
    /// 按原始文件顺序排列的数据块。
    pub chunks: Vec<Chunk>,
}
