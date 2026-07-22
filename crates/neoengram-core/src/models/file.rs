use serde::{Deserialize, Serialize};

use super::Chunk;

/// 生成文件 Manifest 时采用的分块策略。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkingStrategy {
    /// 使用内容定义边界的 FastCDC 分块。
    #[default]
    FastCdc,
    /// 将整个非空文件保存为单个对象。
    WholeFile,
}

/// 一个不可变文件快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileNode {
    /// 使用 `/` 分隔、相对于仓库根目录的 UTF-8 路径。
    pub path: String,
    /// 原始文件的总字节数。
    pub total_size: u64,
    /// 生成此文件 Manifest 的分块策略。
    pub chunking: ChunkingStrategy,
    /// 按原始文件顺序排列的数据块。
    pub chunks: Vec<Chunk>,
}
