use serde::{Deserialize, Serialize};

/// 一个由内容寻址的数据块。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// 数据块内容的 BLAKE3 十六进制摘要。
    pub hash: String,
    /// 数据块在原始文件中的字节偏移量。
    pub offset: u64,
    /// 数据块的字节数。
    pub size: u64,
}
