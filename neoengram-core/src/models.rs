use serde::{Deserialize, Serialize};

/// 当前本地暂存区的序列化格式版本。
pub const INDEX_FORMAT_VERSION: u32 = 2;

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

/// 一次提交所引用的完整目录快照。
///
/// 使用列表而不是 JSON 对象保存文件，并要求按规范化路径排序，以获得跨运行稳定的
/// 序列化结果和 Tree ID。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    /// 快照内的全部文件节点。
    pub files: Vec<FileNode>,
}

/// 可变的本地暂存区。
///
/// `add` 对该快照执行按路径 upsert；`commit` 固化它但不会清空它，因此下一次只需
/// add 发生变化的文件，未修改文件仍会保留在下一棵 Tree 中。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    /// 标识当前本地 Index 的序列化格式。
    pub format_version: u32,
    /// 按仓库相对路径严格升序排列、且路径唯一的文件节点。
    pub files: Vec<FileNode>,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            format_version: INDEX_FORMAT_VERSION,
            files: Vec::new(),
        }
    }
}

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
