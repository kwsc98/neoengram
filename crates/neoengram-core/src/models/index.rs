use serde::{Deserialize, Serialize};

use super::{FileNode, INDEX_FORMAT_VERSION};

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
