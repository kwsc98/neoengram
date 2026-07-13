use serde::{Deserialize, Serialize};

use super::FileNode;

/// 一次提交所引用的完整目录快照。
///
/// 使用列表而不是 JSON 对象保存文件，并要求按规范化路径排序，以获得跨运行稳定的
/// 序列化结果和 Tree ID。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    /// 快照内的全部文件节点。
    pub files: Vec<FileNode>,
}
