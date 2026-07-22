//! NeoEngram 的内容模型核心库。
//!
//! 该 crate 只定义与运行环境无关、可序列化的内容模型与格式常量。文件系统、对象存储、
//! 工作区导入和网络传输由上层组件实现。

pub mod models;

pub use models::{
    Chunk, ChunkingStrategy, Commit, DirectoryEntry, DirectoryEntryKind, FileNode, Index, Tree,
    INDEX_FORMAT_VERSION,
};
