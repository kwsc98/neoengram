//! NeoEngram 的本地数据面核心库。
//!
//! 第一阶段只负责把工作区文件切分为内容寻址对象，并生成可序列化的文件树元数据；
//! 网络传输、远端控制面和提交历史将在后续阶段实现。

pub mod chunker;
pub mod models;
pub mod object_store;

pub use chunker::chunk_file;
pub use models::{Chunk, Commit, FileNode, Index, Tree, INDEX_FORMAT_VERSION};
pub use object_store::{
    LooseObjectStore, ObjectCheck, ObjectMeta, ObjectPage, ObjectSpec, ObjectStore, PutOutcome,
    MAX_OBJECT_CHECK_BATCH, MAX_OBJECT_LIST_PAGE_SIZE,
};
