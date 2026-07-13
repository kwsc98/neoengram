//! Chunk payload 后端的选择与构造。

use std::{path::Path, sync::Arc};

use serde::{Deserialize, Serialize};

mod contract;
mod loose;

pub(crate) use contract::{ObjectSpec, ObjectStore};
pub(crate) use loose::LooseObjectStore;

#[cfg(test)]
pub(crate) use loose::LOOSE_OBJECT_TEMP_DIR_NAME;

#[cfg(test)]
mod tests;

/// Chunk payload 的物理存储类型，与元数据后端独立选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectStoreKind {
    Loose,
}

pub(crate) fn open_object_store(
    kind: ObjectStoreKind,
    objects_root: &Path,
) -> Arc<dyn ObjectStore> {
    match kind {
        ObjectStoreKind::Loose => Arc::new(LooseObjectStore::new(objects_root.to_path_buf())),
    }
}
