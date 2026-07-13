mod config;
mod history;
mod layout;
mod lock;
mod validation;

use std::{path::PathBuf, sync::Arc};

use crate::local::{
    metadata::MetadataStore,
    objects::{ObjectStore, ObjectStoreKind},
};

pub(crate) use lock::RepositoryWriteLock;
pub(crate) use validation::is_neoengram_dir_name;

pub(crate) const NEOENGRAM_DIR_NAME: &str = ".neoengram";
pub(super) const DEFAULT_HEAD_REFERENCE: &str = "refs/heads/main";

#[derive(Debug, Clone)]
pub(crate) struct Repository {
    root: PathBuf,
    metadata_store: Arc<dyn MetadataStore>,
    object_store: Arc<dyn ObjectStore>,
    object_store_kind: ObjectStoreKind,
}
