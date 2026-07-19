use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context, Result};

use crate::local::{
    fs::durable::{
        ensure_directory, ensure_or_create_directory, is_transaction_draft_name,
        publish_json_if_absent, remove_dir_all_durable,
    },
    metadata::{open_metadata_store, MetadataStoreKind},
    objects::{open_object_store, ObjectStore, ObjectStoreKind},
};

use super::{
    config::{
        metadata_store_kind_name, read_repository_config, RepositoryConfig, CURRENT_FORMAT_VERSION,
    },
    Repository, DEFAULT_HEAD_REFERENCE, NEOENGRAM_DIR_NAME,
};

const OBJECTS_DIR_NAME: &str = "objects";
const FILES_DIR_NAME: &str = "files";
const METADATA_DIR_NAME: &str = "metadata";
const REPOSITORY_FILE_NAME: &str = "repository.json";
const TRANSACTIONS_DIR_NAME: &str = "transactions";
const STAGING_DIR_NAME: &str = "staging";

fn neoengram_dir_exists(root: &Path) -> Result<bool> {
    let path = root.join(NEOENGRAM_DIR_NAME);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "NeoEngram 内部路径不是普通目录: {}",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("无法检查 NeoEngram 仓库目录: {}", path.display()))
        }
    }
}

impl Repository {
    pub(crate) fn at(root: PathBuf) -> Result<Self> {
        Self::with_store_kinds(root, MetadataStoreKind::Sqlite, ObjectStoreKind::Loose)
    }

    pub(crate) fn open_or_with_metadata_store(
        root: PathBuf,
        requested_metadata_store: Option<MetadataStoreKind>,
    ) -> Result<Self> {
        if neoengram_dir_exists(&root)? {
            let repository_dir = root.join(NEOENGRAM_DIR_NAME);
            let repository = Self::open(root).with_context(|| {
                format!(
                    "已有目录不是受支持的 NeoEngram 仓库: {}",
                    repository_dir.display()
                )
            })?;
            if let Some(requested) = requested_metadata_store {
                let actual = repository.metadata_store.kind();
                ensure!(
                    requested == actual,
                    "已有仓库使用 `{}` 元数据后端，不能以 `{}` 重新初始化；NeoEngram 不会自动迁移元数据后端",
                    metadata_store_kind_name(actual),
                    metadata_store_kind_name(requested)
                );
            }
            Ok(repository)
        } else {
            match requested_metadata_store {
                Some(metadata_store) => {
                    Self::with_store_kinds(root, metadata_store, ObjectStoreKind::Loose)
                }
                None => Self::at(root),
            }
        }
    }

    fn open(root: PathBuf) -> Result<Self> {
        let metadata_dir = root.join(NEOENGRAM_DIR_NAME).join(METADATA_DIR_NAME);
        let repository_file = metadata_dir.join(REPOSITORY_FILE_NAME);
        let config = read_repository_config(&repository_file)?;
        ensure!(
            config.format_version == CURRENT_FORMAT_VERSION,
            "不支持的 NeoEngram 仓库格式版本 {}（当前支持 {}）",
            config.format_version,
            CURRENT_FORMAT_VERSION
        );
        Self::with_store_kinds(root, config.metadata_store, config.object_store)
    }

    fn with_store_kinds(
        root: PathBuf,
        metadata_kind: MetadataStoreKind,
        object_kind: ObjectStoreKind,
    ) -> Result<Self> {
        let metadata_dir = root.join(NEOENGRAM_DIR_NAME).join(METADATA_DIR_NAME);
        let objects_dir = root.join(NEOENGRAM_DIR_NAME).join(OBJECTS_DIR_NAME);
        Ok(Self {
            root,
            metadata_store: open_metadata_store(metadata_kind, &metadata_dir),
            object_store: open_object_store(object_kind, &objects_dir),
            object_store_kind: object_kind,
        })
    }

    /// 从当前目录向上寻找最近的真实 `.neoengram` 目录。
    pub(crate) fn discover(start: &Path) -> Result<Self> {
        let canonical_start = fs::canonicalize(start)
            .with_context(|| format!("无法解析当前目录: {}", start.display()))?;

        for root in canonical_start.ancestors() {
            if neoengram_dir_exists(root)? {
                let repository = Self::open(root.to_path_buf())?;
                repository.validate()?;
                return Ok(repository);
            }
        }

        bail!("当前目录不在 NeoEngram 仓库中，请先运行 `neoengram init`")
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn repository_dir(&self) -> PathBuf {
        self.root.join(NEOENGRAM_DIR_NAME)
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.repository_dir().join(STAGING_DIR_NAME)
    }

    pub(crate) fn files_dir(&self) -> PathBuf {
        self.repository_dir().join(FILES_DIR_NAME)
    }

    pub(super) fn metadata_dir(&self) -> PathBuf {
        self.repository_dir().join(METADATA_DIR_NAME)
    }

    pub(crate) fn transactions_dir(&self) -> PathBuf {
        self.repository_dir().join(TRANSACTIONS_DIR_NAME)
    }

    pub(crate) fn unfinished_transactions(&self) -> Result<Vec<PathBuf>> {
        let mut transactions = Vec::new();
        for entry in fs::read_dir(self.transactions_dir()).with_context(|| {
            format!(
                "无法读取 Checkout 事务目录: {}",
                self.transactions_dir().display()
            )
        })? {
            let entry = entry.context("无法读取 Checkout 事务项")?;
            let metadata = fs::symlink_metadata(entry.path())
                .with_context(|| format!("无法检查 Checkout 事务项: {}", entry.path().display()))?;
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "Checkout 事务项不是普通目录: {}",
                entry.path().display()
            );
            if is_transaction_draft_name(&entry.file_name())? {
                continue;
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .with_context(|| format!("事务目录名不是有效 UTF-8: {}", entry.path().display()))?;
            ensure!(
                valid_published_transaction_name(name),
                "transactions 目录包含未知项目: {}",
                entry.path().display()
            );
            transactions.push(entry.path());
        }
        transactions.sort_unstable();
        Ok(transactions)
    }

    pub(crate) fn ensure_no_unfinished_transactions(&self) -> Result<()> {
        let transactions = self.unfinished_transactions()?;
        ensure!(
            transactions.is_empty(),
            "检测到未完成的工作区事务，请先运行 `neoengram recover` 或 `neoengram recover --abort`: {}",
            transactions
                .first()
                .map_or_else(|| self.transactions_dir(), PathBuf::from)
                .display()
        );
        Ok(())
    }

    /// 调用方必须持有排他的工作区或恢复锁，避免删除另一个进程仍在构造的 draft。
    pub(crate) fn cleanup_transaction_drafts(&self) -> Result<()> {
        for entry in fs::read_dir(self.transactions_dir())
            .with_context(|| format!("无法读取事务目录: {}", self.transactions_dir().display()))?
        {
            let entry = entry.context("无法读取事务 draft 项")?;
            if !is_transaction_draft_name(&entry.file_name())? {
                continue;
            }
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("无法检查事务 draft: {}", path.display()))?;
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "事务 draft 不是普通目录: {}",
                path.display()
            );
            remove_dir_all_durable(&path)
                .with_context(|| format!("无法清理遗留事务 draft: {}", path.display()))?;
        }
        Ok(())
    }

    fn repository_file(&self) -> PathBuf {
        self.metadata_dir().join(REPOSITORY_FILE_NAME)
    }

    /// 创建或补全当前仓库布局。已有 index、HEAD 和历史永远不会被覆盖。
    pub(crate) fn initialize_layout(&self) -> Result<()> {
        for directory in [
            self.repository_dir(),
            self.files_dir(),
            self.transactions_dir(),
            self.staging_dir(),
            self.metadata_dir(),
        ] {
            ensure_or_create_directory(&directory)?;
        }

        let current_config =
            RepositoryConfig::current(self.metadata_store.kind(), self.object_store_kind);
        if self.repository_file().try_exists()? {
            let config = read_repository_config(&self.repository_file())?;
            ensure!(
                config == current_config,
                "已有仓库使用不受支持的格式版本 {}",
                config.format_version
            );
        }

        self.object_store.initialize()?;
        self.metadata_store.initialize(DEFAULT_HEAD_REFERENCE)?;
        self.metadata_store.read_index()?;
        self.object_store.validate_layout()?;
        self.metadata_store.validate_layout()?;

        publish_json_if_absent(&self.repository_file(), &current_config)?;
        let config = read_repository_config(&self.repository_file())?;
        ensure!(
            config == current_config,
            "已有仓库使用不受支持的格式版本 {}",
            config.format_version
        );
        self.validate()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (directory, kind) in [
            (self.files_dir(), "完整文件缓存"),
            (self.transactions_dir(), "Checkout 事务"),
            (self.staging_dir(), "输入暂存"),
            (self.metadata_dir(), "元数据"),
        ] {
            ensure_directory(&directory, kind)?;
        }

        let config = read_repository_config(&self.repository_file())?;
        ensure!(
            config.format_version == CURRENT_FORMAT_VERSION,
            "不支持的 NeoEngram 仓库格式版本 {}（当前支持 {}）",
            config.format_version,
            CURRENT_FORMAT_VERSION
        );
        ensure!(
            config.metadata_store == self.metadata_store.kind(),
            "仓库元数据后端配置与已打开后端不一致"
        );
        ensure!(
            config.object_store == self.object_store_kind,
            "仓库对象后端配置与已打开后端不一致"
        );
        self.object_store.validate_layout()?;
        self.metadata_store.validate_layout()?;
        Ok(())
    }
}

fn valid_published_transaction_name(name: &str) -> bool {
    ["checkout-", "rm-"].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .is_some_and(|suffix| !suffix.is_empty())
    })
}
