//! 持久化边界的模块入口。

pub(crate) mod file;
pub(crate) mod metadata;
pub(crate) mod object;

/// 原子发布使用的保留临时文件前缀；逻辑存储键也不能占用这个命名空间。
pub(crate) const STORAGE_TEMP_FILE_PREFIX: &str = ".synapse-tmp-";
