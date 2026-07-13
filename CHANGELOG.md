# Changelog

本项目的所有重要变更都会记录在此文件中。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- 初始化 Cargo Workspace、多模块核心库和命令行程序。
- 增加测试、持续集成与开源协作模板。
- 增加 `rm`、`status`、`log`、`show`、`recover` 和 `fsck` 本地命令。
- 增加 `add -A [PATH]`，支持按路径范围暂存删除。
- 增加 checkout/rm 持久 journal、故障恢复和进程退出故障注入测试。
- 增加分页 `MetadataStore`、单次流式发布的独立 FileManifest、Index version transaction、
  Tree reader/writer、ref CAS 和可复用后端契约测试。
- 增加行式 `SqliteMetadataStore` 并作为新仓库默认后端；保留可显式选择的 JSON 后端。
- 元数据契约、JSON/SQLite 后端、契约测试和逐操作文档集中到独立 `storage/metadata` 模块。
- 增加流式 `ObjectStore`、真实 `LooseObjectStore`、抽象切块入口及对象后端契约测试。
- `repository.json` 格式 3 显式记录 `metadata_store` 和 `object_store`；开发期不兼容旧格式。

### Changed

- `add` 先创建稳定 Reflink/复制快照再 mmap，避免并发截断导致 SIGBUS 或混合快照。
- Commit 在发布 Tree 前校验全部暂存 Chunk；checkout 只物化实际变化文件。
- 仓库写锁改为 OS advisory lock，并同步对象、元数据和 rename 的持久化边界。
- 仓库路径固定为 UTF-8 NFC，并校验 Windows 保留名、大小写与文件/目录冲突。
- Repository 和 `fsck` 改为通过分页存储接口访问 Index、Tree、Commit、refs 和 Chunk，
  不再直接扫描 JSON 或对象目录。
- `add` 通过 ObjectStore 流式发布对象；checkout 单遍校验并组装文件；Commit ref 使用
  expected-parent CAS，拒绝覆盖并发提交。

### Fixed

- 新仓库改为在私有临时目录中完整初始化后原子发布；失败或退出不再暴露会锁定后端选择的
  半初始化 `.neoengram`。
- 已有同长度损坏对象不再被 `add` 接受。
- 缺失或损坏 Chunk 不再能进入新 Commit。
- checkout 支持文件与目录类型互换，并拒绝父级符号链接和路径逃逸。
- 被 kill 后遗留的 lock 文件不再永久阻塞仓库。
