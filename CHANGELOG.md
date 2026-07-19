# Changelog

本项目的所有重要变更都会记录在此文件中。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- 初始化 Cargo Workspace、多模块核心库和命令行程序。
- 增加测试、持续集成与开源协作模板。
- 增加 `rm`、`status`、`log`、`show`、`recover` 和 `fsck` 本地命令。
- 增加 `diff`、`restore` 和 `gc` 本地命令：支持文件/Chunk 级比较、暂存区与工作区恢复，
  以及回收不再被 index 或已保存 Tree 引用的 loose Chunk 对象。
- 增加 `add -A [PATH]`，支持按路径范围暂存删除。
- 增加 checkout/rm 持久 journal、故障恢复和进程退出故障注入测试。
- 增加分页 `MetadataStore`、单次流式发布的独立 FileManifest、Index version transaction、
  Tree reader/writer、ref CAS 和可复用后端契约测试。
- 增加行式 `SqliteMetadataStore` 并作为新仓库默认后端；保留可显式选择的 JSON 后端。
- 元数据契约、JSON/SQLite 后端、契约测试和逐操作文档集中到独立 `local/metadata` 模块。
- 增加流式 `ObjectStore`、真实 `LooseObjectStore`、抽象切块入口及对象后端契约测试。
- `add` 与 `gc` 增加独立对象发布锁，避免对象在 index 发布前被并发回收。
- `checkout --read-only DIR` 可将 Commit 原子导出到仓库外的 Unix 只读快照目录；新增
  `.neoengramignore` 并让 `add`/`status` 共享忽略规则。
- `fsck` 使用有界外部 Chunk 标记归并，避免为对象完整性检查长期保留完整 Chunk Hash 集合。
- 新增 [`docs/implementation-plan.md`](docs/implementation-plan.md)，集中维护当前能力、分布式
  控制面路线、研究事项、验收标准和后续路线变更。
- `repository.json` 格式 4 显式记录 `metadata_store` 和 `object_store`，并支持 attached/direct
  HEAD；开发期不兼容旧格式。
- SQLite 元数据 schema 升级为 2，结构化保存 symbolic/direct HEAD，并提供 HEAD CAS。
- 增加共享/独占 `worktree.lock`，并固定 object -> worktree -> state 的跨进程锁顺序。
- 两个 crate 补齐 crates.io 元数据、包内 MIT/Apache-2.0 许可证，并在 CI 检查 `.crate` 中的
  README、双许可证及许可证副本一致性。

### Changed

- Workspace 源码统一收拢到 `crates/`：核心库迁入 `crates/neoengram-core`，命令行 crate
  改为 `crates/neoengram`，并按 `cli`、`app`、`local/{repository,worktree,metadata,objects,fs}`
  划分职责。
- `neoengram-core` 的模型拆分到 `models/*`；切块入口迁入 `local/worktree/import.rs`，对象存储
  契约与 loose 后端迁入 `local/objects/{contract.rs,loose.rs}`，文件系统实现归入本地仓库层，
  为后续远端同步和服务端存储适配保留清晰边界。
- `add` 先创建稳定 Reflink/复制快照再 mmap，避免并发截断导致 SIGBUS 或混合快照。
- Commit 在发布 Tree 前校验全部暂存 Chunk；checkout 只物化实际变化文件。
- 仓库写锁改为 OS advisory lock，并同步对象、元数据和 rename 的持久化边界。
- 仓库路径固定为 UTF-8 NFC，并校验 Windows 保留名、大小写与文件/目录冲突。
- Repository 和 `fsck` 改为通过分页存储接口访问 Index、Tree、Commit、refs 和 Chunk，
  不再直接扫描 JSON 或对象目录。
- `add` 通过 ObjectStore 流式发布对象；checkout 单遍校验并组装文件；Commit ref 使用
  expected-parent CAS，拒绝覆盖并发提交。
- `checkout <COMMIT_ID>` 采用 Git 式 detached HEAD；`checkout main` 重新附着默认分支，
  detached Commit 的提交只推进 direct HEAD。
- `add` 从初始 `IndexVersion` 计算并以 CAS 发布；status/diff 在输出前复核实际依赖的 Index
  与 HEAD/main，拒绝并发变化产生的混合结果。
- checkout/rm 在首次工作区 mutation 前先原子发布并同步正式事务；遗留 draft 只由后续独占
  写操作或 recover 在验证后清理。
- README 快速开始改为源码安装后在独立 demo 目录试用，并明确忽略规则、平台、备份和开发期
  仓库格式边界。

### Fixed

- 新仓库改为在私有临时目录中完整初始化后原子发布；失败或退出不再暴露会锁定后端选择的
  半初始化 `.neoengram`。
- 已有同长度损坏对象不再被 `add` 接受。
- 缺失或损坏 Chunk 不再能进入新 Commit。
- checkout 支持文件与目录类型互换，并拒绝父级符号链接和路径逃逸。
- 被 kill 后遗留的 lock 文件不再永久阻塞仓库。
- `recover` 对 index 尚未发布的 rm 事务如实报告“已回滚”而不是“已恢复”，并说明 rm
  没有续完路径；checkout 恢复完成时提示重放使用了 `--force` 语义。
- JSON 后端的 Snapshot 在同一把共享 ref 锁内捕获 HEAD、refs 和全部历史对象 ID，
  消除创建瞬间的非单点视图。
- HEAD compare-exchange 与 HEAD 读取按契约对引用名和 Commit ID 执行 `trim()`。
- checkout 在真正 rename 每个文件前按 rm 同等标准二次校验工作区内容，未暂存修改
  不再可能被静默备份后随事务清理丢弃。
- checkout/rm/status 共用 `worktree/workspace.rs` 的路径与内容校验原语，消除三份
  重复实现之间的语义漂移。
- rm 在 rename syscall 成功后立即记录已移动文件；即使随后的目录同步失败也能恢复唯一备份，
  无法证明安全时保留事务供 recover 处理。
- 工作区 restore 在最终发布前重新检查目标；无 `--force` 时不再覆盖预检后出现或变化的文件，
  且目录、符号链接和特殊文件始终拒绝替换。
- checkout 的嵌套 staging/backup/rollback 目录逐级持久化并拒绝父级符号链接；原路径和备份
  同时缺失时明确报错并保留事务。
- checkout/rm 的空目标发布与回滚改用 no-replace rename，外部程序在最终检查后创建的文件
  不再被静默覆盖；file-to-directory 崩溃恢复正确识别被父文件备份遮蔽的子路径。
- Checkout 恢复只把明确的 `NotFound` 当作 staged 缺失，权限和 I/O 错误不再导致外部文件被
  当成事务产物删除；reset 会先清空并同步旧 journal 证据，再清理 staged/backup。
- 已完成事务先原子退役为 cleanup draft 再递归删除，避免权限错误把正式 journal 部分删坏；
  Windows 的原子文件和目录项发布使用 write-through rename。
