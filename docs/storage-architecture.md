# format v8 存储架构

NeoEngram `0.2.0` 的本地仓库格式是 v8。开发期升级允许破坏兼容性：实现明确拒绝 v7 和其他旧
格式，不读取、不迁移，也不提供自动回退。format v8 将可移植内容模型和规范 digest 收敛到
`neoengram-core`，把本地 SQLite/文件系统实现留在 `neoengram-standalone` 与 `neoengram-fs`。

## 内容图与规范身份

```text
refs / Workspace HEAD/base -> Commit(root_directory_id) -> root Directory
                                      |                     +-> child Directory
                                      +------------------------> File -> Manifest -> ordered Objects

Workspace IndexVersion -> paged FileRecord
                       -> bounded IndexDelta { Upsert, Delete }
```

`ObjectId`、`ManifestId`、`DirectoryId`、`CommitId` 和 `ContentDigest` 是不可互换的强类型。
`Commit` 使用 `root_directory_id`，不再保存 `tree_hash`。core 公共模型不包含扁平 `Tree`、
`FileNode` 或物化 `Index`；Standalone 中暂存的 compatibility view 仅服务于 SQLite/worktree 渐进
迁移，不能作为持久格式或跨 crate API。

对象 ID 继续使用既有有效内容域。Manifest、Directory 和 Commit 使用带 domain/version 的规范
二进制编码；IndexVersion、Index snapshot 与 IndexDelta 使用 v8 新增的后端无关规范算法。路径
统一表示为 NFC、`/` 分隔的 `LogicalPath`/`PathComponent`，并拒绝 Windows drive/UNC 前缀、保留
设备名、非法组件、大小写冲突及文件/目录前缀冲突。规范编码和 golden vectors 的唯一实现都在
core，SQLite、Agent 和中心不得各自重写 hash 算法。

Chunk/Object ID 仍是原始 payload 的 BLAKE3。Manifest 将 `FastCdc`/`WholeFile` 策略纳入规范编码，
因此相同字节使用不同策略时仍可得到不同 Manifest。Directory 只包含直接子项，递归统计不参与 ID。

`repository.json` 持久化不可变的 `fastcdc`、`whole-file` 或 `mixed` 仓库策略。前两种要求全部
Index 和历史 Manifest 使用对应策略；`mixed` 才允许逐文件选择。Repository 在 Index 发布、
Directory 构造/读取、Commit 发布和 fsck 遍历时重复检查该约束。

## Standalone 存储职责

Standalone 的逻辑持久化边界拆分为：

- `ImmutableCatalogStore`：不可变 Manifest、Directory 与 Commit 图；
- `WorkspaceIndexStore`：分页 Index、IndexVersion 与 expected-version CAS；
- `RefStore`：HEAD/ref 读取与 compare-exchange；
- `WorkspaceRegistry`：Repository discovery、Workspace 身份与布局。

format v8 的真实元数据适配器仍是 SQLite。现有实现内部可共享连接、事务和迁移代码，但上层用例
不能再把所有职责当成单个跨运行模式的 `MetadataStore`；未来 PostgreSQL 也不会作为本地 store
枚举值塞入 Standalone。

SQLite 使用 WAL、foreign key、`synchronous=FULL` 和即时写事务。Manifest 与 Directory 按 ordinal
分页，Index 使用 keyset 分页与 expected-version transaction，HEAD/ref 使用 CAS。Directory writer
的 staging 批次受条数和字节上限约束；Commit 分页读取 Index 并维护目录路径栈，结束一个目录时
发布其 ID，再追加到父 writer，不创建公共扁平 Tree。Reader 不跨 FUSE 请求长期持有 transaction。

## ObjectStore 与耐久权威

Standalone 的 `LooseObjectStore` 通过流式写入校验并不可变发布对象，通过单遍读取复算大小与
BLAKE3。元数据引用对象前必须完成 durability barrier。GC 在 object lock 和 state lock 下从全部
Workspace Index 与全部 Commit roots 标记对象，再删除未标记对象；当前所有 Commit 都是本地 GC root。

Managed 模式使用不同的权威边界：

- 中心 S3-compatible object store 是不可变对象的耐久权威；
- NFS 只保存 Playground 文件、durable mutation journal 和可重建 cache/staging；
- NFS 上不保存托管 Artifact SQLite，也不把对象目录或 Agent cache 当成 durability 证明；
- Agent 的本地 Ledger/candidate/cache 只支持执行与恢复，不拥有中心 Index 或对象最终状态；
- 当前 P0 只定义 `ObjectTransfer`/`ObjectCatalog` ports、票据 DTO、receipt 和内存适配器，尚未接入
  真实 S3 SDK、NFS fencing 或生产数据库。

Managed Add 的固定发布闭环为：

```text
PrepareAdd
-> missing-object negotiation
-> Agent 使用短期 upload ticket 直传中心 S3
-> 中心验证并记录 object durability
-> Agent 持久化含 exact descriptors/pages 的 TransferReceipt 与 Prepared 状态
-> Agent 上报 descriptor-bound JobPrepared，中心先持久化报告
-> Agent 幂等上传 Manifest / ObjectReceipt / IndexDelta MetadataBatch
-> 中心 staging 完整校验并重算 publication digest
-> canonical Manifests + expected IndexVersion CAS 原子发布
-> decision / finalized 幂等确认
```

对象 payload 不经过 `neoengramd` API 进程。只有所有所需对象已由中心标记 Durable、批次页完整且
digest/资源 scope/base version 全部通过，中心才可执行 CAS；上传完成或 Agent receipt 本身都不能
提前发布 Index。`JobPrepared.publication_digest` 绑定 scope/base/IndexDelta/Manifest/ObjectSpec，
`candidate_digest` 再覆盖 assignment identity、result/publication digest、ordered MetadataBatch
descriptors 与 extensions；替换一个仍然有效的 publication 或 descriptor 都会使校验失败。
Manifest 通过带十进制 `chunk_start` 的 fragment 跨页；单页只校验局部连续性，中心按 Manifest ID 和
chunk ordinal 重组，拒绝缺口、重复和 metadata 变化，再复算完整 canonical Manifest ID。成功
publisher 在同一原子边界持久化这些 canonical Manifests 与新 IndexVersion，因此 staging TTL 清理后
Index 仍可解析；Conflict/Rejected 不发布候选 Manifest。

## 一致性、锁与 mutation

Standalone 锁顺序固定为 `objects.lock -> Workspace worktree.lock -> write.lock`。Index transaction
与 HEAD/ref CAS 提供线性化点；Commit 先发布 Object/Manifest/Directory/Commit，最后 CAS 更新
HEAD/ref。失败可留下不可达不可变元数据，但不能发布缺失依赖的引用。

Engine 的统一 mutation 契约与 `neoengram-fs` adapters 遵循
`MutationPlan -> durable journal -> WorktreeReceipt`：journal 必须在第一次工作区 mutation 前原子
发布并同步，文件系统适配器只返回 receipt，不隐式更新权威 Index、HEAD 或 ref。

Standalone 的 `checkout`、工作区 `restore` 和工作区 `rm` 已把 format-v8 durable transaction 包装成
Engine `Worktree` adapter：Engine journal 先进入 durable `Applying`，本地事务随后执行工作区变更并
返回 `WorktreeReceipt`。`rm --cached` 和 `restore --staged` 不修改工作区，因此只走权威元数据路径。

checkout/rm 从 plan 携带 expected `IndexVersion`，在 receipt 之后通过 SQLite CAS 发布 Index/HEAD，
只有 CAS 成功才调用 `finalize_mutation`；worktree restore 不发布 Index，工作区 replacement 成功后
直接 finalize，`restore --staged` 则走独立的 SQLite Index 更新路径。`recover` 会恢复或回滚本地
checkout/rm/restore transaction，枚举并清理 active/finalized Engine journal 与 stale operation lock。
失败或权威状态无法判定时必须保留 journal 并要求显式恢复。Managed publisher 仍由中心状态机完成
决策，文件系统适配器不能暗中替代它。

`export --mode hardlink` 仅适用于 WholeFile + LooseObjectStore 同文件系统：实现复算对象大小和
BLAKE3，并校验设备号/inode 后 no-replace 发布。它与仓库对象共享 inode，只适用于可信本地用户，
不构成不可变安全边界，Managed S3/NFS 模式不使用该能力。

## FUSE 读取路径

挂载时 TARGET 只解析一次，session 保存 Commit 时间和根 `DirectoryId`。请求流程为：

```text
lookup(name) -> DirectoryReader::get_entry
readdir(cookie) -> DirectoryReader::scan_entries(after ordinal)
read(offset,size) -> Manifest range query -> verified object LRU -> reply slice
```

root inode 为 1；其他 inode 从 Commit ID、逻辑路径、kind 和 salt 派生 63-bit BLAKE3。活动表检测
碰撞，lookup/forget/open/release 控制生命周期。缓存按实际字节计费、线程安全、single-flight；
读取缺失、大小冲突、Hash 损坏和 recipe 空洞都映射为 `EIO`。FUSE 当前属于 Standalone，本阶段
不支持远端按需下载或可写 overlay。

## 平台与规模边界

- Linux：`fuser` pure Rust FUSE3，运行时使用 `fusermount3 -u`。
- macOS：`fuser` + macFUSE SDK/runtime，使用 `/sbin/umount`。
- Windows：常规仓库命令可构建，挂载命令返回 unsupported/未启用。

FUSE、Commit Directory 构造和 Manifest range read 已分页；Standalone 的部分 compatibility view 和
GC 可达集合仍可能随仓库规模增长。后续需继续迁移到 engine 分页 ports，并在 Linux/macOS 实挂
环境记录启动时间、RSS、目录分页、冷热随机读和顺序吞吐。
