# 当前仓库存储架构

> 本文同时记录当前 Standalone/Managed 存储实现和已确认的 NeoEngram Gateway 目标边界。
> Gateway Registry、管理面、H2/mTLS 控制 tunnel 和一跳 Replica forwarding 已进入 G1；固定 Ready Snapshot 的
> 一期只读 S3 Access Point、Gateway Web/S3 listener 和 Agent 流式读取已经实现，公网 DNS/TLS、生产凭据
> provisioner 与真实双 Replica 故障演练仍属于部署验收项。Gateway 也没有任何 Volume/CAS I/O 能力；其专项设计见
> [`gateway.md`](gateway.md)。

关于 Commit 对象分散在多个 Volume、按对象建立覆盖并由多个来源物化到目标 Volume 的 v2 调研与目标设计，见
[`commit-materialization-v2.md`](commit-materialization-v2.md)。当前 v2 已有 Domain、Authority、Central planner/API、
Agent source/target executor 和 Gateway relay 纵切及契约测试，但仍处迁移中：v1 replication/PlacementSet 私有
实现残留与 v2 代码并存，旧 API/wire/capability 不再注册或接受；真实跨 Gateway route、三 Agent 多源失败恢复和
生产 payload E2E 尚未验收。下文凡标注 legacy 的完整 PlacementSet/单源 Ticket 均不代表 v2 主链路。

NeoEngram `0.2.0` 的本地仓库格式为 9。升级允许破坏兼容性：实现明确拒绝所有旧
格式，不读取、不迁移，也不提供自动回退。仓库格式 9 将可移植内容模型和规范 digest 收敛到
`neoengram-domain`，本地 SQLite/文件系统实现统一留在 `neoengram-runtime`。

## 内容图与规范身份

```text
refs / Workspace HEAD/base -> Commit(root_directory_id) -> root Directory
                                      |                     +-> child Directory
                                      +------------------------> File -> Manifest -> ordered Objects

Workspace IndexVersion -> paged FileRecord
                       -> bounded IndexDelta { Upsert, Delete }
```

`ObjectId`、`ManifestId`、`DirectoryId`、`CommitId` 和 `ContentDigest` 是不可互换的强类型。
`Commit` 使用 `root_directory_id`，不再保存 `tree_hash`。core 公共模型不包含扁平兼容 snapshot、
`WorkspaceFileRecord` 或物化 `WorkspaceIndex`；Standalone 只在 SQLite/worktree 文件系统边界按页读取并直接消费
文件记录，不将其作为持久格式或跨 crate API。

对象 ID 继续使用既有有效内容域。Manifest、Directory 和 Commit 使用带 domain/version 的规范
二进制编码；IndexVersion、Index snapshot 与 IndexDelta 使用当前后端无关规范算法。路径
统一表示为 NFC、`/` 分隔的 `LogicalPath`/`PathComponent`，并拒绝 Windows drive/UNC 前缀、保留
设备名、非法组件、大小写冲突及文件/目录前缀冲突。规范编码和 golden vectors 的唯一实现都在
core，SQLite、Agent 和中心不得各自重写 hash 算法。

Chunk/Object ID 仍是原始 payload 的 BLAKE3。Manifest 将 `FastCdc`/`WholeFile` 策略纳入规范编码，
因此相同字节使用不同策略时仍可得到不同 Manifest。Directory 只包含直接子项，递归统计不参与 ID。

`metadata.sqlite3` 持久化格式身份、repository ID、对象后端和不可变的 `fastcdc`、`whole-file` 或 `mixed` 仓库策略。前两种要求全部
Index 和历史 Manifest 使用对应策略；`mixed` 才允许逐文件选择。Repository 在 Index 发布、
Directory 构造/读取、Commit 发布和 fsck 遍历时重复检查该约束。

## Standalone 存储职责

Standalone 的逻辑持久化边界拆分为：

- `ImmutableCatalogStore`：不可变 Manifest、Directory 与 Commit 图；
- `WorkspaceIndexStore`：分页 Index、IndexVersion 与 expected-version CAS；
- `RefStore`：HEAD/ref 读取与 compare-exchange；
- `WorkspaceRegistry`：Repository discovery、Workspace 身份与布局。

仓库格式 9 的真实元数据适配器仍是 SQLite。现有实现内部共享连接和事务，但上层用例
不能再把所有职责当成单个跨运行模式的 `MetadataStore`；未来 PostgreSQL 也不会作为本地 store
枚举值塞入 Standalone。

SQLite 使用 WAL、foreign key、`synchronous=FULL` 和即时写事务。Manifest 与 Directory 按 ordinal
分页，Index 使用 keyset 分页与 expected-version transaction，HEAD/ref 使用 CAS。Directory writer
的 staging 批次受条数和字节上限约束；Commit 分页读取 Index 并维护目录路径栈，结束一个目录时
发布其 ID，再追加到父 writer，不创建公共扁平兼容 snapshot。Reader 不跨 FUSE 请求长期持有 transaction。

## Managed AuthorityStore

Managed 中心的逻辑权威通过异步 `AuthorityStore` 组合统一 `TaskRepository`（OperationTask 生命周期/审计）与
领域 `JobRepository`、`AssignmentOutbox`、
`MetadataBatchStager`、`ObjectCatalog`、`IndexPublisher` 和 `AuditSink`，不绑定具体数据库。
默认后端是单节点生产可用的 SQLite：显式目录内固定创建独立 `authority.sqlite3` 和生命周期独占
`authority.lock`，连接池固定单连接，并启用 WAL、foreign keys、`synchronous=FULL` 和 busy timeout。

该数据库不属于 Standalone 仓库格式 9，也不得与 Standalone 共用文件。它只接受当前
`application_id`/`user_version=20` 和当前 JSON record format；旧格式、错误 schema 和未知非空数据库
直接拒绝，不提供 migration、双读、字段别名或回退。所有租户查询和复合键都包含 tenant ID，但
SQLite 仍是应用层隔离，不伪装成数据库级 RLS、HA 或多进程后端。

未来 PostgreSQL/MySQL 适配器只共享 ports、capabilities 和行为契约；SQL、migration、物理 schema、
JSON、锁、CAS 与 RLS 设计均由各后端独立拥有。PostgreSQL 仍是多实例、HA 和数据库级 RLS 的目标。

## ObjectStore 与耐久权威

Standalone 的 `LooseObjectStore` 通过流式写入校验并不可变发布对象，通过单遍读取复算大小与
BLAKE3。元数据引用对象前必须完成 durability barrier。GC 在 object lock 和 state lock 下从全部
Workspace Index 与全部 Commit roots 标记对象，再删除未标记对象；当前所有 Commit 都是本地 GC root。

Managed 模式使用不同的权威边界：

- Artifact、Commit、Manifest、Index 和 Ref 由 Server metadata authority 管理；
- 不可变 Chunk 字节由 Artifact 所在的用户 StorageVolume 持有，布局为
  `<mount>/.neoengram/objects/tenants/<tenant>/artifacts/<artifact>/objects/<object_id>`；
- Agent 在获批 mount 内完成流式写入、size/BLAKE3 复核、`fsync` 和原子发布；Server 不接收、
  不代理也不保存 Chunk payload；
- Server `ObjectCatalog` 只权威保存经过身份验证的 Volume placement evidence，并将其绑定到
  Assignment 的 Tenant、Artifact、StorageVolume、ArtifactPlacement 和 `placement_generation`；
- Agent 的 identity、Ledger、outbound 和 candidate 位于独立 `state_dir`，不得在业务 Volume 上创建
  SQLite/WAL；状态盘丢失不会删除 Volume 中的业务对象；
- 当前不承诺跨 Volume 对象复制的生产执行、强 storage-side fencing、NeoEngram Gateway payload 数据链
  或生产数据库。v1 replication 的 Central route/ticket 控制链和 Agent/Gateway 协议边界仅作为私有迁移残留；旧
  API、旧 ALPN/capability 和旧 Ticket 不再接受。v2
  `MaterializationJob`/多源 planner、ObjectPlacement、Coverage、Ticket/Receipt/Lease 有代码和契约测试。
  Agent 只有在 replication 配置启用且 Gateway QUIC 预检成功后才声明 `commit_materialization_v2`；当前 Gateway
  生产 listener 使用 `neoengram-transfer-v2`、拒绝旧 v1 ticket，并可通过有界 relay 转发 v2 frame。目标复制链路
  固定为源 Agent -> 源 GatewayPool -> 目标 GatewayPool -> 目标 Agent；Server 仍只下发计划和记录凭证，不进入
  payload 路径。真实 upstream route、凭据和跨节点 E2E 仍待验收。

### v2 对象级事实与 Coverage

v2 将 Commit 与物理 Volume 解耦。Commit 只引用不可变的 namespace-scoped ObjectSet；初期
`ObjectNamespaceId == ArtifactId`，但所有对象、Placement、Job、Lease、Ticket 和 authority key 都必须显式带
namespace。一个对象的耐久事实是 `ObjectPlacement`：它绑定 tenant、namespace、object、size、encoding、BLAKE3
digest、一个 StorageVolume/archive、placement generation、failure domain 和状态。只有 Agent 完成校验、`fsync`/
durability barrier 并提交幂等 receipt 后，Placement 才能进入 `verified`。

`VolumeCommitCoverage` 是从目标 Volume 上的 verified ObjectPlacement 重新计算的 `partial/complete` 摘要，不是
可独立信任的对象目录。目标容量按 `missing_bytes + staging_reserve + concurrency_reserve` 估算；切换 source、route
或 batch attempt 不能改变 `(materialization_id, namespace, object_id)` staging key 或 confirmed offset。Workspace、
SnapshotDelivery 和 S3 只有在 Coverage 完整且视图校验通过后才能 Ready；全局对象并集完整不等于任一 Volume 可读。

当前 authority schema 为 `user_version=20`，已安装 v2 表（含 `object_placements`、Coverage、Materialization、OperationTask、
object-read/staging lease），并通过 InMemory/SQLite 的 CAS、幂等 Receipt 和重规划 checkpoint 契约。v1
`commit_placement_sets`、`replications`、`replication_objects` 和旧 ObjectCatalog/Assignment mapper 尚未删除，
因此该 schema 仍是开发阶段 clean-slate；v19 及更早 authority 数据库与 v20 DDL 不兼容，升级不隐式删除或迁移，
clean-slate reset/inventory rebuild 必须由显式运维动作完成。

Managed Add 的固定发布闭环为：

```text
PrepareAdd
-> Agent 将新 Chunk 写入 Assignment 指定的 Volume-local CAS
-> Agent 逐对象复核 size/BLAKE3，执行 durability barrier
-> Agent 持久化含 exact descriptors/pages 的 TransferReceipt 与 Prepared 状态
-> Agent 上报 descriptor-bound JobPrepared，中心先持久化报告
-> Agent 幂等上传 Manifest / ObjectReceipt / IndexDelta MetadataBatch
-> 中心 staging 完整校验，绑定 Volume placement evidence 并重算 publication digest
-> canonical Manifests + expected IndexVersion CAS 原子发布
-> decision / finalized 幂等确认
```

对象 payload 不经过 Central/Gateway 控制 listener 或 `neoengram-central` API 进程。只有所有所需对象均有当前 Assignment
精确 Volume/placement generation 的有效凭证、批次页完整且 digest/资源 scope/base version 全部通过，
中心才可执行 CAS。`ObjectReceipt` 是已审批 Agent 对 Volume 中指定对象执行完整性与耐久化
校验的证据，不包含物理路径或 payload。v2 `ObjectPlacement` 是同一类对象级 Durable 事实，必须带
`ObjectNamespaceId`、size、encoding、digest、Volume 和 generation；`VolumeCommitCoverage` 只由这些证据派生，
不能替代逐对象校验。`JobPrepared.publication_digest` 绑定
scope/base/IndexDelta/Manifest/ObjectSpec，
`candidate_digest` 再覆盖 assignment identity、result/publication digest、ordered MetadataBatch
descriptors 与 extensions；替换一个仍然有效的 publication 或 descriptor 都会使校验失败。
Manifest 通过带十进制 `chunk_start` 的 fragment 跨页；单页只校验局部连续性，中心按 Manifest ID 和
chunk ordinal 重组，拒绝缺口、重复和 metadata 变化，再复算完整 canonical Manifest ID。成功
publisher 在同一原子边界持久化这些 canonical Manifests 与新 IndexVersion，因此 staging TTL 清理后
Index 仍可解析；Conflict/Rejected 不发布候选 Manifest。

### NeoEngram Gateway 存储边界

2026-08-09 已确认每个 EdgeCluster 使用一个多副本 GatewayPool 作为控制入口和后续数据/S3 入口。
这项网络拓扑调整不改变任何存储权威：

- Gateway Replica 不挂载业务 PVC、NFS export 或 StorageVolume；
- Gateway 不读取或写入 Volume CAS、Playground、journal、Agent Ledger 或 candidate database；
- 所有 Manifest 驱动的 Chunk I/O、Hash 复核、`fsync`、durability barrier 和原子发布仍在 Volume
  Owner Agent 执行；
- Gateway 的多副本和跨 Replica forwarding 只提供入口可用性，不构成对象或 metadata 副本；
- Gateway 可以流式转发后续的跨集群对象和只读 S3 响应，但不得把 payload 缓存为可恢复业务副本；
- legacy `ObjectCatalog` 与 v2 placement authority 只接受 Agent 的签名 placement evidence/receipt，不能用“流量
  经过 Gateway”替代 Volume durability 证明；
- GatewayPool 整体不可用时，控制/传输/S3 失败关闭，本地 Volume 数据和已完成 durability 的对象不受损。

当前只读 S3 是 Gateway 暴露固定 Commit/Snapshot 的访问协议，不是 `ObjectStoreKind`、中心归档后端或
新的 durability authority。S3 `LIST` 查询 Central metadata，`GET`/Range 由 owning Agent 根据 Manifest
读取 Volume CAS；内部对象目录不映射为公开 Bucket 或 Key。v2 实际调用要求目标 Volume 的完整
`VolumeCommitCoverage`、视图校验、GatewayPool、Agent route、signed ticket 和生产凭据；历史 v1
PlacementSet 检查不构成当前 Ready 依据，不能把 partial Coverage 当作可读。

## 一致性、锁与 mutation

Standalone 锁顺序固定为 `objects.lock -> Workspace worktree.lock -> write.lock`。Index transaction
与 HEAD/ref CAS 提供线性化点；Commit 先发布 Object/Manifest/Directory/Commit，最后 CAS 更新
HEAD/ref。失败可留下不可达不可变元数据，但不能发布缺失依赖的引用。

Engine 的统一 mutation 契约与 `neoengram-runtime` adapters 遵循
`MutationPlan -> durable journal -> WorktreeReceipt`：journal 必须在第一次工作区 mutation 前原子
发布并同步，文件系统适配器只返回 receipt，不隐式更新权威 Index、HEAD 或 ref。

Standalone 的 `checkout`、工作区 `restore` 和工作区 `rm` 已把仓库格式 9 的 durable transaction 包装成
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
不构成不可变安全边界，Managed Volume 模式当前不使用该导出能力。

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

FUSE、Commit Directory 构造和 Manifest range read 已分页；Standalone 的部分 workspace snapshot 和
GC 可达集合仍可能随仓库规模增长。后续需继续迁移到 engine 分页 ports，并在 Linux/macOS 实挂
环境记录启动时间、RSS、目录分页、冷热随机读和顺序吞吐。
