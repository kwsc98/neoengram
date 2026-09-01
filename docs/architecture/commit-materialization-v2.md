# Commit 分布式对象物化 v2 调研报告

> 状态：调研结论与目标设计；v2 Domain/Authority/Central planner 以及 Agent/Gateway 本地 payload
> executor 纵切已观察，真实跨节点数据面与生产 E2E 尚未验收
>
> 核验日期：2026-09-01
>
> 本报告只讨论 Managed 模式的 Commit 对象分布、跨 Volume 物化和相关控制协议。它不修改当前实现
> 的能力状态；v1 replication 仍以单个完整 `CommitPlacementSet` 作为 legacy 复制前置条件。v2 已有对象级
> 模型、Authority CAS/幂等、Central planner/API、Agent/Gateway executor、staging/receipt 数据面纵切，
> 但在真实 route 编排和跨节点 E2E 通过前，不能把 v2 写成已完成复制能力。

## 1. 结论摘要

当前模型把“Commit 副本”定义成一个 Volume 上完整的 Commit 对象集。这无法表达以下合法场景：

```text
Volume A: object-1, object-2
Volume B: object-3, object-4
全局并集: 覆盖完整 Commit，但没有任何一个 Volume 是完整副本
```

建议在开发阶段直接切换到 clean-slate v2：

1. Commit 保持纯逻辑、不可变的 ObjectSet 引用，不绑定磁盘。
2. “副本”降为对象级 `ObjectPlacement`，表示一个对象在一个 Volume/generation 上已校验且 Durable 的物理事实。
3. 新增 `VolumeCommitCoverage`，表示某个 Volume 对某个 Commit 的 `partial` 或 `complete` 覆盖；它是可重算索引，不是耐久事实。
4. 用一个面向目标 Volume 的 `MaterializationJob` 取代单源 `ReplicationRecord`；Job 内部包含多个按数据源拆分的 `MaterializationBatch`。
5. 全局可读性、耐久性、目标覆盖率和本地视图 Ready 分开计算。
6. 目标 Volume 只有在对象完整覆盖、校验和视图物化完成后，才能让 Workspace、SnapshotDelivery 或本地 S3 读取进入 Ready。
7. v2 不保留 v1 replication/placement 双读或 nullable 兼容字段；旧数据库、旧 Agent state、旧 ALPN 和旧 Ticket 直接拒绝。

## 2. 调研范围与非目标

### 2.1 本次要解决的问题

- 单个 Commit 的对象可以分散在多个 Volume；
- 新目标可以从多个 Volume 并行拉取缺失对象；
- 已存在的目标对象不重复传输；
- 某个源 Agent 断线后，未完成对象可以切换备用源并从目标检查点继续；
- 多个相同请求不会产生多个目标物化任务；
- GC 不会在源对象仍被传输时回收最后一份可读副本；
- API 能准确区分“全局可恢复”和“目标 Volume 已完整”。

### 2.2 明确不在第一阶段实现

- 多个 Volume 的 POSIX/FUSE 联合按需读取；
- 把 Gateway 变成对象缓存或对象权威；
- PostgreSQL HA、自动跨区域重平衡和复杂纠删码；
- 跨 Artifact 的隐式对象共享；
- 让 partial Coverage 直接作为 Workspace 或 SnapshotDelivery 的可读视图。

第一阶段仍采用“先物化到目标 Volume，再提供本地视图”的边界。联合按需读取需要另一个带缓存、一致性和远程失败语义的产品设计。

## 3. 当前实现证据

以下结论是从当前源码、持久化 schema 和测试入口反推的“已观察”事实：

| 当前事实 | 证据 | 影响 |
| --- | --- | --- |
| `CommitObjectSet` 保存 Commit 的完整对象清单 | [`placement.rs`](../../crates/neoengram-domain/src/protocol/placement.rs) 的 `CommitObjectSet`/`ObjectSet` | 逻辑对象集合本身可以继续复用 |
| `CommitPlacementSet::Published` 要求包含全部对象 | [`placement.rs`](../../crates/neoengram-domain/src/protocol/placement.rs) 的 `CommitPlacementSet::validate` | 不能表示单 Volume partial 覆盖 |
| 复制源只选择完整 Published PlacementSet | [`service/placement.rs`](../../services/neoengram-central/src/service/placement.rs) 的 `source_placement_for_replication` | 多个 partial 源的并集不会被选中 |
| 一个复制记录固定一个 source 和一个 target | [`placement_authority.rs`](../../services/neoengram-central/src/placement_authority.rs) 的 `ReplicationRecord` | 无法在同一个目标任务内切换多个源 |
| Ticket 只有一个 source endpoint 和一个 allowed object 列表 | [`transfer.rs`](../../crates/neoengram-domain/src/protocol/transfer.rs) 的 `TransferTicket` | Ticket 与多 Batch、对象级 source 绑定不匹配 |
| Agent CAS 实际按 Artifact 隔离 | [`storage.md`](storage.md) 与 Agent/runtime 的 Volume CAS 实现 | placement authority 不能只用 tenant + object hash 判断可读 |
| 当前 SQLite 表围绕完整 PlacementSet 建模 | [`authority.rs`](../../services/neoengram-central/src/datasource/sqlite/authority.rs) 的 `commit_placement_sets`、`replications`、`replication_objects` | v2 应重建表和唯一约束，而不是继续堆兼容字段 |
| 当前健康计算会统计完整 PlacementSet | [`mapper/sqlite/placement.rs`](../../services/neoengram-central/src/mapper/sqlite/placement.rs) 的 `commit_availability` | partial 并集会被误报为不可用或被忽略 |
| v2 Domain 已定义 namespace、对象 Placement、Coverage、Materialization、Manifest、Ticket/Receipt/Lease 和 availability | [`materialization.rs`](../../crates/neoengram-domain/src/protocol/materialization.rs)、schema golden 和 domain tests | 可表达对象并集和目标覆盖，但不等于数据面已经传输 |
| v2 Authority 已安装 schema 18 并覆盖对象 Receipt 幂等、Coverage 重算和 checkpoint CAS | [`authority.rs`](../../services/neoengram-central/src/datasource/sqlite/authority.rs)、[`authority_v2_schema.rs`](../../services/neoengram-central/tests/authority_v2_schema.rs)、materialization tests | v1 表/mapper 仍并存，clean-slate 删除和 inventory rebuild 尚未完成 |
| v2 Central planner/API 可按缺失对象选择多源和 fallback | [`service/materialization.rs`](../../services/neoengram-central/src/service/materialization.rs)、[`materialization_v2.rs`](../../services/neoengram-central/tests/materialization_v2.rs) | 目前是可查询控制面计划，未驱动真实 QUIC payload executor |
| v2 Agent/Gateway 具备 ALPN、Ticket/generation、manifest/checkpoint 校验、stream/relay 和 staging/receipt 执行路径 | [`materialization_quic.rs`](../../services/neoengram-agent/src/materialization_quic.rs)、Agent/Gateway transfer code、协议/单元测试 | 真实跨 Gateway route、三 Agent 多源失败恢复、背压/配额、生产凭据和跨节点 E2E 仍待验收 |

前六行代码路径证明的是 v1 当前行为；新增行证明 v2 控制面、协议和本地执行纵切。现阶段仍应把跨
Volume materialization 标记为进行中，不能因为本报告提交或单节点测试通过就改变为已完成的生产数据面能力。

## 4. v2 核心对象模型

### 4.1 分层关系

```text
Artifact / ObjectNamespace
    └── Commit
          └── CommitObjectSet（完整逻辑对象清单）

ObjectPlacement（单对象、单 Volume、单 generation 的 Durable 证据）
    └── VolumeCommitCoverage（该 Volume 对 Commit 的 partial/complete 摘要）

MaterializationJob（一个目标 Volume 的一次补齐意图）
    ├── MaterializationObject（对象级状态、检查点、主源和备用源）
    └── MaterializationBatch（按 source Agent/route 分组的内部传输批次）
```

### 4.2 `ObjectNamespaceId`

v2 必须引入独立且必填的 `ObjectNamespaceId`。初期可以规定：

```text
ObjectNamespaceId == ArtifactId
```

但协议和数据库不能省略这个字段。以下对象都必须带 namespace：

- `CommitObjectSet` 和 `ObjectRef`；
- `ObjectPlacement`、ObjectReceipt 和 Managed Add 的 placement evidence；
- `VolumeCommitCoverage`、MaterializationJob/Batch/Object；
- TransferTicket、Agent inventory、checkpoint 和 GC lease；
- 所有 Central repository key、SQLite 主键和唯一索引。

`ObjectId` 仍然表示对象内容的 BLAKE3 身份；相同 hash 不代表不同 namespace 可以互相读取。`ObjectSetDigest` 建议继续表示对象描述符集合的内容身份，namespace、Artifact 和精确对象范围另行纳入 `MaterializationPlanDigest` 与 Ticket 签名。跨 Artifact 复用必须经过显式派生、授权和物理 namespace 映射，禁止通过 hash 自动授权。

### 4.3 物理事实与覆盖索引

`ObjectPlacement` 是唯一的对象级耐久事实，至少包含：

```text
tenant_id
object_namespace_id
object_id
size / encoding / verified_digest
storage_volume_id 或 archive_id
placement_generation
state = verified | retiring | deleted | lost
failure_domain
```

每个对象只有在 Agent 完成写入、size/BLAKE3 校验、`fsync`/durability barrier 并提交可验证 receipt 后，才能进入 `verified`。

`VolumeCommitCoverage` 至少包含：

```text
tenant_id + object_namespace_id + commit_id + storage_volume_id + placement_generation
verified_object_count / verified_bytes
object_set_digest
state = partial | complete | retiring | deleted
```

Coverage 可以由 ObjectPlacement 重新计算。它不能单独证明对象存在，也不能绕过对象级 digest、size、namespace 和 generation 校验。

### 4.4 MaterializationJob、Batch 和 ObjectTask

MaterializationJob 是用户可见的唯一父任务，固定以下身份：

```text
(tenant_id, object_namespace_id, commit_id, target_storage_volume_id, coverage_goal)
```

同一身份最多有一个活动 Job。目标已有 partial Coverage 时复用并继续；目标已有 complete Coverage 时返回 no-op/replayed；不能再用 `TARGET_COMMIT_PLACEMENT_EXISTS` 把已有部分数据当作冲突。

`MaterializationBatch` 是 Job 内部的调度单元，按 source Agent/Volume/route 分组。Batch 不是独立用户复制任务，不单独占用父任务幂等身份。

`MaterializationObject` 记录精确 ObjectRef、目标已确认 offset、主源 placement、备用源 placement、当前 batch、plan revision、attempt 和错误。聚合对象数/字节只用于展示，完成状态必须由对象级证据重算。

## 5. 健康与 Ready 语义

不能再用单个 `data_health` 或 `verified_placements` 表达所有状态。v2 API 应返回至少以下维度：

| 维度 | 判断标准 | 示例 |
| --- | --- | --- |
| `content_presence` | 每个 ObjectRef 是否至少有一份 Verified ObjectPlacement | A/B 两个 partial Volume 的并集可为完整 |
| `source_serving` | 当前是否存在可访问的 Agent、Volume 和 route 能提供缺失对象 | 所有证据存在但 Agent 全断线时可降级 |
| `durability` | 每个对象是否满足副本数和 failure domain 策略 | 每个对象只有一份时为 under-replicated |
| `target_coverage` | 指定目标已覆盖的对象数、字节数和缺失列表 | `partial` 不等于失败，也不等于 Ready |
| `view_readiness` | 目标完整 Coverage、视图校验、Agent/mount 状态是否全部满足 | 只有此维度允许本地读取 |

例如：

```text
Global content_presence = available
complete_volume_count = 0
target C coverage = 3/4 objects
view_readiness(C) = not_ready
```

Workspace、SnapshotDelivery 和首期 S3/POSIX 读取必须要求目标 `complete` Coverage；不能因为全局对象并集完整就直接 Ready。后续若增加 federated read，应单独定义每对象路由、缓存、超时和一致性契约。

## 6. 多源调度方案

### 6.1 规划流程

1. 校验 tenant、Artifact、ObjectNamespace、Commit 和调用方权限。
2. 读取 CommitObjectSet，并验证 object_set_digest。
3. 批量读取目标 Volume 当前 Verified ObjectPlacement，扣除已有对象和字节。
4. 按缺失 ObjectRef 查询所有候选源 placement。
5. 过滤 namespace/tenant/size/encoding/digest 不匹配、`retiring/lost`、过期 generation、Volume 非 Ready、Agent/route 不可用、无权限或 capability 不匹配的候选。
6. 对每个对象保留 primary 和有序 fallback；同一个目标对象在同一时刻只允许一个活动 owner。
7. 使用确定性的加权 greedy set-cover 选择少量来源：优先同 EdgeCluster/Region、低成本/低 RTT、低负载、较高可用性和能覆盖更多稀有对象的 Volume；权重相同按 source/volume ID 稳定排序。
8. 按 source Agent/route 生成多个 Batch，并施加每源、每目标和每租户并发/带宽上限。
9. 将计划以 `plan_revision` 持久化，再签发每 Batch 的短期 Ticket。

不允许用户直接指定任意物理路径或绕过 Central 选择源。用户可以选择目标 Volume 和策略，不能获得源对象的越权读取能力。

### 6.2 失败与重规划

- 源断线、Ticket 过期或 route generation 变化只影响尚未完成的 ObjectTask；目标已 fsync 且校验成功的对象保留。
- Central 增加 `plan_revision`，重规划时递增 revision，并以 CAS 拒绝旧 Batch 的 report/finalize。
- fallback 可以换 source placement、Agent 或 route，但不改变目标 staging 的稳定对象身份。
- 暂时没有可用来源时进入 `waiting_for_sources`/`stalled` 观测状态；确认全局缺块且策略不允许等待时才进入 Failed。
- 目标已有对象时 Agent 使用 per-object lock、no-replace/CAS 和 digest 校验，重复传输最终只发布一份 ObjectPlacement。

### 6.3 容量与配额

目标容量检查使用：

```text
missing_bytes + target_staging_reserve + concurrency_reserve
```

不再按整个 Commit 总大小预留。计划、对象清单和缺失列表必须支持分页，避免一次把超大 Commit 全部加载进 HTTP 或单个 Ticket。

## 7. 状态机与一致性不变量

### 7.1 Job 状态

```text
Queued -> Planning -> WaitingForSources -> Materializing -> Verifying -> Complete
                                      \\-> Stalled / Failed / Cancelled
```

`Stalled` 是可恢复观测状态；`Failed` 和 `Cancelled` 是终态。Job 进入 Complete 的唯一条件是 Central 在事务中重新读取全部 ObjectTask、目标 generation 和 ObjectPlacement，并确认 Coverage 完整、对象集合 digest 一致。

### 7.2 Batch 与对象状态

```text
Batch:  queued -> assigned -> transferring -> verifying -> succeeded | failed
Object: missing -> reserved -> transferring -> verified -> published
```

允许 `already_present` 作为对象观测状态，但它必须由目标端现有 Verified placement 证明。Agent 不能用一个“Published”汇总断言替代逐对象 receipt；旧的 Replication `Published` 语义应删除或改为 Job `Complete`。

### 7.3 必须保持的不变量

- Central 不接收、不保存、不代理 Chunk payload；Gateway 只流式转发。
- ObjectPlacement 的 tenant、namespace、object、size、digest、Volume 和 generation 必须与 ObjectRef 完全匹配。
- 只有 Agent 完成 durability barrier 后，Central 才能登记 ObjectPlacement。
- 目标 local view 只有在完整 Coverage 原子发布后才可读。
- 同一 `(namespace, commit, target volume)` 只有一个活动 MaterializationJob。
- 旧 session/route/mount/placement generation 的报告必须失败关闭。
- request identity 重放必须返回同一个 Job；相同 request identity 绑定不同 payload 必须冲突。
- 取消或失败不能删除已经成为有效 ObjectPlacement 的对象；清理只处理仍受 staging lease 保护的临时数据。

## 8. v2 传输协议与 API

### 8.1 Ticket 与 Agent/Gateway

每个 Batch 使用独立的中央签名 Ticket。Ticket 必须绑定：

```text
materialization_id
plan_revision / batch_id / batch_attempt
tenant_id / object_namespace_id / artifact_id / commit_id
exact ObjectRef list 或 BatchManifest digest
source ObjectPlacement identity + generation
target Volume + placement generation
source/target Agent、session、mount、route generations
max_bytes / deadline / capability
```

目标 staging identity 固定为：

```text
(materialization_id, object_namespace_id, object_id)
```

source、route 或 batch attempt 变化不能重置目标 offset。对象清单按 BatchManifest 分页，不能继续用一个承载完整 Commit 的超大 `allowed_objects` 列表。

建议破坏性切换：

- 当前控制/数据协议升为 v2，旧 Envelope/Assignment/Report 直接返回 `PROTOCOL_UNSUPPORTED`；
- Transfer ALPN 改为 `neoengram-transfer-v2`；
- capability 改为 `commit_materialization_v2`；
- 删除旧的 ReplicationAssignment、Published 汇总报告和单源 Ticket 语义；
- Ed25519/JCS 签名覆盖 namespace、plan、Batch、对象范围、generation、byte limit 和 TTL；
- Gateway 不缓存 payload，也不把转发流量登记成 ObjectPlacement。

### 8.2 公开 API

建议把用户动作改为 Materialization 语义：

```text
/api/commit/materialize
/api/commit/materialization/query
/api/commit/materialization/list/query
/api/commit/materialization/retry
/api/commit/materialization/cancel
/api/commit/coverage/query
/api/commit/availability/query
```

创建响应返回 Job ID、状态、plan revision、目标覆盖对象/字节、缺失对象/字节、source count 和 issue。Ticket 不再通过面向用户的公开查询接口暴露，而由受信控制通道向 Agent 下发。

`queryCommitAvailability` 应返回全局内容、服务可达性、耐久性、完整 Volume 数量以及可分页的缺失对象；不能只返回 `verified_placements` 和 Volume ID 列表。

## 9. Authority 与 Agent 持久化

建议在 clean-slate authority schema 中删除：

```text
commit_placement_sets
replications
replication_objects
```

新增或重建：

```text
object_placements
volume_commit_coverages
materializations
materialization_batches
materialization_objects
object_read_leases
staging_leases
```

所有主键/唯一索引都包含 namespace。建议的关键约束：

- `object_placements` 唯一键为 namespace + object + backend + placement_generation；
- `materializations` 对 namespace + commit + target Volume + goal 建活动唯一索引；
- `materialization_objects` 以 materialization + object 为主键，保存稳定 staging identity 和 checkpoint；
- Batch/plan revision 的 CAS 防止旧 source 或旧 route 覆盖新计划；
- Coverage 由对象证据重算，并在完整校验后原子发布。

Authority schema/user_version 已提升到 clean-slate v2 版本 18；当前仍需完成 v1 表删除、显式 reset 和 Volume inventory rebuild。遇到 v1 数据直接拒绝启动，不在启动时隐式删除业务对象。

Managed Add 产生的 `ObjectPlacementEvidence` 与复制产生的对象 receipt 必须最终进入同一对象级权威，或明确区分两者的生命周期和可读资格；不能维护两套互相矛盾的“对象已 Durable”事实。

## 10. Workspace、SnapshotDelivery 与 S3

### Workspace

带 base Commit 的 Workspace 创建时进入 `Provisioning/Hydrating`，自动 attach 或创建对应 MaterializationJob。只有目标 Coverage complete、视图目录校验完成且 Volume/Agent 可达，才进入 `Active`。

### Snapshot 与 SnapshotDelivery

Snapshot 仍然是没有物理位置的逻辑 Commit 引用。SnapshotDelivery 绑定目标 Volume 和模式，创建时可以复用已有 MaterializationJob；Delivery 只有目标完整物化和只读视图校验完成后才 `Ready`。一个 Snapshot 可以有多个不同目标 Delivery。

### S3/POSIX

第一阶段 S3/POSIX 只消费本地完整 Coverage。全局 union 可用但目标 partial 时，接口应返回未就绪/不可读，而不是在请求中临时扇出多个远程 Volume。

## 11. GC、租约与故障边界

多源传输需要两类持久租约：

1. `ObjectReadLease`：绑定 source ObjectPlacement、object、placement generation、Materialization/Batch、plan revision 和 TTL，保护源对象不被 GC 回收。
2. `StagingLease`/`MaterializationLease`：绑定目标、generation 和 staging identity，保护未完成目标数据。

GC roots 至少包括 Commit/Ref、Snapshot、Delivery、Workspace、retention hold、活动 Materialization、Batch、ObjectReadLease 和 StagingLease。

对象回收继续采用两阶段：

```text
Central CAS: verified -> retiring
Agent physical delete + durability result
Central: deleted tombstone
```

源失联、Ticket 过期或 Job 取消不能直接把对象标为 Deleted。lease 到期且没有其他 root 后才允许清理；物理删除和 authority tombstone 必须幂等。`Lost` 只能是明确的管理声明，不能由一次 heartbeat 超时自动推断。

## 12. 迁移与实施顺序

这是开发阶段的破坏性迁移，不做 v1 双读。建议按以下顺序实施：

1. **Domain**：加入 `ObjectNamespaceId`、ObjectRef、ObjectPlacement v2、Coverage、Materialization Job/Batch/Object、Lease；重写纯函数校验和 schema golden。
2. **Authority**：重建 InMemory/SQLite repository 和 schema；先完成对象级索引、Coverage 重算、活动 Job 唯一约束和 clean-slate 启动拒绝。
3. **Central planner**：实现目标缺失计算、确定性多源选择、容量/权限/route 过滤、plan revision 和对象级 checkpoint。
4. **Agent/Gateway**：实现 v2 BatchManifest、Batch Ticket、多 QUIC stream、稳定 staging、receipt 和 source failover；Gateway 继续只转发。
5. **Workspace/Delivery**：在 partial 目标上自动 hydrate，完整 Coverage 后再发布 Active/Ready。
6. **OpenAPI/Web**：同步 action registry、OpenAPI、生成类型、Mock 和页面，删除单 source/单 replication 展示。
7. **运行与运维**：补充 inventory rebuild、reset、lease 清理和 metrics，再做真实跨 Gateway/Agent E2E。

不要在第一步同时实现 federated read、自动重平衡和生产 HA；这些会掩盖对象物化本身的正确性问题。

## 13. 最低验收矩阵

### Domain/Authority

- namespace collision：相同 ObjectId 在两个 Artifact 中不能互相充当 placement；
- 两个 partial Volume 的对象并集可以得到全局 `content_presence=available`，且 `complete_volume_count=0`；
- 缺少任意一个对象时 Coverage 不能 Complete；
- 进度由 ObjectPlacement 重算，不信任客户端 aggregate counters；
- InMemory 与 SQLite 对相同计划、CAS 和幂等键得到相同结果；
- v1 schema/database/record 直接拒绝，clean reset 不隐式删除 Volume 对象。

### Planner/Materialization

- A/B 各持有部分对象时从两个源并行补齐 C；
- C 已有部分对象时 attach/resume，不重复传输；
- C 已 complete 时 no-op/replayed；
- 缺失对象没有任何合规源时不能进入 Complete；
- 源中途断线后只重规划未完成对象，并保留已 fsync checkpoint；
- 相同对象被两个 Batch 竞争时目标只发布一份；
- 容量按 missing bytes + reserve 计算；
- 同一目标的并发请求合并或稳定冲突，不能创建两条活动 Job。

### Protocol/Agent/Gateway

- Ticket 的 namespace、对象清单、size/digest、plan revision、generation 或 TTL 被篡改时硬失败；
- 旧 route/session/mount/placement generation 的 report/finalize 被拒绝；
- source route 切换不改变目标 staging key 和 offset；
- Agent 重启后从 durable checkpoint 继续；
- Gateway 多 Batch 流有界、背压正确，且没有 payload 持久化；
- 旧 ALPN、旧 capability 和旧 Assignment 不会被 v2 Agent 接受。

### Product/Readiness/GC

- global union 完整但目标未物化时，Workspace/SnapshotDelivery/S3 仍不可 Ready；
- 目标完整 Coverage 发布与本地视图 Ready 是原子门槛；
- Agent/route 暂时不可达不会删除对象证据；
- ObjectReadLease 与 GC 竞态下，源对象不会被回收；
- 取消/失败只清理无 lease 的 staging，不删除已验证对象。

## 14. 待冻结的产品决策

以下事项不阻塞 v2 核心对象模型，但必须在进入生产验收前定案：

1. 默认副本数、failure domain 层级和 under-replicated 的告警/修复策略；
2. `ObjectSetDigest` 是否长期保持纯内容身份，还是在未来引入显式 namespace digest；本报告推荐保持内容身份稳定，由 Ticket/Plan 签名绑定 namespace；
3. 源选择的延迟、带宽、成本和区域权重，以及用户是否能查看 source region 摘要；
4. Materialization 失败后的 staging 保留时长和用户可见的 retry 窗口；
5. federated read 是否作为独立产品，以及它是否允许 partial Coverage 作为只读数据源。

## 15. 证据与状态说明

- `已观察`：本报告第 3 节引用的 v1 代码，以及 v2 Domain/Authority/Central planner 的纵切代码、schema 和测试能直接证明的行为。
- `推断`：由当前 Artifact-scoped CAS、placement API、Agent transport 和现有状态机组合推导出的约束。
- `目标`：本报告第 4 节以后尚未接通的 v2 数据面、调度、生命周期和产品行为；在代码和验收测试完成前不得写入当前能力表为“已实现”。

后续实现必须同时更新 [`current-state.md`](../current-state.md)、[`roadmap.md`](../roadmap.md)、[`storage.md`](storage.md)、[`control-plane.md`](control-plane.md)、[`gateway.md`](gateway.md)、OpenAPI/action registry、领域 schema、Agent/Gateway 测试和 Web 生成类型。
