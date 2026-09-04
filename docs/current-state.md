# 当前实现基线

> 核验日期：2026-09-04
>
> 本文从当前源码、OpenAPI/action registry、测试和 Web 路由反推“现在能证明的产品行为”。
> 它不是目标架构，也不替代 [`roadmap.md`](roadmap.md)。当本文与代码或测试冲突时，先修正本文；
> 当本文与目标产品语义冲突时，在 [`product.md`](product.md) 中把目标明确标成未实现。

## 证据规则

| 标记 | 证据 | 可以支持的结论 |
| --- | --- | --- |
| `已观察` | 生产代码路径与可重复测试直接覆盖 | 可以描述当前行为，但仍要注明验证范围 |
| `推断` | 多个实现、DTO、页面或测试组合得到的结论 | 只能作为实现假设，不能当作生产承诺 |
| `目标` | 产品文档、架构文档、路线、未接入 DTO 或 Mock | 只能描述计划或设计 |

证据优先级固定为：运行时代码和测试 > action registry/OpenAPI/Schema > Web 真实模式 > Web Mock >
叙述文档和历史设计。Mock 页面可证明交互意图和 DTO 形状，不能证明 Central 已实现该路由。

## P2P 对象物化 v2 当前状态

> v2 是开发阶段的 clean-slate 迁移，当前代码处于纵切并存状态。下表只描述已经能由源码和测试证明的
> 部分；它不等于真实跨 Agent/Gateway 数据面或生产 E2E 已完成。

| 层 | 证据状态 | 已观察事实 | 尚未证明/未完成 |
| --- | --- | --- | --- |
| Domain 契约 | `已观察` | `ObjectNamespaceId`、对象级 `ObjectPlacement`、派生 `VolumeCommitCoverage`、`DurabilityPolicy`、`MaterializationJob/Batch/Object`、分页 `BatchManifest`、v2 Ticket/Receipt/Lease、统一 OperationTask 和四维 availability DTO 已进入 `neoengram-domain`，并有 schema golden | v1 wire/assignment/report 仅可作为私有拒绝/迁移残留，生产协议切换和源码删除仍待完成 |
| Authority | `进行中` | SQLite `user_version=20`、v2 表、namespace 复合键、对象 Receipt 幂等/CAS、Coverage 重算、统一 OperationTask 和 InMemory/SQLite round-trip/replan 契约已有测试；Snapshot 创建原子绑定唯一 Delivery 目标 | v1 replication 内部 mapper/测试仍待删除且不参与 v20 调度；v19 及更早数据库与 v20 DDL 不兼容，必须显式 reset/inventory rebuild |
| Central planner/API | `进行中` | `/api/commit/materialize`、`/api/commit/coverage/query`、`/api/commit/availability/query` 与统一 `/api/task/*` 查询/重试/取消已进入 action registry/OpenAPI；planner 可计算缺失对象、按候选 placement 选择多源/fallback、复用目标 checkpoint | 配额/并发的完整运行编排、真实 source route 调度和跨节点计划版本收敛仍待完成 |
| Agent/Gateway v2 数据面 | `进行中` | v2 ALPN `neoengram-transfer-v2`、Ticket/generation fence、分页 manifest、Agent source/target QUIC stream、Gateway `ConnectedTransferRelay`、目标 staging/checkpoint/receipt 和动态 `commit_materialization_v2` capability gate 已有代码与单元/协议测试；Agent 还提供只读 Volume integrity-check 和启动/周期 scrub | 真实跨 Gateway route 编排、三 Agent 多源失败恢复、背压/配额、生产凭据和跨节点 E2E 尚未验收 |
| Readiness/读取面 | `进行中` | Workspace、SnapshotDelivery、S3 的 v2 门控代码要求目标完整 Coverage；availability 已拆分 `content_presence`、`source_serving`、`durability`、`target_coverage`、`view_readiness` | 三 Agent、多 Gateway、多源失败恢复、Lease/GC 竞态和跨节点 Ready E2E 尚未验收 |

v2 的正式副本单位是已验证的对象级 `ObjectPlacement`，不是完整 Commit。Commit 仍是不可变逻辑
`ObjectSet`；初期 `ObjectNamespaceId` 等于 `ArtifactId`，但协议、数据库键和租约仍必须显式携带 namespace。
`VolumeCommitCoverage` 由对象证据派生，`partial` 不能使 Workspace、SnapshotDelivery 或 S3 Ready。
旧 PlacementSet、单源 TransferTicket 和 replication API 不属于 v20 公开主链路；开发阶段升级通过显式 reset/inventory rebuild 完成，不提供旧协议兼容读取。

### 副本完整性检查与补齐（当前可证明能力）

- Agent 的 `integrity-check` 和启动/周期 scrub 使用同一只读扫描：校验 Volume 上对象的文件名、类型、大小
  和 BLAKE3。运行中的启动/周期 scrub 会将 `missing`/`corrupt` 观测上报 Central，手工命令只输出诊断结果。
  观测会使对应 Placement 健康状态和派生 Coverage 降级，但不会删除、恢复或重新登记对象；当前没有公开的
  “立即 scrub” HTTP action。
- 发现异常后，运维流程是先查询 `coverage/query`（各目标 Volume 的对象覆盖）和 `availability/query`（对象
  是否仍有可服务来源），再对对应 `commit.materialize` 任务调用 `/api/task/retry`。Planner 会排除异常
  Placement，从其他健康 Volume/Agent 选择 primary/fallback，并为缺失或损坏对象生成新的 Batch；目标上已验证
  的对象和有效 staging checkpoint 保留，已失效对象从零重新传输。
- `retry` 需要携带查询时的 `plan_revision`。发生并发变更返回 `409` 时，应重新查询任务后使用最新 revision 重试。
  没有健康来源时任务会进入 `WaitingForSources`/`Stalled` 或 `Failed`，不会把 partial Coverage 标记为可读。
- 该流程已由 Central InMemory/SQLite 规划与 CAS 测试覆盖；跨 Gateway/Agent 的真实 payload、定时自动修复和
  生产级多源故障恢复仍未验收。可执行命令和请求样例见 [`reference/cli.md`](reference/cli.md#managed-volume-完整性检查与副本补齐)。

## 真实产品形状

当前仓库同时包含两条产品链路：

```text
Standalone 本地模式：CLI -> runtime/domain -> SQLite metadata + local object CAS -> worktree/FUSE/export

Managed 中心模式：Web/automation -> Central public action API -> SQLite AuthorityStore
                 -> (可选) Agent/Gateway execution -> user StorageVolume object CAS
```

因此当前不能把项目概括成“已经可用的中心化训练数据平台”：

- 本地文件版本控制是最完整、可独立运行的用户链路；
- Central 的资源目录、权限、幂等、分页、Commit/Index 纵切和部分生命周期已经有代码与测试；
- Agent enrollment、内部 Job/Assignment 控制链和 Gateway 需要显式运行配置，生产凭据、真实双副本业务 E2E、跨 Volume
  payload 复制和 Kubernetes 故障切换仍未完成；
- Web 有真实 API 模式和 MSW Mock 模式。Mock、页面和 OpenAPI 不能单独证明后端能力。

## 用户资源的实际语义

| 资源 | 当前实现语义 | 主要证据 | 当前限制 |
| --- | --- | --- | --- |
| `Tenant` | 权限和可见性边界；列表、查询、创建已由 Central catalog 提供 | `services/neoengram-central/src/service/catalog.rs`、`tests/artifact_catalog.rs` | 成员/RoleBinding 管理不在公开 P0 API |
| `Project` | Tenant 内 Artifact 分组；列表、创建有 public action | `src/controller/catalog.rs`、`src/service/catalog.rs` | 更新、删除和成员管理待实现 |
| `Artifact` | 无固定 Volume/Region 的逻辑资产；当前 Central 只接受空初始化 | `src/catalog.rs`、`src/service/catalog.rs::create_artifact`、`tests/catalog_http.rs` | OpenAPI 保留 `derived` 初始化形状，但当前请求返回 `409 ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED` |
| `Commit` | 不可变内容图，当前单 parent；由 Playground commit 产生 | `src/service/workspace_commit.rs`、`tests/workspace_commit.rs` | 派生 Artifact root Commit 仍未实现；无 merge/rebase/命名 branch |
| `Playground` | 一个 Artifact 在一个 StorageVolume 上的可写工作区；带 IndexVersion、base/head 和存储可达性 | `src/dto/catalog.rs`、`src/service/catalog.rs`、`tests/catalog_index_version.rs` | 依赖 Agent/Volume 的真实扫描和 mutation 需要启用 storage execution |
| `Pre-commit` | Playground 的显式检查会话，独立 `start/query/restart/cancel`，有 `state + phase + attempt` | `src/service/catalog.rs`、`tests/playground_precommit.rs` | 页面刷新不应隐式创建会话；完整 Agent E2E 待验收 |
| `ObjectNamespaceId` | 对象权限和物理隔离的必填 namespace；当前初期值等于 `ArtifactId` | `crates/neoengram-domain/src/protocol/ids.rs`、materialization schema/tests | 旧 ObjectCatalog/replication 键尚未全部迁移到 namespace |
| `ObjectPlacement` | 单个 namespace/object 在一个 Volume/generation 上经 Agent 校验并持久化的 Durable 事实 | `crates/neoengram-domain/src/protocol/materialization.rs`、Central placement repository/tests | v2 receipt 仍未接入真实 Agent/Gateway payload；旧 placement evidence 仍并存 |
| `VolumeCommitCoverage` | 由对象 Placement 重算的目标 Volume `partial/complete` 覆盖摘要，不是独立耐久事实 | `materialization.rs`、`service/materialization.rs`、coverage tests | 覆盖重算尚未覆盖生产多 Volume/跨节点故障流程 |
| `Snapshot` | 创建时固定 `artifact_id + commit_id` 及一个目标 EdgeCluster/StorageVolume/模式，并与唯一 Delivery 原子写入 `Creating` | `src/catalog.rs` 的 `SnapshotRecord`、`src/service/catalog.rs::create_snapshot`、`src/dto/snapshot.rs` | 只有关联 Delivery 完成 Coverage、物化和视图校验后才转为 `Ready`；失败进入 `Abnormal` |
| `SnapshotDelivery` | Snapshot 在指定 StorageVolume 上的唯一物理只读投影，拥有 `delivery_id`、目标 Volume、模式和独立状态 | `src/catalog.rs` 的 `SnapshotDeliveryRecord`、`src/service/snapshot_delivery.rs`、`src/dto/snapshot_delivery.rs` | 每个 Snapshot 只能有一个 Delivery；`Ready` 前不可浏览、挂载或启用 S3；执行依赖 coordinator/Agent |
| `OperationTask` | 所有写操作的统一生命周期和审计入口（即时操作也记录完整生命周期）；领域执行细节仍由 `control_jobs`、`materializations`、`precommit_records` 等记录 | `crates/neoengram-domain/src/protocol/task.rs`、`src/service/task.rs`、`tests/task_repository.rs` | 旧 Assignment/report 与 v1 replication 仅是私有源码残留且不参与 v20 调度；跨进程原子性和完整业务 E2E 尚未验收 |
| `StorageVolume` | 已登记的区域存储及健康、策略和 Owner 观测；不是 Artifact 身份 | `src/dto/mod.rs`、`src/service/enrollment.rs` | 真实 NFS/挂载和凭据需要 Agent enrollment 配置 |
| `GatewayPool/Replica` | EdgeCluster 的控制入口和 Central 主动连接对象；不挂载 Volume、不保存对象权威 | `src/controller/gateway.rs`、`src/service/gateway.rs`、Gateway tests | readiness/failover/cutover 尚缺真实集群证据 |
| `AgentInstance` | 获批 Volume-bound 执行器，维护本地 identity/Ledger/session 并执行 Volume I/O；启动和运行期间会对本地 CAS 做只读完整性 scrub | `services/neoengram-agent/src/agent_core/`、`services/neoengram-agent/src/volume_integrity.rs`、`neoengram-agent-api.yaml` | 手工 `integrity-check` 需独占 Agent 状态数据库；scrub 只上报健康观测，不直接改写对象。发现缺失/损坏后需显式重试对应 `commit.materialize` 任务并从其他健康 Placement 补齐；自动定时修复和真实跨节点执行仍未完成 |

### Commit 详情页（当前可观察）

`apps/neoengram-web/src/pages/CommitDetailPage.vue` 提供独立路由
`/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/commits/:commitId`。在真实 API/Mock
页面测试中可以观察到：

- Commit 基本信息、父 Commit 和 Diff 独立加载；
- Availability 五维状态和各 StorageVolume 的对象级 Coverage 可查询；
- “检查”按目标 Volume 刷新 Coverage/Availability 观测并重新拉取列表，不直接启动 Agent scrub；
- “复制到目标”“修复副本”都调用 `useCommitMaterialization` 的 `materializeOrRepair`，由同一
  `commit.materialize` OperationTask 及其 `MaterializationJob` 明细决定 `materialize`、幂等复用或 `retry`；
- v2 物化查询和操作受 `artifact.commit.replicate` 权限及 `commit_materialization_v2` capability
  门控，缺少权限时不请求副本控制面。

证据范围是 Web 单元测试、OpenAPI/action registry 和 Central 的 InMemory/SQLite 契约；真实跨
Gateway/Agent payload、物理 scrub 触发和生产 E2E 仍未验收。

### Snapshot 与 SnapshotDelivery 的边界

这是当前产品设计中最容易被旧文档混淆的地方：

```text
Commit --(create)--> Snapshot (固定 Commit + 目标，视图尚未物化)
                         |
                         +--(atomic create target Delivery)-------> SnapshotDelivery[1]
                                                                    target Volume + mode
                                                                    Requested -> ... -> Ready
                                                                    (then Snapshot -> Ready)
```

代码和契约明确表明：

1. `CreateSnapshotRequest` 同时接收 Tenant、Project、Artifact、Commit、目标 EdgeCluster、目标
   `storage_volume_id`、交付模式和 request identity；`SnapshotView` 返回该目标及唯一 `delivery_id`。
2. Central 创建 Snapshot 时校验已发布 Commit、目标 Volume 和模式，并原子写入 `SnapshotState::Creating`
   与 `SnapshotDeliveryState::Requested`；这表示物化已排队，不表示目标视图已可读。
3. Delivery 执行和进入 `Ready` 前要求目标 Volume Ready、目标 Commit Coverage 完整、模式符合 Volume 策略，
   并由 coordinator/Agent 调度物化；Delivery `Ready` 后 Central 才将 Snapshot 转为 `Ready`。
4. 同一 Snapshot 只能有一个 Delivery；目标 Volume/EdgeCluster/模式不可变，重试只复用该 Delivery。需要
   不同区域的物理交付时，为同一 Commit 创建另一个 Snapshot。
5. Web 的创建和详情页面必须展示目标与 Delivery 聚合状态；在 Delivery `Ready` 前不得开放文件浏览、挂载
   或 S3 入口。当前 Web Mock 仍只证明 DTO/交互形状，不证明真实执行链。

### Artifact 初始化的当前边界

OpenAPI 和 Web 契约已经预留 `ArtifactInitialization` 的 `empty`/`derived` discriminator，但这不等于
派生能力已接通。当前 `CentralService::create_artifact` 对 `derived` 请求在写入 authority 之前直接返回
`409 CONFLICT`，稳定错误码为 `ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED`；HTTP 测试
`services/neoengram-central/tests/catalog_http.rs` 覆盖该行为。因此当前可运行流程只有空 Artifact：

```text
CreateArtifact(initialization=empty) -> Artifact(no head Commit)
CreateArtifact(initialization=derived) -> 409 ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED
```

“从明确 Commit 创建独立 root Commit、复用对象并记录来源血缘”是产品目标，不应在当前能力矩阵、验收
主链路或实现状态中写成已完成。实现该能力前还需要完成源 Commit 的租户/权限校验、对象可达性和
幂等持久化、root Commit 发布以及对应的 Central/HTTP/恢复测试。

## 能力矩阵

| 能力 | 证据状态 | 当前可见入口 | 不应作出的结论 |
| --- | --- | --- | --- |
| 本地 `init/add/commit/checkout/export/fsck/gc/mount` | `已观察` | `apps/neoengram-cli/src/cli/`、`crates/neoengram-runtime/src/standalone_api.rs` | 不等于远端 push/fetch/clone |
| Tenant/Project/Artifact catalog | `已观察` | Central controller/service、HTTP/catalog tests | 不等于完整成员/权限管理产品 |
| Artifact 派生初始化 | `未实现` | OpenAPI `ArtifactInitialization.derived`；`services/neoengram-central/src/service/catalog.rs::create_artifact` | Central 在 authority 写入前返回 `409 ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED`；不能声称已创建 derived root Commit |
| Playground 查询、创建、文件/变化/元数据查询 | `已观察` | Central catalog service、Web real/mock pages | 不能把 Web Mock 当成真实 Agent 扫描 |
| Pre-commit 与 Playground Commit | `已观察`（执行面有条件） | `/api/playground/precommit/*`、`/api/playground/commit/create` | 不等于生产级 Agent/Gateway E2E |
| Snapshot 创建/查询/列表 | `已观察` | `/api/snapshot/{create,query,list/query}` | 创建时绑定目标 Volume/模式并原子生成唯一 Delivery；Delivery 未 Ready 前 Snapshot 不可读 |
| SnapshotDelivery query/list/retry/delete | `已观察`（Central/service 路径；执行面有条件） | `/api/snapshot/delivery/*`、Web Snapshot detail | 每个 Snapshot 仅一个 Delivery；需完整 Coverage、视图校验并启用 coordinator/Agent execution |
| v1 Commit replication / Placement | `遗留（私有/不可调度）` | 旧 replication 实现、`service/placement.rs`、旧 Agent capability（公开 replication 路由已移除） | 仅记录历史完整 PlacementSet/单源语义；旧表和 mapper 不参与 v20 调度、Ready 或 v2 多源覆盖证据，旧协议直接拒绝 |
| v2 Commit materialization / Coverage | `进行中` | `/api/commit/materialize`、`/api/commit/coverage/query`、`/api/commit/availability/query`、统一 `/api/task/*`；`service/materialization.rs`、v2 Authority tests；Agent/Gateway 已有 v2 payload executor、relay 和 receipt 发布路径 | Central planner/Authority 与本地协议可测；真实 route 编排、跨 Gateway/三 Agent、多源失败恢复和生产 E2E 尚未验收 |
| S3 read-only Access Point | `进行中（门控已接入）` | `/api/s3/*`、Gateway S3 listener、SnapshotDelivery/Coverage gate | 仅 `SnapshotDelivery=Ready` 的 Snapshot 可创建或读取；还要求目标完整 Coverage、Ready GatewayPool、Agent route 和 signed ticket；不支持写入、不是中心对象存储权威；生产跨节点读取尚未验收 |
| Resource deletion / retention hold | `已观察`（需生命周期 coordinator） | `/api/resource/*` | 不等于完整回收站运营和灾备 |
| Agent enrollment/session/metadata transport | `已观察`（需 `--agent-enrollment-enabled`；已覆盖 channel EOF/写入传输失败、ACK 超时和旧 route/session fencing 进入有界重连，重连期间 readiness 降级、outbox 报告保留及不可用 Gateway route 的 retryable 503 映射） | Agent OpenAPI、Central registry handler、`approved_runtime`/registry handler 单元测试 | 不等于外部 issuer、真实 PVC 或完整双 Replica 断线恢复 E2E |
| Gateway Registry/activation/mTLS/forwarding | `已观察`（协议和局部契约） | `services/neoengram-gateway`、Gateway controller | 不等于真实多副本 readiness/failover/cutover |
| Web 真实模式 | `已观察`（页面/API 集成） | `apps/neoengram-web`，由 capabilities 控制 | 不等于所有 OpenAPI action 都有 Central handler |
| Web MSW Mock | `已观察`（测试/演示） | `apps/neoengram-web/src/mocks` | 不得作为后端交付证据 |

### Central 能力开关

`SystemService::query_api_version` 根据运行时组合返回 capabilities：

- 无 Agent enrollment 时仍有 `artifact_catalog`、Commit graph/diff、`managed_add`、`playground_browser` 和
  `sqlite_authority` 基础能力；
- `--agent-enrollment-enabled` 且 keyring/coordinator 初始化成功后，才声明 v2 materialize、
  pre-commit、SnapshotDelivery 模式等 storage execution 能力；
- command keyring 再启用 S3 read ticket 和资源生命周期 coordinator；生产模式还要求 OIDC、RBAC 和
  外部 S3 secret envelope/KMS-HSM 适配；
- Web 应按 capability 隐藏操作，而不是仅凭路由存在显示按钮。
- 旧跨 Volume replication 仅保留为拒绝/显式迁移边界，不再注册公开 action；Central 统一通过
  `/api/commit/materialize` 创建 MaterializationJob，并通过 `/api/task/*` 查询、重试和取消。
- v2 Central planner 过滤 namespace、digest、size、encoding、generation、Volume/Agent/route 健康后选择多源和
  fallback。SQLite/InMemory 已验证对象 Receipt 幂等、同一对象竞争只保留一个 placement，以及重规划保留
  confirmed offset；真实 route/带宽调度和跨节点执行仍未验收。
- v2 Agent runtime 仅在 replication 配置和 Gateway QUIC 预检成功后声明 `commit_materialization_v2`；Gateway
  生产 listener 使用 `neoengram-transfer-v2`、拒绝旧 v1 ticket，并可通过有界 `ConnectedTransferRelay` 转发
  v2 frame。真实 upstream route、凭据、跨 Gateway payload 和故障恢复仍需 E2E 验收。

## 契约与实现差异

`crates/neoengram-domain/src/protocol/action_registry.rs` 是公开 action 的路由事实表：

- `PUBLIC_ACTION_REGISTRY` 当前包含 85 个公开路径（含 2 个 health probe）；
- 其中 3 个显式标记 `routed_by_central = false`，是契约/Web surface，不是当前 Central handler：
  `/api/snapshot/file/list/query`、`/api/snapshot/activity/list/query`、
  `/api/snapshot/dataset/profile/query`；
- `docs/openapi/neoengram-api.yaml` 会保留这 3 个路径以冻结前端契约，Web Mock 也提供了对应数据；
- Central controller 当前安装其余公开业务路由，另有不进入公共 OpenAPI 的 `/internal/s3/authorize`；
- 因此“OpenAPI 有接口”“action registry 有接口”“Central 当前可调用”必须在任务和文档中分开写。

涉及 Snapshot 文件、活动或 Dataset Profile 的后端迭代，必须先补 Central controller/service/测试，再把
`routed_by_central` 从 `false` 改为 `true`，然后同步 OpenAPI 生成类型和 Web 真实模式测试。

## AI 迭代最短路径

1. 先读本文确定当前产品对象和证据等级；不要先读 2400 行控制面全文。
2. 从 [`iteration-guide.md`](iteration-guide.md) 填任务卡，明确目标、非目标、不变量和验收。
3. 用 [`AGENTS.md`](../AGENTS.md) 的能力映射定位入口、调用链、持久化和测试。
4. 修改公共行为时，按 action registry -> OpenAPI -> controller -> service -> ports/mapper/datasource -> Web
   生成类型的顺序推进；修改本地行为时按 CLI -> runtime facade -> engine -> local adapter 推进。
5. 完成后更新本文的实现证据和 `roadmap.md` 状态；目标设计只在 `product.md`/architecture/roadmap 中保留，
   不在当前能力表中冒充已完成。
