# 当前实现基线

> 核验日期：2026-08-24
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
- Agent enrollment、Job、Gateway 控制链需要显式运行配置，生产凭据、真实双副本业务 E2E、跨 Volume
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
| `Snapshot` | 固定 `artifact_id + commit_id` 的逻辑只读引用，不拥有 Volume/Region；创建时校验已发布 Commit 并写入 `Ready` | `src/catalog.rs` 的 `SnapshotRecord`、`src/service/catalog.rs::create_snapshot`、`src/dto/snapshot.rs` | 物理可读性不由 Snapshot 本身保证；数据健康是 PlacementSet 的动态视图 |
| `SnapshotDelivery` | Snapshot 在指定 StorageVolume 上的物理只读投影，拥有 `delivery_id`、目标 Volume、模式和独立状态 | `src/catalog.rs` 的 `SnapshotDeliveryRecord`、`src/service/snapshot_delivery.rs`、`src/dto/snapshot_delivery.rs` | 目标 Volume 必须已有已发布 Commit 对象；执行依赖 coordinator/Agent |
| `Job` | 异步执行的权威记录，Managed Add、Pre-commit、复制和 Delivery 等动作可产生 Job | `src/service/job.rs`、`src/service/coordinator.rs`、`tests/query_job.rs` | 统一 Job list、诊断和 operator 操作待补齐 |
| `StorageVolume` | 已登记的区域存储及健康、策略和 Owner 观测；不是 Artifact 身份 | `src/dto/mod.rs`、`src/service/enrollment.rs` | 真实 NFS/挂载和凭据需要 Agent enrollment 配置 |
| `GatewayPool/Replica` | EdgeCluster 的控制入口和 Central 主动连接对象；不挂载 Volume、不保存对象权威 | `src/controller/gateway.rs`、`src/service/gateway.rs`、Gateway tests | readiness/failover/cutover 尚缺真实集群证据 |
| `AgentInstance` | 获批 Volume-bound 执行器，维护本地 identity/Ledger/session 并执行 Volume I/O | `services/neoengram-agent/src/agent_core/`、`neoengram-agent-api.yaml` | 不能作为用户业务资源直接操作 |

### Snapshot 与 SnapshotDelivery 的边界

这是当前产品设计中最容易被旧文档混淆的地方：

```text
Commit --(create)--> Snapshot (逻辑、固定 Commit、没有物理放置)
                         |
                         +--(replicate Commit objects first)--> SnapshotDelivery
                                                                    target Volume + mode
                                                                    Requested -> ... -> Ready
```

代码和契约明确表明：

1. `CreateSnapshotRequest` 只有 Tenant、Project、Artifact、Commit 和 request identity，不接收
   `storage_volume_id` 或 Region；`SnapshotView` 也不返回二者。
2. Central 创建 Snapshot 时直接绑定已发布 Commit，并写入 `SnapshotState::Ready`；这表示逻辑引用已就绪，
   不表示某个 Volume 上已经物化只读视图。
3. `CreateSnapshotDeliveryRequest` 才接收目标 Volume 和 `fuse/copy/hardlink` 模式；创建前要求目标
   Volume Ready、Commit PlacementSet 已发布、模式符合 Volume 策略，并由 coordinator 调度物化。
4. 同一 Snapshot 可以有多个 Delivery；同一 Commit 在不同区域的交付不应复制成多个 Snapshot 资源，
   而应体现为同一 Snapshot 下的多个 Delivery。
5. Web 的 `SnapshotCreatePage` 和 `SnapshotDetailPage` 已按这个边界实现：先创建逻辑 Snapshot，详情页再
   选择复制目标和 Delivery 模式。

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
| 逻辑 Snapshot 创建/查询/列表 | `已观察` | `/api/snapshot/{create,query,list/query}` | 不包含 Volume 物化或区域副本 |
| SnapshotDelivery create/query/list/retry/delete | `已观察`（Central/service 路径；执行面有条件） | `/api/snapshot/delivery/*`、Web Snapshot detail | 不等于目标 Volume 已完成生产物化；需先 Placement published，并启用 coordinator/Agent execution |
| Commit replication / Placement / availability | `已观察`（复制前置条件和目标占用均已门控） | `/api/commit/*`、`service/placement.rs`、Agent `replication.enabled` | 同一 Commit 到同一 Volume 的 active 任务，或已有目标 PlacementSet 的新任务，均有稳定 `409 REPLICATION_ALREADY_ACTIVE`；仍不等于跨集群生产传输已验收 |
| S3 read-only Access Point | `已观察`（需 command keyring 与 storage execution） | `/api/s3/*`、Gateway S3 listener | 读取还依赖 Ready PlacementSet、Ready GatewayPool、Agent route 和 signed ticket；不支持写入、不是中心对象存储权威 |
| Resource deletion / retention hold | `已观察`（需生命周期 coordinator） | `/api/resource/*` | 不等于完整回收站运营和灾备 |
| Agent enrollment/session/metadata transport | `已观察`（需 `--agent-enrollment-enabled`） | Agent OpenAPI、Central registry handler | 不等于外部 issuer、真实 PVC 和全链路 E2E |
| Gateway Registry/activation/mTLS/forwarding | `已观察`（协议和局部契约） | `services/neoengram-gateway`、Gateway controller | 不等于真实多副本 readiness/failover/cutover |
| Web 真实模式 | `已观察`（页面/API 集成） | `apps/neoengram-web`，由 capabilities 控制 | 不等于所有 OpenAPI action 都有 Central handler |
| Web MSW Mock | `已观察`（测试/演示） | `apps/neoengram-web/src/mocks` | 不得作为后端交付证据 |

### Central 能力开关

`SystemService::query_api_version` 根据运行时组合返回 capabilities：

- 无 Agent enrollment 时仍有 `artifact_catalog`、Commit graph/diff、`managed_add`、`playground_browser` 和
  `sqlite_authority` 基础能力；
- `--agent-enrollment-enabled` 且 keyring/coordinator 初始化成功后，才声明 replication、materialize、
  pre-commit、SnapshotDelivery 模式等 storage execution 能力；
- command keyring 再启用 S3 read ticket 和资源生命周期 coordinator；生产模式还要求 OIDC、RBAC 和
  外部 S3 secret envelope/KMS-HSM 适配；
- Web 应按 capability 隐藏操作，而不是仅凭路由存在显示按钮。
- 跨 Volume replication 只有 Agent 显式启用 `replication.enabled`、启动预检成功并声明
  `commit_replication_quic_v1` 后才会被 Central 路由；缺少 command trust、QUIC/TLS 或 capability
  会在创建/签发 ticket 前返回 `REPLICATION_PREREQUISITES_UNMET`，不会等 assignment 下发后才失败。
- 目标占用由 Placement authority 再次校验：SQLite 在 `BEGIN IMMEDIATE` 写事务内检查目标
  PlacementSet，内存 adapter 也持有同一组 authority 锁；因此 create 与 finalize 之间不会把同一
  Commit/backend 接受成两条并行复制。已有 Staged/Published/Retiring/Deleted PlacementSet
  都必须先按生命周期处理，不能直接覆盖。

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
