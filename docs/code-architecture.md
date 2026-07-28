# 源码架构

NeoEngram `0.2.0` 已完成 P0 crate 边界改造：本地 CLI、可复用领域模型、执行端口、文件系统
适配器、Standalone 应用、wire protocol、Agent 和中心控制状态机分别拥有独立 crate。当前仍以
本地 format v8 工作流为可运行产品；Agent 与中心仍是无网络 library，中心已提供后端无关
`AuthorityStore` 和默认 SQLite 单节点权威后端。Vue 3 Web 控制台可通过 MSW 运行多租户资源浏览与
Managed Add Job 流程，但尚未
连接真实中心。这不代表 HTTP、PostgreSQL、mTLS、真实 S3 或 daemon 已经实现。能力状态和后续路线统一见
[`implementation-plan.md`](implementation-plan.md)。

## Workspace 与职责

```text
crates/
├── neoengram-core/        # 领域类型、强类型内容 ID、逻辑路径、规范编码与校验
├── neoengram-engine/      # 结构化用例、执行 ports、错误分类、进度与 mutation 协议
├── neoengram-fs/          # 安全物理路径、Loose ObjectStore、锁、journal、worktree 适配器
├── neoengram-protocol/    # v1 wire DTO、Schema、RFC 8785 JCS digest、稳定协议错误码
├── neoengram-standalone/  # format v8 SQLite、Repository、FUSE 与本地命令编排
├── neoengram-agent/       # 无网络 Agent 状态机、ports、Ledger 和测试适配器
└── neoengram/             # Clap、cwd 输入、typed Result/progress/diagnostic 的唯一终端渲染入口
services/
└── neoengramd/            # 无网络中心状态机、AuthorityStore、InMemory/SQLite 权威后端
apps/
└── neoengram-web/         # 独立 Vue 3 SPA；公开 OpenAPI 生成类型与 MSW 开发适配器
```

除 `neoengram-core` 和 `neoengram` 外，P0 新增 package 均为 workspace-private。Agent 和
`neoengramd` 当前均为 library-only；需要真实用户 API transport 时再创建 `neoengram-client`，
不提前维护空 client crate。

`neoengram-web` 不属于 Cargo workspace。它只能依赖 `docs/openapi/neoengram-api.yaml` 定义的公开
HTTP 契约，不得导入 Rust crate、Agent JSON Schema、数据库结构或中心内部恢复方法。
当前租户由 `/tenants/:tenantId/...` 路由确定；前端缓存 key 和每个资源请求都必须携带完整 tenant
scope，服务端仍从认证结果执行 RBAC，不能信任浏览器选择。

边界规则：

- `neoengram-core` 只包含环境无关的领域模型。它拥有 `ObjectId`、`ManifestId`、`DirectoryId`、
  `CommitId`、`ContentDigest`、`LogicalPath`、`PathComponent`、`Manifest`、`Directory`、`Commit`、
  `FileRecord`、`IndexVersion` 和有界 `IndexDelta`，以及唯一规范 digest 实现；不依赖 CLI、数据库、
  文件系统、网络或终端。
- core 公共 API 不再暴露扁平 `Tree`、`FileNode` 或物化 `Index`。Standalone 内部暂存的同名
  compatibility view 只是 format v8 SQLite/worktree 迁移细节，后续继续收敛到分页端口，不能跨
  crate 成为新契约。
- `neoengram-engine` 不读取 cwd、环境变量、CLI 字符串、SQLite 或物理路径，也不直接输出。
  外部能力经 `IndexSnapshotReader`、`ImmutableCatalogReader`、`ObjectStore`、`Worktree`、
  `JournalStore`、`LockManager`、`Clock`、`ProgressSink` 和 `FailureInjector` 等 ports 注入；统一的
  `execute_mutation`/`finalize_mutation` executor 与 `WorktreeReceipt` 契约已经实现。
- `neoengram-fs` 把物理路径、loose object、锁和 durable journal 限制在适配器内部，并已提供 Engine
  `Worktree`、`JournalStore` 和 `LockManager` adapters。经该执行边界运行的 mutation 遵循
  `MutationPlan -> durable journal -> WorktreeReceipt`，权威 Index/ref 发布不藏在文件系统层。
- `neoengram-standalone` 拥有 Repository discovery、SQLite、FUSE 和本地最终 CAS。每个命令接收显式
  cwd 和独立 Request，并返回领域化 typed Result；包括只读、`add`/`commit`/`gc`、mutation 和
  lifecycle 全部命令。Standalone 不再暴露通用 `CommandResult`，成功文本不进入应用层；
  `OutputEvent` 只保留异步诊断用途，并继续与执行进度分离。
- `checkout`、工作区 `restore` 和工作区 `rm` 已把 format-v8 transactional Worktree 适配器组合到
  Engine executor，并遵循 `MutationPlan -> durable journal -> WorktreeReceipt`。checkout/rm 使用 plan
  中的 expected `IndexVersion` 在 SQLite 发布权威状态，成功后才 finalize；worktree restore 不发布
  Index，`restore --staged` 独立更新 SQLite Index；`recover` 会恢复事务并收尾 active/finalized
  Engine journals。
- `neoengram` 只做 Clap 解析、cwd 注入和 Result/Error/成功文本渲染。`add`、`commit`、`mount`、
  `checkout`、`rm`、`restore` 与 `recover` 都从 facade 接收 caller-owned `ProgressSink`，CLI 直接渲染
  结构化 `ProgressEvent`。CLI 之外不得写 stdout/stderr，debug 环境变量故障点只在 CLI adapter 转为
  `FailureInjector`。
- `neoengram-protocol` 只依赖 core 和序列化/Schema/JCS 库，不依赖 engine、CLI、SQLite、文件系统、
  HTTP 或存储 SDK。未知扩展字段可 round-trip，未知消息类型以稳定错误拒绝。
- `neoengram-agent` 的生产依赖只有 core、engine 和 protocol，不依赖 standalone；它对 `neoengramd`
  的 `dev-dependency` 只用于内存组合测试。`neoengramd` 只依赖 core 和 protocol，不依赖 engine、fs
  或 standalone。

## 生产依赖方向

```text
neoengram CLI -> standalone -> engine <- agent
                     |          ^       ^
                     +-> fs ----+    protocol <- neoengramd
                         ^             ^
                         +---- core ---+

neoengram-web -> public OpenAPI -> neoengramd HTTP adapter (future)
```

该图表示 production/runtime 的主要分层方向，不枚举所有直接 manifest 边；例如 CLI 还直接导入
core 的参数类型和 engine 的错误/故障注入接口。Agent 到 `neoengramd` 的边只存在于 dev/test
组合测试，不属于生产图。箭头只指向更稳定的边界。协议 DTO 不携带 SQLite connection、NFS 本地
路径或 CLI 文本；业务层交换强类型 ID、规范化逻辑路径、结构化 Request/Result、稳定错误码和明确的
版本条件。

## P0 执行与发布边界

Managed Add 的 engine 输出是 `PreparedAdd { index_delta, manifests, object_specs, statistics }`，只表示
候选结果，不能直接发布 Index。Standalone 由 SQLite publisher 完成最终 CAS；Managed 模式由
`neoengramd` 在对象 durability 和 MetadataBatch 完整校验后，把 canonical Manifests 与 expected
`IndexVersion` CAS 作为一个幂等 publication 原子发布；Conflict/Rejected 不写入候选 Manifest。
`Prepared -> Publishing` 的 Job CAS 同时冻结完整 publication candidate；Publishing recovery 只重放
该候选，不再依赖 staging、当前 ObjectCatalog、壁钟或可变 ACL。外部 `FinalizeAdd` 始终通过
Authorizer；不接受 actor 且只处理 Publishing 的 `ResumePublication` 是内部恢复用例，transport 不得
对外映射。

Commit 同样分为 canonical graph builder 与最终 publisher。这样 core/engine 的确定性计算可以被
Standalone 和中心复用，而 SQLite 或未来 PostgreSQL 的权威事务不会渗入计算层。format v8 的
Standalone `commit` 已调用 Engine graph builder，并由独立 SQLite publisher 执行最终 HEAD/ref CAS。

当前已实现 Agent Ledger/Assignment 和中心 Create/Assign/Report/Stage/Finalize 的无网络状态机；
Agent 在持久化 `running` 后通过 `ReportSink` 发出结构化 progress，PreparedAdd 的 IndexDelta、Manifest
与 ObjectSpec 必须组成闭合引用集。对象传输完成后，Agent 把 exact MetadataBatch descriptors/pages
写入 durable `TransferReceipt`，持久化 Prepared，再发送 descriptor-bound `JobPrepared`；只有中心先
持久化该报告后才幂等 staging，全部成功才进入 `awaiting_decision`。报告或 staging 响应丢失会保留
Prepared 并从 Ledger 重放。Core 是 publication digest 的唯一 canonical 实现；Engine、Agent 与中心
分别重算 scope/base/IndexDelta/Manifest/ObjectSpec，`JobPrepared.candidate_digest` 再绑定
assignment identity、result/publication digest、descriptors 和 extensions。Agent 只接受 digest 等于
Prepared 结果且 revision 为 base + 1 的 Publish decision。失败上报统一使用 protocol `JobFailed`。
Manifest record 以 `chunk_start` 分片跨页，中心重组完整 Chunk 序列后重新校验 canonical Manifest ID。
InMemory 与 SQLite 运行同一行为契约。SQLite 是显式路径、单进程、单连接的默认中心权威后端，
不提供 HA 或数据库级 RLS；PG/MySQL 后端将独立实现 SQL/schema/migration，只复用 ports 与契约测试。
HTTP/2、HTTP/3、mTLS、OIDC、PostgreSQL、真实 S3、
NFS fencing 和 daemon 属于后续 adapter/部署阶段，不能放进上述稳定 crate 以伪装成已实现能力。

本地磁盘布局及事务语义见 [`storage-architecture.md`](storage-architecture.md)；中心与 Agent 的详细
边界见 [`agent-central-control.md`](agent-central-control.md)。
