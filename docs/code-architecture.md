# 源码架构

NeoEngram `0.2.0` 已完成 clean-slate 边界收敛：`neoengram-domain` 统一领域模型与 current
wire contract，`neoengram-runtime` 统一执行内核与本地适配器 facade，`neoengram-central` 统一
Central 进程入口，Gateway 与 Web 保持独立部署边界。
当前仍以本地仓库格式 9 工作流为主要产品；`neoengram-central` 同时承载 authority、HTTP API 和 Gateway
composition，已提供后端无关
`AuthorityStore` 和默认 SQLite 单节点权威后端。它通过 Fusen 0.9.0 暴露已实现的
system、Tenant、StorageVolume、Enrollment、Artifact、Playground、Snapshot 基础、Job 和 Gateway Registry action API，并保留
Central 不再提供独立 Agent listener。Agent 控制链只通过 Gateway 建立 HTTP/2 全双工 control
channel；当前 `neoengram-agent` 已改为 Gateway-only 配置，Gateway 控制面、运行时 mTLS、下行命令签名
和一跳 peer forwarding 已接入。Vue 3 Web
控制台可通过 MSW 运行多租户资源浏览、
StorageVolume 登记与放置选择、Artifact/Playground/Snapshot 创建、Playground Commit 与 Managed
Add Job 流程，并可查看 Commit 描述、Tags、父 Commit 信息和文件 Diff，但尚未
连接完整真实中心。这不代表其余 OpenAPI、PostgreSQL、生产凭据签发/轮换、跨 Volume 数据路由或 HA 已经实现。能力状态和后续路线统一见
[`implementation-plan.md`](implementation-plan.md)。

2026-08-09 已确认 `services/neoengram-gateway` 的目标架构。G1 已加入协议、Registry/管理 API、
三 listener、Central outbound tunnel、工作负载证书校验、端到端命令签名和一跳 peer forwarding，
Agent 配置也已切到 Gateway-only；双 Replica listener/H2/peer harness 和真实 Registry RouteLease 接管
契约已分别通过，但完整业务 E2E、外部生产凭据适配、真实集群故障/就绪和切换验收仍未完成。目标状态由
Central 主动连接每个 EdgeCluster 的 GatewayPool，Agent 只连接本集群 Gateway。Gateway 的完整边界见
[`neoengram-gateway-architecture.md`](neoengram-gateway-architecture.md)，在端到端契约测试和部署切换完成前
不得把部分骨架标记为可用 Gateway 能力。

## Workspace 与职责

```text
crates/
├── neoengram-domain/      # 唯一领域模型、强类型 ID、Envelope、Schema、SigV4 与 current wire contract
└── neoengram-runtime/     # 执行内核、本地 SQLite、Repository、FUSE 与读取适配器
services/
├── neoengram-central/     # Central authority + HTTP composition
├── neoengram-agent/       # Agent state machine + Gateway transport adapter
└── neoengram-gateway/       # 三 listener + activation + mTLS + 一跳 peer forwarding
apps/
├── neoengram-cli/         # Clap、cwd 输入、typed Result/progress/diagnostic 的唯一终端渲染入口（package 名仍为 neoengram）
└── neoengram-web/         # 独立 Vue 3 SPA；公开 OpenAPI 生成类型与 MSW 开发适配器
```

除 `neoengram-domain` 和 `neoengram` 外，新增 package 均为 workspace-private。Agent 和
`neoengram-central` 分别提供 Agent 与 Central binary；Central 内部按 bounded context 组织 authority、identity、catalog、jobs、gateway、storage、snapshot、S3、lifecycle 和 API，`neoengram-agent` 同时承载 Agent 状态机与传输适配器。需要公开用户客户端时再创建 `neoengram-client`，
不提前维护空 client crate。

`neoengram-web` 不属于 Cargo workspace。它只能依赖 `docs/openapi/neoengram-api.yaml` 定义的公开
HTTP 契约，不得导入 Rust crate、Agent JSON Schema、数据库结构或中心内部恢复方法。
当前租户由 `/tenants/:tenantId/...` 路由确定；前端缓存 key 和每个资源请求都必须携带完整 tenant
scope，服务端仍从认证结果执行 RBAC，不能信任浏览器选择。

边界规则：

- `neoengram-domain::core` 只包含环境无关的领域模型。它拥有 `ObjectId`、`ManifestId`、`DirectoryId`、
  `CommitId`、`ContentDigest`、`LogicalPath`、`PathComponent`、`Manifest`、`Directory`、`Commit`、
  `FileRecord`、`IndexVersion` 和有界 `IndexDelta`，以及唯一规范 digest 实现；不依赖 CLI、数据库、
  文件系统、网络或终端。
- core 公共 API 只暴露分页 `FileRecord`/`IndexDelta` 和层级 `Directory`。Standalone 的 SQLite/worktree
  边界按页读取并在需要写入文件系统时直接消费 `WorkspaceFileRecord`，不保留扁平兼容模型、迁移视图或跨
  crate snapshot 契约。
- `neoengram-runtime` 不读取 cwd、环境变量或 CLI 字符串，也不直接输出；SQLite、物理路径和
  worktree 只在它内部的显式 adapter 边界出现。
  外部能力经 `IndexSnapshotReader`、`ImmutableCatalogReader`、`ObjectStore`、`Worktree`、
  `JournalStore`、`LockManager`、`Clock`、`ProgressSink` 和 `FailureInjector` 等 ports 注入；统一的
  `execute_mutation`/`finalize_mutation` executor 与 `WorktreeReceipt` 契约已经实现。
- `neoengram-runtime` 把物理路径、loose object、锁和 durable journal 限制在适配器内部，并已提供 Engine
  `Worktree`、`JournalStore` 和 `LockManager` adapters。经该执行边界运行的 mutation 遵循
  `MutationPlan -> durable journal -> WorktreeReceipt`，权威 Index/ref 发布不藏在文件系统层。Managed
  Agent 使用同一 `LooseObjectStore` 原子能力，把 Chunk 放在业务 Volume 的
  `.neoengram/objects/tenants/<tenant>/artifacts/<artifact>/objects/<object_id>`，而不是 Agent state
  目录或 Server 文件系统。
- `neoengram-runtime` 拥有 Repository discovery、SQLite、FUSE 和本地最终 CAS。每个命令接收显式
  cwd 和独立 Request，并返回领域化 typed Result；包括只读、`add`/`commit`/`gc`、mutation 和
  lifecycle 全部命令。Standalone 不再暴露通用 `CommandResult`，成功文本不进入应用层；
  `OutputEvent` 只保留异步诊断用途，并继续与执行进度分离。
- `checkout`、工作区 `restore` 和工作区 `rm` 已把仓库格式 9 transactional Worktree 适配器组合到
  Engine executor，并遵循 `MutationPlan -> durable journal -> WorktreeReceipt`。checkout/rm 使用 plan
  中的 expected `IndexVersion` 在 SQLite 发布权威状态，成功后才 finalize；worktree restore 不发布
  Index，`restore --staged` 独立更新 SQLite Index；`recover` 会恢复事务并收尾 active/finalized
  Engine journals。
- `neoengram` 只做 Clap 解析、cwd 注入和 Result/Error/成功文本渲染。`add`、`commit`、`mount`、
  `checkout`、`rm`、`restore` 与 `recover` 都从 facade 接收 caller-owned `ProgressSink`，CLI 直接渲染
  结构化 `ProgressEvent`。CLI 之外不得写 stdout/stderr，debug 环境变量故障点只在 CLI adapter 转为
  `FailureInjector`。
- `neoengram-domain::protocol` 只依赖序列化/Schema/JCS 库，不依赖 engine、CLI、SQLite、文件系统、
  HTTP 或存储 SDK。current wire contract 默认拒绝未知字段、未知 action 和未知消息类型。
- `neoengram-domain::protocol::action_registry` 是固定 action method/path/operationId 的唯一清单；其
  JSON 导出同时驱动架构检查和 public/Agent OpenAPI 集合检查。Central 的 Fusen 编译 descriptor、
  Gateway 的 Agent 转发映射与 registry 必须精确一致；动态 S3 bucket/key 和 Web 静态资源不属于
  action registry。
- `neoengram-central` 的 controller 只绑定 Fusen DTO 并调用 service；service 调用自身的
  `ControlPlane`/ports，不能访问 SQLx。SQLite datasource 只管理连接、锁、schema、迁移和完整性，
  repository 查询、行映射与 port 实现集中在 mapper。调用方向固定为
  `controller -> service -> ControlPlane/ports -> mapper -> datasource`。
- Central 在 Fusen listener 注册已实现的公开 action API；Agent action 契约只经 Gateway 转发，生产进程
  不暴露独立 Agent listener。认证业务接口要求 API version 与经外部
  OIDC/JWKS 验证的 Bearer JWT，RBAC 缺省拒绝；Agent action 使用一次性 token 与逐帧 Ed25519 proof。
  Agent 主动建立 channel，Assignment/Decision 由 server 权威状态派生并通过 channel 下推。Server
  不提供 Chunk missing/upload payload action；`AssignJob`、
  `ExpireAddJob`、`ResumePublication` 仍是内部方法，不能注册为 HTTP 路由。
- 目标 `neoengram-gateway` 只依赖 domain 以及网络、TLS、签名和观测组件；禁止依赖
  `neoengram-central` datasource/mapper、engine、fs、standalone、Volume adapter 或 authority schema。它不挂载
  StorageVolume，也不持久化 metadata/object。Gateway 是唯一 Agent 网络入口。
- `neoengram-agent` 的生产依赖只有 `neoengram-domain` 和 `neoengram-runtime`，不依赖 standalone、Central
  数据库或存储适配器。Central 通过同一 authority SQLite 提供自身的 bounded contexts。

## 生产依赖方向

```text
neoengram CLI -> neoengram-runtime <- neoengram-agent
       |                 ^                 ^
       +-> neoengram-domain <- neoengram-central

neoengram-web -> public OpenAPI/action contract -> neoengram-central -> domain/runtime

生产控制链：neoengram-central -> neoengram-gateway <- neoengram-agent

G1 控制面：Agent 配置已切 Gateway-only，Gateway 到 Central 的 H2/mTLS tunnel 和一跳 forwarding 已接入

目标：neoengram-central -> neoengram-gateway <- neoengram-agent
                                  |
                                  +-> domain
```

该图表示 production/runtime 的主要分层方向，不枚举所有直接 manifest 边；例如 CLI 还直接导入
domain 的参数类型和 runtime 的错误/故障注入接口。Agent 与 Central 的生产控制边经 Gateway 传输，
Agent 到 `neoengram-central` 的直接边只存在于 dev/test 组合测试。箭头只指向更稳定的边界。协议 DTO 不携带 SQLite connection、NFS 本地
路径或 CLI 文本；业务层交换强类型 ID、规范化逻辑路径、结构化 Request/Result、稳定错误码和明确的
版本条件。

## P0 执行与发布边界

Managed Add 的 engine 输出是 `PreparedAdd { index_delta, manifests, object_specs, statistics }`，只表示
候选结果，不能直接发布 Index。Standalone 由 SQLite publisher 完成最终 CAS；Managed 模式由
Agent 先把 Chunk 原子写入所分配业务 Volume 的 immutable CAS，再通过 `ObjectReceipt` 上报
Volume、Artifact placement 和 generation 证据。`neoengram-central` 在 placement evidence 与 MetadataBatch
完整校验后，把 canonical Manifests 与 expected
`IndexVersion` CAS 作为一个幂等 publication 原子发布；Conflict/Rejected 不写入候选 Manifest。
`Prepared -> Publishing` 的 Job CAS 同时冻结完整 publication candidate；Publishing recovery 只重放
该候选，不再依赖 staging、当前 ObjectCatalog、壁钟或可变 ACL。外部 `FinalizeAdd` 始终通过
Authorizer；不接受 actor 且只处理 Publishing 的 `ResumePublication` 是内部恢复用例，transport 不得
对外映射。

Commit 同样分为 canonical graph builder 与最终 publisher。这样 core/engine 的确定性计算可以被
Standalone 和中心复用，而 SQLite 或未来 PostgreSQL 的权威事务不会渗入计算层。仓库格式 9 的
Standalone `commit` 已调用 Engine graph builder，并由独立 SQLite publisher 执行最终 HEAD/ref CAS。

当前已实现 Agent Ledger/Assignment 和中心 Create/Assign/Report/Stage/Finalize 的无网络状态机；
Agent 在持久化 `running` 后通过 `ReportSink` 发出结构化 progress，PreparedAdd 的 IndexDelta、Manifest
与 ObjectSpec 必须组成闭合引用集。Agent 使用 `LooseObjectStore` 完成 Chunk hash/size 校验、fsync 和
原子发布，随后只把 Manifest、IndexDelta、ObjectReceipt 的 exact MetadataBatch descriptors/pages
写入 durable `TransferReceipt`，持久化 Prepared，再发送 descriptor-bound `JobPrepared`；Chunk 字节
不经过 Server。只有中心先
持久化该报告后才幂等 staging，全部成功才进入 `awaiting_decision`。报告或 staging 响应丢失会保留
Prepared 并从 Ledger 重放。Core 是 publication digest 的唯一 canonical 实现；Engine、Agent 与中心
分别重算 scope/base/IndexDelta/Manifest/ObjectSpec，`JobPrepared.candidate_digest` 再绑定
assignment identity、result/publication digest、descriptors 和 extensions。Agent 只接受 digest 等于
Prepared 结果且 revision 为 base + 1 的 Publish decision。失败上报统一使用 protocol `JobFailed`。
Manifest record 以 `chunk_start` 分片跨页，中心重组完整 Chunk 序列后重新校验 canonical Manifest ID。
InMemory 与 SQLite 运行同一行为契约。SQLite 是显式路径、单进程、单连接的默认中心权威后端；
`neoengram-central` 使用 SQLite 时只能部署一个副本。生产 HTTP 明文监听位于受控网络，TLS 必须由
Ingress/反向代理终止；多副本、HA/RLS 仍需要 PostgreSQL adapter。
不提供 HA 或数据库级 RLS；PG/MySQL 后端将独立实现 SQL/schema/migration，只复用 ports 与契约测试。
HTTP/2 Agent session/Job delivery 统一通过 H2+mTLS 的 `Central -> GatewayPool <- Agent` 控制链路，使用 Central 权威
AgentRouteLease 和最多一跳 Replica forwarding。后续跨 Volume 数据链路固定为源 Agent -> 源 Gateway
-> 目标 Gateway -> 目标 Agent；Server 只授权/校验路由范围和 placement evidence，不能代理 Chunk
payload。Gateway 不挂载 Volume。HTTP/3、外部生产凭据 provisioner、PostgreSQL 和 NFS 强 fencing
属于后续 adapter/部署阶段。开发 profile 不能被描述为
具备生产传输安全或 HA 的完整业务 Agent。

本地磁盘布局及事务语义见 [`storage-architecture.md`](storage-architecture.md)；中心与 Agent 的详细
边界见 [`agent-central-control.md`](agent-central-control.md)。
