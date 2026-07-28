# NeoEngram 实现路线与能力清单

> 本文是 NeoEngram 的唯一实现路线、能力状态和研究计划记录。代码、架构文档或
> README 中出现的路线描述应与本文保持一致；如果出现冲突，以本文为准。

最后更新：2026-07-28
当前阶段：`0.2.0` P0 与中心 `AuthorityStore`/SQLite 默认后端已实现；生产 transport、PG/MySQL 与对象存储适配器待实现

## 1. 产品目标

NeoEngram 的目标是一个面向模型权重和大规模训练数据的分布式文件版本管理系统，重点是
训练数据的可复现维护、发布和读取，而不是完整的 AI 训练平台：

- 客户端负责工作区、切块、校验、本地缓存和离线提交；
- 中心控制面负责 tenant/project/artifact/ref、Commit/Directory/Manifest、并发提交、权限、
  会话和审计；
- 数据面使用 S3-compatible 对象存储保存 Chunk/Pack payload，客户端保留本地缓存；
- 读取面以固定的 Snapshot、Shard 分页和租约向训练任务提供一致数据；
- 所有不可变对象通过内容 ID 校验，ref 更新通过条件 CAS 线性化；
- 系统应支持断点续传、失败重试、幂等 push/fetch，以及大规模数据集。

本项目只维护训练数据的文件、快照、分片和来源摘要，不建设训练调度、样本标注、特征工程、
实验管理或训练 Run 平台。

本地读取面已增加固定 Commit 的只读 FUSE。`export` 提供 copy 权限快照和可信本地环境下的
WholeFile hardlink 只读视图；
FUSE 是独立的内核只读视图，不自动跟随 HEAD，也不提供远端下载或可写 overlay。

第一阶段继续保持线性历史。Playground 创建可以原子创建并独占分支，但暂不提供独立
`branch`/`switch`、merge、rebase 和远端协作分支管理；这些功能不能阻塞中心元数据和对象同步主链路。

### 1.1 规范术语

| 术语 | 规范定义 |
| --- | --- |
| `Artifact` | 一个版本化抽象文件系统，是 Commit、Ref、Playground、Snapshot 和对象归属的领域根 |
| `Commit` | Artifact 的不可变版本节点；v1 单 parent，形成可分支的 Commit 历史树 |
| `Playground` | 基于某个 Commit 的可读写工作区，拥有独立 IndexVersion，能够发布新 Commit |
| `Snapshot` | 固定 `artifact_id + commit_id` 的只读快照；Ref 移动不能改变其内容 |
| `MetadataBatch` | Agent 向中心分页上传的 IndexDelta/ObjectReceipt 临时批次，不是 Artifact |

当前 format v8 和 CLI 中的 `repository`、`workspace` 是 Standalone 名称：在中心领域模型中分别
映射为 `Artifact`、`Playground`。本地保留 `repository.json` 和 `workspace` 命令，不得因此在新
API、数据库 schema 或协议中继续引入第二套概念名称。

当前已确认的边界（远端生产适配器尚未实现）：

- 中心服务采用模块化单体，逻辑权威经 `AuthorityStore` 与数据库解耦；SQLite 是单进程默认后端，
  PostgreSQL 是多实例/HA/RLS 目标，S3-compatible 存储保存 Chunk payload；
- 客户端通过 header-versioned、模块/动作式 HTTP JSON API 访问中心服务，不直接访问数据库；
- Vue 3 Web 控制台作为独立 `apps/neoengram-web` npm 应用，只消费公开 OpenAPI；首版 MSW 可运行，
  已覆盖租户切换/创建、StorageVolume 登记与放置选择、Project 筛选、Artifact/Playground/Snapshot
  创建、带描述和 Tag Ref 的 Playground Commit、父版本文件 Diff、资源浏览和 Managed Add Job；
  真实联网依赖后续 HTTP/OIDC adapter；
- 第一版远端同步只围绕 `main`/detached Commit，暂不解决多分支合并；
- 服务端保存不可变历史，ref/对象的保留和 GC 由中心策略统一管理；
- 默认部署边界是企业内部多租户；首版认证抽象使用外部 OIDC/JWKS 签发的 Bearer JWT；
- 对象通过短期签名票据访问，客户端不持有长期 S3 凭证；
- v1 只做租户内对象去重，避免跨租户对象存在性侧信道。

### 产品决策状态

| 决策 | 当前建议 | 状态 |
| --- | --- | --- |
| Chunk payload 位置 | 中心 S3-compatible 存储是 Managed 耐久权威；NFS 仅放 Playground/journal/cache | P0 边界已冻结，真实 SDK 待实现 |
| API 传输 | protocol 与 transport 分离；后续评估版本化 HTTP/HTTPS | P0 不实现 transport |
| 一致性模型 | metadata/ref 强一致 CAS；对象经中心确认 Durable 后才能发布 | P0 状态机已实现 |
| 中心权威存储 | `AuthorityStore` + 默认 SQLite；PG/MySQL 独立实现相同行为契约 | SQLite 单节点已完成，HA/RLS 待实现 |
| 身份认证 | `Authenticator` 抽象；v1 外部 OIDC/JWKS + Bearer JWT | 已确定设计，待实现 |
| 授权范围 | tenant → project → artifact → ref；服务端 RBAC，默认拒绝 | 已确定设计，待实现 |
| 对象访问 | 中心 API 鉴权后签发短期、对象/会话范围的 Signed URL | 已确定设计，待实现 |
| 训练快照 | `artifact_id + commit_id` 的固定 Snapshot；可选 sidecar 描述 | 已确定设计，待实现 |
| 历史保留 | ref、pin/hold、active lease/session/有效 ObjectTicket 作为 GC roots；隔离期后再回收 | 已确定设计，待实现 |
| 规模与可靠性 | 千万文件、上亿 Chunk、PB 级 payload；99.9%、RPO 0、RTO 1 小时 | 后续基准验证 |

## 2. 状态标记

| 标记 | 含义 |
| --- | --- |
| 已完成 | 已有代码、测试和文档，行为可作为当前能力依赖 |
| 进行中 | 已开始实现，但验收条件尚未全部满足 |
| 下一步 | 下一条应优先执行的实现任务 |
| 研究 | 需要基准、原型或架构决策后再实现 |
| 暂缓 | 明确不进入当前里程碑 |

## 3. 当前能力（已完成）

### 3.1 本地命令

| 能力 | 状态 | 当前语义 |
| --- | --- | --- |
| `init` | 已完成 | 创建 format v8 SQLite 仓库，并不可变绑定 fastcdc/whole-file/mixed 策略；旧格式明确拒绝 |
| `workspace create/list/remove` | 已完成 | 独立 HEAD/Index/base、分支独占、内外部 Playground 与安全删除 |
| `add` / `add -A` | 已完成 | 固定仓库强制既定策略；mixed 可逐文件选择，支持 BLAKE3 去重和删除暂存 |
| `rm` | 已完成 | 安全移除工作区或仅移除 index，支持持久事务和可验证回滚 |
| `status` | 已完成 | 报告 staged、unstaged、deleted 和 untracked，并拒绝混合状态视图 |
| `diff` | 已完成 | 比较工作区、index 和 Commit，并在输出前复核 Index/HEAD |
| `restore` | 已完成 | 恢复 index 或工作区文件，最终发布时重新检查覆盖条件 |
| `commit` | 已完成 | 从分页 Index 流式发布 Manifest/Directory DAG/Commit，并 CAS 更新 HEAD/ref |
| `log` / `show` | 已完成 | 查看线性历史和 Commit 文件清单 |
| `checkout` | 已完成 | 物化 Commit，支持 detached HEAD 和 `main` 重新附着 |
| `export TARGET DIR` | 已完成 | 原子生成 copy 权限快照，或严格的 WholeFile/Loose 同文件系统硬链接视图 |
| `recover` | 已完成 | 恢复被中断的 checkout/rm 事务 |
| `gc` | 已完成 | 从全部 Playground Index 与全部 Commit roots 标记并回收 Chunk |
| `fsck` | 已完成 | 校验 refs、历史、Directory/Manifest、Chunk 和对象完整性 |
| `mount` / `unmount` | 进行中 | FUSE 协议和生命周期已实现；Linux/macOS 实挂矩阵与百万文件基准待完成 |
| `.neoengramignore` | 已完成 | `add` 与 `status` 共用根目录忽略规则 |
| 独立 `branch` / `switch` | 暂缓 | Playground `--branch` 已有；不提供独立管理命令 |

### 3.2 P0 架构、存储与一致性

- Workspace 已拆为 core、engine、fs、protocol、standalone、agent、CLI 和 `neoengramd`；版本统一为
  `0.2.0`。CLI 只解析输入和渲染，Standalone 每个命令使用独立 Request、显式 cwd 和领域化 typed
  Result；只读、`add`/`commit`/`gc`、mutation 与 lifecycle 均已完成迁移。Standalone 通用
  `CommandResult` 已删除，成功文本只在 CLI 生成。
- `neoengram-core` 提供强类型内容 ID、NFC 逻辑路径、Manifest/Directory/Commit/FileRecord、
  有界 IndexDelta 和唯一规范 digest；公共 API 不再包含扁平 Tree、FileNode 或物化 Index。
- `neoengram-engine` 提供执行 ports、闭合校验的 `PreparedAdd`、结构化错误/重试分类、进度事件、
  故障注入和 mutation plan/journal/receipt 契约；Standalone `commit` 已组合 canonical graph builder
  与 SQLite publisher，Agent 在 durable `running` 后发出 progress report。Engine 不读取 cwd、环境、
  SQLite 或 CLI 文本，也不直接输出。
- Engine 的 `execute_mutation`/`finalize_mutation` 与 `neoengram-fs` journal/lock adapters 已实现；
  Standalone `checkout`、工作区 `restore` 和工作区 `rm` 已通过 transactional Worktree adapter 接入
  `MutationPlan -> durable journal -> WorktreeReceipt`。`add`、`commit`、`mount`、`checkout`、`rm`、
  `restore` 和 `recover` 已把 caller-owned `ProgressSink` 从 facade 透传到 CLI。
- Standalone 的持久化职责拆为 immutable catalog、workspace index、ref 和 workspace registry；
  内部仍有仅限 SQLite/worktree 的过渡物化 view，后续继续迁移到分页 engine ports。
- SQLite 是唯一元数据后端；JSON 后端和旧格式兼容已删除。
- `repository.json` 持久化不可变分块策略；Artifact 在 Index、Directory、Commit 和 fsck 路径强制校验。
- `ObjectStore` 提供流式发布、校验读取、分页枚举、durability barrier、协调删除和可选硬链接能力。
- 本地锁固定按 object -> Playground worktree -> state 获取；工作区读操作共享、mutation 独占，冲突立即失败。
- `add` 基于最初的 `IndexVersion` 做最终 CAS；status/diff 在输出前复核实际依赖的 Index 与
  HEAD/main。
- checkout/rm/restore 在工作区 mutation 前先持久化 Engine journal，再由 format-v8 本地事务执行实际
  文件变更并返回 receipt。checkout/rm 用 plan.expected `IndexVersion` 完成 SQLite 权威 CAS 后才
  finalize；worktree restore 不更新 Index，`restore --staged` 独立更新 SQLite Index。`recover` 同时
  恢复本地事务、清理 stale lock 并收尾 Engine journals，任何无法证明安全的状态仍保留 journal。
- `add`、`gc` 和 `fsck` 使用独立对象锁，避免发布、校验和回收竞态。
- `fsck` 的 Chunk 引用检查已使用有界外部排序，避免完整 Chunk Hash 集合常驻内存。
- protocol v1 已包含资源/代次强类型、ControlEnvelope、Add Assignment、MetadataBatch、S3 Add DTO、
  RFC 8785 JCS + BLAKE3 digest、统一限额 validator 和提交的 JSON Schema。Missing-object 响应绑定
  原请求 scope/digest 并精确分割对象集合；Manifest 使用 `chunk_start` fragment 跨页表达，中心重组后
  校验完整 canonical ID。
- Agent 已有 ledger-first 幂等 Assignment 状态机；durable `TransferReceipt` 保存 exact descriptors/pages，
  Prepared 报告在 metadata staging 前由中心持久化，响应丢失时从 Prepared 幂等重放；失败报告统一为
  protocol `JobFailed`。`neoengramd` 已有 Create/Assign/Report/Stage/Finalize 状态机、异步
  `AuthorityStore`、InMemory 契约后端和默认 SQLite 持久 CAS。
  `JobPrepared.candidate_digest` 绑定 assignment identity、base IndexVersion、descriptors 和 extensions。
  两者都是 library-only，不包含网络；SQLite authority 支持单进程生产持久化，不支持 HA/RLS。
- SQLite authority 独立使用 `authority.sqlite3`/`authority.lock`，不复用 Standalone format v8；只接受
  当前 `application_id`、`user_version` 和 record format，不迁移、双读或回退旧格式。

### 3.3 质量基线

- workspace 测试覆盖 SQLite、CLI、跨进程锁、Index CAS、故障恢复、完整性、FUSE core、只读快照和忽略规则。
- fmt、Clippy `-D warnings`、rustdoc warnings、crate 归档内容检查和 CI 三平台测试已纳入质量门槛。
- 当前代码和文档仍处开发期，仓库格式允许直接演进，不提供旧格式自动迁移。

本地默认 feature 验证不要求 macOS 安装 macFUSE SDK/runtime：

```bash
cargo fmt --all -- --check
bash .github/check-architecture.sh
cargo run -p neoengram-protocol --example generate_schemas --offline
cargo test --workspace --all-targets --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --offline
cargo package --locked --allow-dirty -p neoengram-core
cargo package --locked --allow-dirty --no-verify --exclude-lockfile -p neoengram
```

Schema 命令确定性重建已提交的 `crates/neoengram-protocol/schemas/v1`。CI 在 Linux 运行 workspace
`--all-features` 测试、Clippy、rustdoc 和 MSRV check；Linux、macOS、Windows 的通用矩阵运行默认
feature。core 执行可验证 package，CLI 因依赖 workspace-private crates 只检查 archive assembly。

### 3.4 当前能力与未来能力边界

| 能力层 | 当前能力（已完成） | 下一步/未来能力 |
| --- | --- | --- |
| 客户端数据面 | 工作区、Index、FastCDC/WholeFile Chunk、对象校验、本地恢复 | 远端 push/fetch、断点续传、并发上限和缓存 quota |
| 本地控制面 | SQLite 元数据、Merkle Directory、线性历史、HEAD/ref CAS、fsck/gc | SQLite adapter 继续收敛到 engine 分页 ports |
| 中心/Agent | protocol v1、Ledger、异步 AuthorityStore、中心状态机与 SQLite 单节点权威 | HTTP/mTLS、PostgreSQL HA/RLS、授权、调度与生产 daemon |
| 远端数据面 | 缺块协商、短期票据、receipt/durability DTO 与 ports | 真实 S3、幂等上传、Signed PUT/GET、对象生命周期 |
| 读取面 | checkout、权限快照和固定 Commit FUSE | Snapshot、Shard 分页、mount lease、训练读取票据 |
| 安全治理 | 本地路径安全和普通 Unix 权限 | JWT、RBAC、RLS、租户隔离、审计、密钥轮换和威胁模型 |
| 训练数据语义 | 普通文件版本控制 | 可选 dataset sidecar、schema/source 摘要、确定性文件级 ShardSet |

表中未来能力不得在没有代码、契约测试和验收记录时标记为“已完成”。

## 4. 当前限制与技术债务

这些限制不能在分布式服务上线前被忽略：

1. **没有可联网生产控制面**：已有版本化 protocol、状态机和 SQLite 单进程权威，但尚无 HTTP、
   mTLS、PostgreSQL HA/RLS、认证、授权、生产调度或可运行 daemon。
2. **没有真实对象同步**：已有缺块/票据/完成/durability DTO，但尚无 S3 SDK、multipart、断点续传、
   fetch/clone/push/pull。
3. **文件语义不完整**：当前模型未保存 POSIX mode、符号链接、xattr、ACL 或 sparse 信息。
4. **规模热点仍存在**：Standalone 的部分 SQLite/worktree compatibility view 和 GC 仍可能物化完整索引或引用集；
   loose object 目录仍是平铺扫描，完整文件缓存没有 quota/lease；远端分页、租约和 GC 尚未实现。
5. **历史能力有限**：只有单父线性历史，没有命名分支、merge、rebase、tag 和 reflog。
6. **只读快照不是安全边界**：`0444/0555` 可被拥有权限的用户或 root 修改；hardlink 视图还与
   Loose 对象共享 inode 和权限，写入会污染所有引用该对象的快照。损坏必须从可信副本恢复。
7. **没有训练读取语义**：当前不存在固定 Snapshot、ShardSet、schema/source 摘要或
   训练期间的 lease/retention root。
8. **没有远端安全治理实现**：尚无 JWT 验证、RBAC、租户 RLS、Signed URL、审计、KMS/密钥轮换或
   跨租户隔离实现。

## 5. 目标架构

P0 已冻结 production/runtime 的主要源码依赖方向：

```text
neoengram CLI -> standalone -> engine <- agent
                     |          ^       ^
                     +-> fs ----+    protocol <- neoengramd
                         ^             ^
                         +---- core ---+
```

该简图不枚举 CLI 对 core/engine 的直接类型导入；Agent 对 `neoengramd` 的 `dev-dependency` 只用于
内存端到端组合测试，不属于生产依赖方向。

Managed 运行时的目标数据流是用户 API 进入 `neoengramd`，中心经 `AuthorityStore` 持久化
Job/Assignment 并通过未来
transport 交给 Agent；Agent 访问 Playground/NFS 和中心 S3 数据面，结构化结果返回中心做最终 CAS。
当前已实现 protocol、异步 ports、状态机和 SQLite 单进程 authority；transport 与真实 S3 尚未实现。

边界规则：

- PostgreSQL/S3 不作为本地 `MetadataStoreKind` 或 `ObjectStoreKind` 的简单枚举值。
- 客户端只访问 `neoengramd` 的版本化 API，不直连 PostgreSQL，也不持有长期 S3 凭证。
- 服务端必须在 Index/ref CAS 前验证 Commit → Directory → Manifest → Object 的完整引用图。
- 控制面负责认证、授权、元数据强一致、租约和审计；数据面只接受服务端签发的受限票据。
- 读取面先把 ref 解析为固定 Commit，再分页读取 Manifest/Shard；ref 后续移动不能改变已打开
  Snapshot 的内容。
- 所有上传接口必须幂等；客户端重试不能产生不同对象或重复提交。
- tenant-owned 表的唯一约束、外键、cursor、session、lease 和 quota 都必须保留租户边界；
  对象存储 key 由服务端生成，禁止客户端传入物理路径。
- v1 的远端对象单元是独立 Object；Pack、hash fanout 和可验证 Pack range 是 P5 的存储优化，
  不得被 P2 的 Chunk 上传接口隐含承诺。

节点侧 Agent 和中心 Job/Assignment/Finalize 状态机已有 library + 内存适配器；多租户生产部署、
多 EdgeCluster、CPU/NFS 调度、Gateway 和跨卷 checkout 仍处设计阶段，详细草案见
[`agent-central-control.md`](agent-central-control.md)。该文档不代表当前已经存在网络 Agent、
中心 PostgreSQL、生产 lease/fencing、Gateway 或 daemon。

远程 Agent 设计已冻结以下存储约束：一个 Tenant 每个 EdgeCluster 可有多个 StorageVolume，一个
Volume 可承载该 Tenant 的多个 Artifact，一个 Artifact 每集群最多一个 active `ArtifactPlacement`；
StorageVolume 是 RW ownership/fencing 单元，每个 owner generation 只有一个活动 RW Agent。Managed
不可变对象的最终权威是中心 S3；NFS 只保留 Playground、journal 与可重建 cache。Artifact 根必须
唯一且不重叠，禁止跨 Artifact hardlink；更换 NFS 必须经过
freeze/copy/verify/CAS/drain/cleanup 迁移状态机。

Kubernetes 用户 Pod 只挂载本集群 NFS 上单个 Playground/Snapshot 的精确视图目录。中心通过
`PodMountBinding` 描述和校验已有 Pod 的容器路径、StorageVolume、视图目录与 RO/RW 模式；Pod 的
实际 I/O 经节点 NFS/CSI 客户端直达 NFS，不经过 Agent。Pod、NAS、PV、PVC 和 CSI volume 的创建、
下发与回收不在本设计范围内；Snapshot 强制 RO，Playground RW 由部署策略协调。

## 6. 分阶段实现计划

### P0：项目结构与协议抽象

状态：**已完成**

交付：

- Workspace 版本升级为 `0.2.0`、仓库升级为 format v8，并按 core/engine/fs/protocol/standalone/
  agent/CLI/`neoengramd` 拆分；除 core 和 CLI 外均为 private package。
- core 冻结强类型 ID、逻辑路径、Manifest/Directory/Commit/FileRecord、分页 IndexDelta 和 canonical
  digest；保留既有有效内容域，只统一 IndexVersion 的后端无关算法。
- engine 冻结 ports、每用例 Request/Result、`PreparedAdd`、错误分类、进度、故障注入和 mutation
  journal/receipt；Standalone 接管 SQLite、Repository、FUSE 与本地最终发布，CLI 成为唯一渲染层。
- protocol v1 冻结资源/代次 ID、ControlEnvelope、完整 Add Assignment、MetadataBatch/S3 Add DTO、
  1 MiB/8 MiB/4096 限制、Schema、未知字段 round-trip 和 RFC 8785 JCS + BLAKE3 digest。
- Agent 实现 ledger-first、同 digest 重放与不同 digest `JOB_ID_REUSED` 状态机；中心实现
  CreateAddJob、AssignJob、ReceiveReport、StageMetadataBatch、FinalizeAdd 及内存 ports/CAS。
- 固定 Managed Add 闭环和存储权威：对象先直传中心 S3 并确认 Durable，再验证 MetadataBatch，
  最后执行 expected IndexVersion CAS；NFS 仅保存 Playground、journal 和可重建 cache。

验收：core/protocol golden 与 validator 测试、Agent/中心幂等和 CAS 组合测试、format v8 本地测试及
架构依赖检查通过。P0 明确不包含 HTTP、PostgreSQL、mTLS、OIDC、真实 S3、NFS fencing 或 daemon。

### P1：中心元数据与安全 MVP

状态：**计划**

交付：

- 为现有 `services/neoengramd` library 增加 transport、生产配置和独立 PostgreSQL adapter/migration；
  不共享 SQLite SQL、migration 或物理 schema。
- 表覆盖 tenants、projects、artifacts、refs、commits、directories、manifests、object catalog、
  role bindings、sessions、snapshots、leases/holds 和 append-only audit。
- 实现外部 OIDC/JWKS Bearer JWT 验证、User/Service principal、RBAC、tenant context 和 PostgreSQL
  RLS；数据库运行角色不得拥有 `BYPASSRLS`。
- 提供 artifact 创建、读取 ref、固定 ref→Commit 的 Snapshot resolve、读取 Commit/Directory/Manifest、
  完整引用图校验、DatasetProfile 状态、lease acquire/renew/release、pin/hold 和 ref CAS API；P1 只实现
  元数据和 API 契约，不宣称远端对象已经可供训练读取。
- 校验可选 `.neoengram-dataset.json`；解析 schema/source/lineage/shard 摘要但不建设样本级平台。
- 建立审计字段脱敏、TLS、SSE-KMS/workload identity，以及 JWKS/双凭证无中断轮换能力；P4 再做
  定期轮换、应急吊销和泄漏演练。

验收：两个客户端同时更新同一 ref 时最多一个成功；无效或过期 JWT、越权请求和跨租户查询默认
拒绝；服务重启不丢失已提交 metadata、Snapshot、DatasetProfile、lease、pin 或 audit；返回对象均通过内容
ID 和完整引用图校验。

### P2：授权 Push 纵切

状态：**计划**

交付：

- 客户端 `remote add` 和 `push` 初版，创建绑定 tenant、artifact、principal 和幂等键的上传
  session。
- 服务端返回缺失的 Commit/Directory/Manifest/Chunk inventory；对象存在性、缺块结果和 CAS 当前值不
  泄漏给无权 tenant/project/artifact/ref，已授权调用者才可看到冲突的当前 ref。
- 通过短期 Signed PUT/multipart ticket 上传到 tenant-scoped S3；上传前后验证 hash、size、
  checksum 和 KMS/存储策略。
- 只有对象和 metadata 完整发布且 finalize 时重新鉴权后，才执行 expected-ref CAS 并使 Snapshot
  可读取；sidecar 独立校验，合法时发布 `DatasetProfileState::Ready`，非法时拒绝训练 profile，
  但不改变普通 Snapshot 的只读有效性。
- 支持 quota、失败重试、幂等 session、ACL 撤销、session 过期清理和中断后继续。

验收：Commit 可从空远端完整推送；并发 push 不覆盖他人 ref；权限撤销后不能 finalize 或续签新票据；
任一中断点恢复后不会出现 ref 指向缺失对象的状态。

### P3：Fetch / Clone 与训练读取纵切

状态：**计划**

交付：

- `fetch` 获取经授权的 refs、固定 Commit/Directory graph 和缺失对象；`clone` 初始化本地 SQLite 仓库并恢复
  目标 Commit。
- 提供固定 Snapshot 的 Manifest/Shard 分页、确定性文件级 ShardSet、lease 和续租 API。
- 通过短期 Signed GET 读取完整 Chunk；v1 拒绝普通 Chunk 的 Range GET。Pack/range 只有在 P5
  定义可验证 receipt 后才能通过 capability 开启；票据不允许 list/delete/任意 prefix。
- 下载先进入临时对象，durability barrier 成功后才发布本地 metadata/ref；支持并发上限、重试、
  已有 Chunk 复用和本地 cache quota。

验收：ref 移动不影响已打开 Snapshot；同一 Snapshot/Shard 参数在不同客户端和重启后成员集合
一致；无权限用户不能因知道 Chunk ID 而下载；clone 后 `fsck`、`status`、只读快照和 checkout
结果与源仓库一致。

### P4：安全强化、生命周期与运营

状态：**计划**

- 审计外部不可变归档、检索和告警；执行 JWT/JWKS、数据库、S3/KMS 凭证的定期轮换、应急吊销
  和泄漏演练。
- 服务端 generation/cutoff、两阶段 GC、retention root、lease 过期 24 小时保护和不可达对象
  至少 7 天隔离；支持 pin/hold、备份、恢复和迁移回滚。
- API 限流、动态 tenant quota、metrics、trace、健康检查、SLO 告警和 RPO/RTO 演练；P2/P3 的
  quota 只提供上传/读取安全上限，P4 再提供配额运营、告警和策略调整。
- `pull` 和远端跟踪状态；历史分支能力另开决策，不自动引入 `branch/switch`。

验收：权限、租户隔离、Signed URL 撤销窗口、备份恢复、GC 并发和故障注入测试全部通过；服务端
能够解释每次 ref 变更、票据签发、租约变化和对象回收原因。

### P5：大规模数据路径

状态：**研究**

- add/status/commit/rm/checkout/gc 的分页 merge 和有界内存改造。
- 对象 hash fanout、pack、catalog 和批量标记；P4 已冻结的 generation/cutoff、两阶段 GC、roots、
  grace/quarantine 语义在此阶段只做规模化实现和性能优化。
- 文件缓存 quota/lease/LRU；追加式 checkout/rm/restore journal；研究 Pack range receipt。
- 以千万路径、上亿 Chunk、超大 Manifest 和 PB 级 payload 验证 RSS、吞吐、恢复和 SLO。

验收：命令内存由页大小和有界并发决定，而不能随仓库总量线性增长；大规模授权和 GC 不产生跨
租户泄漏或不可解释删除。

### P6：通用文件系统语义

状态：**进行中**

- 固定 Commit 的 Linux FUSE3/macFUSE 只读视图已实现；Dokan、可写 overlay 和远端下载不在 v1。
- 新格式保存 POSIX mode、符号链接和必要的节点类型。
- 明确跨平台路径、ACL/xattr、sparse 文件和权限恢复策略。
- 普通 `export` 仍是权限快照，不等价于内核只读挂载。

## 7. 远端同步与读取不变量

任何远端实现都必须保持以下顺序和不变量：

1. 不可变对象先写入并校验，不能先发布 ref 或暴露可读取 Snapshot。
2. 服务端 metadata 只引用已存在且内容 ID、大小和完整引用图均正确的对象。
3. ref/HEAD 更新必须带 expected value、幂等键和授权 session，并在服务端事务中完成。
4. 客户端本地 ref 只在下载对象持久化、校验和 durability barrier 成功后更新。
5. 重试同一个 session 的结果必须与首次成功结果相同；session 必须绑定 tenant、artifact、
   principal 和操作范围，不能跨租户或跨主体重放。
6. 客户端和服务端都不能信任对方提供的物理路径，只接受逻辑 ID、大小、checksum 和受限 cursor。
7. 除存活/就绪探针外，所有 inventory、metadata、ticket、lease、ref 和 Snapshot API 先认证再授权；上传 finalize、
   ref CAS 和 lease renew 必须再次检查当前权限。
8. ref 只在读取开始时解析为固定 Commit ID；后续 ref 移动不得改变该 Snapshot 的 Manifest、Shard
   或对象可见性。
9. 对象读取必须带 artifact + 固定 Snapshot/Commit 上下文；知道 Chunk ID 不能单独获得读取权限。
10. lease、pin/hold、ref、活跃 push/fetch session 和有效 ObjectTicket 都是 GC roots；租约或票据有效
    期间不得删除 Snapshot 可达的 metadata 或 Chunk。
11. 缺失、损坏、大小不符或 hash 不符的 Chunk 必须硬失败，禁止静默跳过、替换或返回部分成功。

## 8. 安全与身份架构

### 8.1 认证接口

服务端通过与身份提供方无关的 `Authenticator` 抽象认证请求，v1 实现 Bearer JWT：

- JWT 由外部 OIDC/工作负载身份提供方签发；NeoEngram 不在 v1 自建登录、密码、账号恢复或
  生产静态 API Token 服务。
- 信任配置包含 issuer allowlist、固定 audience、HTTPS JWKS、允许的非对称算法和时钟容差。
  必须验证签名、`iss`、`aud`、`sub`、`exp`、`iat`、存在时的 `nbf` 和 `kid`，拒绝 `alg=none`、
  对称算法和算法降级。
- token 最长有效期 60 分钟，时钟偏差容忍 60 秒；未知 `kid` 可刷新一次 JWKS，无法取得可信
  公钥时 fail closed。
- 认证结果统一转换为 `PrincipalContext`，至少包含 `(issuer, subject)` 全局身份键、principal
  类型（User/Service）和 request ID。JWT 中的 tenant、group、role 只作为审计提示，不能替代
  服务端角色绑定。
- Principal 在服务端有 `active/disabled_at` 状态，每次授权都检查；v1 不维护完整 JWT denylist，
  依靠主体即时禁用、短 TTL 和 issuer/key 轮换控制重放窗口，审计可记录不可逆 token fingerprint。
- CLI 用户和训练任务使用同一接口；CLI 从外部 IdP/平台获得 JWT 并注入请求，NeoEngram 不自行发明
  登录流程；训练任务优先使用 workload OIDC/JWT，不能携带长期 S3 密钥。

### 8.2 授权与资源范围

`Authorizer(principal, action, resource)` 与认证分离，采用默认拒绝、显式允许、向下继承、v1
不支持显式 deny 的 RBAC：

| 资源范围 | 代表性 action |
| --- | --- |
| Tenant | `tenant.read`、`tenant.manage`、`member.manage`、`audit.read`、`retention.manage` |
| Project | `project.read`、`project.manage`、`artifact.create` |
| Artifact | `metadata.read`、`object.read`、`object.write`、`commit.publish`、`snapshot.read`、`snapshot.lease`、`artifact.manage`、`retention.manage` |
| Ref | `ref.read`、`ref.update` |

内置角色为 `TenantAdmin`、`ProjectAdmin`、`ArtifactReader`、`ArtifactWriter` 和
`ArtifactMaintainer`。最小 action 映射如下；角色可绑定在 tenant、project、artifact 或具体
ref，并按表中规则向下生效：

| 角色 | 允许的最小 action | 绑定/继承规则 |
| --- | --- | --- |
| `TenantAdmin` | tenant/member/project/artifact/audit/retention 管理，以及其租户内全部仓库 action | tenant 绑定向下继承；不跨 tenant |
| `ProjectAdmin` | project 管理、`artifact.create/manage`、项目审计读取 | project 绑定只管理该项目；数据读写和 ref 更新仍需仓库角色 |
| `ArtifactReader` | `metadata.read`、`object.read`、`snapshot.read`、`snapshot.lease`、`ref.read` | artifact 绑定覆盖其 refs；ref 绑定只覆盖该 ref |
| `ArtifactWriter` | Reader 全部 action，加 `object.write`、`commit.publish`、`ref.update` | artifact 绑定可更新其全部 refs；ref 绑定只能更新该 ref |
| `ArtifactMaintainer` | Writer 全部 action，加 `artifact.manage`、`retention.manage` | 不能因此获得 tenant/member 管理权 |

角色绑定采用 allow-only 语义；低层级绑定不能绕过上层 tenant/project 隔离，v1 不做路径级、
单文件级或样本级 ACL。绑定的创建、撤销和 bootstrap 只允许 TenantAdmin 或受控 operator，并
必须记录审计；ProjectAdmin 只能管理其 project 内的 artifact 绑定。`snapshot.lease` 不隐式
授予 `object.read`，权限撤销后不能续租或签发新票据。

### 8.3 租户隔离、对象票据与密钥

- 每个 tenant-owned 表使用非空 `tenant_id`；唯一约束、外键、cursor、session、幂等键、lease
  和 quota 均包含租户边界。每个事务设置并校验 tenant context，PostgreSQL 启用 RLS，普通请求
  运行角色不得拥有 `BYPASSRLS`。GC、备份、审计归档和租户统计使用独立的受控 worker/operator
  身份，逐租户执行或调用最小权限的审计存储过程；所有跨租户运维动作必须被审计，不能通过给普通
  runtime role 开超级权限来绕过 RLS。
- S3 key 由服务端生成并带 tenant prefix；v1 只做租户内去重。inventory、404 和缺块协商不能
  暴露其他租户或无权仓库的对象是否存在；共享客户端缓存的索引必须包含 tenant，缓存命中不能
  绕过在线授权。
- 客户端只能通过中心 API 获得短期 `ObjectTicket`。Signed URL 默认 TTL 10 分钟、硬上限 15
  分钟，精确绑定 tenant、artifact、object key、HTTP method、session、size 和 checksum；
  禁止 list、delete、任意 prefix 或永久凭证。
- v1 只允许单对象完整 GET/PUT/multipart part；普通 Chunk 的 Range GET 必须拒绝。Pack/range
  只能在 P5 定义可验证 receipt 后通过 capability 开启，数据进入验证缓存前仍需完成完整 Chunk
  hash 或 receipt 校验。有效 ObjectTicket 在过期前必须是短期 GC root，且其 TTL 不得超过统一
  deletion grace；已签发 URL 在 TTL 内无法即时撤销，该窗口必须作为威胁模型和审计字段记录。
- 传输使用 TLS；对象和数据库使用存储侧加密，生产环境使用 SSE-KMS/租户密钥引用。数据库、
  S3 和 KMS 凭证优先使用 workload identity，无法使用时放入 Secret Manager，支持双凭证重叠轮换。
- IdP 通过重叠 JWKS key 无中断轮换；NeoEngram 不保存 IdP 私钥，也不把 JWT、Signed URL 或密钥
  写入日志。

### 8.4 审计与威胁模型

P1 建立 append-only 审计基线，记录 principal、tenant、action、resource、允许/拒绝、request/
session ID、来源、错误码、expected/current/new ref、票据签发、租约、权限变更和 GC 原因；默认
在线保留 180 天，P4 增加不可变外部归档、检索和告警。原始 JWT、Signed URL、source locator、
sidecar 私密字段和数据内容不得进入审计或普通服务日志。

重点防护恶意或失陷客户端、JWT 伪造/重放、越权 ref 更新、session 劫持、跨租户引用、对象存在性
探测、Signed URL 权限放大、上传损坏、CAS 竞态、恶意大 Manifest、并发/带宽/quota 资源耗尽、
票据出现在 Referer/代理日志/shell history/崩溃转储以及审计字段注入。明确不承诺抵御客户端
root、外部 IdP/数据库/S3/KMS 管理员完全失陷，也不把 `export` 当作不可绕过的安全边界。

## 9. 训练数据 Snapshot、Shard 与生命周期

### 9.1 Snapshot 身份与描述

- `Snapshot` 的稳定身份是 `tenant_id + project_id + artifact_id + commit_id`；Directory ID
  只表示文件内容指纹，不另复制一套文件图。
- ref（例如 `main`）只在训练开始时解析一次；训练、恢复、重试和审计始终记录完整 Commit ID。
- 只有 Commit → Directory → Manifest → Chunk 全图已持久化并校验后，Snapshot 才可读取；Snapshot
  一旦可读取便始终固定该 Commit，不再使用训练状态改变它的只读文件语义。
- sidecar 存在并通过 schema/source/ShardSet 校验时，独立的 `DatasetProfileState` 进入 `Ready`，训练
  API 只接受具有 Ready profile 的 Snapshot。sidecar 缺失时 Snapshot 仍是合法普通文件快照，但
  显示为未声明训练 profile；sidecar 无效时 profile 进入 `Rejected`，Snapshot 本身不失效。
- 根目录可有 `.neoengram-dataset.json`，作为普通版本文件进入 Directory，记录 schema digest、直接
  source locator/version/digest、上游 `artifact_id + commit_id`、transform recipe digest 和
 ShardSet 参数。它不得包含当前 Commit/Directory ID、凭证或签名 URL；默认不进入训练文件选择集。
- source lineage v1 只记录直接来源和稳定 digest/版本；locator 必须移除凭证、签名参数和其他秘密，
  不执行或编排数据转换。
- lineage 引用是不可自动展开的不透明摘要；读取上游 artifact/Commit 的详细 metadata 仍需单独
  通过上游资源的 `metadata.read` 授权，错误信息不得泄露上游租户或 Artifact 是否存在。

### 9.2 Shard 与读取一致性

- Shard 是训练逻辑分片，不等同于存储 Chunk。v1 使用版本化、确定性的 path-hash 对完整文件
  分片，参数包括算法版本、seed、shard count 和 include roots。
- 纳入选择集的文件恰好属于一个 shard，允许空 shard；不承诺样本、行、压缩包内部或任意
  byte-range 分片。Manifest/Shard 分页 cursor 必须绑定 tenant、artifact、Commit 和查询参数。
- `SnapshotHandle` 固定 Commit、Directory、状态和查询上下文；ref 后续移动不影响已经打开的 Snapshot。
- 客户端暴露训练数据前必须完成 Chunk hash/receipt 校验；缺失、损坏或大小不符必须硬失败。

### 9.3 Lease、保留和 GC

- `SnapshotLease` 至少记录 `lease_id`、tenant、artifact、Snapshot/Commit、service principal、
  workload/job ID、TTL、renew token 和撤销原因；默认 TTL 60 分钟，客户端每 20 分钟续租。lease
  只阻止 GC，不隐式授予读取权限；获取、续租、撤销主体和结果都进入审计。
- ref 可达历史、显式 `Pin`/`RetentionHold`、活跃 Snapshot lease 和活跃 push/fetch session 都是
  retention roots；服务重启后这些状态必须保持。
- lease 过期或撤销后不再产生新的可达 root，但仍提供至少 24 小时的额外保护；对象一旦成为不可达
  就进入统一的至少 7 天 quarantine，24 小时不能缩短该下限。GC 使用 generation/cutoff 和两阶段
  mark/sweep，避免删除正在上传、被有效 ticket 保护或正在读取的对象。
- v1 不自动裁剪任何 ref 可达历史；重要 detached Snapshot 必须显式 pin 或持有 lease。失权会
  阻止新票据和续租，但不追溯取消已签发的短期 URL。

## 10. 协议级接口与首版设计目标

协议阶段固定以下概念，具体 HTTP 路径和序列化字段在 `neoengram-protocol` 中定义：

- `PrincipalContext`、`Authenticator`、`Authorizer`、`Action`、`ResourceScope`、`AuthorizationDecision`；
- `PushSession`、`FetchSession`、`ObjectTicket` 和租户/主体绑定的幂等请求；
- `SnapshotHandle`、`DatasetProfileState::{Ready,Rejected}`、`ShardSetSpec`、opaque 分页 cursor；
- `SnapshotLease`、`Pin`、`RetentionHold`；
- ref CAS 请求必须携带 expected/current/new、幂等键和授权 session 身份；
- 401 表示认证失败，403 表示已认证但无权限，404 用于隐藏不可见资源，409 表示 CAS 或 session 冲突。

首版设计目标（P0 基准前的门槛，不代表当前承诺）：

| 指标 | 目标 |
| --- | --- |
| 容量 | 单 Snapshot 千万文件、单仓库上亿 Chunk、PB 级 payload |
| 控制面可用性 | 月度 99.9% |
| RPO / RTO | 目标：已确认 metadata 和 finalized object 的 RPO 0；RTO ≤ 1 小时 |
| 元数据延迟 | 同区域常规负载下 ref/Snapshot/CAS p95 ≤ 250 ms |
| 分页延迟 | 1000 条 Manifest/Shard 页面 p95 ≤ 300 ms |
| 数据吞吐 | Signed 直传达到同并发直接对象存储基线的至少 80% |
| 审计保留 | 在线 180 天；P4 支持外部长期归档 |

RPO 0 只适用于完成同步持久化/复制并通过 durability barrier 的 metadata 和 finalized object，
不适用于未完成的上传 session；PostgreSQL 复制、对象存储 durability、备份频率和跨故障域恢复
必须在 P0/P4 演练中证明。若无法证明，必须在本文记录降级目标，而不能把设计目标当作服务承诺。
P0 基准若需要调整这些值，必须在本文记录问题、实验、结论、风险和新的路线决定。

## 11. 研究清单

| 主题 | 要回答的问题 | 输出 |
| --- | --- | --- |
| PostgreSQL schema/RLS | tenant/project/artifact/ref 如何分页、索引并保证跨租户约束？ | migration + 查询/隔离基准 |
| JWT/JWKS | issuer、audience、算法拒绝、未知 kid 和无中断轮换如何验证？ | verifier contract + failpoint 测试 |
| RBAC/审计 | role × scope × action、404 隐藏和审计保留如何控制写放大？ | 授权矩阵 + audit schema |
| ObjectTicket | Signed URL 的对象范围、TTL、checksum、撤销窗口和 KMS 条件如何落地？ | ticket adapter 原型 |
| Snapshot/Shard | sidecar、Ready 状态、固定 Commit、path-hash 分片和 cursor 如何跨客户端复现？ | Snapshot/Shard contract + golden vectors |
| S3 一致性 | multipart、校验、生命周期和失败清理如何保证？ | object adapter 原型 |
| Push session | 如何恢复部分上传、重新鉴权并避免 ref 竞态？ | 状态机 + failpoint 测试 |
| 协议版本 | 客户端/服务端如何协商能力和升级？ | protocol compatibility matrix |
| GC/生命周期 | 如何处理并发 push、lease、pin/hold、保留策略和 detached 历史？ | generation/cutoff 方案 |
| 大规模性能/SLO | 千万路径、上亿 Chunk 和 PB payload 下 RSS、延迟、吞吐和 RPO/RTO 是否达标？ | 可重复 benchmark + 容量报告 |
| 文件语义 | mode、symlink、sparse 在各平台如何表达？ | 格式扩展决策 |
| NFS placement/owner | export/fsid 别名、重叠根、单 RW Owner 和全卷 failover 如何强制？ | registry 约束 + fencing/迁移状态机 + 故障注入 |

研究项在没有“问题、实验、结论、后续动作”四项内容前，不得标记为已完成。

## 12. 测试与发布门槛

- Core 契约：强类型 ID、Manifest/Directory/Commit/Index canonical golden、NFC/保留名/前缀冲突、
  IndexDelta 排序分页和非法引用。
- Engine/Standalone：结构化 Request/Result、PreparedAdd candidate、错误分类/进度、mutation journal
  顺序、format v8 SQLite、对象完整性、Index/HEAD CAS、固定锁序、恢复和路径安全。
- FUSE 契约：inode/cookie、跨 Chunk range read、LRU/single-flight、只读错误码、固定 Commit、信号和 mount table 卸载验证。
- 本地竞态：shared/shared 成功，shared/exclusive 拒绝；add 暂停期间工作区 mutation 被拒绝，
  index-only 更新使 add CAS 失败，status/diff 不输出跨版本报告。
- 本地故障注入：rename 成功后的目录同步失败、事务 draft 发布前后退出、嵌套 backup、恢复重放、
  restore 预检后目标出现或变化；任何不确定状态都保留 journal，不能丢失唯一副本。
- 协议契约：JSON/Schema/JCS golden、未知字段 round-trip、非法 ID/代次、1 MiB control 限额、
  8 MiB/4096 records page 限额、digest 篡改和未知消息 `PROTOCOL_UNSUPPORTED`。
- 组件组合：create job -> assignment -> ledger -> prepared -> Durable objects -> complete Batch ->
  expected IndexVersion CAS -> decision/finalized；在各边界重复投递，并覆盖 Job digest reuse、缺页、
  非 Durable object 和 CAS conflict。
- 架构检查：protocol 不含 engine/fs/SQLite/HTTP，Agent 不依赖 standalone，`neoengramd` 不依赖
  engine/fs，CLI 之外没有终端输出。
- 认证测试：错误签名、错误 issuer/audience、过期、未来 `nbf`、未知 `kid`、JWKS 轮换、服务身份
  和日志脱敏。
- 授权测试：User/Service principal 的 role × scope × action 矩阵、默认拒绝、ref 级绑定、ACL 撤销
  与 finalize/renew 竞态。
- 租户隔离：相同仓库名、Commit ID、Chunk hash 下的跨租户枚举、引用、下载、cursor/session 重放
  和伪造 tenant context；同租户无权 project/artifact/ref 也不能枚举，RLS 必须拒绝越权；受控
  worker 不得用普通 runtime role 绕过 RLS。
- 对象票据：method/key/session/TTL/size/checksum 限制、有效 ticket 作为 GC root、Signed URL 撤销
  窗口、KMS 条件和完整 URL 不进入日志；普通 Chunk Range GET 必须拒绝。
- Snapshot/Shard：Snapshot 固定性与 DatasetProfile Ready/Rejected 状态矩阵、ref 移动后读取稳定、sidecar 无效硬失败、
  分片无遗漏/重复、lease/pin 在重启后保持、有效 ticket/lease 下 GC 不删对象。
- 资源耗尽：恶意大 Manifest、上传/multipart、分页 cursor、并发 lease、带宽和 quota 绕过必须触发
  限流或硬失败，且不会留下不可回收 session。
- 服务集成：PostgreSQL migration、S3 兼容存储、push/fetch/clone、quota 和全链路审计。
- 故障注入：网络中断、进程终止、重复请求、对象损坏、数据库故障、密钥轮换、并发 CAS 和 GC。
- Agent 存储布局：每 Artifact/EdgeCluster 单 active placement、同租户多 Artifact/Volume、根路径非重叠、
  NFS 别名拒绝、单 Volume RW Owner、全卷 failover、跨 Artifact hardlink 拒绝和显式 placement 迁移。
- Kubernetes 挂载：已有 Pod 的容器路径到本集群物理 NFS、StorageVolume、ArtifactPlacement 和
  `PodMountBinding` 精确视图目录的映射，以及 sibling/objects/journal/Volume root 逃逸拒绝。
- 规模基准：路径数、Chunk 数、Manifest/Shard 大小、峰值 RSS、吞吐、写放大、恢复时间和 SLO。
- 发布门槛：fmt、Clippy `-D warnings`、rustdoc、全量测试、三平台 CI，以及两个 `.crate` 的 README、
  MIT/Apache-2.0 许可证和 metadata 检查；远端阶段另需 migration dry-run、灾备演练和安全审计。
  `neoengram-core` 使用 locked package 归档检查；CLI 因依赖 workspace-private engine/standalone，当前
  `--exclude-lockfile` 检查只证明归档可组装及内容正确，不证明 crates.io 解析或 registry 安装能力。

## 13. 文档维护规则

以后每次实现、研究或架构决策都必须同步更新本文：

1. 开始实现、研究或安全评审时，在对应阶段或研究表中标记状态，并写清目标和验收条件。
2. 完成代码后，更新“当前能力”、阶段状态、实现入口、权限边界和测试结果；未实现能力不得写成已完成。
3. 研究结束后，记录问题、实验、结论、未决风险、容量数据和下一步动作。
4. 改变协议、身份/授权模型、租户边界、存储格式、并发语义、保留策略或 SLO 时，先更新本文的
   目标架构和不变量，再改代码或 migration。
5. README 只保留用户使用说明；详细路线、研究、安全决策和历史统一维护在本文。
6. 每次更新修改“最后更新”日期；已完成事项不能从历史中删除，只能移动到当前能力或变更记录。
7. 任何认证、授权、审计、租约、GC 或密钥轮换变更都必须同步更新威胁模型和对应测试门槛。

## 14. 路线变更记录

| 日期 | 变更 | 原因/依据 |
| --- | --- | --- |
| 2026-07-19 | 明确目标为“中心化元数据 + 分布式对象存储”的文件版本管理系统 | 用户目标确认；本地 Phase 1 作为客户端数据面基础 |
| 2026-07-19 | 完成只读快照、忽略规则和 fsck 有界 Chunk 标记 | 单机核心能力和安全基线完成 |
| 2026-07-19 | 将路线扩展为控制面/数据面/读取面，并纳入 Snapshot、Shard、Lineage、lease 和生命周期语义 | 面向 AI 训练数据维护的可复现读取需求 |
| 2026-07-19 | 确定 Authenticator + 外部 JWT/JWKS、tenant→project→artifact→ref RBAC、短期 ObjectTicket 和租户隔离基线 | 鉴权、认证、审计与跨租户安全要求 |
| 2026-07-19 | 将基础安全从 P4 前移到 P0/P1，P4 保留安全强化、灾备、GC 和运营 | 分布式控制面上线前必须具备默认拒绝和可审计边界 |
| 2026-07-19 | 增加 worktree 读写锁、Index CAS 复核和先发布 journal 的本地事务协议 | 消除并发覆盖、检查后覆盖和 post-rename 同步失败导致的数据风险 |
| 2026-07-19 | 补齐 package metadata、包内双许可证及归档 CI gate | 让归档和未来发布材料可审计；CLI 含 private path dependencies，当前 gate 不代表 crates.io 可安装，且未创建 release 或 tag |
| 2026-07-24 | 明确多 EdgeCluster 是网络隔离边界，跨集群 checkout 通过源 S3 Gateway 传输固定 Commit 对象 | 集群间 Agent/NFS 不互通，中心只协调 TransferRoute/Ticket 且不代理 payload |
| 2026-07-24 | 冻结 Artifact、Commit、Playground、Snapshot 术语，并将 Agent 临时上传结果改称 MetadataBatch | Artifact 只表示版本化抽象文件系统，避免与 Job 输出重名；读写和只读视图具有明确边界 |
| 2026-07-24 | 冻结 ArtifactPlacement、同租户多 Artifact/Volume 和单 Volume RW Owner 约束 | 避免同一 NFS 上多 Agent 写入与 hardlink/根目录重叠；更换 NFS 必须走可恢复迁移状态机 |
| 2026-07-24 | 在跨集群总图中把每个集群分为系统组件、居中 NFS、业务 Pod 三区，并补充 PodMountBinding | 保留 Pod 精确视图挂载与基础设施边界；当时的 NFS object-root/Gateway 假设已被 2026-07-26 中心 S3 权威决策取代 |
| 2026-07-26 | 完成 `0.2.0` P0 crate/protocol/state-machine 改造并升级 format v8 | core 统一 typed IDs/canonical digest；CLI/Standalone/engine/fs 分层；Agent/中心提供无网络内存组合测试 |
| 2026-07-26 | 将 Managed 对象 durability authority 固定为中心 S3，NFS 仅放 Playground/journal/cache | Finalize 必须经过 missing upload、中心 durability、MetadataBatch 完整性和 IndexVersion CAS；避免把 NFS/cache/receipt 当成权威 |
| 2026-07-27 | 合并 R1.1/R1.2，完成 `AuthorityStore` 与默认 SQLite 中心权威后端 | 全部中心端口可跨重开恢复并运行同一后端契约；SQLite 限单进程且无 RLS/HA，PG/MySQL 后端保持独立 schema/migration |
