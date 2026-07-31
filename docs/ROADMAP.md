# NeoEngram Roadmap

> 最后更新：2026-07-31
>
> 本文是迭代执行视图，不重新定义产品能力或架构。
> 能力状态、架构不变量和研究结论以 [`implementation-plan.md`](implementation-plan.md) 为准。
> 面向用户的产品语义和验收主链路见
> [`centralized-agent-product.md`](centralized-agent-product.md)。

## 当前基线

- `0.2.0`、format v8 和 P0 crate/protocol/state-machine 改造已完成。
- Standalone 本地工作流可运行；FUSE 实挂矩阵和大规模基准尚未完成。
- Agent 与 `neoengramd` 仍是无网络 library；中心已具备后端无关 `AuthorityStore`、测试用 InMemory
  后端和默认 SQLite 权威后端。
- SQLite 支持单个 `neoengramd` 进程的中心权威持久化；PostgreSQL/MySQL、HTTP/mTLS、OIDC/RBAC、
  真实 S3、daemon 和分布式 fencing 尚不存在。
- Managed 模式以中心 S3 为对象耐久权威；NFS 仅保存 Playground、journal 和可重建缓存。
- 0.0.1 Kubernetes 部署剖面已冻结为一个业务 PVC/StorageVolume 对应一个常驻 AgentInstance；Agent
  SQLite 身份/Ledger adapter、mount probe 与中心 enrollment/registry 领域纵切已实现，实际 daemon、
  主动注册/审批 transport、证书签发和真实集群闭环仍待实现。

## 迭代规则

- 同一时间只推进一个主要迭代；每轮交付可演示、可恢复、可验收的纵切。
- 每轮开始前写清入口条件、非目标和失败语义，结束时提供测试与容量证据。
- 安全、租户隔离、幂等、重启恢复、审计和可观测性属于完成条件，不留到最后补齐。
- 研究项必须记录问题、实验、结论和后续动作；未满足验收条件不得标记完成。
- Gateway、Pack、跨租户复制和完整文件系统语义不得阻塞首个 Managed 闭环。

## 执行路线

| 迭代 | 状态 | 可交付结果 | 对应战略阶段 |
| --- | --- | --- | --- |
| R0 | 已完成 | format v8、协议 v1、Engine/Agent/中心内存状态机 | P0 / A0 |
| R1 | 已完成 | AuthorityStore + SQLite 默认后端，覆盖全部中心权威状态 | P1 / A2 |
| R2 | 进行中 | 一 PVC 一常驻 Agent：enrollment/审批契约与 UI、持久身份/Ledger adapter、mount probe、Storage Registry 领域状态机、部署模板和人工 cooperative takeover；daemon/transport 与集群验收待完成 | A1 / A2 |
| R3 | 后续 | 把 OIDC/JWKS、RBAC/RLS 扩展到其余只读 Artifact/Commit/Tags/Snapshot API | P1 |
| R4 | 后续 | 完整 mTLS Agent session transport、只读 Job delivery、背压和重连 | A1 / A2 |
| R5 | 后续 | 中心 S3、短期票据和端到端 Managed Add | P2 / A4 |
| R6 | 后续 | 中心 Commit/Ref CAS、固定 Snapshot 和 DatasetProfile | P1 / P2 |
| R7 | 后续 | 客户端 push、fetch、clone 与授权训练读取 | P2 / P3 |
| R8 | 后续 | Playground mutation、Volume Owner、lease，并从人工 cooperative takeover 演进到强 fencing | A3 |
| R9 | 后续 | 生命周期、GC、HA、灾备、配额和规模优化 | P4 / P5 / A5 |
| R10 | 研究 | mode、symlink、ACL/xattr、sparse 等文件语义 | P6 |

## 已完成迭代：R1

| 项目 | 内容 |
| --- | --- |
| 目标 | 把中心逻辑权威从具体数据库解耦，并以 SQLite 提供可重启恢复的单节点生产后端 |
| 范围 | 异步中心 ports、`AuthorityStore`、Job/outbox/MetadataBatch/ObjectCatalog/IndexPublisher/Audit 全量 SQLite 持久化、独占进程锁和契约测试 |
| 验收 | create/assign/finalize 与 publication 重放稳定；全部端口跨重开恢复；同 ID 跨租户隔离；expected-version 只有一个 CAS 成功；错误 schema、损坏记录和第二实例硬失败 |
| 存储边界 | SQLite 固定单连接、WAL、foreign keys、`synchronous=FULL`；应用层租户隔离，不声称数据库级 RLS、HA 或多进程能力 |
| 非目标 | daemon、HTTP、OIDC、真实 S3、PG/MySQL adapter、跨后端迁移、SQLite 复制和用户界面 |

SQLite authority 使用独立 `authority.sqlite3`/`authority.lock`，不复用 Standalone format v8。
当前格式使用 `application_id = 0x4e454f41`、`user_version = 2`；启动时只支持从 v1 原子迁移到
v2 以增加独立 Storage Enrollment 审计表，其他版本、未知表或变更 schema 均失败关闭。
R2 Agent Registry 另外使用 `agent-registry.sqlite3`/`agent-registry.lock`，由单个中心进程组合进
`AuthorityStore`。审批决策和其规范审计事件在同一 Registry CAS aggregate 中原子持久化；
`authority.sqlite3` 中的 enrollment audit 表只是兼容投影，不声称两个 SQLite 文件具有跨库事务。
PostgreSQL/MySQL 后续实现同一行为契约，但各自拥有独立 SQL、migration、物理 schema、锁和 CAS 设计。

## 决策门

| 进入迭代前 | 必须取得的证据 |
| --- | --- |
| PostgreSQL authority | 多实例拓扑、HA、RLS 角色模型、独立 migration 与最小 schema 评审 |
| R2 | 一次性 bootstrap、首次审批和身份重放模型；一个 PVC/Agent 的 Recreate 部署、独立状态盘恢复和错误 mount 演练 |
| R4 | H2 JSON sequence 经目标 Ingress 的双向流、背压和重连原型 |
| R5 | 目标 S3 的 checksum、multipart、Signed URL、KMS 和失败清理原型 |
| R8 | 首批 NFS 产品矩阵、强 fencing 方案、旧写者阻断证据及 RW Playground Pod 策略 |
| R9 | 千万路径 Delta、上亿对象 catalog 和恢复时间的可重复基准 |

## 全局完成条件

- 标准 fmt、架构检查、测试、Clippy、rustdoc、schema golden 和 migration dry-run 全部通过。
- 新增行为覆盖租户隔离、幂等重放、进程退出、并发 CAS、损坏输入和资源上限。
- 指标、trace、审计字段不记录 JWT、Signed URL、凭证、物理路径或数据内容。
- 同步更新 `implementation-plan.md` 的能力状态、研究结论和路线变更记录。
- HTML 原型仅表达交互需求；与中心 S3 权威等最新不变量冲突时不得作为实现依据。
- Kubernetes Agent 发布必须验证 `replicas=1`、Recreate、固定 `/volume`、独立状态 PVC、无
  ServiceAccount token/Kubernetes API，以及人工 takeover 失败关闭；这些约束不等价于存储侧强 fencing。
- R2 的真实审批入口必须先具备可验证 TenantAdmin 身份和 `storage.enrollment.create/read/review` 的
  默认拒绝授权；R3 是把 OIDC/JWKS、RBAC/RLS 扩展到其余公开资源，不允许 R2 用 MSW 身份替代验收。
