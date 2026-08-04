# NeoEngram Public OpenAPI

[`neoengram-api.yaml`](neoengram-api.yaml) 是面向用户、CLI 和 UI 的公开 API 设计契约。
`neoengramd` 保持 library-only，独立的 `neoengram-server` 使用 Fusen 0.9.0 提供可监听 HTTP server。
默认配置注册以下六个接口：

```text
POST /api/system/version/query
GET  /health/live
GET  /health/ready
POST /api/job/add/create
POST /api/job/query
POST /api/job/add/finalize
```

启用 Agent enrollment 时，同一 Fusen 用户 listener 还注册 token create、enrollment list/query、approve
和 reject 五个公开管理接口。独立 Hyper listener 由另一份 OpenAPI 3.1 契约
[`neoengram-agent-api.yaml`](neoengram-agent-api.yaml) 定义；它不是公开 Web API。Agent 契约同样采用
模块/子域/动作命名，全部使用 POST，所有资源 ID 均位于 JSON body：

```text
POST /agent/enrollment/bootstrap
POST /agent/enrollment/status/query
POST /agent/session/open
POST /agent/session/heartbeat/report
POST /agent/session/message/list/query
POST /agent/job/report/create
POST /agent/job/metadata/batch/stage
POST /agent/job/metadata/page/stage
POST /agent/job/index/page/query
POST /agent/job/object/missing/query
POST /agent/job/object/upload
POST /agent/session/close
```

公开契约中的其他 operation 仍可能是目标契约，不表示已经可调用。

业务接口使用外部 OIDC/JWKS Bearer JWT 和服务端 RBAC，无法确认身份或授权时默认拒绝。SQLite
运行模式只支持单副本；生产 TLS 由 Ingress/反向代理终止。

## 设计约定

- 业务路径采用“模块 / 子域 / 动作”，例如 `POST /api/job/add/create`。它借鉴支付宝开放平台
  的模块化方法命名，但不使用单一 gateway method，也不把版本放进 path。
- API 主版本通过必需的 `NeoEngram-API-Version` header 协商。版本查询与健康探针例外。
- 普通调用使用 JSON 请求/响应。成功直接返回业务 DTO；失败使用 RFC 9457
  `application/problem+json`，并附加稳定 `code`、`request_id` 和 `retryable`。
- 服务端使用认证后的 PrincipalRef 和完整 Add operation 计算 request digest；它与 Job ID 形成业务
  幂等边界，参考 Temporal 等开源工作流系统的稳定 execution identity，不叠加另一套通用
  idempotency key，也不要求浏览器复制 canonical digest 实现。
- `resource_version`、generation 和 CAS 语义借鉴 Kubernetes 的版本化并发控制，但公开路径不是
  Kubernetes 风格的资源 CRUD API。
- 方法式 HTTP 调用参考 Connect RPC 的明确 procedure 边界，但不使用 Protobuf、gRPC 或 Connect
  wire protocol。

## 安全边界

除版本查询和健康探针外，公开业务 API 使用 Bearer JWT。服务端必须从配置的 OIDC issuer 验证
token，并从验证后的 issuer/sub 导出 `PrincipalRef`；tenant scope 只能由启动时加载的服务端 RBAC
策略授予。JWT 中的 tenant、role、group 只作审计提示，客户端不能通过 token 或请求 body 覆盖授权。

Tenant、StorageVolume、Artifact、Commit、Playground、Snapshot 与 `JobView` 均为脱敏视图。
StorageVolume 的稳定逻辑 ID、region、EdgeCluster 和公开 PVC reference 可用于放置与运维识别；
不得包含 Assignment target、Agent/Mount identity、generation、fencing token、NFS export、凭据、
PublicationCandidate、Manifest、IndexDelta、物理路径或数据库信息。跨租户查询按
对应资源的 `*_NOT_FOUND` 返回 404，不能泄漏目标资源是否存在。Artifact 不携带放置字段；
Playground 和 Snapshot 的 Region 始终由所选 StorageVolume 派生。

只有 `state=ready` 的 StorageVolume 可以承接新的 Playground 或 Snapshot；`degraded` 和
`unavailable` 均拒绝新放置，但已有资源的公开元数据仍可查询。P0 Dashboard 只展示当前 Tenant、
系统健康和资源导航；资源数量、关注项、区域统计、最近版本与跨资源活动依赖 P1 聚合接口。

Storage Enrollment 使用三个权限：创建 bootstrap token 需要 `storage.enrollment.create`，列表和详情
查询需要 `storage.enrollment.read`，批准与拒绝统一需要 `storage.enrollment.review`。管理员创建 token
时提交并冻结完整的 PVC Volume descriptor，包括逻辑 Volume ID、display name、EdgeCluster、Region、
access mode 和 PVC reference；不要求提前登记 StorageVolume。token 有效期为 15 分钟，只能被内部
Agent bootstrap 成功消费一次。相同 `token_request_id` 和相同 payload 重放同一结果，改变 payload
返回 409。原始 `bootstrap_token` 只在创建成功响应中出现，不进入查询、审批、审计或日志。

Agent 消费 token 后通过内部 API 提交脱敏 enrollment，公开状态为 `pending_approval`，24 小时未审核
则进入 `expired`。审核使用稳定的 approval/rejection request ID 和 `expected_resource_version` CAS。
两种审核 ID 共享 Tenant 级 decision request identity 命名空间，不能跨批准/拒绝或跨 enrollment 复用。
批准 initial enrollment 时原子创建缺失的逻辑 StorageVolume，或精确绑定 descriptor 一致且
`unavailable`、无活动 Owner 的既有 PVC Volume；replacement 则接管既有 Owner，并要求
`confirm_replacement=true`。审批响应同时返回 Enrollment 与
`StorageVolumeView`，此时分别是 `approved` 和 `unavailable`。只有后续认证 Agent session 与健康
probe 才能推进到 `enrolled` 和 `ready`；拒绝进入终态 `rejected`。

资源 mutation 同样只接受公开 DTO：StorageVolume 登记已有 PVC/NFS，不负责创建底层存储资源。
Artifact 创建必须通过 discriminator 明确选择空初始化，或从同 Tenant 另一 Artifact 的明确 Commit
派生；派生来源显式携带来源 Project。Playground 和 Snapshot 创建各自选择一个同 Tenant Volume。

Playground 继续使用完整资源 identity 幂等创建。Snapshot create 使用稳定 request identity；同一
Commit/Volume 的重复创建返回已有未删除 Snapshot，同一 Commit 选择其他 Volume 时创建新的
`snapshot_id`。每个 Snapshot 始终是单 Region、单 Volume，不能静默迁移或返回 placements 数组。
Playground 的主状态仅为 Creating、Ready、Abnormal；扫描、哈希、上传和校验属于独立 Pre-commit。
Pre-commit start 创建新的 `precommit_id`，并在服务端内部冻结当前 Head；running/ready 的重新检测
使用 cancel 后 start，abnormal/cancelled 的失败重试才使用 restart，在同一 ID 上令 `attempt + 1`。
可提交结果固定为 `state=ready, phase=idle`；`Blocked` 是 `state=abnormal, phase=idle` 且 blockers
非空的产品标签，不是新的枚举值。

正式 Commit 以稳定 `commit_request_id` 消费 ready/idle Pre-commit 及其候选 IndexVersion，可附带
详细描述和最多 20 个 Tag。服务端从认证结果建立 actor，并对 Pre-commit attempt 内部冻结的 Head
执行 CAS；Head 改变返回 409，公开请求不增加 `source_head_commit_id`。公开契约只返回 Commit ID、
父 Commit 和 Tags，不要求调用方理解 Ref。Commit Diff 默认比较目标 Commit 与其单一 parent，根
Commit 与空基线比较；公开结果只包含 Commit 视图、逻辑路径、变更类型和大小统计。

Playground 文件、变更、文件元数据和 Dataset Profile 使用拆分分页方法；Snapshot 提供独立详情、
交付重试、Ready 文件清单、活动记录和 Dataset Profile。上述 DTO 仅包含逻辑路径、Schema、统计、
质量和 freshness，不公开 Manifest ID、对象位置、凭据或物理路径。

Dataset Profile 是 Playground/Snapshot 派生的只读元数据，不是 Snapshot 创建参数。用途、保留策略、
Lease/Mount、容量与底层诊断、Agent/assignment、fencing、Manifest/Chunk、文件内容 digest、对象分布
和物理路径均不属于 P0 普通用户契约；后续能力必须通过独立 P1 或 operator API 与相应 RBAC 暴露。

Agent API 不属于公开 OpenAPI。开发链路使用 Agent 主动发起的 HTTP/1 JSON action 请求：短轮询最多
返回 32 条消息，空结果返回 `retry_after_ms=1000`；Ed25519 request proof、session generation fencing、
MetadataBatch 分页和重放规则由以下契约定义：

- [`neoengram-agent-api.yaml`](neoengram-agent-api.yaml)
- [`../agent-central-control.md`](../agent-central-control.md)
- [`../../crates/neoengram-protocol/schemas/v1/control-envelope.schema.json`](../../crates/neoengram-protocol/schemas/v1/control-envelope.schema.json)
- [`../../crates/neoengram-protocol/schemas/v1/metadata-batch.schema.json`](../../crates/neoengram-protocol/schemas/v1/metadata-batch.schema.json)

`AssignJob`、`ExpireAddJob` 和 `ResumePublication` 是中心调度/恢复内部方法，不得加入公开 OpenAPI。
Storage Enrollment 公开 DTO 同样不得暴露 CSR、公私钥、证书、bootstrap/poll credential、PVC UID、
CSI handle、fsid/device、mount path/options/fingerprint、AgentId、AgentMountId、ComputeNodeId、session
或 credential generation、heartbeat/job/assignment，以及 tenant owner、lease 或 fencing 信息。

## 校验

该目录使用锁定版本的 Redocly CLI。安装和检查命令：

```bash
npm ci
npm run lint
npm run bundle
npm run test:contract
```

`bundle` 只在仓库 `target/openapi/` 下生成 JSON 检查产物，不提交生成文件；`test:contract`
基于该 bundle 校验公开路径、认证与版本头、状态映射、示例、u64 编码、ready-only 放置、独立
Snapshot 身份、Pre-commit 会话/attempt 语义、内部 Head CAS、Storage Enrollment token/审批边界、
Artifact 初始化模型和所有公开资源视图的脱敏边界。
CI 会按以上顺序运行相同命令。
