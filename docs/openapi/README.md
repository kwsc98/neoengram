# NeoEngram Public OpenAPI

[`neoengram-api.yaml`](neoengram-api.yaml) 是面向用户、CLI 和 UI 的公开 API 设计契约。
当前 `neoengramd` 仍是 library-only；本文档不表示仓库已经提供可监听的 HTTP server。

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

除版本查询和健康探针外，公开业务 API 使用 Bearer JWT。服务端必须从配置的 OIDC issuer 验证 token，并从认证结果导出
`PrincipalRef` 和 tenant scope；客户端不能通过请求 body 覆盖身份。

Tenant、StorageVolume、Artifact、Commit、Playground、Snapshot 与 `JobView` 均为脱敏视图。
StorageVolume 的稳定逻辑 ID、region、EdgeCluster 和公开 PVC reference 可用于放置与运维识别；
不得包含 Assignment target、Agent/Mount identity、generation、fencing token、NFS export、凭据、
PublicationCandidate、Manifest、IndexDelta、物理路径或数据库信息。跨租户查询按
对应资源的 `*_NOT_FOUND` 返回 404，不能泄漏目标资源是否存在。Snapshot 只使用
`tenant_id + project_id + artifact_id + commit_id` 复合身份，不引入独立 snapshot ID。

资源 mutation 同样只接受公开 DTO：StorageVolume 登记已有 PVC/NFS，不负责创建底层存储资源。
当前已提交契约仍要求 Artifact、Playground 和 Snapshot 创建选择同租户 StorageVolume，region 由
服务端派生；这与 Web 原型冻结的目标产品口径并不完全一致。下一版契约必须按
[`../centralized-agent-product.md`](../centralized-agent-product.md) 将 Artifact 改为无固定放置，只让
Playground 和 Snapshot 各选择一个 Volume。迁移完成前，当前 Artifact placement 字段只能视为待
移除的契约债务，不能作为新后端实现依据。

Playground 和 Snapshot 继续使用完整资源 identity 幂等创建；同一身份改选其他 Volume 必须返回
placement conflict，不能静默迁移。Snapshot 始终是单 Region、单 Volume。
Playground Commit 以稳定 `commit_request_id` 绑定完整 scope、expected IndexVersion 和 message，
可附带详细描述和最多 20 个新 Tag。当前契约仍把 Tag 编码成 `refs/tags/*`；下一版公开响应应直接
提供 Tags，不能要求前端理解 Ref 前缀。服务端从认证结果建立 actor，并在内部版本指针上执行
CAS；公开产品不允许用户选择目标 Ref。Commit Diff 默认比较目标 Commit 与其单一
parent，根 Commit 与空基线比较；公开结果只包含 Commit 视图、逻辑路径、变更类型和大小统计。

Agent API 不属于本 OpenAPI。Agent 的 H2/H3 JSON Text Sequence 双向 session、MetadataBatch 和
重放规则继续由以下契约定义：

- [`../agent-central-control.md`](../agent-central-control.md)
- [`../../crates/neoengram-protocol/schemas/v1/control-envelope.schema.json`](../../crates/neoengram-protocol/schemas/v1/control-envelope.schema.json)
- [`../../crates/neoengram-protocol/schemas/v1/metadata-batch.schema.json`](../../crates/neoengram-protocol/schemas/v1/metadata-batch.schema.json)

`AssignJob`、`ExpireAddJob` 和 `ResumePublication` 是中心调度/恢复内部方法，不得加入公开 OpenAPI。

## 校验

该目录使用锁定版本的 Redocly CLI。安装和检查命令：

```bash
npm ci
npm run lint
npm run bundle
npm run test:contract
```

`bundle` 只在仓库 `target/openapi/` 下生成 JSON 检查产物，不提交生成文件；`test:contract`
基于该 bundle 校验公开路径、认证与版本头、状态映射、示例、u64 编码、Snapshot 复合身份和
所有公开资源视图的脱敏边界。
CI 会按以上顺序运行相同命令。
