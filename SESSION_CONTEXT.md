# NeoEngram 会话上下文记录

更新时间：2026-09-04（Asia/Tokyo）

> 说明：本文件是当前会话中可用于继续开发的完整工作记录。它不包含系统隐藏提示、模型私有推理过程、访问凭据、私钥或一次性 token；这些内容不能作为可移植会话数据导出。

## 1. 会话目标

本会话围绕 NeoEngram 开发环境、P2P 对象副本复制、统一操作任务/审计模型、完整性检查与 Web 控制台改造展开。当前项目仍处于开发阶段，用户明确接受破坏性协议和数据库升级，不需要兼容旧协议或旧版本。

## 2. 已确认的产品与架构决策

### 2.1 P2P 对象副本复制 v2

- Central 是唯一元数据、调度、租约和签名权威。
- Agent 之间传输对象；Gateway 负责 relay，数据路径为“源 Agent -> 源 Gateway -> 目标 Gateway -> 目标 Agent”。
- Gateway 不挂载 Volume、不保存 Chunk、不登记副本；Central/Gateway 不持久化对象字节。
- Commit 是不可变逻辑 ObjectSet，不再绑定磁盘；正式副本单位是对象级 `Verified ObjectPlacement`。
- `ObjectNamespaceId` 是对象、Placement、Ticket、Lease 和数据库键的必填字段，初期等于 `ArtifactId`。
- 完整 Commit 覆盖是从对象 Placement 派生的 Coverage；目标完整覆盖、视图校验和 Agent/mount 可达后，Workspace、SnapshotDelivery、S3 才可 Ready。
- 物化使用多源 Planner、分页 BatchManifest、Central 签名 Ticket、稳定 staging key、checkpoint、receipt 和 durability barrier。
- 新传输协议使用 ALPN `neoengram-transfer-v2` 和 capability `commit_materialization_v2`。
- 首期不做 Agent 直连、跨 Volume federated read、Gateway 缓存、纠删码和自动跨区重平衡。

### 2.2 快照交付

- 一个快照只对应一个 Delivery。
- 创建快照时必须同时选择交付集群和目标磁盘；未物化前不能让 S3 Access Point 伪造为可用。
- Snapshot、Delivery 和 S3 只消费目标完整 Coverage。

### 2.3 统一 OperationTask 模型

- 所有非只读操作都创建任务，包括即时完成的创建、删除和 Commit。
- 一个外部写请求对应一个根任务；Agent 执行、物化、校验是子任务。
- 重试复用原 `task_id`，增加 `attempt` 和事件；取消是原任务的终态控制。
- `operation_tasks` 是统一生命周期、运维查询和审计入口；领域表继续保存领域事实和执行明细。
- 任务状态主线：`queued -> running -> waiting/verifying -> succeeded`，异常为 `stalled`/`failed`，可重试失败回到 `queued`，`cancelled` 为终态。
- 任务类型覆盖 `workspace.create`、`workspace.materialize`、`precommit.check`、`add.scan`、`commit.create`、`snapshot.create`、`snapshot.delivery.materialize`、`commit.materialize`、`integrity.scan`、`resource.repair`、`storage.lifecycle` 和 `gateway.lifecycle`。
- 副本创建和副本修复统一复用 `commit.materialize`，通过 intent 和因果关系区分。
- 旧 `/api/job/*` 和旧 Materialization 专用公开接口删除，不做兼容读取；旧历史记录只能显式 inventory rebuild 后以 legacy、不可执行任务导入。
- Authority schema 目标为 v20；新数据库只允许统一任务入口。

## 3. 代码改造记录

最近一次大提交 `3803443 feat: unify operation tasks and materialization controls` 包含：

- Domain：TaskId、TaskAttemptId、TaskKind、TaskState、TaskScope、OperationTask、Attempt、Event、ResourceLink、Relation、Materialization v2、Coverage、Placement、Schema 和严格校验。
- Central：Task Repository/Coordinator、SQLite 任务表和索引、状态 CAS、Attempt、事件、父子关系、资源关联、查询/summary/retry/cancel API，以及领域流程接入。
- Agent：volume integrity、对象库存/物化和相关 v2 transfer/session 字段。
- Gateway：v2 transfer relay 边界和传输相关状态校验。
- Web：移除旧 Job/Materialization 专用页面，增加 Commit 详情、Task 列表、Task 详情和事件时间线；同步 API 类型、OpenAPI、action registry、MSW handlers 和测试。
- 文档：同步 `docs/architecture/`、`docs/current-state.md`、`docs/product.md`、`docs/roadmap.md`、`docs/openapi/` 和 CLI 参考。

随后提交 `b252c23 docs: add development continuation handoff` 增加了简短的 `CONTINUATION.md`。本文件比它更完整，作为跨设备继续工作的主记录。

## 4. 验证状态

- `git diff --check`：通过。
- `cargo test -p neoengram-central --tests --locked --offline`：通过，203 项。
- 未完成或未运行：完整 workspace 测试、完整 Clippy/doc、Gateway 真实网络 E2E、三 Agent 多源失败恢复 E2E、Web 全量质量检查、OpenAPI 全量契约检查。
- 不应把当前代码描述为已经完成生产级 HA、跨节点 E2E 或外部凭据签发验证。

## 5. 本地运行环境

根目录：`/Users/kwsc98/Desktop/synapse`

运行目录：`/Users/kwsc98/Library/Application Support/NeoEngram/dev/`

- Central：`http://127.0.0.1:8080`
  - authority：`dev/authority`
  - 日志：`dev/logs/central-v20.log`
- Web：`http://127.0.0.1:4173`
  - 日志：`dev/logs/web-v20.log`
- Agent 配置：`dev/agents/mount/agent.yaml`、`mount2/agent.yaml`、`mount3/agent.yaml`
  - 对应桌面目录：`/Users/kwsc98/Desktop/mount`、`mount2`、`mount3`
  - 三个 Agent 当前曾以 `target/debug/neoengram-agent run` 运行。
- 当前 Gateway v20 使用：
  - Agent：`127.0.0.1:8281`
  - Control：`127.0.0.1:8497`
  - Peer：`127.0.0.1:8498`
  - Transfer：`127.0.0.1:8284`
  - Public/S3：`127.0.0.1:8084`
  - 日志：`dev/logs/gateway-v20.log`

## 6. Gateway v21 未完成切换

为解决旧 Gateway workload credential 被 revoke/过期的问题，已经向 Central 创建了新 Replica：

- Replica：`replica-local-v21`
- Pool：`pool-local-v20`
- Control：`http://127.0.0.1:8597`
- Peer：`http://127.0.0.1:8598`
- Bootstrap：`http://localhost:8281`
- 文件目录：`dev/gateway-v21/`
- 已生成：`bootstrap-key.pem`、`activation-token`、`replica-create.json`
- 日志：`dev/logs/gateway-v21-bootstrap.log`

上次启动失败的直接原因是错误使用了参数 `--bootstrap-private-key-file`。Gateway CLI 的正确参数是：

```text
--private-key-file
--activation-token-file
--certificate-chain-file
```

继续步骤：

1. 停止占用 `8281`、`8084`、`8284` 的 Gateway v20，保留 Central、Web 和三个 Agent。
2. 用正确 bootstrap 参数启动 Gateway v21，监听 v21 的 Control/Peer，Bootstrap 仍指向 `8281`。
3. 调用 Central `POST /api/gateway/replica/activate`，请求字段为 `gateway_replica_id`、`expected_resource_version` 和 `activation_token`。
4. 等待 Replica 变为 active、Gateway Pool 恢复 ready。
5. 安装/配置 v21 workload 与 transfer TLS，启动 transfer listener。
6. 检查三个 Agent 的 route/session、Volume 可达性和 materialization 任务进度。

## 7. 会话中遇到过的问题与结论

- Agent 断线后曾出现 Gateway/Agent 状态显示 ready 但实际 route 被 fence；根因是状态汇报和可用 route/preflight 不是同一层事实，后续通过 route/session/generation 校验和 Gateway reconnect 逻辑收紧。
- Commit 复制曾报 `REPLICATION_PREREQUISITES_UNMET`；不能仅根据 Agent control ready 判定复制可用，还必须通过 Commit replication QUIC preflight。
- S3 曾报 `unknown_public_host` 或 SDK 403；浏览器下载使用 Gateway 暴露的原生 S3 HTTP 入口，但 host、签名、Access Point/Delivery 绑定必须匹配 Gateway public listener 和 Central 授权。
- 手动删除 mount/mount2 数据块不会自动修改 Central Placement；必须触发 integrity scan，由 Agent 重算 inventory/摘要并将异常作为任务事件上报，再从其他 Verified Placement 创建 repair materialization。
- “传输任务没有进度”需要从统一 Task 列表、Task detail、Materialization batch/object、Agent checkpoint 和 Gateway route 日志同时判断，不能只看旧 Job 页面。
- 两个 partial Volume 的对象并集可以补齐目标；完整副本覆盖是派生结果，不再要求单个磁盘拥有完整 Commit。

## 8. 回家后建议的第一轮命令

```bash
cd /Users/kwsc98/Desktop/synapse
git pull --ff-only
git log -3 --oneline --decorate
git status --short
sed -n '1,260p' SESSION_CONTEXT.md
sed -n '1,220p' CONTINUATION.md
```

继续实现前先读取根目录 `AGENTS.md`，然后按任务范围读取 `README.md`、`docs/README.md`、`docs/current-state.md`、`docs/roadmap.md` 及对应架构文档。

建议优先做 Gateway v21 切换和实际跨 Gateway/Agent 验证，再处理剩余测试缺口。不要使用 `git reset --hard`，不要提交本地 token、私钥、SQLite 运行库、`target/`、`node_modules/` 或 Web 构建产物。

## 9. 当前 Git 状态

- 分支：`main`
- 主要实现提交：`3803443`
- 简短交接提交：`b252c23`
- 本文件新增后应再产生一个文档提交。
- 已忽略的本机文件：`apps/neoengram-web/docs/.DS_Store`
