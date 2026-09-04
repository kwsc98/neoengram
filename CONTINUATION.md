# NeoEngram 开发交接记录

更新时间：2026-09-04（Asia/Tokyo）

## 当前版本

- 仓库：`/Users/kwsc98/Desktop/synapse`
- 分支：`main`
- 最近提交：`3803443 feat: unify operation tasks and materialization controls`
- 工作区在保存本文件前是干净的；`.DS_Store` 未纳入版本控制。
- 当前仍处于开发阶段，接受 v20/v2 的破坏性协议和数据库升级，不保留旧协议兼容层。

## 已完成的代码改造

- 引入统一 `OperationTask` 领域模型、Attempt、事件、资源关联和父子任务关系。
- Central 增加任务 Repository/Coordinator、SQLite 表、HTTP API、查询过滤、状态聚合、重试和取消。
- Playground、Snapshot/Delivery、Pre-commit/Add/Commit、Materialization、完整性扫描和资源生命周期接入任务模型。
- 完成对象级 Materialization v2、Coverage、Placement、完整性检查和修复相关协议/Schema 改造。
- Agent 增加 volume integrity 处理；Gateway 保留 v2 transfer relay 边界。
- Web 移除旧 Job/Materialization 专用页面，增加 Commit、Task 列表/详情和事件时间线页面。
- OpenAPI、action registry、生成类型、MSW handlers、文档和测试已同步。

## 验证记录

- `git diff --check`：通过。
- `cargo test -p neoengram-central --tests --locked --offline`：通过，203 项。
- 尚未执行完整 workspace、Gateway 网络 E2E、Web 全量检查；不要把本提交描述为完整 E2E 已验证。

## 本机运行状态

当前开发运行目录：`/Users/kwsc98/Library/Application Support/NeoEngram/dev/`

- Central：`http://127.0.0.1:8080`，authority 目录为 `dev/authority`。
- Web：`http://127.0.0.1:4173`。
- 三个 Agent 配置：`dev/agents/{mount,mount2,mount3}/agent.yaml`，分别挂载桌面的 `mount`、`mount2`、`mount3`。
- 现有 Gateway v20 进程使用：Agent `8281`、Control `8497`、Peer `8498`、Transfer `8284`、Public/S3 `8084`。

## Gateway v21 待处理事项

已向 Central 创建新的 Replica：

- Replica：`replica-local-v21`
- Pool：`pool-local-v20`
- Control：`http://127.0.0.1:8597`
- Peer：`http://127.0.0.1:8598`
- Bootstrap：`http://localhost:8281`
- 文件目录：`dev/gateway-v21/`

其中已有 `bootstrap-key.pem`、`activation-token`、`replica-create.json`。

上次启动失败是 CLI 使用了错误参数 `--bootstrap-private-key-file`。正确参数来自 Gateway bootstrap 配置：

```text
--private-key-file
--activation-token-file
--certificate-chain-file
```

启动 v21 前需要先停止占用 `8281/8084/8284` 的 Gateway v20，再用正确参数启动 bootstrap 模式。随后调用 `POST /api/gateway/replica/activate`，请求字段为 `gateway_replica_id`、`expected_resource_version` 和 `activation_token`。激活后再安装/配置 v21 的 workload/transfer TLS，并检查三个 Agent 的 route/session、Volume 可达性和 materialization 任务进度。

Gateway 日志：`dev/logs/gateway-v21-bootstrap.log`；Central 日志：`dev/logs/central-v20.log`。

## 继续工作建议

1. `git pull --ff-only`，确认 `3803443` 和本交接提交都在本地。
2. 阅读本文件后查看 `docs/roadmap.md`、`docs/current-state.md` 和相关架构文档。
3. 先处理 Gateway v21 激活/证书/端口切换，再做真实跨 Gateway/Agent 的 materialization 验证。
4. 完整验证命令按仓库根目录 `AGENTS.md` 执行；至少运行 Central、Agent、Gateway、Web 和 OpenAPI 对应模块检查。

## 注意事项

- 不要执行 `git reset --hard` 或覆盖其他未提交用户改动。
- 不要把 development token、TLS 私钥或 activation token 写入提交或日志。
- Gateway/Central 不应持久化 Chunk 或对象字节；对象字节只应由获批 Volume 上的 Agent 处理。
