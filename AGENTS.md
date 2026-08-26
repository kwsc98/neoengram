# AI 迭代规则

本文件是仓库根目录的 AI/自动化协作入口。它描述的是当前代码边界和验证方法，不能替代
`docs/roadmap.md` 中的能力状态，也不能把路线目标当成已实现行为。子目录目前没有更具体的
`AGENTS.md`；如果以后增加，离目标文件最近的规则优先。

## 开始任务

先按任务范围读取最小但完整的上下文：

1. [`README.md`](README.md)：定位、当前稳定链路、明确限制和 workspace 布局。
2. [`docs/README.md`](docs/README.md)：文档入口、每类事实的唯一来源和同步规则。
3. [`docs/current-state.md`](docs/current-state.md)：代码、契约、测试反推出的当前产品对象、能力证据和已知契约缺口。
4. [`docs/roadmap.md`](docs/roadmap.md)：当前/目标能力边界。看到“目标”“下一步”“研究”时，先按未实现处理。
5. 相关专题：本地代码和存储读 `docs/architecture/code.md`、`storage.md`；控制面读
  `docs/architecture/control-plane.md`；Gateway 读 `docs/architecture/gateway.md`；用户资源和 Web 语义读
  `docs/product.md`；CLI 行为读 `docs/reference/cli.md`。
6. 再读目标模块的入口、调用链和测试。机器契约必须直接读取
   `crates/neoengram-domain/schemas/current/`、`docs/openapi/` 或
   `crates/neoengram-domain/src/protocol/`，不能只根据叙述文档猜测。

涉及 Web 的任务还要读 [`apps/neoengram-web/README.md`](apps/neoengram-web/README.md)；涉及项目级
流程的任务还要读 [`tests/project/README.md`](tests/project/README.md)。先用 `rg` 搜索入口、action、错误码、
配置项和测试名，再扩大阅读范围。

## 如何判断能力

代码中出现类型、controller、页面、配置项或单元测试，只能证明存在一部分实现。把一个能力写成
“已实现”至少要有以下证据：

- 存在可调用的代码路径，而不是仅有 DTO、占位函数、注释或配置字段；
- 相关不变量由测试、契约检查或可重复的项目流程验证；
- 若是 HTTP/Web 行为，action registry、OpenAPI、实现和前端生成类型一致；
- 若是持久化、跨进程或安全行为，有故障/权限/重启边界测试，或明确记录尚缺的 E2E；
- `docs/roadmap.md` 的状态与证据一致，并没有把“代码骨架”写成生产能力。

在任务记录和文档中标记证据类型：`已观察`（代码/测试直接证明）、`推断`（由多个实现行为推导）、
`目标`（设计或路线要求，尚未证明）。不确定时使用较保守的状态，并列出缺失的验证。

## 源码、测试和验证映射

| 能力 | 主要源码 | 主要测试/契约 | 最小验证 |
| --- | --- | --- | --- |
| 领域模型、规范 digest、wire protocol、Schema | `crates/neoengram-domain/src/core/`、`src/protocol/` | `crates/neoengram-domain/tests/`、`schemas/current/` | `cargo test -p neoengram-domain --all-targets --locked`；需要时运行 `cargo run --locked --offline -p neoengram-domain --example generate_schemas` |
| 本地仓库、工作区、对象、事务和 FUSE | `crates/neoengram-runtime/src/engine/`、`src/local/`、`src/app/` | `crates/neoengram-runtime/tests/`、`apps/neoengram-cli/tests/` | `cargo test -p neoengram-runtime --all-targets --locked`；CLI 工作流见下一行 |
| CLI 用户行为 | `apps/neoengram-cli/src/` | `apps/neoengram-cli/tests/`（workflow、recovery、integrity、workspaces 等） | `cargo test -p neoengram --all-targets --locked` |
| Central/API/authority | `services/neoengram-central/src/controller/` -> `service/` -> `ports`/`mapper/` -> `datasource/` | `services/neoengram-central/tests/`；action registry/OpenAPI | `cargo test -p neoengram-central --tests --locked` |
| Agent 状态、Ledger、Volume 和会话 | `services/neoengram-agent/src/agent_core/`、`src/session_*`、`src/execution.rs` | `services/neoengram-agent/tests/` | `cargo test -p neoengram-agent --all-targets --locked` |
| Gateway、mTLS、隧道和 forwarding | `services/neoengram-gateway/src/` | `services/neoengram-gateway/tests/network_e2e.rs` | `cargo test -p neoengram-gateway --test network_e2e --locked` |
| Web 控制台 | `apps/neoengram-web/src/` | `apps/neoengram-web/tests/` | 在 Web 目录运行 `npm run format:check && npm run lint && npm run typecheck && npm run api:check && npm test && npm run build` |
| 公开/Agent HTTP 契约 | `docs/openapi/*.yaml`、`docs/openapi/scripts/` | OpenAPI lint/bundle/contract；`action_registry` | 在 `docs/openapi` 运行 `npm run lint && npm run bundle && npm run test:contract` |
| Kubernetes 部署边界 | `deploy/kubernetes/{agent,gateway}/` | 对应 `check-manifests.sh`、项目 manifests 模块 | `bash deploy/kubernetes/agent/check-manifests.sh` 和 Gateway 对应脚本 |

新增能力或重命名目录后，必须在本表、对应专题文档和 `tests/project/modules/` 的入口中保持映射可查；
不要只写一个模糊的“相关测试”。测试重命名时同步更新表和任务文档。

## 依赖和不变量

保持以下边界，除非任务明确要求重新设计并完成对应架构评审：

- `neoengram-domain` 是依赖叶，不能依赖 runtime、CLI、数据库、文件系统或 HTTP；
  `neoengram-runtime` 只依赖 domain；Agent 依赖 domain/runtime；Gateway 只依赖 domain。
- Central 采用 `controller -> service -> ControlPlane/ports -> mapper -> datasource`；controller 不直接写 SQL。
- Gateway 是 Agent 的网络入口，不挂载 StorageVolume、不保存 metadata/object，也不能依赖 Central datasource/mapper。
- Web 只消费 `docs/openapi/neoengram-api.yaml`，不能导入 Rust crate、Agent schema、数据库结构或内部 `/agent` 路由。
- Server 不接收 Chunk payload；Managed 对象字节由获批 Volume 上的 Agent 处理。
- SQLite authority 当前是单进程/单副本；不能把它描述为 PostgreSQL HA、RLS 或生产故障切换。
- 本地仓库格式、authority schema 和 wire contract 的未知版本/字段默认拒绝；不要默默添加迁移、回退或兼容猜测。
- 目标架构、设计草图和通过单元测试的纵切，不等于生产 E2E、外部凭据签发、HA 或跨 Volume 传输。

## 变更与文档同步

先写清楚任务的目标、非目标、边界和验收，再编辑代码。按变更类型同步：

- CLI/本地用户行为：更新实现测试和 [`docs/reference/cli.md`](docs/reference/cli.md)，必要时更新根 README。
- 公共 HTTP action/DTO：以 `crates/neoengram-domain/src/protocol/action_registry.rs` 为清单，
  同步 `docs/openapi/*.yaml`、契约测试、Web 生成类型（运行 `npm run api:generate`），再更新控制面/产品摘要。
- wire protocol 或 Schema：同步 protocol 实现、`schemas/current/`、golden/contract 测试和控制面文档；不手工编辑生成的 Web 类型。
- 存储格式、事务、锁、恢复或安全边界：先更新对应 `docs/architecture/` 专题和测试，再更新 `docs/roadmap.md`。
- Web 页面和交互：同步 `apps/neoengram-web/tests/`、产品语义；禁止把 Mock mode 当成真实中心能力。
- Kubernetes/运行配置：同步部署 README、manifest 检查和安全边界。

不要新建平行 Roadmap、重复的架构说明或历史 HTML 原型。发现文档与代码冲突时，先修正事实来源，
再在其他文档中保留短链接；不要用复制的状态表掩盖冲突。

## 必跑检查

根据范围选择最小检查，并在提交说明中记录命令和结果：

```bash
# Rust/架构基础门槛
cargo fmt --all -- --check
bash .github/check-architecture.sh
cargo test --workspace --all-targets --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --offline
```

契约或 Web 改动还要运行：

```bash
(cd docs/openapi && npm ci --ignore-scripts && npm run lint && npm run bundle && npm run test:contract)
(cd apps/neoengram-web && npm ci && npm run format:check && npm run lint && npm run typecheck && npm run api:check && npm test && npm run build)
```

跨服务、部署或全链路改动使用 `scripts/project-test.sh`；它是项目测试的统一编排入口：
`scripts/project-test.sh module <domain|runtime|agent|central|gateway|cli|openapi|web|web-e2e|manifests|quality>`、
`scripts/project-test.sh modules` 或 `scripts/project-test.sh full-flow`。真实 mount probe 只在 Linux、准备好
`NEOENGRAM_REAL_MOUNT_PROBE_ROOT` 后单独运行，不能把普通测试结果当作该验证。

CI 使用 Rust 1.97.1、Node 22.12.0，并额外运行 `--all-features --locked`、跨平台默认 feature、Web E2E、
MSRV、打包和项目模块。若本地因平台/依赖无法运行某项，必须在结果中说明“未运行及原因”，不能报告为通过。

## 禁止事项

- 不把路线、产品设计、注释、Mock、DTO 或未连接的页面写成当前能力。
- 不删除失败测试、放宽默认拒绝校验、吞掉恢复错误或为了通过 CI 改变安全/持久化不变量。
- 不绕过 action registry、OpenAPI、Schema golden 或架构脚本直接添加接口。
- 不让 CLI/runtime/domain 写入不属于其边界的 stdout/stderr、cwd、环境变量或数据库连接。
- 不提交 `target/`、`node_modules/`、Web build/report、临时凭据、私钥、token、SQLite 运行库或生成的 OpenAPI bundle。
- 不使用破坏性 Git 命令覆盖用户现有改动；不顺手重排无关文档或依赖。
