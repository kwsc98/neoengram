# AI 迭代指南

这份指南把一次“读代码、反推产品行为、实现变更、留下证据”的迭代固定成可复用流程。它和
[`../AGENTS.md`](../AGENTS.md) 配合使用：`AGENTS.md` 规定仓库不变量，这里规定任务记录和产品反推方法。

## 0. 默认低 Token 模式

日常改造默认只读取和修改任务直接涉及的模块，不做无关重构、全仓库重复审计或重复执行同一批检查。
每次改造完成后只自动运行一次统一全量测试脚本：

```bash
bash scripts/project-test.sh --no-install all
```

首次运行、依赖缺失或依赖锁文件变化时，使用 `bash scripts/project-test.sh all` 允许脚本安装锁定依赖。
脚本非零退出或因平台/依赖不可用时，必须报告失败或“未运行及原因”，不能报告为通过。用户可以自行进行
手工测试；除非用户明确要求或需要定位失败，不重复运行同一批测试。真实服务、凭据和跨节点 E2E 不由默认
迭代自动创建或启动。

## 1. 先写任务卡

开始编码前，在 issue、分支说明或 PR 描述中填完以下字段。没有证据的字段写“未知”，不要用猜测填满。

```text
目标：
当前行为（代码/测试证据）：
用户或调用方：
非目标：
涉及模块和文件：
不变量/兼容性：
产品契约（输入、状态、输出、错误）：
实现步骤：
测试与验收命令：
需要同步的文档/契约：
未验证项和风险：
```

任务必须有一个可观察的结果，例如“某 action 在某状态下返回某错误并且不写入 authority”，而不是
“完善 Central”或“优化架构”。把大任务拆成能独立测试的纵切；每个纵切说明是否只做本地/Mock，还是声称
覆盖真实进程、跨节点或生产安全。

## 2. 从代码反推产品设计

产品文档描述意图，代码和测试描述实际行为。按下面顺序追踪，得到一份“当前契约”再修改设计：

1. **入口和命名**：CLI 从 `apps/neoengram-cli/src/` 的 Clap command 开始；HTTP 从
   `crates/neoengram-domain/src/protocol/action_registry.rs`、`docs/openapi/*.yaml` 和
   `services/neoengram-central/src/controller/` 开始；Web 从 `router/`、`pages/`、`api/operations.ts` 开始。
2. **调用链**：沿真实调用方向阅读。Central 通常是 `controller -> service -> ports/ControlPlane -> mapper -> datasource`；
   本地命令通常是 CLI -> runtime facade -> engine -> local adapters；Agent/Gateway 要同时看 session/transport 和状态持久化。
3. **状态和持久化**：列出输入状态、状态转换、事务边界、CAS/版本条件、重启恢复、幂等键、租户 scope 和失败后的残留物。
   只要状态会写 SQLite、对象 CAS、Ledger、authority 或浏览器缓存，就必须追踪写入和读取双方。
4. **契约与错误**：记录方法/path、身份和权限前置条件、分页/游标、响应字段、稳定错误码、未知字段策略、超时和重试语义。
   错误路径同样是产品设计，不要只记录成功响应。
5. **测试作为示例**：读最接近的 integration/golden/E2E 测试和 fixture；测试名称、断言和构造数据比页面文案更能说明当前功能范围。
6. **对照文档**：最后回到 `docs/product.md`、对应 architecture 专题和 `docs/roadmap.md`，把每条描述标成“已观察、推断或目标”，
   处理冲突后再写新代码。文档与实现冲突时，不要选择看起来更完整的一方；先查测试和调用链，并在任务卡中记录结论。

### 证据标记

| 标记 | 含义 | 可以支持的结论 |
| --- | --- | --- |
| `已观察` | 代码路径和可重复测试直接证明 | 可以描述当前行为，但仍需注明验证范围 |
| `推断` | 由多个实现、数据结构或测试行为推导 | 只能作为待确认假设，不能写成承诺 |
| `目标` | roadmap/架构设计/未连接的 DTO 或页面 | 只能描述计划，不能声称可用 |

## 3. 能力映射与维护

对每个迭代涉及的能力维护一行映射：

```text
能力 -> 入口/源码 -> 状态与持久化 -> 测试/契约 -> 验证命令 -> 权威文档 -> 当前证据标记
```

仓库当前的最短导航如下，具体测试文件以 `rg --files` 和测试模块为准：

| 能力 | 入口/实现 | 验证重点 |
| --- | --- | --- |
| 本地文件版本控制 | `apps/neoengram-cli`、`crates/neoengram-runtime/src/app`、`engine`、`local` | CLI workflow/recovery/integrity/workspaces；runtime `prepare_add` |
| 领域和协议 | `crates/neoengram-domain/src/core`、`src/protocol` | envelope、public_api、schema_golden、wire_golden；current schemas |
| Central 资源/API | `services/neoengram-central/src/controller`、`service`、`mapper`、`datasource` | HTTP、权限、生命周期、placement、workspace 集成测试 |
| Agent 数据面 | `services/neoengram-agent/src/agent_core`、`execution`、`session_*` | state_machine、persistent_adapters、central_managed_add；真实 mount 另验 |
| Gateway 控制链 | `services/neoengram-gateway/src` | `tests/network_e2e.rs`、mTLS/forwarding/双 Replica 相关断言 |
| 本地多 Gateway 开发编排 | `scripts/dev-stack.sh` | `bash -n`、dry-run、隔离 loopback Central/Gateway/Agent 烟测；不代表生产 HA |
| Web 控制台 | `apps/neoengram-web/src` | API、feature、页面、router、Playwright 测试；OpenAPI 生成类型 |

新增或移动能力后，更新三处：本指南的映射、`AGENTS.md` 的源码/测试表、对应专题文档的入口链接。若测试
入口变化，连同 `tests/project/modules/*.sh` 或 `tests/project/README.md` 一起更新。这样 AI 可以从能力名反查
最小阅读范围，而不需要通读大型架构文档。

## 4. 设计变更的边界检查

提交实现前逐项回答：

- 这是新增当前能力，还是把已有目标纵切推进了一步？`roadmap.md` 中状态是否需要改变？
- 输入、状态、输出和错误是否与现有 action registry/OpenAPI/Schema 一致？有无需要升级版本或拒绝未知字段？
- 是否改变持久化 schema、仓库格式、对象布局、Index/HEAD CAS、Ledger、RouteLease 或租户隔离？若改变，先写迁移/兼容策略和故障恢复测试。
- 是否越过依赖边界（domain/runtime/Central/Gateway/Web），或让 Server 接收 Chunk payload？若是，先停下来更新架构决策，而不是从一个快捷 import 开始。
- 是否把 Mock、单进程 SQLite、单元测试或 loopback token 误当成真实中心、HA、生产凭据或跨 Volume E2E？若无法证明，保留为局部能力。
- 哪个事实来源负责以后维护？只留下链接和短摘要，避免复制全套状态表。

## 5. 实现后验证

默认验证入口是上面的统一全量脚本；普通迭代不再额外重复运行下面的分模块命令。只有在统一脚本失败、
任务范围需要更快定位，或用户明确要求时，才选择与改动直接对应的额外检查：

```bash
# 领域/本地 runtime/CLI
cargo test -p neoengram-domain --all-targets --locked
cargo test -p neoengram-runtime --all-targets --locked
cargo test -p neoengram --all-targets --locked

# Central/Agent/Gateway
cargo test -p neoengram-central --tests --locked
cargo test -p neoengram-agent --all-targets --locked
cargo test -p neoengram-gateway --test network_e2e --locked

# 仓库质量门槛
cargo fmt --all -- --check
bash .github/check-architecture.sh
cargo clippy --workspace --all-targets --offline -- -D warnings
```

契约或 Web 改动必须分别运行 `docs/openapi` 和 `apps/neoengram-web` 的检查；跨模块改动使用
`scripts/project-test.sh module ...` 或 `scripts/project-test.sh modules`。需要验证组合进程、Volume 复制或
Central/Gateway 生命周期时运行 `scripts/project-test.sh full-flow`；它不自动证明真实 Agent enrollment 和
跨 Gateway payload transfer。Linux mount boundary 只能通过显式 `mount-probe` 验证。

每次结果记录：命令、是否使用 `--offline`/`--all-features`、平台、通过/失败/未运行、失败日志路径和剩余风险。
不要用“测试通过”代替具体证据，也不要因为依赖缺失而删除或跳过测试。

## 6. 文档、契约和生成文件同步

| 变更 | 必须同步 |
| --- | --- |
| CLI 语义、恢复、FUSE、对象 GC | `docs/reference/cli.md`、CLI/runtime 测试，必要时根 README |
| HTTP action、DTO、权限、分页 | `action_registry.rs`、`docs/openapi/*.yaml`、Central 测试、Web `api:generate`/`api:check` |
| wire protocol、digest、Schema | `crates/neoengram-domain/src/protocol`、`schemas/current`、golden/contract 测试、control-plane 专题 |
| authority/仓库/对象/事务/锁 | 对应 `docs/architecture/*.md`、恢复/并发/完整性测试、roadmap 状态 |
| Agent/Gateway transport、安全、证书 | control-plane/gateway 专题、Agent/Gateway 集成测试、manifest/配置文档 |
| Web 页面、Mock 和用户流程 | `apps/neoengram-web/tests`、`docs/product.md`；Mock 行为要明确标注范围 |
| Kubernetes 资源和运行参数 | 部署 README、`check-manifests.sh`、项目 manifests 模块 |

`schemas/current`、`src/api/generated/openapi.d.ts`、OpenAPI bundle 等生成内容只能由生成命令更新；
改动源文件后检查 diff，确认没有把 `target/`、`node_modules/`、凭据或报告带入提交。路线和状态只在
[`roadmap.md`](roadmap.md) 维护，不新建平行路线文件。

## 7. 完成标准

任务完成前，任务卡应能回答：

1. 当前实现是什么，哪些结论直接由代码/测试观察到；
2. 改动改变了哪个用户可见或服务间契约，哪些行为明确没有改变；
3. 不变量、失败恢复、权限和租户边界由什么测试保护；
4. 运行了哪些验证，哪些因平台/依赖未运行；
5. 哪些权威文档、Schema、OpenAPI 或生成类型已经同步；
6. 仍未完成的目标和风险是否留在 roadmap，而不是藏在“已支持”描述里。
