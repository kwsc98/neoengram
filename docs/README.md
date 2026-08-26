# NeoEngram 文档

本文档目录按读者和事实来源组织。先看项目根目录的 [`../README.md`](../README.md) 了解定位、安装和最短本地工作流。

## 从哪里开始

| 目标 | 文档 |
| --- | --- |
| 安装并完成第一次本地操作 | [`reference/cli.md`](reference/cli.md) |
| 了解当前源码、运行时和存储边界 | [`architecture/code.md`](architecture/code.md)、[`architecture/storage.md`](architecture/storage.md) |
| 了解 Central、Agent、协议和安全边界 | [`architecture/control-plane.md`](architecture/control-plane.md) |
| 了解 Gateway 拓扑、HA 和数据路径 | [`architecture/gateway.md`](architecture/gateway.md) |
| 了解产品资源、用户流程和 Web 语义 | [`product.md`](product.md) |
| 从代码、契约和测试确认当前实际能力 | [`current-state.md`](current-state.md) |
| 查看当前能力、限制和实现顺序 | [`roadmap.md`](roadmap.md) |
| 按代码反推产品行为并执行 AI 迭代 | [`iteration-guide.md`](iteration-guide.md)、[`../AGENTS.md`](../AGENTS.md) |
| 查看机器可读 API 契约 | [`openapi/README.md`](openapi/README.md) |
| 查看 Kubernetes 部署步骤 | [`../deploy/kubernetes/agent/README.md`](../deploy/kubernetes/agent/README.md)、[`../deploy/kubernetes/gateway/README.md`](../deploy/kubernetes/gateway/README.md) |

## 权威来源

每类事实只维护一个主要来源：

- CLI 当前行为以 [`reference/cli.md`](reference/cli.md) 和代码测试为准。
- 源码和本地存储实现以 `architecture/` 下的对应专题和代码为准。
- Central/Agent 内部协议、安全、租户边界和状态机以 [`architecture/control-plane.md`](architecture/control-plane.md) 为准。
- Gateway 专项与其他设计冲突时，以 [`architecture/gateway.md`](architecture/gateway.md) 为准。
- 产品目标、资源语义和用户体验原则以 [`product.md`](product.md) 为准；当前已实现行为先查 [`current-state.md`](current-state.md)，再回到代码和测试。
- 代码反推的当前实现基线以 [`current-state.md`](current-state.md) 为摘要，代码、测试和 action registry 仍是最终证据。
- 当前能力、技术债务、路线和研究结论只以 [`roadmap.md`](roadmap.md) 为准。
- HTTP API 以 [`openapi/`](openapi/) 和 [`../crates/neoengram-domain/schemas/current/`](../crates/neoengram-domain/schemas/current/) 为准。

如果文档与代码、契约或测试不一致，应先修正事实来源，再补充其他文档中的链接或摘要，避免复制一份新的状态表。

## 目录约定

```text
docs/
├── README.md
├── product.md
├── current-state.md
├── roadmap.md
├── architecture/
│   ├── code.md
│   ├── control-plane.md
│   ├── gateway.md
│   └── storage.md
├── reference/
│   └── cli.md
└── openapi/
    ├── README.md
    ├── neoengram-api.yaml
    ├── neoengram-agent-api.yaml
    └── scripts/
```

`openapi/` 是可执行的契约目录，不要把 YAML、Redocly 配置或校验脚本合并进叙述性 Markdown。依赖安装目录、Web 构建产物、Playwright 报告和测试结果也不属于版本库文档。

迁移前架构图和历史观测 HTML 原型已从主文档树移除；它们不能作为当前能力证据。新的交互验证应直接更新 `apps/neoengram-web` 或提交可审阅的源码/测试资产。

## 文档维护

- 公共行为变化：同步更新根 README、CLI 参考或 OpenAPI 契约，并增加测试。
- 架构、协议、安全和存储边界变化：先更新对应专题，再更新 [`roadmap.md`](roadmap.md) 的状态和变更记录。
- 路线状态只在 [`roadmap.md`](roadmap.md) 更新，不再新增平行的 Roadmap 文件。
- 局部部署和模块说明保留在其代码目录旁；它们应链接到本目录的权威专题，不复制完整设计。
- AI 或自动化任务遵循根目录 [`../AGENTS.md`](../AGENTS.md) 和 [`iteration-guide.md`](iteration-guide.md)，
  先从代码、契约和测试确认当前行为，再更新产品或路线描述。
