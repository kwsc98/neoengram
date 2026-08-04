# NeoEngram Web

Vue 3 用户控制台，只消费 [`../../docs/openapi/neoengram-api.yaml`](../../docs/openapi/neoengram-api.yaml)
定义的公开接口。Mock mode 用于稳定的前端回归；默认开发模式连接本机 Server，并使用 Server 的
loopback-only fixed Bearer token profile。

## 本地运行

```bash
npm ci
npm run api:generate
npm run dev:mock
```

浏览器访问 `http://127.0.0.1:4173`。Mock mode 按
[`../../docs/centralized-agent-product.md`](../../docs/centralized-agent-product.md) 提供多租户切换与
创建、StorageVolume 登记、无固定放置的 Artifact、单 Volume Playground、单区域 Snapshot、
Pre-commit、带描述和 Tags 的 Playground Commit、单 parent Commit 图、当前版本与父版本的文件
及元数据 Diff，以及 Managed Add Job 的 create/query/finalize 状态机；它不能用于生产构建。

真实 API 开发模式使用 `npm run dev`，默认 Bearer token 是 `local-development-token`，需与 Server
的 `--development-token` 保持一致；可通过 `VITE_DEVELOPMENT_TOKEN` 和
`VITE_DEVELOPMENT_PRINCIPAL` 覆盖。Vite 将 `/api`、`/health` 代理到
`http://127.0.0.1:8080`，并将 OpenAPI 风格的 `/agent` action API 代理到
`http://127.0.0.1:8081`；目标分别由 `VITE_API_PROXY_TARGET` 和
`VITE_AGENT_PROXY_TARGET` 调整。

`VITE_AGENT_ENDPOINT` 是写入 Agent YAML 的绝对 origin，默认开发值为
`http://127.0.0.1:8081`。它不是浏览器 origin，也不能包含 `/agent` 路径。bootstrap token 只在创建
响应区域显示，不会进入 YAML、localStorage 或 sessionStorage。

## 检查

```bash
npm run format:check
npm run lint
npm run typecheck
npm run api:check
npm test
npm run build
npm run test:e2e
```

`src/api/generated/openapi.d.ts` 由 OpenAPI 生成并提交，不能手工修改。服务端状态由 TanStack Vue
Query 管理；路由中的 `tenantId` 是当前租户的唯一来源。Pinia 只保存认证视图、可见租户的内存
视图、最近选择的 Tenant ID，以及最多 50 条浏览器本地最近 Job identity。token、权限和完整资源
不会写入 localStorage。

## 部署

生产输出是 `dist/` 静态文件，不嵌入 Rust binary。反向代理必须把 `/api`、`/health` 转发到公开
listener，把 `/agent` 转发到独立 Agent listener，其余未知路径回退到 `index.html`；
[`deploy/nginx.conf`](deploy/nginx.conf) 给出同源部署基线。生产构建禁止
`VITE_API_MODE=mock`、`VITE_AUTH_MODE=mock`、`VITE_AUTH_MODE=development` 以及任何
`VITE_DEVELOPMENT_TOKEN`。
