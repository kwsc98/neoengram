# NeoEngram Web

Vue 3 用户控制台，只消费 [`../../docs/openapi/neoengram-api.yaml`](../../docs/openapi/neoengram-api.yaml)
定义的公开接口。当前 `neoengramd` 尚未实现 HTTP/OIDC adapter，因此本应用默认通过显式 mock mode
进行开发和端到端测试。

## 本地运行

```bash
npm ci
npm run api:generate
npm run dev:mock
```

浏览器访问 `http://127.0.0.1:4173`。Mock mode 提供多租户切换与创建、Project 筛选、Artifact
与单 parent Commit 图、Playground/Snapshot 只读详情，以及 Managed Add Job 的
create/query/finalize 状态机；它不能用于生产构建。

真实 API 开发模式使用 `npm run dev`，Vite 默认把 `/api` 和 `/health` 代理到
`http://127.0.0.1:8080`，可通过 `VITE_API_PROXY_TARGET` 调整。OIDC 配置见 `.env.example`；token
只保存在 sessionStorage。

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

生产输出是 `dist/` 静态文件，不嵌入 Rust binary。反向代理必须把 `/api`、`/health` 转发到
`neoengramd`，其余未知路径回退到 `index.html`；[`deploy/nginx.conf`](deploy/nginx.conf) 给出同源
部署基线。生产构建禁止 `VITE_API_MODE=mock` 或 `VITE_AUTH_MODE=mock`。
