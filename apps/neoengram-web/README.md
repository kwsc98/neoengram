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

浏览器访问 Vite 输出的 URL（默认是 `http://127.0.0.1:4173`；该端口被占用时会自动选择下一个可用端口）。Mock mode 按
[`../../docs/product.md`](../../docs/product.md) 提供多租户切换与
创建、StorageVolume 登记、无固定放置的 Artifact、单 Volume Playground、单区域 Snapshot、
Pre-commit、带描述和 Tags 的 Playground Commit、单 parent Commit 图、当前版本与父版本的文件
及元数据 Diff，以及 Managed Add Job 的 create/query/finalize 状态机；它不能用于生产构建。

真实 API 开发模式使用 `npm run dev`，默认 Bearer token 是 `local-development-token`，需与 Server
的 `--development-token` 保持一致；可通过 `VITE_DEVELOPMENT_TOKEN` 和
`VITE_DEVELOPMENT_PRINCIPAL` 覆盖。Vite 只将浏览器使用的 `/api`、`/health` 代理到
`http://127.0.0.1:8080`，目标由 `VITE_API_PROXY_TARGET` 调整。内部 `/agent` action API 不属于
浏览器接口，也不经 Web 开发服务器代理。

`VITE_GATEWAY_ENDPOINT` 是写入 Agent YAML 的 GatewayPool 绝对 origin，默认开发值为
`http://127.0.0.1:8181`。它不是浏览器 origin，也不能包含 `/agent` 路径。非 loopback 环境必须使用
HTTPS。一个 Web 构建只绑定一个 GatewayPool，因此生产或其他 HTTPS 部署还必须配置
`VITE_GATEWAY_EDGE_CLUSTER_ID`，并且只能为该 EdgeCluster 生成 Agent YAML；用户输入其他
`edge_cluster_id` 时会在请求接入凭证前失败关闭。`VITE_GATEWAY_WORKLOAD_TRUST_DOMAIN` 也必须配置；
Web 会把该小写 DNS trust domain 和 `/etc/neoengram/central-command-trust.json` 写入 Agent YAML，
缺失或格式错误时不会签发接入 token。只有 loopback HTTP 开发配置允许省略 EdgeCluster binding。
部署时仍须在 YAML 声明的路径投射 Gateway CA 和 Central command public-key bundle。bootstrap token
只在创建响应区域显示，不会进入 YAML、localStorage 或 sessionStorage。

本地无 DNS 时，S3 使用 path-style loopback endpoint。Mock 模式默认使用当前 Web origin；真实本地开发默认使用
网关公网监听 `http://127.0.0.1:8084`，也可通过
`VITE_S3_ENDPOINT=http://127.0.0.1:<网关公网端口>` 覆盖；请求形如
`http://127.0.0.1:<端口>/<bucket>/<key>`。生产构建由 Central 返回已登记的 HTTPS GatewayPool
S3 endpoint，不使用这个开发默认值。

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

生产输出是 `dist/` 静态文件，不嵌入 Rust binary。反向代理只把 `/api`、`/health` 转发到 Central
公开 listener，其余未知路径回退到 `index.html`。不得从 Web ingress 暴露内部 `/agent` action；
[`deploy/nginx.conf`](deploy/nginx.conf) 给出同源部署基线。生产构建禁止
`VITE_API_MODE=mock`、`VITE_AUTH_MODE=mock`、`VITE_AUTH_MODE=development` 以及任何
`VITE_DEVELOPMENT_TOKEN`。

本机 Gateway 可使用 `npm run build:local-gateway` 生成 `dist/`。这个专用构建连接真实 Central，
但保留 loopback-only development Bearer token；构建配置会拒绝非 `http://localhost`、
`http://127.0.0.1` 或 `http://[::1]` 的 API、Gateway 和 S3 地址，不能作为生产部署产物。
