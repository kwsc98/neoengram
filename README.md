# NeoEngram

NeoEngram 是面向模型权重和大规模数据集的内容寻址版本控制系统。它使用 FastCDC/WholeFile
分块、BLAKE3 去重、不可变 Commit 和可验证恢复，让大型 AI 资产可以像代码一样被暂存、提交、校验和
物化。

当前稳定可用的主链路是本地仓库：SQLite 元数据、事务化工作区 mutation、故障恢复、只读 FUSE 和
copy/hardlink 导出。中心化能力正在持续集成：Central authority、Volume-bound Agent、Gateway 控制面、
公开 HTTP API 和 Vue Web 控制台已有代码与契约；逻辑 Snapshot、SnapshotDelivery 控制链和固定版本只读
S3 也已有实现，但完整生产 E2E、外部凭据签发、PostgreSQL/HA、跨 Volume payload 执行和物理物化/读取
验收仍未完成。Server 不接收 Chunk payload；Managed 模式的对象字节由获批 StorageVolume 上的 Agent 负责。

当前已实现行为以 [`docs/current-state.md`](docs/current-state.md) 和代码/测试为准，目标能力、限制和路线以
[`docs/roadmap.md`](docs/roadmap.md) 为准。文档总入口和 AI 迭代规则见 [`docs/README.md`](docs/README.md)
与 [`AGENTS.md`](AGENTS.md)。

## 快速开始

要求 Rust 1.97.1 或更高版本。安装 CLI：

```bash
git clone https://github.com/kwsc98/neoengram.git
cd neoengram
cargo install --locked --path apps/neoengram-cli --features fuse-mount
```

请在源码 checkout 之外创建试用目录：

```bash
mkdir ../neoengram-demo
cd ../neoengram-demo
cat > .neoengramignore <<'EOF'
.git/
target/
*.tmp
cache/
EOF

neoengram init .
neoengram add -A .
neoengram status
neoengram commit -m "initial snapshot"
neoengram log
neoengram export HEAD exports/model-snapshot
neoengram fsck
```

仓库格式 9 使用 SQLite。初始化时可选择 `fastcdc`（默认）、`whole-file` 或 `mixed`；仓库创建后
分块策略不可重新初始化修改，旧格式不会自动迁移。完整命令语义、Workspace、恢复、FUSE、对象 GC
和故障排查见 [`docs/reference/cli.md`](docs/reference/cli.md)。

## 核心语义

- `add` 只读取稳定输入并更新 index，不直接修改工作区文件；`add -A` 同时处理新增、修改和删除。
- Chunk、Manifest、Directory 和 Commit 都会在发布和读取时校验内容身份；相同 Chunk 只保存一次。
- `commit` 发布不可变 Directory DAG，并使用 CAS 更新 HEAD/ref；每个 Commit 当前最多一个 parent。
- `checkout`、`restore`、`rm` 和 `recover` 使用持久事务，进程中断后不会把半完成状态当成成功。
- Detached HEAD 上的新 Commit 以当前 Commit 为父节点且不移动 `main`；`checkout main` 重新附着。
- FUSE 视图固定到解析时的 Commit，只读且不跟随 HEAD；`export` 生成仓库外的权限快照。

## 使用边界

- 本地仓库格式仍处开发期，升级前应保留可验证备份；它不能替代离线或异机备份。
- 本地工作流支持 Linux、macOS 和 Windows；FUSE 需要 Linux FUSE3 或 macFUSE，Windows 挂载命令不启用。
- 当前不保存 POSIX mode、符号链接、xattr、ACL 或 sparse 语义，提交前请确认这不会影响可复现性。
- 远端 `push`、`fetch`、`clone`、merge/rebase、PostgreSQL、多副本 HA、跨 Volume payload route 和
  完整生产 Gateway 切换不属于当前已完成能力。

## 项目结构

```text
apps/
├── neoengram-cli/       # CLI 和终端渲染
└── neoengram-web/       # 独立 Vue 3 控制台
crates/
├── neoengram-domain/    # 领域模型、强类型 ID、协议和 Schema
└── neoengram-runtime/   # 执行内核、本地 SQLite、对象和 FUSE 适配器
services/
├── neoengram-central/   # Central authority 和公开 HTTP composition
├── neoengram-agent/     # Agent 状态机和 Gateway transport
└── neoengram-gateway/   # Gateway tunnel、mTLS、activation 和 forwarding
deploy/                  # Kubernetes 示例与部署说明
docs/                    # 叙述文档和 OpenAPI 契约
tests/                   # 项目级测试模块
scripts/                 # 开发和测试脚本
```

详细源码、存储、控制面和 Gateway 边界见 [`docs/architecture/`](docs/architecture/)。产品目标与 Web 资源
语义见 [`docs/product.md`](docs/product.md)，代码反推的当前产品基线见 [`docs/current-state.md`](docs/current-state.md)。机器可读 API 契约位于 [`docs/openapi/`](docs/openapi/)，
领域 Schema 位于 [`crates/neoengram-domain/schemas/current/`](crates/neoengram-domain/schemas/current/)。

## 开发与检查

AI/自动化迭代的低 Token 约定见 [`docs/iteration-guide.md`](docs/iteration-guide.md)，统一项目测试脚本的
调用、依赖和报告语义见 [`tests/project/README.md`](tests/project/README.md)。

需要本地同时启动 Central 和多个 Gateway 时，可使用一键开发脚本；例如启动 3 个 Gateway，并让三个
独立 Agent 分别管理三个本地 Volume 目录：

```bash
bash scripts/dev-stack.sh --gateways 3 \
  --disk "$PWD/dev/volume-1" --disk "$PWD/dev/volume-2" --disk "$PWD/dev/volume-3" \
  --auto-approve
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh stop
```

脚本只绑定 loopback，每个 `--disk` 目录由一个独立 Agent/Volume 管理，Gateway 不挂载或保存业务磁盘。
`--disk-gateway` 和 `--volume-id` 可按出现顺序重复；省略时目录按 Gateway 轮询，Volume ID 自动生成。
Agent 默认保持 `pending_approval`；本地演示可增加 `--auto-approve`。这是开发编排器，不代表生产 TLS、HA、PVC fencing
或跨节点 E2E。停止后使用相同 `--data-dir` 再次 `start` 会恢复已保存的网关/端口/磁盘拓扑；自定义
Central token 需要再次通过 `--central-token` 传入。源码变更后可用 `--rebuild` 强制刷新本地二进制。
完整参数见 `bash scripts/dev-stack.sh --help`。

```bash
cargo fmt --all -- --check
bash .github/check-architecture.sh
cargo test --workspace --all-targets --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --offline
```

Web 控制台是独立 npm 应用，开发和检查命令见 [`apps/neoengram-web/README.md`](apps/neoengram-web/README.md)。
Kubernetes 使用说明见 [`deploy/kubernetes/agent/README.md`](deploy/kubernetes/agent/README.md) 和
[`deploy/kubernetes/gateway/README.md`](deploy/kubernetes/gateway/README.md)。贡献流程见
[`CONTRIBUTING.md`](CONTRIBUTING.md)，安全问题请按 [`SECURITY.md`](SECURITY.md) 报告。

## 许可证

NeoEngram 采用 [`MIT`](LICENSE-MIT) 或 [`Apache-2.0`](LICENSE-APACHE) 双许可证。
