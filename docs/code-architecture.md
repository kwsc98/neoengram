# 源码架构

NeoEngram 当前采用两个 crate：`neoengram-core` 定义内容模型与格式常量，`neoengram`
承载命令行程序和本地仓库引擎。现阶段只实现本地工作流；远端同步、配置中心、S3 和服务端
控制面在本文中仅规定未来落点，不代表已经实现。当前能力和分阶段实现计划统一见
[`implementation-plan.md`](implementation-plan.md)。

## 当前目录职责

```text
crates/
├── neoengram-core/
│   └── src/models/              # Chunk、File、Index、Tree、Commit、格式常量
└── neoengram/
    ├── src/
    │   ├── cli/                 # Clap 参数解析、调用 app、渲染结果
    │   ├── app/                 # init/add/commit/checkout 等用例编排
    │   └── local/
    │       ├── repository/      # 仓库门面、布局配置、锁、历史与领域校验
    │       ├── worktree/        # import.rs 切块、输入快照、物化和恢复事务
    │       ├── metadata/        # MetadataStore 契约、attached/direct HEAD、JSON/SQLite 后端
    │       ├── objects/         # contract.rs 契约、loose.rs/未来 pack 本地后端
    │       └── fs/              # 持久写入、原子发布和安全路径原语
    └── tests/                   # CLI 端到端与跨模块集成测试
```

边界规则：

- `neoengram-core` 只包含可复用的领域模型与格式常量，不依赖 CLI、Tokio、SQLite、文件系统
  或具体对象后端。模型统一后，规范内容 ID 计算也应逐步收敛到这里。
- `cli` 只处理输入输出；业务流程由 `app` 编排，不能把数据库或对象路径暴露为命令语义。
- `app` 负责编排 `local::repository` 与 `local::worktree`；worktree 可以使用 repository
  提供的仓库上下文，但具体存储后端不能反向依赖 repository、app 或 cli。
- `local::metadata` 只表示本地 SQLite/JSON 持久化，未来不能把 PostgreSQL 或远端 API
  作为新的 `MetadataStoreKind` 塞入同一枚举。
- `local::objects` 表示客户端本地对象库和缓存；远端 S3 不作为本地 `ObjectStoreKind`。
- checkout/rm 的工作区 journal 属于 `local::worktree`，不进入 MetadataStore 或 ObjectStore。

## 依赖方向

```text
cli ──> app ──> local ──> neoengram-core
                  │
                  ├──> repository ──> metadata / objects / fs
                  └──> worktree ──> repository / metadata / objects / fs
```

箭头只指向更稳定的边界。`neoengram-core` 不知道仓库位于本地还是远端；存储后端也不知道
命令行参数和终端输出。跨模块数据优先使用领域类型和结构化结果，而不是物理路径或 SQL 行。

## 未来扩展落点

开始实现远端功能时再创建对应代码，不提前保留空目录：

```text
crates/neoengram/src/
├── remote/                       # API client、认证、重试、上传/下载票据
└── sync/                         # push/fetch/pull/clone 与本地状态编排

crates/neoengram-protocol/        # 版本化 wire DTO、capability 和协议错误

services/neoengramd/              # 服务端模块化单体
├── api/                          # 对客户端暴露的版本化 API
├── repositories/                # 仓库策略、refs、权限和事务规则
├── sync/                        # 缺块协商与 push/fetch 会话
└── adapters/
    ├── postgres/                # 远端元数据与配置中心持久化
    └── s3/                      # Chunk/pack payload、上传票据与完整性校验
```

客户端只通过 `neoengramd` 的版本化 API 使用远端能力，不直连 PostgreSQL，也不持有服务端
S3 长期凭证。`sync` 负责协调 `local` 与 `remote`，`remote` 负责传输，`protocol` 只定义线上
契约；这些职责不能并入本地存储 trait。

第一版远端服务保持模块化单体。只有在负载、部署或故障域产生明确证据后，才把后台校验、
pack、GC 等任务拆成独立服务。远端数据模型、API 版本、本地仓库格式、SQLite schema 和
PostgreSQL migration 分别版本化，避免一次演进同时锁死所有层。

本地磁盘布局及当前存储事务语义见
[`storage-architecture.md`](storage-architecture.md)；元数据接口逐项契约见
[`local/metadata/README.md`](../crates/neoengram/src/local/metadata/README.md)；路线、研究和
验收标准见 [`implementation-plan.md`](implementation-plan.md)。
