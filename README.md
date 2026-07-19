# NeoEngram

**NeoEngram** 的名字来自 *engram*（记忆痕迹），即人脑中承载和保存记忆的信息痕迹；
“Neo”代表面向 AI 数据的新一代实现。它是一个为模型权重和大规模数据集设计的内容寻址
版本控制系统，通过 FastCDC 切块、BLAKE3 去重和不可变快照，让大型 AI 资产能够像代码一样
被可靠地暂存、提交、校验与恢复。

项目当前聚焦单机仓库与本地工作流，已经提供 SQLite 元数据、事务化 checkout 和故障恢复
能力；分布式对象存储、远程同步与协作能力属于后续阶段。整体产品目标、当前能力清单和
分布式实现路线统一维护在 [`docs/implementation-plan.md`](docs/implementation-plan.md)。

## 核心语义

- `add` 不修改工作区文件，只把稳定输入快照按 FastCDC 分块并更新 index。
- `add -A` 同时暂存指定路径范围内的新增、修改和删除。
- 相同 Chunk 只保存一次；对象路径由 BLAKE3 Hash 决定。
- Chunk 在复用和读取时都会校验大小与 BLAKE3；Tree 和 Commit 在读取时复算内容 ID。
- `checkout <COMMIT_ID>` 会逐 Chunk 校验目标内容，再事务化更新工作区、index 和 detached HEAD。
- `commit` 验证全部暂存 Chunk 后固化不可变 Tree，每个 Commit 最多只有一个父节点。
- Detached HEAD 下的新 Commit 以当前 HEAD 为父节点且不移动 `main`；`checkout main` 重新附着。
- `recover` 可以继续或回滚被 kill/断电打断的 checkout/rm 事务。
- Tree、Commit 和分支引用保存在元数据控制面，文件分块保存在数据面。

## 快速开始

项目使用 Rust Edition 2021，需要 Rust 1.97.0 或更高版本。

```bash
cargo build -p neoengram

./target/debug/neoengram init .
./target/debug/neoengram add README.md
./target/debug/neoengram status
./target/debug/neoengram commit -m "add project readme"
./target/debug/neoengram diff --staged
./target/debug/neoengram log
./target/debug/neoengram checkout HEAD
./target/debug/neoengram checkout HEAD --read-only ../model-snapshot
./target/debug/neoengram gc --dry-run
./target/debug/neoengram fsck
```

`init` 默认使用 SQLite 行式元数据后端：

```bash
./target/debug/neoengram init .
```

需要 JSON 元数据后端时可以显式选择：

```bash
./target/debug/neoengram init --metadata-store json .
```

后端选择会写入仓库配置，后续命令会自动使用已记录的后端；在仓库根目录再次运行 `init` 也会
沿用该后端。NeoEngram 不会自动在 JSON 和 SQLite 之间迁移已有仓库。

新仓库会先在同一文件系统的 `.neoengram-tmp-*` 私有目录中完成初始化和校验，再以
no-replace rename 原子发布为 `.neoengram`。进程在发布前退出只会留下未发布的临时目录，
`add` 和 `status` 会忽略这些未发布目录，不会把半初始化状态当成仓库。

也可以直接通过 Cargo 运行：

```bash
cargo run -p neoengram -- init .
cargo run -p neoengram -- add README.md
cargo run -p neoengram -- status
cargo run -p neoengram -- commit -m "add project readme"
cargo run -p neoengram -- checkout HEAD
```

## 本地布局

```text
.neoengram/
├── objects/                         # 不可变 CDC 数据块：objects/<blake3>
│   └── .tmp/                        # 未发布临时对象
├── files/                           # checkout 使用的完整文件缓存
├── staging/                         # add 切块期间的稳定输入快照
├── transactions/                    # checkout/rm journal、暂存文件和故障备份
└── metadata/
    ├── repository.json              # 仓库格式版本与后端选择
    ├── write.lock                   # 按需创建的仓库写锁；文件可跨进程保留
    ├── objects.lock                 # add 与 gc 共享的对象发布/回收锁
    ├── metadata.sqlite3             # SQLite 后端的行式元数据数据库（选择 SQLite 时）
    ├── index.json                   # JSON 后端的完整暂存快照
    ├── HEAD                         # JSON 后端：symbolic ref 或 detached Commit ID
    ├── index.lock / refs.lock       # JSON 后端按需创建的事务与 CAS 锁
    ├── refs/heads/main              # JSON 后端的当前 Commit ID（首次提交后创建）
    ├── manifests/<manifest-hash>.json # JSON 后端的单文件 Chunk recipe
    ├── trees/<tree-hash>.json       # JSON 后端的不可变目录快照
    └── commits/<commit-hash>.json   # JSON 后端的不可变单父 Commit
```

提交采用追加式发布顺序：先验证全部 Chunk 并完成 ObjectStore durability barrier，再写并同步
Manifest、Tree 和 Commit，最后通过 CAS 原子更新当前分支引用或 direct HEAD。JSON 后端会
同步引用的父目录；SQLite 后端通过 `synchronous=FULL` 的 WAL 事务提交。`add`、`rm`、
`commit`、`checkout`、`recover` 和 `fsck` 共享 OS advisory 写锁；`add` 与 `gc` 另外共享
`objects.lock`，覆盖对象发布和回收的整个窗口。进程被 kill 后内核自动释放锁，遗留的普通
锁文件不会形成永久死锁。

仓库路径固定为 UTF-8 NFC 和 `/` 分隔，并拒绝 Windows drive prefix、设备保留名、非法
字符、大小写碰撞、文件/目录前缀碰撞，以及 `.neoengram` 和 `.neoengram-tmp-*` 保留组件。

## 存储抽象

元数据通过 `MetadataStore` 访问：文件和 Chunk 分页读取，Manifest 单次流式写入并返回
内容 ID，Index 使用 expected-version 事务，Tree 通过追加 writer 发布，Tree/Commit/ref
使用一致读 snapshot 分页枚举，HEAD 和 ref 更新使用 compare-exchange。Chunk payload 由独立
`ObjectStore` 管理，提供流式发布、单遍校验读取、批量状态检查、分页枚举和 durability
barrier。Repository 和命令层都不再依赖对象物理路径。

当前真实实现是 `JsonMetadataStore`、`SqliteMetadataStore` 和 `LooseObjectStore`。开发期仓库
格式直接演进，不提供旧格式自动回退；`repository.json` 必须显式记录两个后端：

```json
{
  "format_version": 4,
  "metadata_store": "sqlite",
  "object_store": "loose"
}
```

完整文件缓存和 checkout/rm journal 不放进上述两个存储接口：它们分别是可淘汰派生数据和
跨工作区 rename/元数据提交的恢复协议。逐项接口语义见
[`local/metadata/README.md`](crates/neoengram/src/local/metadata/README.md)，整体规模预算和迁移顺序
见 [`docs/storage-architecture.md`](docs/storage-architecture.md)，源码职责与未来扩展落点见
[`docs/code-architecture.md`](docs/code-architecture.md)；实现路线和研究记录见
[`docs/implementation-plan.md`](docs/implementation-plan.md)。

## 暂存与查看

暂存新增或修改：

```bash
./target/debug/neoengram add path/to/data
```

同时暂存删除；PATH 省略时范围是当前目录，从仓库根目录执行时才覆盖整个仓库：

```bash
./target/debug/neoengram add -A [PATH]
```

仓库根目录下可用 `.neoengramignore` 排除不应纳入版本控制的输入。`add` 和 `status`
使用同一组规则；支持空行、`#` 注释、`!` 否定、末尾 `/` 目录规则，以及 `*`、`?` 和
`**` 通配符。例如：

```text
*.tmp
checkpoints/
!checkpoints/README.txt
```

忽略规则只影响未跟踪文件的发现；已经在 index 中跟踪的文件仍会被 `add -A` 更新或删除。
该文件只从仓库根目录读取，当前不支持嵌套 `.neoengramignore`。

安全地删除已跟踪内容，或只从 index 取消跟踪：

```bash
./target/debug/neoengram rm path/to/data
./target/debug/neoengram rm --cached path/to/data
```

`rm` 默认拒绝丢弃未暂存修改；只有显式 `--force` 才覆盖该保护。提交前可检查 staged、
unstaged 和 untracked 三类状态，并查看线性历史或具体快照：

```bash
./target/debug/neoengram status
./target/debug/neoengram log --max-count 20
./target/debug/neoengram show HEAD
```

查看文件级和 Chunk 级变化（不会把未跟踪文件自动加入比较）：

```bash
./target/debug/neoengram diff
./target/debug/neoengram diff --staged
./target/debug/neoengram diff HEAD <COMMIT_ID>
./target/debug/neoengram diff --stat
```

撤销暂存或恢复工作区文件：

```bash
./target/debug/neoengram restore --staged path/to/data
./target/debug/neoengram restore path/to/data
./target/debug/neoengram restore --force path/to/data
```

## 恢复版本

重新物化当前 HEAD，不改变 attached/detached 状态：

```bash
./target/debug/neoengram checkout
```

恢复指定 Commit：

```bash
./target/debug/neoengram checkout <COMMIT_ID>
```

完整 Commit ID 会进入 detached HEAD；重新附着到默认分支：

```bash
./target/debug/neoengram checkout main
```

Checkout 默认拒绝丢弃 staged 修改、已跟踪文件修改或冲突的未跟踪文件。明确需要覆盖时：

```bash
./target/debug/neoengram checkout <COMMIT_ID> --force
```

将一个 Commit 导出到仓库之外的独立只读目录（不会改变当前工作区、index 或 HEAD）：

```bash
./target/debug/neoengram checkout HEAD --read-only ../model-snapshot
./target/debug/neoengram checkout <COMMIT_ID> --read-only ../older-model-snapshot
```

目标目录必须尚不存在，且其父级不能是符号链接或位于当前仓库内。Unix/macOS 上导出的普通
文件使用 `0444`、目录使用 `0555`；这是普通文件权限保护，不是不可绕过的 mount、ACL 或
root 级安全边界。导出过程先在同一父目录创建临时目录，完成 Chunk 校验后再原子发布，失败
时不会留下半成品。

切换到旧 Commit 后，`HEAD` 直接指向该 Commit，而 `main` 保持原位置。此时 `log`、
`show HEAD` 和 `status` 都以 detached HEAD 为准；后续 `commit` 以 detached Commit 为父节点，
只推进 direct HEAD，不移动 `main`。切回 `main` 后，未被命名引用保存的 detached Commit
不会出现在 main 的 `log` 中，但当前仍可使用完整 ID 执行 `show` 或再次 checkout。

Checkout 只处理发生变化的文件，校验其 Chunk 后组装完整文件缓存，
并优先使用 Reflink 物化；文件系统不支持时自动退回普通复制。`--force` 支持文件和目录
之间的类型转换，但仍拒绝跟随父级符号链接。

如果进程在工作区 rename、index 或 HEAD 发布期间退出，后续写命令会拒绝继续并给出恢复提示：

```bash
# 验证目标后继续原 checkout，或完成/回滚 rm 的安全状态
./target/debug/neoengram recover

# 回到事务开始前；已经提交 index 的 rm 不允许反向 abort
./target/debug/neoengram recover --abort
```

恢复 journal 会记录每个逻辑路径、原 index、切换前后的 HEAD 和发布阶段。回滚只移除能按
FileNode 验证为事务产物的文件，遇到用户后来创建或修改的未知内容会停止，不会递归删除。

## 完整性检查

```bash
./target/debug/neoengram fsck
```

清理不再被当前 index 或任何已保存 Tree 引用的 Chunk payload。不可变 Manifest、Tree 和
Commit 元数据不会被删除，因此 detached/不可达历史目前仍会保留其对象：

```bash
./target/debug/neoengram gc --dry-run
./target/debug/neoengram gc
```

`fsck` 校验全部 refs、Commit/Tree 内容 ID、单父历史无环、路径规则、Chunk 引用大小，
并复算对象库中每个 BLAKE3。它也检查不可达但仍保留在本地的元数据和 loose object。

## Workspace

```text
.
├── crates/
│   ├── neoengram-core/              # 纯领域模型与格式常量
│   │   └── src/models/
│   └── neoengram/                   # neoengram 命令行程序与本地仓库引擎
│       ├── src/
│       │   ├── cli/                 # 参数解析与终端输出
│       │   ├── app/                 # init/add/commit 等用例编排
│       │   └── local/
│       │       ├── repository/      # 仓库门面、配置、锁与领域校验
│       │       ├── worktree/        # 扫描、import 切块、物化与恢复事务
│       │       ├── metadata/        # 元数据契约及 JSON/SQLite 后端
│       │       ├── objects/         # contract.rs 与 loose.rs 本地对象后端
│       │       └── fs/              # crash-safe 文件系统原语
│       └── tests/                   # CLI 与跨模块集成测试
└── docs/                            # 代码与存储架构说明
```

依赖保持单向：`cli -> app -> local -> neoengram-core`。远端同步、协议 crate 和服务端只定义了
未来落点，当前源码树不创建空模块；完整约束见
[`docs/code-architecture.md`](docs/code-architecture.md)。

## 质量检查

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo rustdoc -p neoengram-core --all-features -- -D warnings
```

## 当前范围

完整的当前能力、已知限制、分布式目标架构、阶段验收标准和研究计划见
[`docs/implementation-plan.md`](docs/implementation-plan.md)。本文只保留面向用户的使用说明和
当前存储边界。

当前版本完成的是本地 Phase 1，不包含网络传输、PostgreSQL 控制面、S3、认证、多分支管理、
`push/fetch/pull/clone` 或服务端 GC。分页、事务、CAS、SQLite 行式元数据和流式对象抽象已经
落地，但 JSON 后端内部仍整份物化 index/Tree，部分命令仍把分页重新收集为完整快照，
checkout/rm journal 也仍保存完整 Index；因此当前实现还不能宣称支持百 TB。

下一步是对 SQLite 大规模工作负载做基准与调优、实现追加式恢复 journal、对象 fanout/pack，
以及 PostgreSQL + S3 的缺块协商和 CAS push；是否增加 Merkle 索引由规模基准决定。目标是
100 TB payload、千万路径和上亿 Chunk 引用下，命令内存由页大小与有界并发决定，而不是随
仓库总量增长。

本地层仍未保存 POSIX 权限位和符号链接，也没有 sparse checkout 或完整文件缓存配额。
这些限制会在开发期直接演进数据模型和仓库格式解决。

## 许可证

本项目由贡献者任选 [Apache-2.0](LICENSE-APACHE) 或 [MIT](LICENSE-MIT) 许可证使用。
