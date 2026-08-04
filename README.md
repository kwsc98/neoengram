# NeoEngram

**NeoEngram** 的名字来自 *engram*（记忆痕迹），即人脑中承载和保存记忆的信息痕迹；
“Neo”代表面向 AI 数据的新一代实现。它是一个为模型权重和大规模数据集设计的内容寻址
版本控制系统，通过 FastCDC/WholeFile 分块、BLAKE3 去重和不可变快照，让大型 AI 资产能够像代码一样
被可靠地暂存、提交、校验与恢复。

项目当前聚焦单机仓库与本地工作流，已经提供 SQLite 元数据、事务化 checkout 和故障恢复
能力。`0.2.0` 同时完成了中心化架构所需的 crate/protocol/状态机；独立 server 默认通过 Fusen
暴露版本、健康和 Managed Add Job 六个接口，启用 enrollment 后再提供五个公开管理接口和独立的
Agent bootstrap/status listener。`neoengram-agentd` 已能执行出站注册与审批状态轮询；证书、mTLS
session、Job delivery、其余公开 API、分布式数据库和对象存储仍属于后续阶段。整体产品目标、当前能力清单和
分布式实现路线统一维护在 [`docs/implementation-plan.md`](docs/implementation-plan.md)。完整
状态模型、命令行为和命令差异见 [`docs/technical-reference.md`](docs/technical-reference.md)。

## 核心语义

- `add` 不修改工作区文件，只把稳定输入快照按持久化的 FastCDC 或 WholeFile 策略描述并更新 index。
- `add -A` 同时暂存指定路径范围内的新增、修改和删除。
- 相同 Chunk 只保存一次；对象路径由 BLAKE3 Hash 决定。
- Chunk 在复用和读取时都会校验大小与 BLAKE3；Manifest、Directory 和 Commit 使用规范二进制 ID。
- `checkout <COMMIT_ID>` 会逐 Chunk 校验目标内容，再事务化更新工作区、index 和 detached HEAD。
- `commit` 分页读取 Index，深度优先发布不可变 Directory DAG，每个 Commit 最多只有一个父节点。
- Detached HEAD 下的新 Commit 以当前 HEAD 为父节点且不移动 `main`；`checkout main` 重新附着。
- `recover` 可以继续或回滚被 kill/断电打断的 checkout/rm/restore 事务。
- Directory、Manifest、Commit 和分支引用保存在 SQLite，文件分块保存在数据面。

## 快速开始

项目使用 Rust Edition 2021，需要 Rust 1.97.0 或更高版本。从源码以 release profile 安装二进制：

```bash
git clone https://github.com/kwsc98/synapse.git
cd synapse
cargo install --locked --path crates/neoengram --features fuse-mount
cd ..
```

请在独立的数据目录中试用，不要在 NeoEngram 源码 checkout 内直接执行 `neoengram init .`。
NeoEngram 不读取 `.gitignore`，源码 checkout 中的 `.git/`、`target/` 和其他构建产物会成为扫描
候选。创建一个独立 demo 仓库：

```bash
mkdir neoengram-demo
cd neoengram-demo
```

在这个目录中创建 `.neoengramignore`，例如内容如下：

```text
.git/
target/
*.tmp
cache/
```

然后完成一次本地工作流：

```bash
neoengram init .
neoengram add -A .
neoengram status
neoengram diff --staged
neoengram commit -m "initial snapshot"
neoengram log
neoengram workspace create experiment --from HEAD
neoengram workspace list
neoengram export HEAD exports/model-snapshot
mkdir mounts/model
neoengram mount HEAD mounts/model
# 另一个终端：neoengram unmount mounts/model
neoengram gc --dry-run
neoengram fsck
```

format v8 固定使用 SQLite；`init --metadata-store` 已删除。新仓库默认绑定 fastcdc，也可以
在初始化时选择 whole-file 或 mixed。仓库策略不可通过重新初始化修改，旧格式不迁移，打开时会
明确要求重新初始化。

Linux 使用 `fuser` 的无系统 libfuse FUSE3 路径，运行时需要内核 FUSE 和 `fusermount3`。
macOS 构建和运行前必须安装并允许 macFUSE；NeoEngram 不自动安装或启用系统扩展。Windows
可以构建常规命令，但 `mount`/`unmount` 返回 unsupported/未启用错误。

新仓库会先在同一文件系统的 `.neoengram-tmp-*` 私有目录中完成初始化和校验，再以
no-replace rename 原子发布为 `.neoengram`。进程在发布前退出只会留下未发布的临时目录，
`add` 和 `status` 会忽略这些未发布目录，不会把半初始化状态当成仓库。

## 使用边界

- 当前仓库格式仍处开发期，可能直接演进且不提供旧格式迁移。升级前应先保留可验证的独立备份。
- `.neoengram` 与工作区位于同一个本地故障域；NeoEngram 不能替代离线或异机备份，也不应保存
  重要数据的唯一副本。
- `rm`、`restore --force` 和 `checkout --force` 会按请求修改工作区。虽然命令间使用 fail-fast
  锁和恢复事务，运行期间仍应避免让其他程序改写同一路径。
- 常规本地工作流支持 Linux、macOS 和 Windows；FUSE 挂载支持 Linux FUSE3 与 macOS macFUSE。
  `export` 仅依赖权限位；hardlink 模式还会与仓库对象共享 inode。FUSE 视图由内核强制只读
  且仅挂载用户可访问。
- 当前模型不保存 POSIX mode、符号链接、xattr、ACL 或 sparse 信息。提交前应确认这些语义不会
  影响数据的可复现性。

## 本地布局

```text
repository-root/                     # 默认 main Workspace
├── .neoengram/
│   ├── objects/                     # 不可变 CDC Chunk
│   ├── cache/materialized/          # Manifest ID 分片的完整文件缓存
│   ├── staging/                     # add 的稳定输入快照
│   ├── transactions/                # main Workspace 恢复事务
│   └── metadata/
│       ├── repository.json          # format v8、repository_id、对象后端、分块策略
│       ├── objects.lock
│       ├── write.lock
│       └── metadata.sqlite3         # 共享对象、refs、Workspace-scoped HEAD/Index
├── workspaces/<name>/               # 其他受管可写 Workspace
│   └── .neoengram/{locks,transactions}/
├── mounts/<name>/                   # 固定 Commit FUSE 挂载点
└── exports/<name>/                  # 物化只读导出
```

提交采用追加式发布顺序：先验证全部 Chunk 并完成 ObjectStore durability barrier，再写并同步
Manifest、Directory 和 Commit，最后通过 CAS 原子更新当前分支引用或 direct HEAD。SQLite
使用 `synchronous=FULL` 的 WAL 事务和发布 staging 表。

本地并发协调使用三层 OS advisory lock，固定获取顺序为
`objects.lock -> workspace worktree.lock -> write.lock`。`add`、`status` 和 `diff` 共享工作区锁；checkout、
rm、工作区 restore 和 recover 独占工作区锁；commit 与 `restore --staged` 只锁元数据状态；
gc/fsck 先协调对象再锁状态。
锁冲突会立即失败并要求重试。`add` 最终以最初读取的 `IndexVersion` 做 CAS，status/diff 在
输出前复核实际使用的 IndexVersion 与 HEAD/main，因此并发的 index-only 更新不会被覆盖或
混入报告。进程被 kill 后内核会释放锁；磁盘上保留的 lock 文件本身不会形成永久死锁。

仓库路径固定为 UTF-8 NFC 和 `/` 分隔，并拒绝 Windows drive prefix、设备保留名、非法
字符、大小写碰撞、文件/目录前缀碰撞，以及 `.neoengram` 和 `.neoengram-tmp-*` 保留组件。

## 存储抽象

元数据职责拆为 immutable catalog、workspace index、ref 和 workspace registry；文件和 Chunk
分页读取，Index 使用 expected-version 事务，Directory 通过追加 writer 发布，HEAD/ref 使用
compare-exchange。Chunk payload 由独立 `ObjectStore` 管理，提供流式发布、校验读取、分页枚举和
durability barrier。Repository 和命令层都不依赖对象物理路径。

当前 Standalone 实现使用 SQLite 和 `LooseObjectStore`。开发期仓库格式直接演进，不提供
旧格式自动回退；`repository.json` 为：

```json
{
  "format_version": 8,
  "repository_id": "<64-char lowercase id>",
  "object_store": "loose",
  "chunking": "fastcdc"
}
```

完整文件缓存和工作区 mutation journal 不放进上述两个存储接口：缓存按 Manifest ID 共享，
journal 位于各 Workspace 自身文件系统中。逐项接口语义见
[`local/metadata/README.md`](crates/neoengram-standalone/src/local/metadata/README.md)，整体规模预算和迁移顺序
见 [`docs/storage-architecture.md`](docs/storage-architecture.md)，源码职责与未来扩展落点见
[`docs/code-architecture.md`](docs/code-architecture.md)；实现路线和研究记录见
[`docs/implementation-plan.md`](docs/implementation-plan.md)。

## 暂存与查看

暂存新增或修改：

```bash
neoengram add path/to/data
```

分块策略在初始化时绑定到仓库。默认 fastcdc 适合经常发生局部变化的数据；需要保证 Commit
具备硬链接导出条件时，应创建 whole-file 仓库：

```bash
neoengram init --chunking whole-file .
neoengram add path/to/model.bin
```

需要在同一仓库中按文件选择策略时，必须显式创建 mixed 仓库；只有 mixed 接受
`add --chunking`，未指定时已跟踪文件沿用原策略，新文件使用 FastCDC：

```bash
neoengram init --chunking mixed .
neoengram add --chunking whole-file path/to/model.bin
neoengram add --chunking fastcdc path/to/dataset
```

固定 fastcdc/whole-file 仓库会拒绝 `add --chunking`，再次执行 `init` 也不能改变仓库策略。
FastCDC 使用 256 KiB/1 MiB/4 MiB 的 min/avg/max 参数。WholeFile 对非空文件只生成一个覆盖
完整文件的 BLAKE3 对象，空文件仍为零对象；它全程流式导入，但任意字节变化都要重新发布
完整文件，只提供整文件级跨版本去重。

同时暂存删除；PATH 省略时范围是当前目录，从仓库根目录执行时才覆盖整个仓库：

```bash
neoengram add -A [PATH]
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
该文件只从仓库根目录读取，当前不支持嵌套 `.neoengramignore`，也不会自动合并 `.gitignore`。

安全地删除已跟踪内容，或只从 index 取消跟踪：

```bash
neoengram rm path/to/data
neoengram rm --cached path/to/data
```

`rm` 默认拒绝丢弃未暂存修改；只有显式 `--force` 才覆盖该保护。提交前可检查 staged、
unstaged 和 untracked 三类状态，并查看线性历史或具体快照：

```bash
neoengram status
neoengram log --max-count 20
neoengram show HEAD
```

查看文件级和 Chunk 级变化（不会把未跟踪文件自动加入比较）：

```bash
neoengram diff
neoengram diff --staged
neoengram diff HEAD <COMMIT_ID>
neoengram diff --stat
```

撤销暂存或恢复工作区文件：

```bash
neoengram restore --staged path/to/data
neoengram restore path/to/data
neoengram restore --force path/to/data
```

工作区 restore 在最终发布前会重新检查目标。没有 `--force` 时拒绝覆盖检查后出现或变化的内容；
`--force` 也只替换已经观察到的普通文件，始终拒绝目录、符号链接和特殊文件。

## 多 Workspace

每个 Workspace 有独立 `base_commit_id`、HEAD、Index、worktree lock 和恢复事务，但共享外层
`.neoengram` 的 Chunk、Manifest、Directory、Commit、refs 和物化缓存：

```bash
# Detached Workspace
neoengram workspace create experiment --from HEAD

# 创建并独占新分支
neoengram workspace create feature --from main --branch feature/data

# 注册仓库外目录；该目录通过 repository_id + workspace_id 指针发现仓库
neoengram workspace create training --from HEAD --path /Volumes/Data/training

neoengram workspace list
neoengram workspace remove experiment
neoengram workspace remove experiment --force
```

同一个分支最多绑定一个可写 Workspace；Detached Workspace 不受限制。`commit` 的不可变对象
始终写入共享仓库，Attached Workspace 通过 CAS 同时推进分支和自己的 base Commit，其他
Workspace 不会自动移动。GC/fsck 会遍历所有 Workspace Index。

## 恢复版本

重新物化当前 HEAD，不改变 attached/detached 状态：

```bash
neoengram checkout
```

恢复指定 Commit：

```bash
neoengram checkout <COMMIT_ID>
```

完整 Commit ID 会进入 detached HEAD；重新附着到默认分支：

```bash
neoengram checkout main
```

Checkout 默认拒绝丢弃 staged 修改、已跟踪文件修改或冲突的未跟踪文件。明确需要覆盖时：

```bash
neoengram checkout <COMMIT_ID> --force
```

将一个 Commit 复制导出到仓库之外的独立只读目录（不会改变当前工作区、index 或 HEAD）：

```bash
neoengram export HEAD ../model-snapshot
neoengram export --mode copy <COMMIT_ID> ../older-model-snapshot
```

目标目录必须尚不存在，且其父级不能是符号链接或位于当前仓库内。Unix/macOS 上导出的普通
文件使用 `0444`、目录使用 `0555`；这是普通文件权限保护，不是不可绕过的 mount、ACL 或
root 级安全边界。导出过程先在同一父目录创建临时目录，完成 Chunk 校验后再原子发布，失败
时不会留下半成品。

可信本地环境中，whole-file 仓库的 Commit 可以严格硬链接导出：

```bash
neoengram export --mode hardlink HEAD ../model-hardlink-view
```

该模式仍会逐 Manifest 验证每个文件都是 WholeFile；mixed 仓库只有全 WholeFile 的 Commit 才能
通过。目标与对象目录必须位于同一文件系统，并且对象后端
支持硬链接；任何条件不满足都会使整个导出失败，不会回退复制或留下半成品。非空文件与仓库
对象共享 inode，清除写位也会同时改变对象权限，所有者仍能改回并写坏对象；因此输出只是
“hard-linked read-only view”，不是独立、不可变的快照。Pack/S3 后端和跨文件系统目标不能使用。

切换到旧 Commit 后，`HEAD` 直接指向该 Commit，而 `main` 保持原位置。此时 `log`、
`show HEAD` 和 `status` 都以 detached HEAD 为准；后续 `commit` 以 detached Commit 为父节点，
只推进 direct HEAD，不移动 `main`。切回 `main` 后，未被命名引用保存的 detached Commit
不会出现在 main 的 `log` 中，但当前仍可使用完整 ID 执行 `show` 或再次 checkout。

Checkout 只处理发生变化的文件，校验其 Chunk 后组装完整文件缓存，
并优先使用 Reflink 物化；文件系统不支持时自动退回普通复制。`--force` 支持文件和目录
之间的类型转换，但仍拒绝跟随父级符号链接。

## 只读 FUSE 挂载

把 TARGET 一次解析为固定 Commit 并前台挂载：

```bash
mkdir mounts/model
neoengram mount <HEAD|BRANCH|COMMIT_ID> mounts/model
neoengram mount HEAD mounts/model --cache-size-mib 1024
```

挂载点必须已存在、为空且没有符号链接祖先。仓库受管的 `mounts/<name>` 可以直接使用；其他
路径必须与仓库存储和全部 Workspace 互不包含。挂载后 HEAD/ref 移动
不会改变视图。文件报告 `0444`，目录报告 `0555`；只支持普通文件，所有写入、删除、rename、
truncate、chmod/chown、link/symlink 返回 `EROFS`，xattr 返回 `ENOTSUP`。随机读通过 Manifest
offset 索引定位 Chunk，完整读取并校验后进入按字节计费的 single-flight LRU；损坏或缺失映射
为 `EIO`。WholeFile 对象超过 `--cache-size-mib` 时会在分配前明确失败；可以增大缓存上限或
使用同文件系统的 hardlink 导出。内核页缓存和只读 mmap 可用。

`mount` 默认前台阻塞，`Ctrl-C`/`SIGTERM` 正常卸载。也可以从另一个终端执行：

```bash
neoengram unmount mounts/model
```

`unmount` 会先验证 mount table 中的 NeoEngram 标识；Linux 调用 `fusermount3 -u`，macOS 调用
`/sbin/umount`，不会卸载其他文件系统。v1 不支持 daemon、`allow_other`、远端下载或可写 overlay。

如果进程在工作区 rename、index 或 HEAD 发布期间退出，后续写命令会拒绝继续并给出恢复提示：

```bash
# 验证目标后继续 checkout，或完成/回滚 rm/restore 的安全状态
neoengram recover

# 回到事务开始前；已经提交 index 的 rm 不允许反向 abort
neoengram recover --abort
```

checkout/rm/restore 会先在当前 Workspace 的 `.neoengram/transactions` 构造并同步本地事务 draft，
再以 no-replace rename 发布正式 journal；Engine journal 也必须在第一次工作区 mutation 前进入
durable 状态。事务返回 `WorktreeReceipt` 后，checkout/rm 先完成 SQLite expected-version CAS 再
finalize；worktree restore 不发布 Index，`restore --staged` 则独立更新 SQLite Index。recover 恢复
正式本地事务、清理经过验证的遗留 draft/stale lock，
并收尾相应 Engine journals。回滚只移除能按 journal fingerprint 和预期内容证明为事务产物的文件；
遇到用户后来创建或修改的未知内容、原路径与备份同时缺失等歧义时会停止并保留事务。

## 完整性检查

```bash
neoengram fsck
```

清理不再被当前 Index 或任何 Commit root Directory 引用的 Chunk payload。不可变元数据目前
不会删除，且所有 Commit 都是 GC root，因此 detached 历史仍会保留其对象：

```bash
neoengram gc --dry-run
neoengram gc
```

`fsck` 校验全部 refs、Commit/Directory/Manifest 内容 ID、单父历史无环、路径规则、Chunk 引用大小，
并复算对象库中每个 BLAKE3。它也检查不可达但仍保留在本地的元数据和 loose object。

硬链接视图被修改会同步污染仓库对象，后续 `fsck`、commit、checkout 或 export 会在大小或 Hash
校验处明确失败。当前不自动修复：先删除或废弃相关硬链接视图，再从远端、备份或原始数据取得
可信字节并重新导入，最后运行 `neoengram fsck`。不要用已损坏的硬链接内容覆盖对象。

## 源码 Workspace

```text
.
├── crates/
│   ├── neoengram-core/              # 强类型领域模型、规范 digest 与路径校验
│   ├── neoengram-engine/            # 结构化用例与执行 ports
│   ├── neoengram-fs/                # worktree/object/journal/lock 适配器
│   ├── neoengram-protocol/          # v1 DTO、Schema、JCS digest 与限额
│   ├── neoengram-standalone/        # SQLite、Repository、FUSE 与本地编排
│   ├── neoengram-agent/             # 无网络 Agent 状态机与测试适配器
│   └── neoengram/                   # Clap、cwd 输入和唯一终端渲染入口
├── services/
│   ├── neoengramd/                  # 无网络中心状态机、ports 与 SQLite datasource/mapper
│   ├── neoengram-server/            # Fusen 用户 HTTP、Hyper Agent enrollment 与运行时组装
│   └── neoengram-agentd/            # 可运行的 Agent enrollment/polling daemon 与健康命令
├── apps/
│   └── neoengram-web/               # Vue 3 用户控制台；首版由 OpenAPI/MSW 驱动
└── docs/                            # 代码与存储架构说明
```

生产依赖主要保持单向：`CLI -> standalone -> engine <- agent`，Standalone 同时依赖 fs；protocol
只依赖 core，服务端方向固定为 `neoengram-server -> neoengramd -> protocol/core`，Agent 进程方向为
`neoengram-agentd -> neoengram-agent/protocol`。该简图不枚举 CLI 对 core/engine 的直接类型导入；
Agent library 到 `neoengramd` 的依赖仅存在于 dev/test 组合测试。CLI 之外不渲染终端输出；完整约束见
[`docs/code-architecture.md`](docs/code-architecture.md)。

`neoengram-web` 是独立 npm 应用，不进入 Cargo workspace，也不导入 Rust crate、Agent Schema 或
数据库类型。它只从公开 OpenAPI 生成客户端类型；当前可通过 MSW 运行租户切换/创建、
StorageVolume 登记与放置选择、无固定放置 Artifact、单 Volume Playground、单区域 Snapshot、
Playground Commit、资源浏览和
Commit 描述/Tag、父版本文件 Diff、Managed Add Job。真实 server 默认实现版本、健康和 Managed
Add Job 六接口；启用 enrollment 时另实现 token、列表、查询、批准和拒绝五个管理接口，其余 Web
operation 仍由 MSW 提供。

中心化 Agent 的产品定位、用户角色、资源语义、页面规格、Pre-commit/Commit/Snapshot 主链路和
OpenAPI 对齐清单见 [`docs/centralized-agent-product.md`](docs/centralized-agent-product.md)；技术权威
边界继续以 [`docs/agent-central-control.md`](docs/agent-central-control.md) 为准。

## 质量检查

```bash
cargo fmt --all -- --check
bash .github/check-architecture.sh
cargo run -p neoengram-protocol --example generate_schemas --offline
cargo test --workspace --all-targets --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --offline
cargo package --locked --allow-dirty -p neoengram-core
cargo package --locked --allow-dirty --no-verify --exclude-lockfile -p neoengram
```

Schema 命令确定性重建已提交的 `crates/neoengram-protocol/schemas/v1`。这组本地命令使用默认
feature，因而不要求 macOS 已安装 macFUSE SDK/runtime。CI 在 Linux 运行 `--all-features` 测试、
Clippy、rustdoc 和 MSRV check，并在 Linux、macOS 和 Windows 运行默认 feature 的 workspace 测试。
最后两个命令分别验证可发布的 core crate 和 CLI `.crate` 归档；CLI 使用 workspace-private 的
engine/standalone 依赖，因此 `--exclude-lockfile` 只验证归档组装和内容，不证明这些依赖可从
crates.io 解析或 CLI 可从 registry 安装。

## 当前范围

完整的当前能力、已知限制、分布式目标架构、阶段验收标准和研究计划见
[`docs/implementation-plan.md`](docs/implementation-plan.md)。本文只保留面向用户的使用说明和
当前存储边界。

当前可运行产品完成的是本地 format v8、多 Workspace 与固定 Commit 只读 FUSE；中心另提供 protocol、
Agent/控制面状态机、SQLite 单节点权威，以及 Fusen 用户 listener 与可选 Hyper Agent enrollment
listener。`neoengram-agent` binary 已能持久化身份、探测挂载、签名 bootstrap 并轮询审批状态。业务接口
使用外部 OIDC/JWKS 与默认拒绝 RBAC；SQLite 模式只支持单副本，生产 TLS 由 Ingress/反向代理终止。
它仍不包含其余 OpenAPI、Agent 证书/session/Job transport、PostgreSQL、真实 S3、HA、merge/rebase、
`push/fetch/pull/clone` 或服务端 GC。分页、事务、CAS、分层 Merkle Directory 和流式 Commit
已经落地；CLI 通过结构化 Request/typed Result facade 调用 Standalone，并拥有全部成功文本与
错误/结果渲染，Standalone 不再暴露通用 `CommandResult`。
`commit` 已接入 Engine canonical graph builder/publisher；`checkout`、工作区 `restore` 和工作区 `rm`
已经接入 `MutationPlan -> durable journal -> WorktreeReceipt`，其中 checkout/rm 在 SQLite expected
`IndexVersion` CAS 后才 finalize，`recover` 会恢复本地事务并清理 Engine journals。`add`、`commit`、
`mount`、`checkout`、`rm`、`restore` 和 `recover` 的 Engine `ProgressEvent` 已通过 caller-owned
`ProgressSink` 透传到 CLI。
部分非 FUSE compatibility view 和 mutation journal 仍会物化完整 Index，因此当前实现还不能宣称
所有命令都适用于千万路径。

下一步是继续把 Standalone 过渡物化 view 收敛到 engine 分页 ports，对 SQLite 大规模工作负载做
基准与调优，并实现 PostgreSQL、其余用户 API、Agent session/Job transport、mTLS、真实 S3 adapters 和 CAS push。目标是
100 TB payload、千万路径和上亿 Chunk 引用下，命令内存由页大小与有界并发决定，而不是随
仓库总量增长。

本地层仍未保存 POSIX 权限位和符号链接，也没有 sparse checkout 或完整文件缓存配额。
这些限制会在开发期直接演进数据模型和仓库格式解决。

## 许可证

本项目由贡献者任选 [Apache-2.0](LICENSE-APACHE) 或 [MIT](LICENSE-MIT) 许可证使用。
