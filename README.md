# NeoEngram

NeoEngram 是一个面向 AI 模型和海量数据集的分布式文件版本控制系统。当前阶段实现了
本地内容定义切块（CDC）、BLAKE3 内容寻址、暂存区以及无合并的线性 Commit。

## 核心语义

- `add` 不修改工作区文件，只把稳定输入快照按 FastCDC 分块并更新 index。
- `add -A` 同时暂存指定路径范围内的新增、修改和删除。
- 相同 Chunk 只保存一次；对象路径由 BLAKE3 Hash 决定。
- 已有 Chunk、Commit 和 checkout 都会校验大小与完整 BLAKE3，而不是只看文件名。
- `commit` 验证全部暂存 Chunk 后固化不可变 Tree，每个 Commit 最多只有一个父节点。
- `checkout` 事务化恢复工作区和 index，但不会回退当前分支引用。
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
./target/debug/neoengram log
./target/debug/neoengram checkout HEAD
./target/debug/neoengram fsck
```

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
    ├── index.json                   # add 维护的完整暂存快照
    ├── HEAD                         # ref: refs/heads/main
    ├── write.lock                   # OS advisory lock；文件可跨进程保留
    ├── index.lock / refs.lock       # 后端事务与 CAS 锁
    ├── refs/heads/main              # 当前 Commit ID（首次提交后创建）
    ├── manifests/<manifest-hash>.json # 不可变的单文件 Chunk recipe
    ├── trees/<tree-hash>.json       # 不可变目录快照
    └── commits/<commit-hash>.json   # 不可变单父 Commit
```

提交采用追加式发布顺序：先验证全部 Chunk 并完成 ObjectStore durability barrier，再写并同步
Manifest、Tree 和 Commit，最后原子更新当前分支引用并同步父目录。`add`、`rm`、`commit`、
`checkout` 和 `recover` 共享 OS advisory
lock；进程被 kill 后内核自动释放锁，遗留的普通 `write.lock` 文件不会形成永久死锁。

仓库路径固定为 UTF-8 NFC 和 `/` 分隔，并拒绝 Windows drive prefix、设备保留名、非法
字符、大小写碰撞、文件/目录前缀碰撞及任何 `.neoengram` 组件。

## 存储抽象

元数据通过 `MetadataStore` 访问：文件和 Chunk 分页读取，Manifest 单次流式写入并返回
内容 ID，Index 使用 expected-version 事务，Tree 通过追加 writer 发布，Tree/Commit/ref
使用一致读 snapshot 分页枚举，ref 更新使用 compare-exchange。Chunk payload 由独立
`ObjectStore` 管理，提供流式发布、单遍校验读取、批量状态检查、分页枚举和 durability
barrier。Repository 和命令层都不再依赖对象物理路径。

当前真实实现是 `JsonMetadataStore` 和 `LooseObjectStore`。开发期仓库格式直接演进，不提供
旧格式自动回退；`repository.json` 必须显式记录两个后端：

```json
{
  "format_version": 3,
  "metadata_store": "json",
  "object_store": "loose"
}
```

完整文件缓存和 checkout/rm journal 不放进上述两个存储接口：它们分别是可淘汰派生数据和
跨工作区 rename/元数据提交的恢复协议。逐项接口语义见
[`storage/metadata/README.md`](crates/neoengram-cli/src/storage/metadata/README.md)，整体规模预算和
迁移顺序见 [`docs/storage-architecture.md`](docs/storage-architecture.md)。

## 暂存与查看

暂存新增或修改：

```bash
./target/debug/neoengram add path/to/data
```

同时暂存删除；PATH 省略时范围是整个仓库：

```bash
./target/debug/neoengram add -A [PATH]
```

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

## 恢复版本

恢复当前版本：

```bash
./target/debug/neoengram checkout
```

恢复指定 Commit：

```bash
./target/debug/neoengram checkout <COMMIT_ID>
```

Checkout 默认拒绝丢弃 staged 修改、已跟踪文件修改或冲突的未跟踪文件。明确需要覆盖时：

```bash
./target/debug/neoengram checkout <COMMIT_ID> --force
```

恢复旧 Commit 只会改变工作区和当前后端的 Index，不会把 `main` 指针向后移动。此时执行
`commit` 会以当前 `main` 为唯一父节点追加一个新的恢复提交，继续满足 no-merge 和
append-only 约束。Checkout 只处理发生变化的文件，校验其 Chunk 后组装完整文件缓存，
并优先使用 Reflink 物化；文件系统不支持时自动退回普通复制。`--force` 支持文件和目录
之间的类型转换，但仍拒绝跟随父级符号链接。

如果进程在工作区 rename 或 index 发布期间退出，后续写命令会拒绝继续并给出恢复提示：

```bash
# 验证目标后继续原 checkout，或完成/回滚 rm 的安全状态
./target/debug/neoengram recover

# 回到事务开始前；已经提交 index 的 rm 不允许反向 abort
./target/debug/neoengram recover --abort
```

恢复 journal 会记录每个逻辑路径、原 index 和发布阶段。回滚只移除能按 FileNode 验证为
事务产物的文件，遇到用户后来创建或修改的未知内容会停止，不会递归删除。

## 完整性检查

```bash
./target/debug/neoengram fsck
```

`fsck` 校验全部 refs、Commit/Tree 内容 ID、单父历史无环、路径规则、Chunk 引用大小，
并复算对象库中每个 BLAKE3。它也检查不可达但仍保留在本地的元数据和 loose object。

## Workspace

```text
.
├── neoengram-core/          # Chunk、FileNode、Index、Tree、Commit 与 CDC 核心库
└── crates/neoengram-cli/    # 命令与仓库编排
    └── src/storage/
        ├── metadata/        # 元数据契约、JSON 后端、契约测试与操作文档
        ├── object.rs        # 对象后端选择
        └── file.rs          # 共用 crash-safe 文件原语
```

## 质量检查

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo rustdoc -p neoengram-core --all-features -- -D warnings
```

## 当前范围

当前版本完成的是本地 Phase 1，不包含网络传输、PostgreSQL 控制面、S3、认证、分支、
`push/fetch/pull/clone` 或服务端 GC。分页、事务、CAS 和流式对象抽象已经落地，但 JSON
后端内部仍整份物化 index/Tree，部分命令仍把分页重新收集为完整快照，checkout/rm journal
也仍保存完整 Index；因此当前实现还不能宣称支持百 TB。

下一步是 SQLite 行级 Index/Manifest、追加式恢复 journal、对象 fanout/pack，以及
PostgreSQL + S3 的缺块协商和 CAS push；是否增加 Merkle 索引由规模基准决定。目标是
100 TB payload、千万路径和上亿 Chunk 引用下，命令内存由页大小与有界并发决定，而不是
随仓库总量增长。

本地层仍未保存 POSIX 权限位和符号链接，也没有 sparse checkout 或完整文件缓存配额。
这些限制会在开发期直接演进数据模型和仓库格式解决。

## 许可证

本项目由贡献者任选 [Apache-2.0](LICENSE-APACHE) 或 [MIT](LICENSE-MIT) 许可证使用。
