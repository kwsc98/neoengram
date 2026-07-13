# 百 TB 存储架构

NeoEngram 的目标是管理至少 100 TB 的逻辑 payload。这个目标首先约束接口和算法：命令的
内存占用必须由分页大小与有界并发决定，不能随文件数、Chunk 数、历史数或对象数线性增长。

项目仍处开发阶段。仓库格式可以直接演进，不提供旧格式自动兼容；但同一格式内的内容 ID、
事务和恢复语义必须稳定且可校验。

## 已实现边界

### MetadataStore

当前元数据接口已经提供：

- `PageRequest` / `Page<T>`：所有可能无界的枚举都使用排他 cursor，单页最多 4096 项；
- `FileRecord`：只保存路径、大小、Chunk 数和不可变 `manifest_id`；
- `ManifestReader`：按页读取单文件的 Chunk recipe；
- `put_manifest`：单次消费 Chunk iterator，由后端返回内容 ID 和 Chunk 数；
- `IndexReader`：分页读取固定版本的 Index；
- `IndexTxn`：基于 expected `IndexVersion` 执行 upsert 或前缀删除，再原子提交；
- `MetadataReader`：Commit、Tree、Manifest 和 ref 的点查，不触发全仓库枚举；
- `MetadataSnapshot`：固定 refs 以及 Tree、Manifest、Commit ID，用于一致分页枚举；
- `TreeWriter`：追加有序 `FileRecord`，由 `finish` 发布不可变 Tree 并返回 root ID；
- `compare_exchange_reference`：按 expected target 创建、更新或删除 ref。

`IndexVersion` 同时包含单调 revision 和内容摘要。revision 与 Index 在一次原子写入中更新，
因此即使内容发生 A -> B -> A，也不会产生 ABA 误判。

`JsonMetadataStore` 和 `SqliteMetadataStore` 均实现上述完整契约。JSON 后端会整份读取或
重写元数据；SQLite 后端使用行式存储、keyset 分页、MVCC snapshot 和数据库内 ref CAS。
新仓库默认使用 SQLite，也可以在初始化时显式选择 JSON。后端类型持久化在
`metadata/repository.json` 中，后续打开以该配置为准；当前不提供后端间自动迁移。
新控制目录先在同文件系统的私有临时目录中完整初始化，再通过 no-replace rename 原子发布，
因此配置或后端初始化失败不会暴露半初始化的 `.neoengram`。缺少原子 no-replace 目录重命名
能力的平台会拒绝新仓库初始化，而不是退化为存在覆盖竞态的发布方式。

每个元数据操作的输入、输出、事务、Drop 和错误语义以
[`local/metadata/README.md`](../crates/neoengram/src/local/metadata/README.md) 为准；本文只维护
跨模块发布顺序、规模热点和演进方向。源码模块职责和依赖方向见
[`code-architecture.md`](code-architecture.md)。

### 本地 ObjectStore

本地 Chunk payload 使用独立、object-safe 的同步流式接口：

- `put_from`：边读边校验精确大小和 BLAKE3，再 immutable/no-clobber 发布；
- `copy_to`：单遍校验并输出，不要求调用方获得物理路径；
- `verify`、`stat` 和有界 `check_many`；
- `list_page`：按 opaque cursor 分页枚举；
- `durability_barrier`：成功后，此前发布的对象才允许被元数据引用。

当前 `local::objects` 中的 `LooseObjectStore` 每个对象保存为一个本地文件。add、commit、
checkout 和 fsck 已经通过 `ObjectStore` 工作，不再拼接 `objects/<hash>` 路径。

完整文件缓存是可淘汰派生数据，不属于 ObjectStore。checkout/rm journal 协调工作区 rename
和元数据提交，也不属于 MetadataStore。

## 发布顺序

Commit 必须遵守以下顺序：

1. 流式发布 Chunk，并完成 ObjectStore durability barrier；
2. 发布不可变 FileManifest；
3. 发布不可变 Tree；
4. 发布不可变 Commit；
5. 使用旧父 Commit 作为 expected target 执行 ref CAS。

ref CAS 是新 Commit 对读者可见的最后线性化点。失败可以留下不可达 Manifest、Tree 或
Commit，但不能覆盖并发提交，也不能让 ref 指向未完整发布的依赖。

Index transaction 必须全有或全无。expected `IndexVersion` 不匹配时不能提交；事务被丢弃时不能
产生可见 Index 变化。路径 portable-key 唯一性、文件/目录祖先冲突和 Chunk 连续性仍属于
Repository 的领域校验。

## 分页与快照

分页结果必须严格有序、无重复且不超过请求上限。cursor 只允许传回产生它的同一个 reader
或 snapshot，调用方不得跨查询混用。当前 cursor 不承诺可持久化；可恢复 fsck checkpoint 是
后续独立能力。

`MetadataReader` 用于点查，`MetadataSnapshot` 只用于需要固定枚举边界的操作，避免 log 每读
一个 Commit 都加载全部历史。JSON snapshot 固定 refs 和历史 ID 集合；SQLite snapshot 使用
WAL read transaction 固定同一个 MVCC 视图。

本地 ObjectStore 的当前分页是 weak scan，没有固定 generation。它可用于检查，但不能作为
并发 GC 的删除依据；本地 pack 需要 generation/cutoff。远端对象协商、传输和 GC 属于独立的
异步协议与服务端 catalog，不实现这个本地 trait。

## 当前全量热点

接口已经解除后端约束，但以下实现仍不能用于百 TB：

- JsonMetadataStore 整份物化 Index、Tree、Manifest 和 snapshot ID 列表；
- Repository 的现有 `read_index`、`read_tree` 和 `list_*` 会把分页重新收集为 Vec；
- add 会收集完整目录遍历、任务结果和 staged files，单个 FileNode 仍持有完整 Chunk Vec；
- commit 在写锁内扫描完整 Index，并重新读取、Hash 全部暂存 payload；
- status、checkout 和 rm 使用全量 BTreeMap/BTreeSet，恢复日志保存完整 Index；
- checkout 预先建立全部变更文件的永久完整缓存，缺少 quota/lease；
- fsck 虽已分页扫描 ObjectStore，仍在内存中保存完整元数据图并长期持有写锁；
- LooseObjectStore 的平铺目录每翻一页都要重扫，完整 fsck 近似 `O(N^2 / page_size)`；
- log/show 的展示路径尚未全面改成 cursor 输出。

因此“实现了 trait”不等于“后端已适合百 TB”。README 和命令输出不能做这种承诺。

## 下一步

1. 让 add 直接使用 IndexTxn 的 scope replace/upsert，并用 staging 批次控制内存和事务时长。
2. 为新写入对象保存 verified receipt；commit 只核对本次变化和持久化状态，完整 rehash 留给
   fsck，避免每次提交读取整个 100 TB。
3. 将 status/commit 改为有序分页 merge；rm 使用 revision token 和前缀事务。
4. 将 checkout 改为逐文件物化到事务 staging；缓存变成有配额的 best-effort 层。
5. 将 checkout/rm journal 改为追加式有界批次，只保存 before/after revision 与 root。
6. 让 fsck 使用固定 cutoff、可恢复 cursor 和磁盘临时集合，不长期持有全仓库写锁。
7. 用 fanout、pack 或带 catalog 的对象后端替代平铺 loose 目录，并为后端配置增加 locator。
8. 基于实际基准决定是否引入 Merkle Tree；SQLite 分页记录和独立 FileManifest 应先解除主要
   内存瓶颈，Merkle 是局部更新与 root 计算优化，不是预先锁死的实现。

## 规模推导

100 TB payload 在约 1 MiB 平均 Chunk 大小时接近一亿条 Chunk 引用；若大量 Chunk 接近当前
256 KiB 下界，引用数可能接近四亿。仅 Chunk recipe 就可能达到数十 GB，所以任何“元数据
可以全部放入内存”的假设都不成立。

最终验收至少要覆盖：千万路径局部 add/rm、单个超大文件 manifest、Commit CAS 冲突、
checkout 中断恢复、可恢复 full fsck，以及对象后端高延迟和部分失败。具体页字节上限、事务
批次、并发数、SQLite schema 和是否使用 Merkle，都应由这些基准确定。
