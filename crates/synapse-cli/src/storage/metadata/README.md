# 元数据存储模块

本目录定义 Synapse 的元数据持久化契约，并提供开发期使用的 JSON 后端。接口面向未来的
SQLite 或其他行式后端设计，不暴露文件路径、SQL 连接或物理表结构。

`MetadataStore` 可以跨线程共享；它返回的 Reader、Snapshot、Transaction 和 Writer 只在
创建它们的线程内使用，因此 SQLite read transaction 不需要额外实现 `Sync`。

## 模块边界

本模块负责：

- 可变 Index；
- 不可变 File Manifest、Tree 和 Commit；
- HEAD 符号引用与 `refs/...` 引用；
- 有界分页、固定读视图、Index 事务和 ref compare-exchange；
- 元数据对象的原子、不可变发布。

本模块不负责：

- Chunk payload 的保存、校验和 durability barrier，这些属于 `ObjectStore`；
- `repository.json` 中的仓库格式和后端选择；
- 仓库级命令写锁、工作区文件、完整文件缓存、输入 staging；
- checkout/rm 恢复 journal 和命令编排；
- 路径的跨平台规范化、文件/目录祖先冲突等 Repository 领域校验；
- Commit ID 计算、元数据图可达性检查、GC 和缓存配额。

元数据后端仍必须在访问物理存储前拒绝非法元数据 ID、引用名和引用前缀。

## 数据模型

| 数据 | 可变性 | 说明 |
| --- | --- | --- |
| Index | 可变 | 当前暂存文件集合，只能通过事务修改 |
| File Manifest | 不可变 | 单个文件的大小和有序 Chunk recipe |
| Tree | 不可变 | 一次提交中的完整文件集合 |
| Commit | 不可变 | 指向 Tree 和可选父 Commit |
| HEAD | 初始化后只读 | 指向一个 `refs/...` 引用名 |
| Reference | 可变 | 名称到目标 ID 的映射，只能通过 CAS 修改 |
| Metadata Snapshot | 固定读视图 | 固定 HEAD、refs 和历史对象 ID 集合 |

Index 和 Tree 只保存轻量 `FileRecord`：

- `path`
- `total_size`
- `chunk_count`
- `manifest_id`

读取 Chunk 时先调用 `MetadataReader::open_manifest`，再通过
`ManifestReader::scan_chunks` 分页读取。Index 和 Tree 不直接返回完整 Chunk 列表。

与持久化相关的构造 helper：

| 操作 | 语义 |
| --- | --- |
| `FileRecord::from_manifest` | 用已发布的 `ManifestRef` 构造轻量文件记录 |
| `describe_manifest` | 校验内存中的完整 recipe，并计算 Manifest ID 与 Chunk 数，不发布数据 |
| `file_manifest_id` | 只返回完整 recipe 的规范 Manifest ID |
| `tree_records_id` | 根据有序 `FileRecord` 计算规范 Tree ID |
| `open_metadata_store` | 根据 `MetadataStoreKind` 构造配置指定的后端 |

这些 helper 不替代分页接口。`describe_manifest` 用于当前命令层已有完整 recipe 的校验路径，
`file_manifest_id` 用于后端实现和测试计算规范 ID；未来流式写入应直接调用 `put_manifest`。

## 通用约束

- 元数据对象 ID 是 64 位小写十六进制字符串。
- 所有可能无界的枚举必须分页，单页最多 4096 项。
- 文件、对象 ID 和引用按逻辑键严格递增返回，不能重复。
- Cursor 是排他的，只能继续产生它的同一个 Reader、Snapshot 和查询。
- Cursor 不能跨后端、跨 Snapshot 使用，也不是可持久化 checkpoint。
- `IndexVersion` 是不透明并发令牌，调用方只能比较，不能解释其字段。
- 每次成功提交 Index 都产生新版本；内容发生 A -> B -> A 时也不能复用旧版本。
- Manifest、Tree 和 Commit 不可覆盖：同 ID 同内容重复发布成功，不同内容必须报错。
- 不可变元数据当前没有删除操作，Snapshot 的一致性依赖这一点。
- 点查不能为了读取一个对象而枚举整个仓库。

## 分页值

`PageRequest::new(after, limit)` 创建分页请求，`PageRequest::first(limit)` 创建第一页请求。
`limit` 必须大于零且不超过 4096。

`PageRequest::validate` 重新检查上限；`limit_usize` 在检查后把 limit 转成当前平台的
`usize`，供后端分配有界结果页。

`Page<T>` 包含：

- `items`：数量不能超过请求上限；
- `next`：`None` 表示结束，否则作为下一页的 `after`。

文件前缀包含路径本身及其 `/` 子孙；`None` 或空前缀表示全部文件。引用前缀采用同样
规则，`refs` 表示全部引用。Chunk cursor 对调用方是不透明值。

当前上限约束记录数，不约束单页序列化后的字节数。

## 读取操作

| 操作 | 语义 |
| --- | --- |
| `ManifestReader::total_size` | 返回 Manifest 的文件总字节数 |
| `ManifestReader::chunk_count` | 返回 Chunk 总数 |
| `ManifestReader::scan_chunks` | 按文件顺序分页读取 Chunk recipe |
| `FileSetReader::get_file` | 按完整路径点查文件 |
| `FileSetReader::scan_files` | 按可选路径前缀分页读取文件 |
| `IndexReader::format_version` | 返回 Index 数据格式版本 |
| `IndexReader::version` | 返回该固定 Index 视图的并发令牌 |
| `TreeReader::file_count` | 返回 Tree 文件总数 |
| `MetadataReader::read_head_reference` | 返回 HEAD 指向的引用名 |
| `MetadataReader::get_reference` | 点查引用，不存在时返回 `None` |
| `MetadataReader::open_manifest` | 按 ID 打开 Manifest，不存在时返回 `None` |
| `MetadataReader::open_tree` | 按 ID 打开 Tree，不存在时返回 `None` |
| `MetadataReader::get_commit` | 按 ID 读取 Commit，不存在时返回 `None` |

`read_index`、已打开的 Tree/Manifest 和事务内读取都基于各自的固定状态，翻页时不能漂移。

## Index 事务

| 操作 | 语义 |
| --- | --- |
| `MetadataStore::read_index` | 打开当前固定版本的 Index Reader |
| `MetadataStore::begin_index_transaction` | 基于当前 Index 创建 read-modify-write 事务 |
| `IndexTxn::upsert_file` | 按完整路径新增或替换 `FileRecord` |
| `IndexTxn::delete_prefix` | 删除路径本身及其子孙并返回数量；空前缀删除全部 |
| `IndexTxn::commit` | 原子发布全部修改并返回新 `IndexVersion` |

传入 `expected: Some(version)` 时，版本不匹配不得返回事务。`None` 表示调用方不附加前置
版本条件，但事务仍固定自己的 base version，并在提交前复核。

Transaction 同时实现 `IndexReader`，读取包含事务内修改的工作副本；`version()` 仍返回
base version。只有 `commit` 会发布 Index。Transaction 被 Drop 时，所有修改必须丢弃并
释放后端事务或锁。

## 不可变写入

| 操作 | 语义 |
| --- | --- |
| `MetadataStore::put_manifest` | 单次消费 Chunk iterator，校验并发布 Manifest，返回 ID 和 Chunk 数 |
| `MetadataStore::begin_tree_write` | 创建追加式 Tree Writer |
| `TreeWriter::append_file` | 追加 `FileRecord`，路径必须跨调用严格递增 |
| `TreeWriter::finish` | 计算并不可变发布 Tree，返回 Tree ID |
| `MetadataStore::put_commit` | 使用调用方提供的 ID 不可变发布 Commit |

Manifest recipe 必须满足：

- Chunk Hash 是合法元数据 ID；
- Chunk 大小大于零；
- offset 从零开始连续；
- 大小和数量计算不溢出；
- 最终累计大小等于 `total_size`；
- 空 Manifest 只表示零字节文件。

Iterator 或校验失败时不得发布 Manifest。`put_manifest` 必须根据实际消费的内容返回
`ManifestRef`，调用方不需要预先计算 ID。

Tree Writer 被 Drop 时不得发布 Tree，`finish` 是唯一发布点。追加文件时必须确认引用的
Manifest 存在，且大小和 Chunk 数与 `FileRecord` 一致。

`put_commit` 只负责 ID 格式、不可变性和持久化。Commit ID 是否匹配内容、Tree 和父
Commit 是否有效，由 Repository 校验。

## Snapshot 操作

| 操作 | 语义 |
| --- | --- |
| `MetadataStore::snapshot` | 创建固定的历史元数据读视图 |
| `MetadataSnapshot::scan_tree_ids` | 分页枚举固定 Tree ID 集合 |
| `MetadataSnapshot::scan_manifest_ids` | 分页枚举固定 Manifest ID 集合 |
| `MetadataSnapshot::scan_commit_ids` | 分页枚举固定 Commit ID 集合 |
| `MetadataSnapshot::scan_references` | 按前缀分页枚举固定引用集合 |

Snapshot 同时实现 `MetadataReader`。创建后，后续 ref 更新或历史对象发布不能改变其枚举
结果和引用点查结果；Drop 时释放对应 read transaction 或内存视图。

JSON 后端先在共享 ref 锁内固定 HEAD 和 refs，再按 Commit、Tree、Manifest 顺序捕获 ID。
结合 Manifest -> Tree -> Commit -> ref 的发布顺序和不可变对象不删除规则，已捕获 ref 的
依赖不会遗漏。不可达对象不保证来自同一个物理时刻。

## 引用 CAS

| 操作或结果 | 语义 |
| --- | --- |
| `MetadataStore::compare_exchange_reference` | 当前目标等于 `expected` 时原子更新为 `new_target` |
| `ReferenceCas::Updated` | CAS 成功 |
| `ReferenceCas::Mismatch` | 当前值不匹配，`actual` 是锁内重新读取的值 |

参数含义：

- `expected = None`：期望引用不存在；
- `new_target = None`：删除引用；
- 两者均为 `None`：只在引用不存在时成功，不创建文件。

CAS 不提供无条件覆盖。冲突是正常结果，不是存储损坏。引用名必须位于 `refs/...`，不能
包含空组件、`.`、`..`、反斜杠、非法字符或保留临时文件前缀。

CAS 比较和写入前会对现值、`expected` 与 `new_target` 执行 `trim()`。后端只保证规范化后的
target 非空且不包含换行；目标格式和可达性由 Repository 与 fsck 校验。

## 生命周期操作

| 操作 | 语义 |
| --- | --- |
| `MetadataStore::kind` | 返回写入仓库配置的后端类型 |
| `MetadataStore::initialize` | 幂等创建空布局，不覆盖已有状态 |
| `MetadataStore::validate_layout` | 验证打开仓库所需的目录、文件类型和 HEAD 格式 |
| `MetadataStore::verify_integrity` | 执行该后端当前提供的结构完整性检查 |

`initialize` 不是 reset。`validate_layout` 只确认仓库能按当前后端打开，不等价于 fsck。

当前 JSON `verify_integrity` 检查布局、历史目录可枚举性、Tree 内容 ID 和路径顺序、Manifest
内容 ID、Commit 可解析性以及引用可枚举性。它不负责：

- 解析 Index JSON、检查 Index 排序或验证其 Manifest 关联；
- Commit 文件名与内容 ID 的一致性；
- Tree -> Manifest、Commit -> Tree/parent、ref -> Commit 的完整图；
- 引用目标的领域格式和可达性；
- Chunk payload。

因此该方法不能描述为完整仓库 fsck。

## 发布与原子性

一次 Commit 的可见性顺序必须是：

1. 通过 `ObjectStore` 发布 Chunk payload；
2. 完成 ObjectStore durability barrier；
3. 发布不可变 Manifest；
4. 发布不可变 Tree；
5. 发布不可变 Commit；
6. 以旧父 Commit 为 expected target 执行 ref CAS。

ref CAS 是新 Commit 对引用读者可见的最后线性化点。失败可以留下不可达元数据，但不能
覆盖并发提交，也不能让 ref 提前指向未完整发布的依赖。

Index 事务是独立原子边界。MetadataStore 不提供跨 Index、历史对象和 refs 的全局事务。

JSON 后端使用同目录临时文件、文件同步和原子 rename/no-clobber；Unix 上额外显式同步
目录，其他平台依赖系统 rename 语义。Index 事务使用排他 advisory lock；ref CAS 使用
排他 ref 锁；Snapshot 固定 refs 时使用共享 ref 锁。

持久写入可能在状态已经可见、结果返回前发生 I/O 错误。调用方遇到此类错误时应重新读取
Index 版本、ref 或不可变对象确认结果，不能假定错误必然代表零副作用。

rename 前退出可能留下 `.synapse-tmp-` 开头的普通临时文件。枚举会忽略这些文件，但同前缀
的目录、符号链接或其他异常项仍必须报错。

## 错误语义

下列情况返回错误：

- 非法 page size，或非法 Chunk cursor 格式/范围；
- 非法元数据 ID、引用名或引用前缀；
- Index expected version 不匹配；
- Manifest recipe、Tree 顺序或关联 Manifest 不一致；
- 同一内容 ID 已存在不同内容；
- JSON 损坏、布局异常、符号链接或非普通文件；
- 后端锁竞争和 I/O 失败。

对象不存在使用 `Option::None`。ref CAS 冲突使用 `ReferenceCas::Mismatch`，不转换成后端
损坏错误。

## JSON 后端

```text
metadata/
  HEAD
  index.json
  index.lock
  refs.lock
  refs/heads/[main]    # 首次 ref CAS 后才存在
  manifests/<id>.json
  trees/<id>.json
  commits/<id>.json
```

JSON 是功能后端，不是百 TB 性能后端：

- Index 和 Tree 会整份物化 `FileRecord`；
- Manifest 会整份物化 Chunk recipe；
- Index commit 重写完整 `index.json`；
- Tree Writer 收集完整 Tree；
- Snapshot 收集全部 ref 和历史对象 ID；
- 分页限制返回结果，但不能消除 JSON 后端的全量读取；
- 文件锁只提供单机进程间协调；
- 没有元数据 GC generation、条件删除或可恢复 scan cursor。

SQLite 后端必须保持上述可观察契约，并使用行式 Index/Manifest、短写事务、MVCC Snapshot、
数据库内 ref CAS 和真正有界的分页查询。
