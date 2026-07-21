# 本地元数据存储

NeoEngram format v5 只使用 SQLite 元数据后端。旧 JSON 后端、后端选择参数和旧仓库读取均已
删除；版本不匹配的仓库必须重新初始化。

## 边界

`MetadataStore` 管理：

- 可变的 `Index`、`HEAD` 和 `refs/...`；
- 不可变的 Manifest、Directory 和 Commit；
- Index transaction 与 HEAD/ref compare-exchange；
- 固定 SQLite read snapshot、名称点查和有界分页；
- Manifest/Directory 的 staging、规范 ID 计算和不可变发布。

Chunk payload 由 `ObjectStore` 管理。仓库锁、工作区、checkout/rm journal、完整文件缓存和
FUSE inode/cache 均不属于本模块。

`MetadataStore` 可跨线程共享；Reader、Snapshot、Index transaction 和 Directory writer
只在创建它们的线程使用。长期操作每次只持有一个有界 read transaction，FUSE 请求之间不
保留 SQLite transaction。

## 内容模型

| 对象 | 内容 | ID 语义 |
| --- | --- | --- |
| Chunk | 原始 payload | payload 的 BLAKE3，沿用原格式 |
| Manifest | `total_size + ordered chunks` | `neoengram-manifest-v3` 规范编码 |
| Directory | 按名称排序的直接子项 | `neoengram-directory-v1` 规范编码 |
| Commit | 根 Directory、父 Commit、消息、时间 | `neoengram-commit-v3` 规范编码 |

规范编码使用 domain 长度、domain、编码版本、LE 整数、长度前缀 UTF-8、原始 32-byte ID 和
固定枚举值。ID 不依赖 JSON、SQLite 行布局或 Rust 序列化细节。Directory 的递归文件数和逻辑
字节数是校验/查询统计，不参与 ID。

Manifest Chunk 必须从 offset 0 连续覆盖 `total_size`；空 Manifest 只能表示空文件。
Directory 只保存直接子项：

- File：`name + manifest_id + total_size`
- Directory：`name + directory_id`，`total_size` 固定为 0

Directory 子项名称必须是规范 UTF-8 单组件，按字节严格递增，ordinal 从 0 连续递增。

## SQLite 布局

核心表分为：

- 发布 staging：`manifest_sets`、`manifest_chunks`、`directory_sets`、`directory_entries`
- 不可变对象：`manifests`、`directories`、`commits`
- 可变状态：`index_state`、`index_files`、`head_state`、`refs`

Manifest Chunk 同时按 ordinal 和 offset 索引，`scan_chunks_from_offset` 可从覆盖请求 offset 的
Chunk 开始读取。Directory entry 同时保存 name 和 ordinal，分别支持点查与稳定 cookie 分页。

Manifest 使用单次 iterator 写入。Directory writer 以有界批次写 staging；`finish` 计算 ID、
校验递归统计并发布，不调用 `finish` 的 writer 在 Drop 时清理 staging。相同 ID/内容重复发布
成功，冲突内容或损坏引用失败。

## Reader 契约

`PageRequest` 的 limit 必须在 1..=4096；cursor 排他，只能交还产生它的 Reader/Snapshot。

- `ManifestReader::scan_chunks`：ordinal 分页
- `ManifestReader::scan_chunks_from_offset`：range reader 起点
- `DirectoryReader::get_entry`：直接子项名称点查
- `DirectoryReader::scan_entries`：持久 ordinal 分页
- `FileSetReader::scan_files`：扁平 Index 路径分页
- `MetadataSnapshot`：固定 Directory/Manifest/Commit/ref 枚举

Reader 不因一次点查枚举完整对象集合。调用方必须拒绝超大页、不前进 cursor、header 数量
不一致、缺失引用和不连续 recipe。

## Index 与 Commit

Index 只保存 `path/manifest_id/total_size/chunk_count`，不重复 Chunk Vec。Index transaction
使用 opaque `IndexVersion` 做 CAS；A -> B -> A 仍产生新版本。

Commit 在仓库写锁内分页读取有序 Index。实现使用深度优先目录栈：路径前缀结束时先发布子
Directory，再把其 ID 追加到父 writer；内存只保留当前深度、一个 Index 页和 writer 的有界
批次，不构造完整 Tree。

## 完整性与 GC

SQLite `integrity_check` 和元数据校验覆盖全部持久化行、规范 ID、引用类型和递归统计。fsck 与
GC 的 Chunk 可达性从当前 Index 以及所有 Commit root Directory 出发，Directory/Manifest 均
分页遍历。当前所有 Commit 都是 GC root；引入历史裁剪前必须先实现活动 mount lease/pin。

不可达的 staging 或发布失败元数据不会成为 Chunk root。元数据对象本身目前不删除。
