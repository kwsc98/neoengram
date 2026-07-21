# format v6 存储架构

NeoEngram 当前仓库格式是 v6。它允许破坏性开发期升级，不读取或迁移旧格式。目标规模要求
分页、Directory 构造和 FUSE namespace 的内存由页大小、目录深度、活动句柄和配置缓存决定。

## 内容图

```text
refs / Workspace HEAD/base ──> Commit ──> root Directory
                            ├── File ──> Manifest ──> ordered Chunk payloads
                            └── Directory ──> child Directory

Workspace Index ──> FileRecord(path, manifest_id, total_size, chunk_count)
```

Chunk ID 仍是原始 payload BLAKE3。Manifest、Directory 和 Commit 使用带 domain/version 的规范
二进制编码；整数是 LE，字符串带 u64 长度，引用是原始 32-byte ID。Directory 只包含直接子项，
递归统计不参与 ID。

## SQLite MetadataStore

format v6 只有 SQLite 后端。`MetadataStore` 提供：

- Manifest ordinal/offset 分页与 range 起点查询；
- Directory 名称点查和持久 ordinal 分页；
- Workspace-scoped Index keyset 分页与 expected-version transaction；
- 固定 metadata snapshot；
- HEAD/ref CAS；
- Manifest iterator 和 Directory staging writer 的不可变发布。

Directory writer 的 staging 批次受条数和字节上限约束。Commit 分页读取 Index，维护当前目录
路径栈；结束一个目录时发布其 ID，再追加到父 writer。整个过程不创建扁平 Tree。

SQLite 使用 WAL、foreign key、`synchronous=FULL` 和即时写事务。Reader 不跨 FUSE 请求长期
持有 transaction。逐项契约见
[`local/metadata/README.md`](../crates/neoengram/src/local/metadata/README.md)。

## ObjectStore

`LooseObjectStore` 通过 `put_from` 流式校验/不可变发布 Chunk，通过 `copy_to` 单遍复算大小与
BLAKE3。元数据引用对象前必须完成 durability barrier。GC 在 object lock 和 state lock 下从
全部 Workspace Index 与全部 Commit roots 标记 Chunk，再删除未标记对象。

当前所有 Commit 都是 GC root。引入历史裁剪前，必须为活动 FUSE mount 增加 lease/pin，不能
回收已挂载 Commit 的图。

## 一致性和锁

锁顺序固定为 `objects.lock -> Workspace worktree.lock -> write.lock`。Index transaction 与 HEAD/ref CAS
提供线性化点；Commit 先发布 Chunk/Manifest/Directory/Commit，最后 CAS 更新 HEAD/ref。失败
可以留下不可达不可变元数据，但不能发布缺失依赖的引用。

checkout/rm 的 journal 与完整文件缓存不属于 MetadataStore/ObjectStore。它们继续使用同父目录
原子 rename、fsync 和可恢复事务。

## FUSE 读取路径

挂载时 TARGET 只解析一次，session 保存 Commit 时间和根 Directory ID。请求流程为：

```text
lookup(name) -> DirectoryReader::get_entry
readdir(cookie) -> DirectoryReader::scan_entries(after ordinal)
read(offset,size) -> Manifest range query -> verified Chunk LRU -> reply slice
```

root inode 为 1；其他 inode 从 Commit ID、逻辑路径、kind 和 salt 派生 63-bit BLAKE3。活动表
检测碰撞，lookup/forget/open/release 控制生命周期。readdir 不递归扫描，不激活全部子项。

Chunk cache 按实际字节计费、线程安全、single-flight，默认 512 MiB。读取缺失、大小冲突、Hash
损坏和 recipe 空洞都映射为 `EIO`。不可变文件使用 `FOPEN_KEEP_CACHE` 支持内核页缓存和 mmap。

## 平台生命周期

- Linux：`fuser` pure Rust FUSE3，运行时使用 `fusermount3 -u`。
- macOS：`fuser` + macFUSE SDK/runtime，使用 `/sbin/umount`。
- Windows：常规仓库命令可构建，挂载命令返回 unsupported/未启用。

挂载点必须已存在、为空、没有符号链接祖先，并与仓库互不包含。filesystem 设置 `ro`、
`nodev`、`nosuid`、`noexec`、owner-only 和 `neoengram` subtype/source。显式卸载先检查系统 mount
table，拒绝非 NeoEngram 文件系统。Ctrl-C/SIGTERM 通过 `SessionUnmounter` 正常结束 session。

## 剩余规模热点

FUSE、Commit Directory 构造和 Manifest range read 已有界；部分 `status/diff/show/checkout/rm`
仍会在上层收集扁平结果，GC 的可达 Chunk map 也仍与唯一 Chunk 数线性相关。百万文件基准需在
Linux/macOS 实挂环境记录启动时间、RSS、目录分页、冷热随机读和顺序吞吐，并据此继续收敛。
