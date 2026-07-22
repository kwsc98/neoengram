# NeoEngram 技术参考

本文描述 NeoEngram 当前实现的状态模型、仓库策略、全部 CLI 命令、命令之间的区别、
一致性保证和已知边界。它以当前 format v7 代码为准，面向需要操作仓库、集成命令行或排查
故障的使用者。

源码模块职责见 [`code-architecture.md`](code-architecture.md)，磁盘布局、内容图和锁协议见
[`storage-architecture.md`](storage-architecture.md)，尚未实现的远端能力与路线见
[`implementation-plan.md`](implementation-plan.md)。

## 1. 当前能力边界

NeoEngram 当前是本地、内容寻址的版本控制系统，支持：

- FastCDC 或 WholeFile 文件描述、BLAKE3 对象去重和完整性校验；
- 独立工作区、index、单父 Commit、branch/detached HEAD；
- 事务化 checkout/rm、故障恢复、copy/hardlink 导出和只读 FUSE 挂载；
- SQLite 元数据、Loose ObjectStore、fsck 和本地对象 GC。

当前没有远端仓库、push/fetch/clone、服务端权限、Pack/S3 后端、merge/rebase/tag/reflog，
也不保存符号链接、POSIX mode、ACL、xattr 或 sparse 文件语义。普通文件内容是当前唯一进入
版本模型的工作区对象。

## 2. 状态模型

```text
工作区文件 -- add/add -A --> Index -- commit --> Commit/Directory/Manifest
    ^                          |                    |
    |---- restore ------------|                    |
    |-------- checkout ----------------------------|
                                                     \
                                                      +--> export / mount
```

| 名称 | 可变性 | 作用 |
| --- | --- | --- |
| Worktree（工作区） | 可变 | 用户实际读写的普通文件目录 |
| Index（暂存区） | 可变、每 Workspace 独立 | 下一次 Commit 的完整文件清单 |
| HEAD | 可变、每 Workspace 独立 | 附着到分支，或直接指向 detached Commit |
| Ref | 可变、仓库共享 | 例如 `refs/heads/main`，指向一个 Commit |
| Commit | 不可变 | 消息、时间、父 Commit 和根 Directory 的内容寻址记录 |
| Directory/Manifest | 不可变 | 目录 DAG，以及文件的策略、大小和有序 Chunk recipe |
| Chunk Object | 不可变约定 | 以原始字节 BLAKE3 为 ID 的 Loose payload |

`add` 只更新 Index，不创建 Commit；`commit` 只提交 Index，不自动扫描工作区。工作区、Index、
HEAD 三者可能处于不同状态，这也是 `status` 和 `diff` 分别比较多个层次的原因。

## 3. TARGET 解析

大多数接受 `TARGET` 的命令支持：

- `HEAD`：当前 Workspace 的 HEAD；
- `main` 等短名称：解析为 `refs/heads/<name>`；
- `refs/...`：完整引用名；
- 64 位小写十六进制 ID：完整 Commit ID，不支持缩写 ID。

`checkout` 到分支会让当前 Workspace 附着到该分支；checkout 到完整 Commit ID 会进入
detached HEAD。`show` 是例外：只接受 `HEAD` 或完整 Commit ID。`log` 没有 TARGET 参数，始终
从当前 Workspace 的 HEAD 沿单父链向前读取。

TARGET 在 `export` 和 `mount` 开始时只解析一次，因此之后分支移动不会改变已导出或已挂载
视图。

## 4. 仓库分块策略

初始化命令：

```bash
neoengram init [--chunking fastcdc|whole-file|mixed] [PATH]
```

| 仓库策略 | `add` 行为 | 适用场景 | 代价 |
| --- | --- | --- | --- |
| `fastcdc` | 所有文件固定 FastCDC | 大文件局部变化、跨版本块级去重 | 不能直接硬链接还原多 Chunk 文件 |
| `whole-file` | 所有文件固定 WholeFile | 同文件系统硬链接导出、整文件对象复用 | 任意字节变化都会产生完整新对象 |
| `mixed` | 可按次/按文件选择 | 同一仓库确实需要两种行为 | Commit 不保证可硬链接导出，运维规则更复杂 |

未指定时新仓库固定为 `fastcdc`。策略持久化在 `repository.json`，仓库创建后不可修改；对已有
仓库再次执行 `init` 只补全布局，显式指定不同策略会失败。

FastCDC 参数固定为 256 KiB / 1 MiB / 4 MiB（min/avg/max）。WholeFile 对非空文件生成一个
`offset=0,size=total_size` 的对象，空文件生成零对象。WholeFile 以两遍流式读取完成 Hash 和
对象发布；FastCDC 使用独占的稳定输入快照和 mmap 切块，避免把完整文件复制到普通堆内存。
策略属于 Manifest 内容，字节相同但策略不同的文件具有不同 Manifest ID。

固定策略仓库拒绝 `add --chunking`。只有 mixed 仓库支持：

```bash
neoengram add --chunking fastcdc PATH
neoengram add --chunking whole-file PATH
```

mixed 仓库未显式指定时，已跟踪文件继承原策略，新文件默认 FastCDC。显式切换策略属于可提交
变化，即使文件字节没有变化也会反映在 Index/Manifest 中。

## 5. 命令总览

| 命令 | 读取 | 修改 | 核心用途 |
| --- | --- | --- | --- |
| `init` | 仓库配置 | 仓库布局 | 创建 format v7 仓库 |
| `workspace create/list/remove` | Commit、注册表 | Workspace 注册和目录 | 管理共享对象库的可写工作区 |
| `add` | 工作区 | ObjectStore、Index | 暂存新增或修改 |
| `add -A` | 工作区 | ObjectStore、Index | 暂存新增、修改和删除 |
| `rm` | Index、工作区 | Index，默认也改工作区 | 安全移除已跟踪路径 |
| `status` | HEAD、Index、工作区 | 无 | 分类显示 staged/unstaged/untracked |
| `diff` | Commit、Index、工作区 | 无 | 文件和 Chunk 级比较 |
| `restore` | HEAD 或 Index | Index 或工作区 | 恢复指定路径，不切换 HEAD |
| `commit` | Index、HEAD | Commit、Directory、HEAD/ref | 固化暂存状态 |
| `log` / `show` | Commit 历史 | 无 | 查看历史或快照内容 |
| `checkout` | Commit | 工作区、Index、HEAD | 整体切换版本 |
| `export` | 固定 Commit | 新目标目录 | 生成 copy 快照或 hardlink 视图 |
| `mount` / `unmount` | 固定 Commit | 系统挂载状态 | 按需读取只读快照 |
| `recover` | 事务 journal | 工作区、Index、HEAD | 恢复中断的 checkout/rm |
| `fsck` | 整个本地仓库 | 无 | 完整性检查 |
| `gc` | 全部 roots 和对象 | ObjectStore | 回收无引用 Chunk |

## 6. 初始化与 Workspace

### `init`

```bash
neoengram init [--chunking POLICY] [PATH]
```

`PATH` 默认是当前目录，不存在时会创建。新仓库先在同一父目录的私有临时目录中完成初始化，
再以 no-replace rename 发布 `.neoengram`。已有完整仓库可以幂等重新初始化，但旧格式、半成品
仓库或策略冲突会明确失败。format v7 只使用 SQLite 元数据和 Loose 对象后端，不迁移 v6。

### `workspace create`

```bash
neoengram workspace create NAME [--from TARGET] [--path PATH] [--branch BRANCH]
```

`--from` 默认 `HEAD`。未指定 `--path` 时创建受管 `workspaces/NAME`；指定后可以把 Workspace
放在外部目录，并通过 repository/workspace ID 指针发现共享仓库。

每个 Workspace 拥有独立的 worktree、Index、HEAD、base Commit、锁和事务目录，但共享 Chunk、
Manifest、Directory、Commit 和 refs。省略 `--branch` 会创建 detached Workspace；指定后创建并
独占新分支。同一分支最多只能由一个可写 Workspace 占用。

### `workspace list` 与 `workspace remove`

```bash
neoengram workspace list
neoengram workspace remove NAME [--force]
```

`list` 显示名称、路径和 attached/detached 状态，`*` 标记当前 Workspace。`remove` 不能删除
默认 Workspace，也不能从目标 Workspace 自己内部删除它。默认要求目标没有 staged、unstaged
或 untracked 内容；`--force` 明确丢弃这些内容。删除先原子移动目录，再注销注册，失败时尽量
恢复，避免注册状态和目录状态静默分离。

## 7. 暂存与删除

### `add`

```bash
neoengram add [--chunking fastcdc|whole-file] [PATH]
neoengram add -A [--chunking fastcdc|whole-file] [PATH]
```

`PATH` 默认当前目录，可指向普通文件或目录。普通 `add` 暂存扫描到的新增和修改，不会因为文件
缺失而从 Index 删除。`add -A` 则让指定路径范围的 Index 与扫描结果一致，因此也暂存删除。
从子目录运行时，范围仍归一化为仓库相对路径；只有在仓库根对 `.` 使用 `-A` 才覆盖整个仓库。

导入会取得稳定输入快照，流式计算 Chunk 和 BLAKE3，并在发布 Index 前验证对象。相同对象只
保存一次。多个文件可以并行处理；最终 Index 通过扫描前读取的版本做 CAS，如果并发命令改变了
Index，当前 add 失败并要求重试，不覆盖对方结果。

根目录 `.neoengramignore` 同时影响 `add` 和 `status` 的未跟踪文件发现，支持注释、否定、目录
规则、`*`、`?` 和 `**`。它不会合并 `.gitignore`；已跟踪文件即使后来被忽略，仍会被 `add -A`
更新或删除。NeoEngram 内部目录、受管 mounts/exports/workspaces 和不满足跨平台路径协议的路径
不能加入 Index。

### `rm`

```bash
neoengram rm [--cached] [--force] PATH
```

默认同时从工作区和 Index 移除已跟踪路径，类似 `git rm`。`--cached` 只从 Index 移除，保留
工作区文件，使其随后成为未跟踪文件。两种模式默认都检查工作区内容是否与 Index 一致；
`--force` 表示允许丢弃冲突内容或已暂存但未提交的版本。

非 cached 删除不会直接 unlink：它先把文件原子移动进事务备份，再发布新 Index，最后清理。
进程崩溃后由 `recover` 根据 Index 位于 before/after 哪一侧决定回滚或完成。

### `add -A` 与 `rm` 的区别

| 场景 | 推荐命令 | 原因 |
| --- | --- | --- |
| 用户已经在外部删除文件，需要暂存删除 | `add -A PATH` | 扫描范围并同步 Index |
| 希望由 NeoEngram 删除已跟踪文件 | `rm PATH` | 有工作区事务备份和冲突保护 |
| 停止跟踪但保留本地文件 | `rm --cached PATH` | 只更新 Index |
| 只暂存仍存在文件，不记录缺失项 | `add PATH` | 普通 add 不暂存删除 |

## 8. 查看状态与差异

### `status`

```bash
neoengram status
```

输出当前 branch 或 detached HEAD，并分三组显示：

- staged：HEAD 到 Index 的 added/modified/deleted；
- unstaged：Index 到工作区的 modified/deleted；
- untracked：未被 Index 跟踪且未被 ignore 的普通文件。

`status` 按 Index 中保存的分块策略校验工作区文件。扫描结束前会复核 HEAD 和 Index 版本，避免
把两个并发时刻拼成一份报告。它只分类路径，不提供 Chunk 复用统计。

### `diff`

```bash
neoengram diff [--staged] [--stat] [TARGET [TARGET]]
```

| 调用方式 | 左侧 | 右侧 |
| --- | --- | --- |
| `diff` | Index | 工作区 |
| `diff --staged` | HEAD | Index |
| `diff TARGET` | 指定 Commit | 工作区 |
| `diff --staged TARGET` | 指定 Commit | Index |
| `diff LEFT RIGHT` | LEFT Commit | RIGHT Commit |

`--staged` 不能与两个 TARGET 同用。`--stat` 只输出汇总，否则按路径显示文件大小、Chunk 数、
可复用 Chunk、新增/移除 Chunk和预计新增对象字节。预计值不是实际磁盘增量，因为新 Chunk 可能
已被仓库中其他文件引用。未跟踪文件不进入 diff。

工作区比较不会发布对象。它按基准侧已保存策略重新描述文件，因此 WholeFile 不会因算法不一致
被误报；策略本身发生切换时，即使字节相同也属于变化。

### `log` 与 `show`

```bash
neoengram log [--max-count COUNT]
neoengram show [HEAD|COMMIT_ID]
```

`log` 从当前 HEAD 开始按新到旧遍历单父历史，`COUNT` 必须大于零。`show` 输出 Commit、根
Directory、父节点、时间、消息，以及完整文件清单中的大小、Chunk 数和分块策略。它们都是只读
命令；`show` 当前不直接接受分支短名。

## 9. 恢复、切换与提交

### `restore`

```bash
neoengram restore --staged PATH...
neoengram restore [--force] PATH...
```

`restore --staged` 从当前 HEAD 恢复指定范围的 Index，不修改工作区，用于取消暂存；如果路径是
HEAD 中不存在的新增项，则从 Index 删除。无 `--staged` 时从 Index 恢复工作区，只处理 Index
已经跟踪的文件。已有工作区文件与 Index 不一致时默认拒绝覆盖，必须显式 `--force`。

工作区恢复先在目标父目录写入、校验并同步临时文件，发布前再次检查目标。目标原本不存在时
使用 no-replace rename，避免覆盖命令执行期间由其他程序新建的文件。

### `commit`

```bash
neoengram commit -m MESSAGE
```

Commit 消息去除首尾空白后不能为空。命令读取 Index，验证所有 Manifest/Chunk 和仓库策略，
发布 Directory DAG 与 Commit，最后以 CAS 推进 attached branch 或 direct HEAD。提交只有一个
父节点；Index 与当前 Commit 完全一致时拒绝创建空提交。

detached HEAD 下提交只推进 direct HEAD，不移动 `main`。不可变元数据可能在最后 CAS 失败前
已经发布，但不会出现引用指向缺失依赖；这些不可达记录不会破坏已发布历史。

### `checkout`

```bash
neoengram checkout [TARGET] [--force]
```

TARGET 默认 `HEAD`。命令把目标 Commit 恢复到整个工作区和 Index，并更新 HEAD。默认拒绝：

- Index 有尚未提交的变化；
- 已跟踪文件被修改、删除或变成异常类型；
- 未跟踪文件会被目标文件覆盖。

`--force` 明确允许丢弃 staged/tracked 冲突，并处理文件与目录转换；父级符号链接和无法证明
安全的特殊文件仍会失败。Checkout 只组装实际需要写入的文件，逐 Chunk 校验，优先 Reflink
完整文件缓存，不支持 Reflink 时回退普通复制。

工作区修改前会持久化 journal，之后按工作区、Index、HEAD 的顺序发布。中断后其他写命令会
要求先运行 `recover`。

### `restore`、`checkout` 与 `commit` 的区别

| 命令 | 范围 | 改工作区 | 改 Index | 改 HEAD/历史 |
| --- | --- | --- | --- | --- |
| `restore --staged PATH` | 指定路径 | 否 | 是，从 HEAD 恢复 | 否 |
| `restore PATH` | 指定路径 | 是，从 Index 恢复 | 否 | 否 |
| `checkout TARGET` | 整个快照 | 是 | 是 | 是，切换 HEAD |
| `commit` | 整个 Index | 否 | 否 | 是，创建并推进 Commit |

## 10. 只读视图

### `export --mode copy`

```bash
neoengram export [--mode copy] TARGET DIR
```

默认 copy 模式把固定 Commit 完整复制为一个新目录，不改变当前工作区、Index 或 HEAD。目标
父目录必须已经存在、不能经过符号链接，DIR 必须尚不存在。目标通常位于 Workspace 外，也允许
仓库受管的 `exports/<name>`。

命令在 DIR 同父目录创建 staging，逐对象验证大小和 BLAKE3，文件和目录全部完成后才以
no-replace rename 原子发布。失败不会留下半成品。Unix/macOS 上文件设为 `0444`、目录设为
`0555`；这是权限约定，文件所有者或 root 仍可改回，不是强安全边界。copy 输出与对象 inode
不同，之后修改输出不会污染仓库对象。

### `export --mode hardlink`

```bash
neoengram export --mode hardlink TARGET DIR
```

hardlink 使用与 copy 相同的目录校验和原子发布，但每个非空文件直接链接到对应 WholeFile
Loose Object。空文件正常创建。它严格要求：

- Commit 中每个文件都是合法 WholeFile Manifest；
- ObjectStore 支持硬链接，当前只有 Loose ObjectStore；
- 对象目录和 DIR 位于同一文件系统；
- 链接前对象大小、BLAKE3、设备号和 inode 校验全部通过。

任一文件不满足条件会让整个导出失败，不会混合复制或静默回退。whole-file 仓库天然保证策略
条件；mixed 仓库只有目标 Commit 全部为 WholeFile 时才能成功；fastcdc 仓库不能使用。

非空输出文件与仓库对象共享 inode 和权限。清除输出写位也会改变对象权限，而所有者仍能改回
并写入；一旦写入，所有引用该对象的 Commit 都会读到损坏内容。因此它是可信本地环境中的
“hard-linked read-only view”，不是独立不可变快照。

### `mount` 与 `unmount`

```bash
neoengram mount TARGET MOUNTPOINT [--cache-size-mib MIB]
neoengram unmount MOUNTPOINT
```

二进制必须以 `fuse-mount` feature 构建。Linux 需要内核 FUSE3 和 `fusermount3`，macOS 需要
macFUSE；其他平台返回 unsupported。MOUNTPOINT 必须已存在、为空、无符号链接祖先，并与仓库
存储及全部 Workspace 互不包含；受管 `mounts/<name>` 例外地允许位于容器目录中。

`mount` 前台运行并将 TARGET 固定为一个 Commit。文件系统由内核强制只读：写入、删除、rename、
truncate、chmod/chown、link/symlink 返回 `EROFS`，xattr 不支持。读取按 Manifest offset 定位
Chunk，完整校验后进入按字节计费的 single-flight LRU，默认 512 MiB；损坏或缺失返回 `EIO`。
WholeFile 对象大于缓存上限时会在分配前失败，需要增大缓存或改用同文件系统 hardlink 导出。

`Ctrl-C`/`SIGTERM` 会正常结束前台挂载。独立执行 `unmount` 时会先核对系统 mount table 中的
NeoEngram 标识，拒绝卸载其他文件系统。

### copy、hardlink 与 FUSE 的区别

| 特性 | copy export | hardlink export | FUSE mount |
| --- | --- | --- | --- |
| 是否复制 payload | 是 | 否 | 否，按需读取 |
| 是否要求 WholeFile | 否 | 是，整个 Commit | 否 |
| 是否要求同一文件系统 | 否 | 是 | 否 |
| 是否共享对象 inode | 否 | 是 | 否 |
| 只读强度 | Unix 权限位 | 权限位且会联动对象 | 内核只读文件系统 |
| 对象被修改的风险 | 输出修改不影响对象 | 输出修改直接污染对象 | 挂载拒绝写入 |
| 生命周期 | 普通目录，持续存在 | 普通目录，持续存在 | 需要挂载进程/session |
| 大 WholeFile 内存要求 | 流式复制 | 无完整文件缓存 | 单对象不能超过 Chunk cache |

## 11. 故障恢复与维护

### `recover`

```bash
neoengram recover [--abort]
```

只处理当前 Workspace 的正式 checkout/rm journal。没有未完成事务时安全返回。

- checkout + `recover`：先回滚到可验证起点，再以 force 语义重放原目标；冲突的本地修改可能被丢弃；
- checkout + `recover --abort`：尝试恢复事务开始前的工作区、Index 和 HEAD；
- rm 尚未提交 Index：无论是否 `--abort` 都恢复备份并回到开始前；
- rm 已提交 Index：`recover` 完成清理，`recover --abort` 拒绝逆向回滚。

恢复只删除或替换能够用 journal 和 FileNode 证明身份的事务产物。用户在崩溃后创建或改写的未知
内容、原文件和备份同时缺失等歧义会导致恢复停止并保留事务，等待人工处理。

### `fsck`

```bash
neoengram fsck
```

fsck 是只读完整性检查。它验证 SQLite、全部 refs/HEAD、所有 Commit 和单父历史无环、Directory
DAG、Manifest recipe、仓库分块策略、路径规则、引用大小，以及对象库中每个 Loose Object 的
实际大小和 BLAKE3。检查包括所有 Workspace Index、可达和不可达但仍保留的元数据/对象。

发现缺失或损坏会明确失败并给出对象/引用上下文，不会跳过、覆盖或自动修复。

### `gc`

```bash
neoengram gc --dry-run
neoengram gc
```

GC 从全部 Workspace Index 和全部已保存 Commit 标记 Chunk，再删除未标记的 Loose Object。
`--dry-run` 只报告对象数和可回收字节。当前所有 Commit 都是 root，因此 detached 或未被分支
引用但仍保存的历史对象不会被回收；Manifest、Directory、Commit 等不可变元数据也不会删除。

删除前会重新验证对象。GC 与 add/commit/fsck 通过 object/write lock 协调，不会把正在发布的
对象误判为垃圾。

### `fsck` 与 `gc` 的区别

| 命令 | 目的 | 是否修改仓库 | 是否修复损坏 |
| --- | --- | --- | --- |
| `fsck` | 验证引用图、元数据和所有对象 | 否 | 否 |
| `gc --dry-run` | 估算无引用 Chunk | 否 | 否 |
| `gc` | 删除经过验证的无引用 Chunk | 是 | 否 |

硬链接视图被改写后，应先停止使用并删除/废弃相关视图，从远端、备份或原始数据取得可信字节，
重新导入后运行 `fsck`。不能把已经损坏的硬链接内容当作恢复源。

## 12. 并发、原子性与损坏处理

仓库锁顺序固定为：

```text
objects.lock -> Workspace worktree.lock -> write.lock
```

锁冲突立即失败并要求重试，不无限等待。`add/status/diff` 共享工作区锁；checkout/rm/工作区
restore/recover 独占工作区锁；commit 和 `restore --staged` 只修改元数据；gc/fsck/export 还会
协调对象集合。

不可变数据先发布，HEAD/ref 最后 CAS。工作区文件使用同父目录临时文件、fsync 和原子 rename；
可能覆盖用户路径的地方会在最终发布点重新检查。进程崩溃会由 OS 释放 advisory lock，磁盘上的
lock 文件不是永久锁；checkout/rm 的 journal 才是需要 `recover` 处理的持久状态。

对象每次关键读取都会核对期望大小和 BLAKE3，包括对象复用、commit 发布、checkout、restore、
export、FUSE cache miss、fsck 和 GC 删除前验证。损坏会 fail closed，不会用工作区或硬链接内容
静默覆盖对象。当前没有自动修复 API。

## 13. 存储格式与升级

当前版本组合为：

| 层 | 版本 |
| --- | --- |
| Repository format | v7 |
| Manifest | v4 |
| Index | v4 |
| SQLite schema | v5 |

`repository.json` 保存 format、repository ID、ObjectStore 类型和不可变分块策略。SQLite 保存
Workspace-scoped Index/HEAD、refs、Manifest、Directory 和 Commit；`.neoengram/objects` 保存
Loose Chunk payload；工作区恢复 journal 和物化缓存不属于上述两个存储接口。

项目仍处开发期，格式升级可以拒绝旧仓库，不提供自动迁移。升级二进制前必须保留经过验证、
不与仓库对象共享 inode 的独立备份；hardlink export 不能作为这种备份。

## 14. 常用工作流

FastCDC 常规版本控制：

```bash
neoengram init --chunking fastcdc .
neoengram add -A .
neoengram status
neoengram diff --staged
neoengram commit -m "initial snapshot"
```

WholeFile 硬链接视图：

```bash
neoengram init --chunking whole-file .
neoengram add -A .
neoengram commit -m "model snapshot"
neoengram export --mode hardlink HEAD ../model-view
neoengram fsck
```

取消暂存和恢复工作区：

```bash
neoengram restore --staged path/to/file
neoengram restore --force path/to/file
```

创建隔离实验 Workspace：

```bash
neoengram workspace create experiment --from main --branch experiment/data
neoengram workspace list
```

维护检查：

```bash
neoengram fsck
neoengram gc --dry-run
neoengram gc
```
