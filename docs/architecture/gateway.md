# NeoEngram Gateway 架构

> 决策状态：**已确认的目标架构；G1 控制面正在实施，尚不可上线**。
>
> 生效日期：2026-08-09。
>
> 最后更新：2026-09-01。
>
> 本文是 Gateway 拓扑、资源、连接方向、安全、可用性、跨集群传输和 S3 暴露方式的专项权威文档。
> 当前代码已包含 Gateway 协议、Gateway Registry、管理 API、三 listener、有界 H2 tunnel、Central outbound
> connector、Agent RouteLease 原子路径和最多一跳 Replica peer forwarding。Replica activation 的
> challenge/proof、证书 prepare/commit、Gateway/Agent/Central 的 mTLS 身份校验，以及 Central 下行
> Ed25519 command signing/trust bundle 已接入；Central control session 会下发并按 heartbeat 刷新当前
> Pool 的 Replica peer credential directory（叶子证书 fingerprint、credential generation、30 秒 TTL），
> Gateway 在 peer 请求上同时校验 mTLS URI SAN 和该目录，control 断开立即清空目录并 fail-closed；loopback
> 双 Replica listener/H2/peer 协议 harness 已通过，但尚未组成包含真实 Central Registry 原子租约、durable
> outbox、命令签名和完整业务回路的 E2E。P2P 对象物化 v2 已接入 `neoengram-transfer-v2` ALPN、Ticket/
> generation fence、分页 manifest、Agent source/target stream、Gateway relay、目标 staging/checkpoint/receipt
> 和动态 capability gate；现有单元/协议 harness 尚不能替代真实跨 Gateway、三 Agent、失败恢复和生产凭据验收。
> 外部生产
> `WorkloadCertificateIssuer`/KMS-HSM 适配、完整双 Replica E2E、真实集群 readiness/failover 和维护窗口
> 切换验收仍未完成。
> 任何文档或原型与本文冲突时，目标架构以本文为准。当前能力和实施顺序见
> [`../roadmap.md`](../roadmap.md)；本文是 Gateway 专项唯一事实来源。

## 1. 决策摘要

NeoEngram 从“Central 直接承载 Agent listener”迁移为“Central 和 Agent 都连接所属 EdgeCluster 的
GatewayPool”。一个 EdgeCluster 对应一个逻辑 `GatewayPool`，Pool 由多个 `GatewayReplica` 提供服务。

```text
                         Central
                    /       |       \
                   / H2+mTLS|        \
                  v         v         v
       GatewayPool A     GatewayPool B     GatewayPool C
       [Replica x N]     [Replica x N]     [Replica x N]
             ^                 ^                 ^
             | H2+mTLS         |                 |
         Agent A[*]        Agent B[*]        Agent C[*]
             |                 |                 |
         Volume A[*]       Volume B[*]       Volume C[*]
```

固定结论：

- Central 是全局逻辑权威；Gateway 不拥有 Tenant、Artifact、Commit、Index、Placement、Job、Lease 或
  Ticket 的最终决定权；
- 每个 EdgeCluster 有一个逻辑 GatewayPool，生产默认至少两个 Replica，开发环境允许单 Replica；
- Central 主动连接持久化登记的 GatewayReplica；Agent 只主动连接本集群 GatewayPool；
- Agent 不再直接连接 Central，也不接受 Central、用户或其他 Agent 的入站连接；
- Gateway 首版不挂载任何 StorageVolume，所有 Volume I/O 都由 Volume Owner Agent 执行；
- Gateway 只路由控制帧和后续的数据流，不保存对象 payload 或权威 metadata；
- Gateway 多副本只提升网络入口可用性，不表示 Chunk、Volume 或 metadata 多副本；
- Central 下行命令和 Agent 上行报告保留端到端签名，Gateway 不能修改已签名 payload；
- Gateway 控制面与固定 Ready Snapshot 的一期只读 S3 数据面已实现；跨集群 v1 replication 控制记录仍为
  legacy，v2 Materialization 的 ticket/fence/manifest 边界已有代码，payload 执行与生产验收仍是独立里程碑；
- 迁移采用维护窗口一次性切换，不保留 Agent 直连 Central 的双栈或回退协议；
- Fusen 不属于 Gateway 架构依赖或传输决策。

## 2. 当前实现与目标状态

| 主题           | 当前实现（G1 迁移中）                                                                                                                                                                                                                                                                                                                                                                  | 目标架构                                            |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| Agent 控制连接 | Agent 配置只接受 GatewayPool endpoint/trust bundle；Gateway H2 channel、下行 command trust 校验和 Agent 转发已接入，生产凭据由部署侧提供                                                                                                                                                                                                                                               | Agent 主动连接本集群 GatewayPool                    |
| Central 连接   | runtime 按 Registry endpoint 启动 outbound connector；连接身份、证书 generation、RouteLease fencing 和 mTLS 校验已接入                                                                                                                                                                                                                                 | Central 主动连接已登记 GatewayReplica               |
| Gateway        | 三 listener、限额/健康检查、有界 H2 tunnel、activation challenge/proof、运行时 mTLS 和一跳 peer forwarding 已实现；`desired_replicas`/`minimum_ready_replicas` 已持久化为声明态配置，但尚未驱动副本编排或 Pool 级 observed readiness；loopback listener/H2/peer 协议 harness 已通过，完整 Central 业务 E2E、外部生产 issuer/KMS-HSM、真实集群 readiness/failover 和 cutover 验收待完成 | 每 EdgeCluster 一个多副本 GatewayPool               |
| Agent 路由     | Gateway Registry 已持久化 RouteLease，Central 原子 session+lease grant 和 Agent stream/unary 转发代码已接入；InMemory/SQLite 共用的双 Replica 契约已覆盖活跃租约拒绝抢占、过期接管和旧 owner fencing，但尚无把真实 Registry、双 Replica transport、outbox 和签名串起来的故障恢复 E2E                                                                                                        | Central 权威 `AgentRouteLease` + Replica forwarding |
| 传输安全       | Agent trust bundle、Central command signing、activation 防重放、Gateway/Agent mTLS/SAN/EKU、过期 credential CAS fencing、RouteLease owner 复核、Gateway 入站对端证书 notAfter deadline、Agent 半程续签安装和重连，以及 Gateway server leaf 到期驱动 Agent/Central 既有 H2 断开已实现；外部生产 issuer/KMS-HSM adapter、Gateway 证书交付切换和真实轮换演练待完成                        | H2 + mTLS，另保留端到端 Ed25519 签名                |
| Volume 访问    | Agent 挂载并读写所属 Volume                                                                                                                                                                                                                                                                                                                                                            | 仍只有 Volume Owner Agent 挂载和访问 Volume         |
| 跨集群 payload | v1 replication/route/ticket 仍为 legacy；v2 `MaterializationJob` 的 Ticket、分页 manifest、generation fence、Agent source/target stream 和 Gateway relay 已接入，目标 staging/checkpoint/receipt 也有执行路径；真实 route 编排、跨节点 payload、限流/背压和多 Agent E2E 待验收                                                                                                                                                                                                                                                                                                                                                                                 | Agent A -> Gateway A -> Gateway B -> Agent B        |
| S3             | Ready Snapshot Access Point、SigV4/预签名、ListObjectsV2、HEAD/GET/Range、Agent 二进制流和 Web 对象浏览已实现；公网 DNS/TLS、生产 CA、真实双 Replica 故障演练仍由部署验收完成                                                                                                                                                                                                              | 固定 Ready Snapshot 的只读 S3 Access Point          |

“目标架构”不能在对应代码、契约测试和部署验收完成前标记为当前能力。当前仓库处于不提供旧 endpoint
fallback、控制面代码已闭环但尚未完成生产切换验收的迁移中状态；旧 Agent listener 已从生产代码、部署和
测试入口删除，历史说明只保留在变更记录中。

## 3. 权威边界

### 3.1 Central

Central 继续权威管理：

- Tenant、Project、Artifact、Commit、Directory、Manifest、IndexVersion 和 Ref；
- EdgeCluster、StorageVolume、ArtifactPlacement、AgentInstance 和 Volume Owner generation；
- GatewayPool、GatewayReplica、AgentRouteLease 和工作负载证书状态；
- Job、Assignment、PlaygroundLease、fencing token、Decision 和审计事件；
- v1 `TransferRoute`/`TransferTicket`/`TransferSession` legacy 控制记录，以及 v2
  `MaterializationJob`/`MaterializationBatch`/batch Ticket/Lease 的控制记录；当前只读数据面的 S3AccessPoint、
  S3Credential 和策略 generation；payload 是否可执行由 route/keyring/capability 决定。

Central 持久化 metadata 和 placement evidence，但不接收、不代理、不保存 Chunk payload。当前 SQLite
仍只支持单进程 Central；Central 多副本和生产 HA 以 PostgreSQL adapter 为前置条件。

### 3.2 Gateway

Gateway 是区域网络与协议边界，负责：

- 终止 Agent、Central 和 Replica peer 的 mTLS 连接；
- 校验对端工作负载身份、帧版本、大小、deadline 和路由 scope；
- 把 Agent enrollment、heartbeat、report、metadata 和 Index 请求转发给 Central；
- 把 Central Assignment、Decision、drain 和控制命令投递到 Agent owner Replica；
- 维护可丢弃的连接目录、限流计数和短期转发状态；
- 按 v2 batch Ticket 流式转发跨集群对象；`ConnectedTransferRelay` 只转发有界 frame，保留 route/keyring/
  capability 和 generation fence；旧 v1 Ticket 在生产 listener 上拒绝。Relay 不持久化 payload，生产端仍需
  配置真实 upstream route 与凭据；
- 为固定 Ready Snapshot 提供只读 S3 协议入口和 Web 静态资源；

Gateway 不得：

- 成为 Tenant、Artifact、Commit、Index、Job、Placement、Lease 或 Ticket 的权威；
- 挂载 NFS/PVC/StorageVolume，或读取 Playground、journal、Agent state database；
- 把对象、metadata batch 或 S3 响应持久化为可恢复的业务副本；
- 自行调度 Agent、签发 Ticket、提升 route generation 或抢占 Agent owner；
- 修改、重新签名或降级绕过 Central/Agent 端到端签名；
- 枚举其他 Tenant、Artifact、Agent 或内部 CAS namespace。

### 3.3 Agent 与 StorageVolume

Agent 仍是唯一 Volume 执行者。一个 AgentInstance 在当前部署剖面只绑定一个 Tenant 和一个
StorageVolume，并以 Volume Owner generation、Assignment generation 和 fencing token 约束写入。
Gateway 的引入不改变 Volume-local CAS 权威：

```text
Central metadata authority
        |
        | signed assignment through Gateway
        v
Volume Owner Agent -> durability barrier -> Volume-local CAS
        |
        | signed placement evidence through Gateway
        v
Central ObjectCatalog
```

业务 Pod 继续通过本集群 NFS/CSI 直接访问精确 Playground/SnapshotDelivery 目录，不经过 Gateway 或 Central。

## 4. Central 资源模型

现有未使用的通用 `GatewayId` 不再作为新资源身份；协议拆为不可互换的 `GatewayPoolId` 和
`GatewayReplicaId`，避免把逻辑入口和进程实例混用。

### 4.1 GatewayPool

`GatewayPool` 是一个 EdgeCluster 的逻辑入口，至少包含：

- `gateway_pool_id`、`edge_cluster_id` 和显示名；
- Agent 逻辑入口与预留的 S3 逻辑入口；
- `desired_replicas`、`minimum_ready_replicas`；
- `Provisioning | Ready | Draining | Disabled` 状态；
- 配置 generation、创建/更新时间和审计主体。

一个 EdgeCluster 只能有一个未删除的 GatewayPool。Pool endpoint 是发现入口，不表示某个 Replica 的
连接所有权。当前 Gateway 证书轮换交付/切换尚未完成，因此 Agent 入口和可选 S3 入口一旦创建即
不可变；它们的主机名会写入每个已激活 Replica 的 DNS/IP SAN。需要更换入口时必须等待后续轮换
里程碑（或创建新的 Pool/Replica），不能直接通过管理更新覆盖 endpoint。

G1 必须区分声明态和观测态：`desired_replicas` 是外部 provisioner/controller 应当收敛的副本目标，
`minimum_ready_replicas` 是目标可用门槛；两者目前只由 Registry 做取值一致性校验并持久化，Central
不会据此自动创建、删除或 drain Replica。当前 `GatewayPool.state` 是 Central 管理的生命周期/准入门
（`Ready` 才允许 connector 建立业务控制 session），不是按活动连接或 heartbeat 自动计算的健康状态。
connector 只发现 `Ready` Pool 中 `Active` 且 credential 有效的 Replica；Gateway `/health/ready` 也
只表示单个 Replica 有活动的 Central control session。因此在 G1 中，Pool 处于 `Ready` 不保证已经
满足 `minimum_ready_replicas`，字段也不能直接作为 HA/SLO 或故障切换完成的证明。

后续 observed-readiness 实现必须单独持久化或计算 `ready_replica_count`/conditions，定义 heartbeat
新鲜度、证书和 drain 的贡献，并在副本数低于门槛时撤销业务准入。实现不得把 Pool 级计数检查直接塞进
当前 `Ready` 前置的 connector 流程而造成激活启动死锁；应保留独立的 bootstrap/control-plane 收敛
路径。G1 上线前置是外部 provisioner 与真实双 Replica readiness/failover 演练提供这层证据。

### 4.2 GatewayReplica

`GatewayReplica` 表示一个可独立连接和 drain 的进程实例，至少包含：

- `gateway_replica_id`、`gateway_pool_id`、`edge_cluster_id`；
- Central control、Replica peer 和 bootstrap endpoint；
- capability、软件版本、协议版本和最近 heartbeat；
- certificate generation、credential status；
- `Pending | Active | Draining | Revoked` 状态。

Replica endpoint 由 Central 持久化管理。Central 只能连接 `Active` 且身份与 Pool/Cluster 一致的
Replica，不读取静态 endpoint 列表作为权威配置。证书 prepare 开始后，control、peer 和
bootstrap endpoint 在 Registry 层固化；在证书轮换交付/切换协议完成前，任何底层替换也必须
拒绝，避免 endpoint 与证书 SAN 分离。轮换完成后应以新的 generation 和完整的交付协议一次性
切换，而不是原地修改 endpoint 字段。

创建 Replica 时 Central 返回一次性 activation token，只持久化 token 摘要、有效期和使用状态。
激活成功或过期后 token 不可重放。

### 4.3 AgentRouteLease

`AgentRouteLease` 表示某个 Agent 当前控制连接的唯一 owner，至少包含：

- `agent_id`、`gateway_pool_id`、`gateway_replica_id`；
- `connection_id`；
- `session_generation` 和 `route_generation`；
- `lease_expires_at` 和最近续租时间。

默认租约为 30 秒，owner 每 10 秒续租。获取和续租由 Central 原子判定：

- 同一 Agent 同一时刻最多一个活动 RouteLease；
- 新 generation 成功后立即 fencing 旧连接；
- Replica 断连不会立刻把另一个连接当作 owner，必须由 Central 确认旧租约失效或显式撤销；
- Gateway 本地目录只用于快速转发，Central 记录才是 owner 判定依据；
- Central 不可达时 Gateway 可以维持已建立连接的传输层存活，但不得延长权威租约或接收新业务命令。

### 4.4 Repository 与管理动作

Central 增加 `GatewayRegistryRepository`，InMemory 和 SQLite 必须运行同一行为契约。GatewayPool、
GatewayReplica、activation/certificate 记录和 AgentRouteLease 写入现有 Agent Registry 数据库；该库从
  authority 使用单一 clean-slate schema identity `application_id = 0x4e454155`、`user_version = 18`；
  已知的合并 authority v13-v17 显式迁移到 v18，未知 schema 与旧的拆分数据库布局直接拒绝，不能留下
  半初始化 schema。

管理面至少提供 GatewayPool create/get/list/update/drain，以及 GatewayReplica create/list/drain/revoke。
所有 mutation 使用稳定 request identity、expected resource version 和审计主体；重复 create 返回同一
结果，冲突更新不能覆盖新状态。Replica create 响应只展示一次 activation token，后续 get/list 永不
返回明文 token。

## 5. 连接、协议与路由

### 5.1 连接方向

```text
Central --主动 H2+mTLS--> GatewayReplica control endpoint
Agent   --主动 H2+mTLS--> local GatewayPool edge endpoint
Replica --按需 H2+mTLS--> same-pool/remote-pool peer endpoint
```

Agent 连接 Pool 服务地址，负载均衡到任意 Ready Replica。Agent 只保持一个活动 Gateway 连接；断线后以
有界退避重新解析 Pool 地址并连接，不同时维持多个可投递 session。

Agent 控制 channel 的 EOF、读取错误、写入关闭或写入超时都只结束当前 channel，并进入上述重连循环；
已持久化但尚未收到 ACK 的报告继续留在 Agent outbox，重连后按原 message identity 重放。Gateway 控制
连接断开时立即调度旧 RouteLease 释放；单次 Authority 清理 RPC 最多等待 1 秒，整次替换最多等待 3 秒。
如果 Authority 或清理任务卡住，仍允许新 session 建立，旧 route 由 generation fencing 和最长 30 秒的 lease
TTL 兜底回收。Agent 通过 Gateway 建立新 session 时，如果旧 RouteLease 仍在清理窗口内，Central 返回可
重试的 `503 GATEWAY_ROUTE_UNAVAILABLE`（带 1 秒重试提示），而不是把暂时不可达误报为 bootstrap 拒绝。
身份、协议、签名和未知错误仍保持失败关闭；明确的旧 session/route fencing 只关闭当前 channel，Agent 随后
以有界退避重新打开 session 并重新绑定当前 route。

Central 可以与 Pool 内多个 Replica 建立连接。控制命令先按 Central 的 AgentRouteLease 选择 owner
Replica；如果 Central 当前连接的是非 owner Replica，则该 Replica 最多转发一跳到 owner。禁止广播、
多跳转发和非 owner 自行抢占。owner 不可达时返回 `GATEWAY_ROUTE_UNAVAILABLE`，并触发 Central 刷新
Lease/目录；不能把请求悄悄投递给另一条旧 Agent 连接。

### 5.2 版本化 H2 帧

首版使用 HTTP/2 长连接和有界双向帧，至少覆盖：

- Replica hello、heartbeat、capability 和 drain；
- Agent bootstrap/status 请求转发；
- Agent session hello、heartbeat、report、metadata 和 Index action；
- Central Assignment、Decision 和 session control；
- RouteLease acquire、renew、release 和 fencing 通知；
- forwarding request/response、deadline、backpressure 和 protocol error。

每帧必须携带协议版本、request/trace ID、源/目标工作负载身份、generation、deadline 和长度。实现使用
有界队列、请求与帧大小上限、并发上限及 H2 flow control；连接断开不能丢失 Central/Agent 各自已有的
幂等恢复状态。

Agent stream 的 data/end 帧 envelope `request_id` 必须与对应 open 帧一致；Replica peer forwarding
的 request ID 在原 frame deadline 内保持绑定，只有已完成且已过期的 replay 记录才可清理，不能通过
容量驱逐在有效窗口内重新投递同一 payload。

Gateway 错误分为可重试不可达、已过期/被 fencing、身份或 scope 拒绝、协议错误和资源耗尽。身份、
签名、mount/owner generation 或租户 scope 错误必须失败关闭，不能转为重试或降级到明文连接；仅代表
旧控制 channel 的 session/route fencing 会触发 Agent 重连。

### 5.3 Replica peer 凭证目录

Replica 的 mTLS CA、URI SAN 和 `clientAuth`/`serverAuth` EKU 只证明“这是某个 Gateway Replica”，
不证明该叶子仍是 Central Registry 当前允许使用的凭证。Central 在每条 control H2 建链完成后发送一次
`peer_directory`，之后每次接受到 heartbeat 都重新生成并发送。目录按 Pool/EdgeCluster 限定，最多 256
条记录；每条记录绑定 Replica ID、certificate generation 和叶子证书 DER 的 BLAKE3 fingerprint，
快照 TTL 固定不超过 30 秒，generation 在同一 control session 内严格递增。

Gateway peer listener 在完成 TLS URI SAN 校验后，必须用握手中实际的 leaf DER fingerprint 查当前目录；
目录缺失、过期、Replica 不在目录或 fingerprint/generation 不匹配都拒绝该请求。Central revoke、drain 或
证书轮换通过下一次目录刷新传播，最迟在当前快照 TTL 到期时失效；control session 断开时 Gateway
立即清空目录，因此不会把旧目录当作本地持久配置。该机制是短 TTL 的传播边界，不替代 Central 的
Registry CAS fencing，也不声称提供即时的跨进程连接关闭。

## 6. 身份与安全

### 6.1 工作负载 PKI

Central 管理工作负载 CA：Root CA 离线保存，在线 Intermediate 私钥由 KMS/HSM 托管，并通过
`WorkloadCertificateIssuer` 接口签名；生产私钥不得落入应用文件系统。

每张工作负载叶子证书必须包含且只包含一个 canonical SPIFFE URI SAN。身份格式固定为：

- Central：`spiffe://<trust-domain>/workloads/central`；该身份不属于任何 EdgeCluster，Gateway control
  listener 只接受这一精确 URI，Agent 或 GatewayReplica URI 即使由同一 CA 签发也必须拒绝；
- Gateway Replica：
  `spiffe://<trust-domain>/workloads/edge-clusters/<edge_cluster_id>/gateway-pools/<gateway_pool_id>/gateway-replicas/<gateway_replica_id>`；
- Agent：`spiffe://<trust-domain>/workloads/edge-clusters/<edge_cluster_id>/agents/<agent_id>`，并与已审批
  StorageVolume/Tenant 绑定记录核对。

Central 与 Agent 工作负载叶子证书要求 `clientAuth` EKU；GatewayReplica 同时要求 `clientAuth` 和
`serverAuth` EKU。激活后的 GatewayReplica workload leaf 除唯一 URI SAN 外，还必须包含 Registry 中
该 Replica 所属 Pool 的 Agent 入口、可选 S3 入口，以及该 Replica 的 bootstrap、control、peer endpoint
主机所对应的完整 DNS/IP SAN 集合；多个 endpoint 使用同一主机时去重。Issuer 返回的 DNS/IP SAN
集合必须与签发请求完全一致，不能缺少名称或附加未登记名称。TLS hostname/IP 校验与 workload URI
身份校验是两个独立门槛：共享 CA、匹配 endpoint 主机或匹配 SPIFFE URI 中任意一个都不能单独授权连接。

TLS 身份只证明连接对端，不自动授予租户或资源权限。每个业务动作仍由 Central Authorizer 按
tenant/resource/action 默认拒绝校验。

工作负载叶子证书默认有效期 6 小时，在半生命周期续签。撤销时 Central 提升 credential/session
generation 并关闭现有 control/Agent 会话；Gateway peer 请求还必须通过上面的短 TTL credential
directory，拒绝旧 fingerprint，即使证书的墙钟有效期尚未结束。

当前实现已具备 Gateway credential 过期扫描、CAS revoke/generation fencing、RouteLease owner
credential/notAfter 复核、Central peer credential directory 的分页构造/heartbeat 刷新/断链清空，以及
Gateway 入站 TLS 对端叶子 notAfter 到期关闭既有 H2 连接；Agent status
可在半程生成并持久化严格递增的下一代公开证书 bundle，Agent daemon 会主动取证、原子安装到本地身份
库和两个 TLS client，正常关闭旧 channel 后以新 mTLS 身份重连。Agent 和 Central 会从握手叶子证书
计算 `notAfter` 单调时钟 deadline；Central 同时取 Registry expiry 的更早值，到期后关闭既有 H2 并
完整重新握手。生产 KMS/HSM issuer/provisioner、Gateway 新证书的自动安装切换和真实轮换演练仍是
部署/runtime 集成缺口，不能由应用内临时私钥或静态文件签发器替代。

### 6.2 Bootstrap

GatewayReplica bootstrap 使用 server-auth TLS：Central 主动连接登记的 bootstrap endpoint，Replica
提交一次性 activation token，并使用本地 Ed25519 私钥完成 challenge proof。Central 的 bootstrap HTTP
transport 必须由强制 `Policy::none()` 的专用 client builder 构造，禁止跟随 3xx 到未登记 origin；成功后获取工作负载证书，
后续 control/peer 连接强制 mTLS。激活前使用的独立 bootstrap server certificate 至少以 DNS/IP SAN
精确覆盖登记的 bootstrap endpoint 主机，它只建立取证通道，不声明已激活的 workload 身份；激活并
安装新证书后，所有 Gateway listener 必须使用满足 6.1 完整 URI、DNS/IP SAN 和 EKU 契约的 workload
leaf。

workload trust domain 是 Gateway 的长期身份边界，不能与一次性 activation token 共用生命周期。所有
TLS 或非 loopback Gateway listener 启动时都必须显式配置 trust domain；缺失时启动失败，而不是接受
任意由共享 CA 签发的 SPIFFE 域。工作负载证书完成精确提升和重启后，部署必须同时移除 bootstrap
private-key 引用、activation-token 引用和临时 certificate-delivery 路径，但保留 trust domain、活动
listener key/certificate 和 CA bundle。删除 token 后不得因残留的半套 bootstrap 配置进入循环失败。

Agent enrollment/status 全部经本集群 Gateway 转发。未取证 Agent 只允许在 server-auth TLS 下调用
bootstrap/status；审批后所有 session 和业务动作必须使用 mTLS。已有已审批 Agent 使用原 Ed25519 私钥
对 status/取证请求签名，不重复触发人工审批。

### 6.3 端到端签名

mTLS 保护每一跳，端到端 Ed25519 签名保护权威 payload：

- Agent 上行报告继续由 Agent identity key 签名，Central 最终验证；
- Central 下行命令由独立 KMS/HSM-backed Ed25519 keyring 签名，包含 `key_id`；
- Agent 持有可轮换的 Central trust bundle，验证命令和 key status；
- Gateway 只校验转发 envelope 和 hop identity，不修改已签名业务 payload；
- 日志禁止记录 activation token、Authorization、私钥、完整 Ticket、业务 payload 或文件内容。

## 7. 服务与部署边界

新增 `services/neoengram-gateway` 作为独立 binary。它只依赖 domain 以及网络、TLS、签名和观测
组件，不得依赖：

- `neoengram-central` 的 Authority datasource/mapper；
- `neoengram-runtime`；
- SQLite/PostgreSQL authority schema；
- NFS/PVC/Volume adapter 或对象存储 SDK。

Gateway 运行三类 listener：Agent edge、Central control bootstrap/mTLS、Replica peer。Kubernetes
生产部署使用 Deployment/Service、至少两个 Replica、同 Pool 跨 `kubernetes.io/hostname` 的 required Pod
anti-affinity、PodDisruptionBudget、readiness、preStop drain 和 NetworkPolicy。Gateway Pod 不挂载业务
PVC；必要的身份状态只放入 Secret/独立最小状态目录。

当前 Kubernetes 模板的 `preStop` 通过同一 binary 的 `--pre-stop-drain` helper 向 PID 1 发送 `SIGUSR1`，
使 readiness 立即失败，并让新连接或既有 H2 连接上的所有新协议请求失败关闭。Gateway 同时停止
heartbeat 与 RouteLease acquire/renew，在两秒全局预算内并发尝试释放当前 RouteLease，然后向 Central
发送 `Drain`；Central 持久化 Replica `Draining`、fence control session 并关闭活动 stream。若 Central
已经不可达，本地续租 fence 仍生效，未确认释放的短租约按 TTL 到期。helper 摘流等待结束后 Kubernetes
发送 `SIGTERM` 关闭剩余连接。这个进程级 drain 不替代逐 Replica 的运维编排和真实集群切换演练。

`neoengram-central` 在目标架构中保留用户 API 和 Central composition root，但不再对 Agent 暴露公网
listener；它根据 GatewayRegistry 主动连接各 Replica。`neoengram-agent` 配置只接受 GatewayPool
endpoint 和 trust bundle，不保留 Central endpoint fallback。

### 7.1 Web 控制台的集群绑定

`apps/neoengram-web` 是静态构建物，不具备按 `edge_cluster_id` 动态发现 GatewayPool 的能力。一个
构建产物的 `VITE_GATEWAY_ENDPOINT` 因而必须与一个预配置的
`VITE_GATEWAY_EDGE_CLUSTER_ID` 成对部署；HTTPS/non-loopback 配置缺少 binding，或用户选择的
`edge_cluster_id` 与 binding 不一致时，Web 在创建 enrollment token 前失败关闭，并且不会生成可部署的
Agent YAML。输入框在存在 binding 时自动填充并锁定该 ID。只有 loopback HTTP 开发 profile 允许不配置
binding，以便本地 mock/开发使用任意测试集群 ID。

这只是静态 Web 的安全边界，不是 Central 的资源授权；Central 仍必须校验 enrollment 请求的
`edge_cluster_id`、GatewayPool 归属和 token scope。未来若要在一个控制台管理多个 EdgeCluster，应由
Central 返回带完整 GatewayPool endpoint、CA/trust metadata 和 cluster identity 的 enrollment
descriptor，再由 Web 按 descriptor 选择，不应把多个 endpoint 拼进一个未认证的全局环境变量。

## 8. 高可用与故障语义

| 故障                     | 预期行为                                                                       |
| ------------------------ | ------------------------------------------------------------------------------ |
| 单 Replica 退出          | 已连接 Agent 断线并重连 Pool；其他 Replica 继续服务                            |
| Agent owner Replica 退出 | 旧 RouteLease 失效或被撤销后 Agent 获得新 generation；期间命令返回 unavailable |
| Replica 间网络分区       | 禁止广播或多跳；无法到达 owner 时失败关闭并刷新目录                            |
| Central 暂时不可达       | 已连接 transport 可存活；不得新建/续签权威 Lease、签发命令或 Ticket            |
| GatewayPool 整体不可用   | 该集群控制、跨集群传输和 S3 不可用；本地 Volume 字节不受损                     |
| Agent 失联               | 不推断 Job 失败，不让旧 generation 和新 generation 同时发布                    |
| 证书撤销/过期            | Central fencing/断链关闭控制会话；peer directory 在刷新或 30 秒 TTL 到期后拒绝旧 leaf，不降级到 token-only 业务 session |
| 请求队列耗尽             | 有界拒绝并返回资源耗尽；不能无限缓存或丢弃已接受的权威动作                     |

Gateway 不承担 durable outbox。命令是否已接受、Agent report 是否已持久化仍由 Central/Agent 的幂等
状态机证明；连接恢复后基于 request identity、Assignment generation 和 report sequence 重放。

命令转发必须以 owner 控制队列的接受边界决定是否可换路：仅当完整单帧尚未进入 owner 队列，且入队操作
明确返回 `Closed` 时，Central 才能选择一个非 owner Replica 做最多一次 peer fallback。一旦 owner 队列
已经接受该帧，后续 H2 write/response 失败属于结果不确定，禁止再向 peer 重放；Central 保留 durable
outbox 记录，并在 Agent 的下一有效会话按原 request identity/sequence 重投。这个边界优先避免同一命令
产生重复 sequence，不能用“owner 可能没收到”作为二次 peer 投递的依据。

## 9. 跨集群对象传输（G2 v2，执行路径已接入，生产验收待完成）

固定数据链路为：

```text
Source Volume
    -> Source Owner Agent
    -> Source GatewayPool
    -> Destination GatewayPool
    -> Destination Owner Agent
    -> Destination Volume temporary object
    -> size/BLAKE3 verify + durability barrier
    -> destination placement evidence
```

Central 新增并权威管理：

- v1 `TransferRoute`/`TransferTicket`/`TransferSession`（迁移中的 legacy，不能表达多源对象并集）；
- v2 `MaterializationJob`/`MaterializationBatch`：按 namespace、Commit、目标 Volume 和 coverage goal 幂等，
  批次按 source Agent/Volume/route 分组；
- v2 batch Ticket：签名绑定 materialization/plan/batch attempt、tenant/namespace/artifact/commit、分页
  `BatchManifest` digest、源/目标 Placement 与 Volume/session/mount/route generation、byte limit、deadline 和 capability；
- v2 object-read/staging lease 与幂等 ObjectPlacement receipt 的控制记录。

Agent A 先从自己的 Volume 读取，字节经 Gateway A 和 Gateway B 流式传给 Agent B。Gateway 只做
v2 Ticket/manifest 校验、转发、限速和观测不缓存为业务副本。Agent B 必须重新计算 size/BLAKE3、执行 durability
barrier 并原子发布后，Central 才能登记目标 ObjectPlacement。切换 source/route/attempt 时，目标 staging key
`(materialization_id, object_namespace_id, object_id)` 和 confirmed offset 保持不变。Agent source/target session
与 Gateway relay 已有执行入口，但真实跨 Gateway route 配置、三 Agent/多源失败恢复和生产 E2E 仍待验收。
禁止目标 Agent 挂载源 NFS、Agent 跨集群直连、无 Ticket 传输或 Central API payload relay。

## 10. S3 暴露（一期只读数据面）

S3 是 Gateway 对外暴露固定版本的读取协议，不是 Central durability backend，也不改变 Volume-local
CAS 权威。

Central 通过 `S3AccessPoint` 把全局唯一的外部 bucket name 绑定到 tenant/project/artifact、Ready
Snapshot、固定 Commit、GatewayPool 和策略 generation；Bucket 不直接等于 Volume、CAS 或 Artifact。v2 下目标
Volume 还必须具有完整 `VolumeCommitCoverage` 和通过视图校验；partial Coverage 或仅全局 content presence
完整都不能使 S3 Ready。
每个 Access Point 的 `S3Credential` 独立轮换，Secret 只在创建时返回一次并使用 envelope encryption
保存。停用 Access Point 会撤销其所有凭证。

首版只支持 AWS SigV4、预签名读取、`ListObjectsV2`、`HEAD`、`GET` 和 Range；不支持 PUT、DELETE、
Multipart Upload 或 Versioning。请求路径为：

```text
S3 Client -> Gateway S3 Access Point -> owning Agent -> Volume-local Manifest/Chunk CAS
```

S3 key 使用现有 `LogicalPath` 受限文件路径语义，拒绝空段、`.`、`..`、尾空格/点、大小写冲突和
文件/prefix 冲突；不承诺任意 S3 keyspace。`LIST` 查询 Central 权威 metadata，不枚举 Volume CAS；
`GET`/Range 由 owning Agent 按 Manifest 流式读取。内部 Chunk namespace 永不公开。

未来 PUT/DELETE 必须先定义 Agent durability、Central publication CAS、Multipart 和 Versioning 语义，
不能直接把 Gateway 变成可写对象仓库。

## 11. 一次性切换

控制面迁移按维护窗口执行：

1. 先部署 GatewayPool/Replica，完成 Replica 激活、证书、Central 连接和真实集群双 Replica 故障演练；
2. 停止旧 Agent session 和新 Assignment 下发，等待已接受 mutation 到达安全点；
3. 发布只接受 GatewayPool endpoint 的 Agent 与只主动连接 Gateway 的 Central；
4. Agent 经 Gateway 重新 enrollment/status、取证并建立新 generation RouteLease；
5. 验证 enrollment、heartbeat、Assignment、MetadataBatch、report、decision 和 finalize；
6. 通过 NetworkPolicy 禁止 Agent 访问 Central Agent endpoint；
7. 保留迁移审计和回滚所需数据库备份，但不恢复旧网络协议作为在线 fallback。

由于不提供双栈兼容，切换前必须完成所有目标集群的 Gateway readiness、证书信任、Agent 配置和
网络策略预检。任何集群未通过时，不开始该维护窗口。

## 12. 验收与观测

控制面首个里程碑至少验证：

- InMemory/SQLite 运行同一 GatewayRegistry 契约，覆盖 schema 迁移、CAS、租约过期和 fencing；
- JSON Schema/golden、未知字段、帧大小、deadline、重复帧、错误映射和 H2 背压；
- activation token 重放、错误 URI SAN、跨集群证书、过期/撤销证书和 generation 不匹配失败关闭；
- peer credential directory 缺失、过期、旧 generation/fingerprint、Central 断链后的 peer forwarding 必须失败关闭；
- 两个 Replica 下 Agent 可经任意入口建立唯一 RouteLease，Central 命令最多一跳到达 owner；
- owner 退出后，在旧 Lease 失效前没有第二个活动 owner，之后 Agent 可用新 generation 恢复；
- loopback 双 Replica listener/H2/peer 协议 harness 已通过
  （`cargo test -p neoengram-gateway --test network_e2e --locked --offline`）；该测试手工注入
  `RouteGranted`/`RouteFenced` 和未签名的转发 payload，不经过真实 Central Registry 的原子
  `acquire_agent_session_route`、durable outbox、命令签名或完整 report/finalize 业务链，不能作为完整双
  Replica 业务 E2E 证据；
- 真实 Central Registry + 双 Replica transport 的唯一 owner/fencing、outbox 重放、签名保持、租约过期
  恢复，以及 Kubernetes readiness/failover 和维护窗口切换仍需验收；
- readiness 验收必须分别报告声明态 `desired_replicas`/`minimum_ready_replicas` 与观测态：覆盖
  `minimum_ready_replicas=2` 仅一个活动 Replica、全部 Replica heartbeat 超时、Replica draining/revoked
  和恢复后的状态矩阵；在该矩阵完成前，不得把 Pool `Ready` 当作满足最小可用副本数的承诺；
- 端到端 payload 被 Gateway 或中间代理修改时，Agent/Central 拒绝；
- v2 BatchManifest digest、namespace、plan/attempt、source/target Placement generation、session/mount/route
  generation、byte limit、deadline 或 capability 被篡改时硬失败；旧 v1 Ticket 在生产 v2 ALPN 上拒绝；
- v2 目标 staging checkpoint 在 source/route failover 和 Agent 重启后保持，只有 durability barrier 后的
  幂等 receipt 才能登记 ObjectPlacement；当前仍缺真实跨 Gateway 三 Agent E2E、故障注入和生产 route 凭据验收；
- enrollment、heartbeat、Assignment、MetadataBatch、report、decision 和 finalize 全链路经 Gateway；
- Agent 无法访问 Central Agent endpoint，Gateway Pod 无 Volume mount，Central/Gateway 不产生 Chunk
  payload 持久副本。

最小观测指标包括 Pool/Replica readiness、Central/Agent 连接数、RouteLease 获取/续租/过期、fencing、
forwarding 一跳命中/失败、队列使用率、H2 flow-control stall、帧拒绝、证书到期和端到端签名失败。
日志统一携带 request/trace、Pool/Replica、Agent 和 generation 标识，但不记录秘密或 payload。

## 13. 架构决策记录

### 2026-08-09：NeoEngram Gateway 成为每集群固定入口

决定用每个 EdgeCluster 一个多副本 GatewayPool 统一承载 Central/Agent 控制连接，并作为后续跨集群
传输和 S3 入口。此前“Agent 直接连接 Central”“Gateway 只是可选的 source Volume 只读端点”以及
“Agent 自带公网 S3 endpoint”的方案保留为历史探索，不再是目标架构。

该决策没有改变两条存储不变量：Central 仍是逻辑 metadata 权威，Volume Owner Agent 仍是唯一
StorageVolume I/O 执行者。Gateway 是无业务持久状态的网络边界，不是新的对象仓库。
