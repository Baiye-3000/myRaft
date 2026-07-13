# DistributedKV 架构设计

## 1. 设计目标

DistributedKV 是一个面向学习与工程实践、具有生产级风格的分布式 KV 系统。第一版
部署目标为 3 节点集群，使用 Raft 保证已提交写入在多数节点故障边界内不丢失，并提供：

- `SET key value`
- `GET key`
- `DELETE key`

首要原则是先保证正确性、可测试性和可恢复性，再进行性能优化。系统不追求第一版就具备
完整商业数据库能力；成员动态变更、跨分片事务和多 Raft Group 不在初始范围内。

## 2. 正确性与可用性语义

- 写操作只由 Leader 接收。命令写入本地 WAL、复制到多数节点并提交后才返回成功。
- Follower 对写请求返回 Leader 提示；客户端负责重定向和有限重试。
- 第一版 `GET` 由 Leader 提供线性一致读：Leader 先通过多数派心跳确认领导权，再读取
  状态机。后续可用 ReadIndex 或安全 Lease 优化。
- 每条客户端写请求携带 `client_id` 和单调递增的 `request_id`，状态机保存最近响应，
  避免网络重试导致命令重复执行。
- 3 节点集群可容忍任意 1 个节点故障；失去多数派时拒绝提交写入。
- 单个 key/value 的最大长度由配置限制，避免畸形请求耗尽内存。

## 3. 总体分层

```text
Client CLI / Client Library
            |
            v
Client TCP Listener ---- Protocol Codec
            |
            v
Request Dispatcher ---- Raft Node <----> Peer TCP Transport
            |                |
            |                v
            |          Raft Log + WAL
            |                |
            v                v
        KV State Machine <--- Apply Queue
            |
            v
         Snapshot
```

客户端流量和 Raft 节点间流量使用独立监听端口，防止慢客户端阻塞共识消息，也便于分别
设置连接数、帧大小和超时。

## 4. 目录与模块职责

```text
DistributedKV/
├── CMakeLists.txt
├── README.md
├── docs/
│   ├── architecture.md
│   ├── development_log.md
│   ├── test_report.md
│   └── optimization.md
├── src/
│   ├── network/
│   │   ├── server.*
│   │   ├── connection.*
│   │   ├── tcp_client.*
│   │   └── protocol.*
│   ├── raft/
│   │   ├── raft_node.*
│   │   ├── leader_election.*
│   │   ├── log_replication.*
│   │   ├── raft_log.*
│   │   └── state_machine.*
│   ├── storage/
│   │   ├── kv_store.*
│   │   ├── wal.*
│   │   └── snapshot.*
│   ├── client/
│   │   ├── client.*
│   │   └── main.cpp
│   └── server/
│       ├── node_service.*
│       └── main.cpp
└── tests/
    ├── unit/
    ├── integration/
    └── cluster/
```

### 4.1 network

- `Server`：创建非阻塞 socket，管理 epoll 实例、accept、读写事件和连接生命周期。
- `Connection`：以 RAII 管理 fd，维护输入缓冲区、输出队列、半包/粘包解析和背压状态。
- `Protocol`：只负责帧的编码、解码和字段校验，不包含业务逻辑。
- `TcpClient`：用于 CLI 和节点间 RPC，支持超时、断线重连及请求关联。

网络层向上暴露完整消息，不暴露裸字节流。第一版使用 epoll LT 模式降低实现风险；
Phase 7 在所有读写循环满足“直到 EAGAIN”后再评估 ET 模式。

### 4.2 storage

- `KVStore`：线程安全内存表，保存已提交状态。第一版使用
  `std::unordered_map<std::string, std::string>` 和读写锁。
- `WAL`：持久化 Raft 的 term、vote 和日志条目。记录包含长度、类型、序号、载荷和
  checksum；只认可完整且校验通过的记录。
- `Snapshot`：保存某个 `last_included_index/term` 对应的 KV 数据、客户端去重表及
  元数据；通过临时文件写入、`fsync`、原子 rename 发布。

WAL 是 Raft 正确性存储，不把未提交日志直接恢复进 KVStore。节点启动时先加载 Snapshot，
再恢复 Raft 日志，并仅重放已提交部分到状态机。

### 4.3 raft

- `RaftNode`：维护角色、term、投票、commit index、定时器和 RPC 状态机。
- `LeaderElection`：处理随机选举超时、RequestVote 和角色迁移。
- `LogReplication`：处理 AppendEntries、`next_index`、`match_index` 和提交推进。
- `RaftLog`：封装日志索引、term 查询、冲突截断、追加和快照边界。
- `StateMachine`：按严格递增的 committed index 应用 KV 命令，并生成请求响应。

Raft 状态只由一个逻辑事件循环串行修改，网络线程不得直接改写共识状态。耗时磁盘操作可
交由专用存储线程执行，但持久化完成事件必须回到 Raft 事件循环后才能发送成功响应。

### 4.4 client

客户端库维护节点地址列表和已知 Leader，自动处理 `NOT_LEADER`、连接失败与超时。重试
使用相同的请求标识，写请求不无限重试。CLI 只负责解析用户输入和显示结果。

## 5. 自定义二进制协议

所有消息使用网络字节序和固定帧头：

| 字段 | 大小 | 含义 |
|---|---:|---|
| magic | 4 bytes | 协议标识，快速拒绝错误流量 |
| version | 1 byte | 协议版本 |
| message_type | 1 byte | 请求或 RPC 类型 |
| flags | 2 bytes | 响应、错误等标志 |
| body_length | 4 bytes | 消息体长度 |
| request_id | 8 bytes | 请求关联标识 |

消息体采用显式长度字段编码字符串和数组，不依赖结构体内存布局。解码器先检查 magic、
版本、类型、长度上限和字段边界，再构造领域消息。未知类型返回协议错误并关闭连接。

客户端消息包括 `GET`、`SET`、`DELETE` 和响应；Raft 消息包括 `REQUEST_VOTE`、
`REQUEST_VOTE_RESPONSE`、`APPEND_ENTRIES`、`APPEND_ENTRIES_RESPONSE`，后续增加
`INSTALL_SNAPSHOT`。协议层预留版本升级能力。

## 6. 并发模型

Phase 6 每个进程使用以下三个执行单元：

1. 客户端 epoll I/O 线程：接入客户端、解析帧和发送响应。
2. Peer epoll I/O 线程：处理节点间连接与 Raft RPC。
3. Raft 事件线程：串行执行选举、同步持久化、复制、提交和状态机应用。

首版把 save barrier 与 apply 保留在 Raft 线程，优先保证顺序与故障语义。Phase 7 只有在
建立有序 completion barrier 后才会拆分 WAL/group commit 或状态机应用线程。

跨线程通信使用有界队列并定义背压：队列满时暂停相关连接读事件或返回过载错误。关闭时
按“停止接入、排空任务、持久化、关闭 fd”的顺序执行。所有线程由拥有者对象 RAII 管理，
禁止 detached thread。

Phase 7 起，Raft thread 将 ClientResponseEvent 成功放入有界队列后写 Server wake
eventfd；Client epoll 被立即唤醒并 drain response queue。同一 eventfd 也承载停止信号，
计数器可安全合并并发通知，不再依赖周期 polling。

Phase 6 `PeerTransport` 在独立 epoll 线程中拥有全部 Peer socket。每个 RPC frame 都携带
source NodeId，连接第一帧绑定身份，后续身份变化会断开。出入站 typed message 通过固定
容量 `BoundedQueue` 与 Raft 线程交换；每 Peer 未连接暂存最多 256 帧，TCP 连接本身另有
2 MiB 输出上限。连接失败不会修改共识状态，由 transport 后续重新建立。

## 7. Raft 核心设计

### 7.1 持久状态

- `current_term`
- `voted_for`
- 日志条目 `{index, term, command}`
- 已知 `commit_index`（用于完整集群重启后立即恢复已确认前缀）
- Snapshot 元数据

在依赖这些状态发送 RPC 或成功响应之前，必须先完成 WAL 持久化。

### 7.2 易失状态

算法上所有节点维护易失的 `commit_index`、`last_applied`；当前 journal 额外保存最近
已知 commit index 作为恢复加速/状态机重建边界。Leader 还维护每个 Follower 的
`next_index` 和 `match_index`。

### 7.3 选举

Follower 使用随机化选举超时。收到合法 Leader 心跳或向有效 Candidate 投票时重置计时器。
Candidate 自增 term、持久化自身投票并并行发起 RequestVote。获得多数票后成为 Leader，
立即发送心跳。观察到更高 term 时，任何角色立即降级并持久化新 term。

投票除 term 外还比较日志新旧：先比较最后日志 term，再比较最后日志 index。

### 7.4 日志复制与提交

Leader 为每个 Follower 维护独立复制进度，AppendEntries 带前置日志 index/term。
Follower 只在前置日志匹配时追加；冲突时截断冲突条目并持久化。响应可携带冲突 term 与
首索引，用于 Leader 快速回退。

Leader 只通过“多数节点已复制且条目属于当前 term”的规则推进 commit index。旧 term
条目通过当前 term 条目的提交被间接提交。状态机只消费 committed entry。

### 7.5 Phase 4 实现边界

- `RaftNode` 是单事件线程驱动的编排器，不创建线程或访问 socket。
- `LeaderElection` 封装日志新旧比较和严格多数派计算。
- `LogReplication` 封装 conflict hint 回退和当前 term 提交计算。
- `RaftLog` 维护 absolute-index Snapshot boundary、连续索引及冲突后缀替换。
- typed `OutboundRpc` 由宿主负责传输；Phase 6 再接入 Peer TCP codec。

Phase 5 已加入同步 `RaftPersistence` 门禁：current term、vote 或 log 改变后，RaftNode
必须先得到 save 成功才能返回依赖新状态的 RPC；失败被视为 fatal。Phase 6 的
`FileRaftPersistence` 首条记录使用完整状态镜像，并把已知 commit index 一并保存。
Phase 7 后续记录只保存 metadata、truncate index 和变化日志后缀。恢复节点重放 full +
delta 后仍从 Follower 启动，只应用持久化 committed prefix。

### 7.6 Raft 与 KV 状态机组合

- `KVCommandCodec` 将 SET/DELETE、client id 和 request id 编码为 opaque log command。
- `StateMachine` 只按严格递增顺序应用 committed entry，no-op 不修改 KVStore。
- 每个 client 缓存最近 request id/response，重试不会重复执行。
- `RaftKVService` 跟踪本地 pending log index，多数派提交后才产生客户端完成结果。
- `getApplied` 是屏障后的内部读取原语，不允许直接暴露给多节点客户端。

Phase 6 的客户端 GET 由 Raft event thread 建立 ReadIndex 风格屏障：Leader 必须已经提交
本 term entry，随后发送携带唯一 read context 的 AppendEntries probe。只有同 term 成功
响应达到多数派，且本地 `lastApplied` 不小于创建时 commit index，才访问 KVStore。角色/
term 变化、deadline 到期或失去多数派均不返回数据。此实现不允许 Follower lease read。

Phase 7 将同一次 Raft event-loop drain 中已经排队的 GET 合并为一个 read batch，共享
context、term、read index、ack set 和 deadline；确认后再逐个 key 读取与响应。默认不设置
聚合等待时间，避免为了等待未来请求而增加首读延迟。

### 7.7 真实节点运行时

`NodeService` 是进程级 owner，按声明顺序构造 KVStore、Raft journal、RaftKVService、
四条有界队列、Client Server 和 PeerTransport。Client IO 保留 connection id 并异步完成；
Peer IO 只搬运 typed RPC；Raft event thread 串行处理 timer、RPC、持久屏障、commit/apply、
pending write 和 read barrier。关闭路径先设置共享 stop，再唤醒 Client epoll，最后 join
三个线程并释放 fd。

## 8. WAL 与恢复

Phase 3 WAL 文件由文件头和连续记录组成：

- 16-byte 文件头：8-byte magic、32-bit version、32-bit header size。
- 12-byte 记录头：32-bit magic、32-bit payload length、32-bit CRC32C。
- payload：operation、reserved、64-bit sequence、key/value length 和原始字节。
- 当前 operation 支持 SET/REMOVE；所有整数使用网络字节序。

每条记录的写入流程为：

1. 序列化不可变记录。
2. 追加完整记录到 WAL。
3. 按 durability 策略执行 `fdatasync`。
4. 通知 Raft 事件线程持久化完成。

恢复时顺序扫描；文件尾部不完整记录可安全截断，中间 checksum 错误视为存储损坏并停止
启动，避免静默接受错误状态。WAL 格式包含版本号，拒绝未知版本。

Phase 3 WAL 是单机状态机命令日志，不与 Raft 数据文件混用。Raft journal 使用独立
magic/version；full record 保存完整状态，delta record 保存 term、vote、commit index、
共同前缀截断位置和变化后缀。两类 payload 都由 CRC32C 保护并在 `fdatasync` 后才完成
save barrier，原有 full-only 文件保持可读取。启动时扫描并重放到最后完整记录；不完整
文件尾可截断，中间 magic/checksum/语义损坏拒绝启动。`RaftLog::restore` 再检查 boundary、
absolute index 连续性、entry type 和 no-op 内容，防止磁盘字节虽完整但状态非法。

同一个 AppendEntries handler 内发生的 term、suffix 和 commit 变化在最终 response 边界
合并为一个状态镜像和一次 `fdatasync`。该合并不跨 RPC，也不允许响应先于 durability；
因此减少同步次数但不改变 Raft 持久化先行规则。

delta 数或文件大小达到配置阈值后，journal compaction 将最新完整状态写入同目录临时文件，
完成 `fdatasync` 后 atomic rename，并 `fsync` 父目录；新文件在发布前已持有独占锁。
compaction 仅减少历史 record replay，不删除 Raft log prefix，后者必须等 Snapshot 发布后
才能安全执行。

## 9. Snapshot 与日志压缩

当 WAL 大小或已应用日志数超过阈值时，由状态机在一致的 applied index 上生成 Snapshot。
发布成功后，Raft 才能删除该 index 之前的日志段。落后于 Snapshot 边界的 Follower 通过
InstallSnapshot 分块接收，校验后原子替换本地状态。

已实现的状态镜像以 `(last_included_index, last_included_term)` 为边界，同时包含按 key
排序的完整 KV 数据和按 client id 排序的去重 session。恢复先在临时容器中校验 index/term
一致性、session 枚举与唯一性、KV key 唯一性，再一次替换 KVStore、session 和 applied
index；因此损坏镜像不会留下半恢复状态。排序不影响语义，但保证后续文件编码和 checksum
可复现。

`FileSnapshotStore` 将该镜像编码为独立版本化文件；header 记录 magic、version、payload
长度和 CRC32C，payload 的整数使用 big-endian，并对总大小、条目数及每个可变字段设置
读取上限。发布采用同目录临时文件、`fdatasync`、atomic rename、parent-directory
`fsync`，因此重启只能观察到旧完整镜像或新完整镜像。

节点启动先读取 Snapshot 和 Raft journal。Snapshot boundary 必须不高于 durable
`commit_index`，且 journal 在该 absolute index 的 term 必须与
`last_included_term` 一致；任一文件损坏或边界不匹配都拒绝启动。校验后先恢复
KV/session/applied index，再重放 boundary 之后的 committed suffix。

`RaftLog` 使用 absolute-index offset 模型：内部 `entries_[0]` 是 retained Snapshot
boundary，保存 `last_included_index/term` 但不保存原 command；后续元素的 absolute
index 等于 boundary index 加 vector offset。`compactTo` 只接受本地已存在且 term 匹配
的边界，并以临时 vector 完整构造后交换，失败不会部分截断。

Leader conflict backtracking 不得越过 boundary 后的首个可复制 index；Follower 收到
boundary 之前的 `previous_log_index` 时返回该 floor。待 InstallSnapshot 实现后，
Leader 看到 Follower 仍落后于 boundary 将改发 Snapshot。

NodeService 尚未周期性生成 Snapshot，`FileRaftPersistence` 也尚未持久化 offset image，
因此生产 journal 暂时仍保留完整前缀。

## 10. 配置与可观测性

节点配置至少包含：

- `node_id`
- client/peer 监听地址
- 集群成员及 peer 地址
- 数据目录
- 选举超时范围、心跳间隔和 RPC 超时
- 最大帧、最大 key/value、连接数和队列容量
- WAL 同步策略与 Snapshot 阈值

日志使用结构化字段记录时间、级别、node、term、role、request 和 log index。后续提供
运行指标：请求延迟、吞吐、当前 term、commit/applied index、复制落后量、选举次数、
WAL 同步延迟及队列深度。

## 11. 错误处理与安全边界

- syscall 错误使用明确的状态对象向上传递，析构函数不抛异常。
- `EINTR` 重试，`EAGAIN` 结束当前非阻塞 I/O 循环。
- SIGPIPE 被禁用或发送时使用 `MSG_NOSIGNAL`。
- 所有长度与整数运算先做溢出检查，再分配内存。
- 数据目录使用进程锁，防止两个节点实例同时写同一份 WAL。
- 首版协议不提供 TLS 与身份认证，因此仅适合受信网络；生产部署前必须补充 mTLS、
  ACL 和静态数据加密。

## 12. 测试策略

- 单元测试：协议边界、KVStore、WAL 损坏恢复、Raft 日志和角色迁移。
- 确定性 Raft 测试：注入虚拟时钟、内存传输和可控持久化，复现丢包、重复、乱序。
- 集成测试：真实 TCP、进程重启和磁盘恢复。
- 集群测试：Leader 宕机、Follower 落后、网络分区、延迟、Snapshot 安装和重新加入。
- 故障注入：WAL 尾部截断、部分写、磁盘同步失败和进程在关键步骤崩溃。
- sanitizer：ASan、UBSan、TSan 分开执行；发布构建启用严格警告。

测试不仅检查返回值，还检查任意时刻单个 term 不出现两个 Leader、已提交日志不回退、
所有状态机按相同顺序应用相同命令等不变量。

## 13. 开发阶段与阶段门禁

1. **Phase 0**：工程骨架、构建配置和架构文档。
2. **Phase 1**：线程安全单机 KVStore 与 GoogleTest。
3. **Phase 2**：epoll TCP Server/Client 和客户端命令协议。
4. **Phase 3**：WAL、恢复及崩溃边界测试。
5. **Phase 4**：可独立测试的 Raft 选举与日志复制。
6. **Phase 5**：Raft 提交路径与 KV 状态机结合。
7. **Phase 6**：三节点真实进程和故障场景测试。
8. **Phase 7**：基准驱动的网络、存储、Raft 与线程优化。

每阶段必须满足：Debug 和 Release 构建成功、该阶段测试通过、文档与时间线更新、已知限制
记录完整。未经确认不提前实现下一阶段。

## 14. 关键设计决策

- 采用自定义二进制协议，而非文本协议或 protobuf，以练习边界安全和版本化，同时避免
  引入运行时依赖。
- 共识状态采用单线程事件模型，优先避免细粒度锁造成的数据竞争和难以复现的竞态。
- KVStore 只保存已提交结果，Raft Log/WAL 是复制与恢复的事实来源。
- 首版采用 LT epoll；ET、批量 WAL、zero-copy 和 worker pool 必须经基准验证后引入。
- 首版集群成员固定，避免把联合共识与核心正确性同时实现。

这些决策会在后续阶段通过测试和性能数据复核，变更必须记录在开发日志和本文件中。
