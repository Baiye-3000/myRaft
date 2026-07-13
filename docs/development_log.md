# DistributedKV 开发日志

本文件是项目的追加式开发时间线。已完成记录不删除；设计变更通过新增记录说明原因。

## 2026-07-13 15:49 (UTC+8) — Phase 0 启动

### 目标

从空目录建立工程骨架，明确系统边界、正确性语义、模块职责和分阶段交付门禁。

### 完成

- 创建 `src/network`、`src/raft`、`src/storage`、`src/client`、`tests` 和 `docs` 目录。
- 添加 C++17 CMake 顶层配置；本阶段不创建业务代码或可执行目标。
- 添加 README，说明项目目标、目录和当前状态。
- 完成架构设计，确定双端口网络、长度前缀二进制协议、Raft 单线程事件模型、
  WAL/Snapshot 恢复路径和分层测试策略。
- 初始化测试报告与优化记录。

### 设计决策

- 自定义版本化二进制协议。
- 第一版使用 epoll LT，验证正确后再评估 ET。
- Raft 状态由单一事件线程串行修改。
- KVStore 仅应用已提交命令。
- 第一版固定集群成员，Leader 提供线性一致读。

### 验证

- 使用 GCC 8.5.0 执行 `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug` 成功。
- 执行 `cmake --build build` 成功；Phase 0 没有编译目标。

### 已知限制

- Phase 0 无业务代码、可执行程序和单元测试。
- TLS、认证、动态成员变更和分片不属于首版范围。

### 下一步

等待 Phase 0 设计确认。确认后进入 Phase 1，实现线程安全单机 KVStore 及 GoogleTest。

## 2026-07-13 15:53 (UTC+8) — Phase 1 单机 KVStore

### 目标

在不引入网络、WAL 或 Raft 的前提下，实现可独立测试的线程安全内存 KV 存储。

### 完成

- 实现 `KVStore::put`，支持新建和覆盖，并通过返回值区分两种结果。
- 实现 `KVStore::get`，使用 `std::optional` 明确表达 key 不存在。
- 实现 `KVStore::remove`，删除操作可安全重复执行。
- 实现 `KVStore::size`，提供线程安全的瞬时条目数。
- 使用 `std::shared_mutex`：查询共享加锁，写入和删除独占加锁。
- 禁止复制和移动，避免锁对象及存储所有权产生歧义。
- CMake 增加 storage 静态库、Threads 和 GoogleTest 1.17.0。
- 新增 6 个 GoogleTest 用例，覆盖插入、覆盖、缺失查询、删除、空字符串及并发访问。

### API 语义

- KVStore 层允许空 key 和空 value；协议层将在后续阶段负责业务输入限制。
- `get` 返回 value 副本，调用方不持有容器内部引用。
- 每个公开操作均为线程安全；多个操作组合不提供事务语义。

### 验证

- Debug 构建成功，6/6 测试通过。
- Release 构建成功，6/6 测试通过。
- 编译启用 `-Wall -Wextra -Wpedantic -Wconversion -Wshadow`，无项目代码告警。

### 已知限制

- 数据只存在于内存中，进程退出后丢失。
- 暂不提供批量操作、迭代接口和容量限制。
- 并发测试验证行为结果；数据竞争检测将在独立 TSan 配置中补充。

### 下一步

等待 Phase 1 确认。确认后进入 Phase 2，实现基于 Linux epoll 的 TCP Server、Client
及 `SET`/`GET` 请求响应协议。

## 2026-07-13 15:58 (UTC+8) — Phase 2 启动

### 目标

实现版本化二进制协议、非阻塞 Connection、Linux epoll TCP Server、TCP Client，以及
可交互验证的 `SET`/`GET` 请求链路。

### Checkpoint 2.0：阶段初始化

- 确认网络层与 KVStore 解耦，Server 通过明确的请求分发调用存储接口。
- 计划按 Protocol → Connection → Server → Client → 集成测试顺序逐项保存。
- Phase 2 暂不引入 WAL、Raft 或 DELETE 网络命令，避免跨阶段实现。

### Checkpoint 2.1：Protocol 已保存并验证

- 保存固定 20-byte 帧头、网络字节序、协议版本、消息类型和 request id。
- 保存 SET/GET 请求及统一响应的编码、增量解帧和字段校验。
- 增加 key/value/body 长度上限，错误帧不会触发不受控分配。
- 6 个 Protocol 测试全部通过，覆盖往返、半包、粘包、错误 magic 和超限输入。

### Checkpoint 2.2：Connection 已保存并验证

- 保存 socket fd 的 RAII 所有权和非阻塞 recv/send 循环。
- 输入、输出均设置 2 MiB 有界缓冲，输出积压时明确返回背压错误。
- 正确处理 EINTR、EAGAIN、EOF、连接重置和 `MSG_NOSIGNAL`。
- 4 个 Connection 测试全部通过，覆盖读、写、对端关闭和缓冲上限。

### Checkpoint 2.3：epoll Server 已保存并通过编译

- 保存 IPv4 listener、`SO_REUSEADDR`、non-blocking accept4 和 epoll LT 循环。
- 使用 eventfd 实现跨线程可重复 stop，不依赖信号中断事件循环。
- 保存连接数限制、EPOLLIN/EPOLLOUT 动态关注和连接级错误隔离。
- 保存 SET/GET 分发路径；仅已解码请求可以访问 KVStore。
- Server 模块在严格告警选项下编译成功，端到端行为将在 Client 完成后验证。

### Checkpoint 2.4：TCP Client 已保存并通过编译

- 保存非阻塞 connect、绝对 deadline、poll 等待和 `SO_ERROR` 连接确认。
- 保存完整帧发送、增量响应接收和 request id 关联校验。
- SET/GET API 使用原子 request id，可由多个调用线程安全使用。
- 每次请求使用独立短连接；连接复用留作有基准数据后的优化。
- Client 模块在严格告警选项下编译成功。

### Checkpoint 2.5：可执行程序与端到端测试已保存

- 保存 `dkv_server`，使用专用 sigwait 线程将 SIGINT/SIGTERM 转换为 eventfd 停止请求。
- 保存 `dkv_client` 交互命令行，支持大小写无关的 SET、GET、QUIT。
- 保存真实 loopback TCP 集成测试，覆盖写后读、NOT_FOUND 和 8 客户端并发写入。
- 手工链路验证结果依次为 `OK`、`tom`、`NOT_FOUND`，服务端可有序停止。

### Checkpoint 2.6：最终持久化复验

- 最终复验发现 Phase 1 测试文件的磁盘内容一度缺少测试块，与编辑缓冲内容不一致。
- 重新保存完整 6 项 KVStore 回归测试后再次执行全量构建。
- 最终磁盘源码构建成功，19/19 测试通过；后续以该复验结果作为阶段门禁依据。

### Phase 2 阶段门禁

- Debug 构建成功，19/19 测试通过。
- Release 构建成功，19/19 测试通过。
- 新增源码通过 IDE lint 及严格编译告警检查。

### 已知限制

- Client 每次请求建立一个 TCP 连接，尚未实现连接池和自动重连。
- 当前只支持 IPv4 数字地址，不执行 DNS 解析。
- Server 当前为单 epoll 线程；worker pool 属于后续性能阶段。
- Phase 2 数据仍是易失的，重启恢复将在 Phase 3 实现。

### 下一步

等待 Phase 2 确认。确认后进入 Phase 3，实现带 checksum 的 WAL、同步策略和服务端
重启恢复测试。

## 2026-07-13 16:11 (UTC+8) — Phase 3 启动

### 目标

实现版本化 WAL、CRC32C 完整性校验、可配置同步策略、尾部残缺记录恢复，以及服务端
重启后的 KV 数据重放。

### Checkpoint 3.0：持久化语义确定

- SET 必须先成功追加并按策略同步 WAL，之后才能修改 KVStore 和返回 `OK`。
- WAL 完整 checksum 错误视为数据损坏并拒绝启动；仅文件尾部不完整记录允许截断恢复。
- 数据目录使用非阻塞独占文件锁，防止两个进程同时写同一个 WAL。
- Phase 3 不进行日志压缩；WAL compaction 与 Snapshot 在后续阶段实现。

### Checkpoint 3.1：WAL 模块已保存并验证

- 保存 16-byte 版本化文件头和 record magic/length/CRC32C/payload 格式。
- 保存网络字节序 SET/REMOVE 记录、严格递增 sequence 和 4 KiB/1 MiB 字段限制。
- 保存 `kAlways` 与 `kNone` 同步策略；默认在响应前执行 `fdatasync`。
- 保存独占 `flock`、完整记录校验和尾部残缺记录原子截断恢复。
- 5 个 WAL 测试全部通过：重放、尾部截断、checksum 损坏、并发 writer 和恢复门禁。

### Checkpoint 3.2：WAL 写路径与启动恢复已保存并验证

- Server 支持注入已打开且完成恢复的 WAL；测试可显式选择无持久化 transient 模式。
- SET 处理顺序固定为 append → fdatasync → KVStore put → `OK`。
- `dkv_server` 启动时锁定 WAL、校验并重放 SET/REMOVE，再开始监听客户端。
- 服务端命令行新增可选 `wal-path`，默认使用 `distributed-kv.wal`。
- 新增真实 TCP 重启测试：首次服务端确认 SET，销毁全部对象后重建服务端，GET 返回原值。
- WAL 与网络恢复相关 9/9 测试通过。

### Checkpoint 3.3：持久化边界与阶段门禁完成

- 新 WAL 文件完成 header `fdatasync` 后同步父目录，保证文件创建元数据可持久化。
- 手工启动服务端写入 `name=tom`，完全停止并重启后成功读取 `tom`，恢复记录数为 1。
- Debug 构建成功，25/25 测试通过，总耗时 0.06 秒。
- Release 构建成功，25/25 测试通过，总耗时 0.05 秒。
- Phase 3 修改通过 IDE lint 与严格编译告警检查。

### 已知限制

- WAL 每次确认写默认执行一次 `fdatasync`，正确性优先但吞吐较低；group commit 留待 Phase 7。
- WAL 持续增长，尚无 segment、Snapshot 或 compaction。
- `kNone` 同步策略只保证写入内核页缓存，不承诺断电持久性，不能用于严格 durability。
- fdatasync 失败后的客户端结果具有不确定性：记录可能在重启后出现，后续通过 request id
  去重保证重试安全。
- 当前命令 WAL 是 Phase 3 单机过渡格式；接入 Raft 后写入路径将改为持久化 Raft 状态与日志。

### 下一步

等待 Phase 3 确认。确认后进入 Phase 4，先实现可确定性测试的 Raft 状态机、RequestVote、
AppendEntries、Leader 选举和日志复制，不提前接入客户端 KV 写路径。

## 2026-07-13 16:22 (UTC+8) — Phase 4 启动

### 目标

实现与传输层解耦、由单事件线程驱动的 Raft Core，包括角色迁移、RequestVote、
AppendEntries、Leader 选举、日志冲突修复、复制进度和多数派提交。

### Checkpoint 4.0：核心边界确定

- Raft Core 不创建线程、不访问 socket，时间推进和 RPC 投递均由外部事件循环驱动。
- Phase 4 RPC 使用强类型消息和 OutboundRpc；Peer TCP 编解码与真实多进程接线留到 Phase 6。
- Leader 当选后追加当前 term 的 no-op，确保旧 term 日志可按 Raft 提交规则间接提交。
- Phase 4 不修改 KVStore；仅暴露 commit index，Phase 5 才应用 committed command。

### Checkpoint 4.1：RPC 类型与 RaftLog 已保存并验证

- 保存 NodeId/Term/LogIndex、角色、日志类型和 RequestVote/AppendEntries 强类型消息。
- AppendEntriesResponse 携带原请求位置和 conflict term/index，支持识别过期响应与快速回退。
- 保存 index-0 sentinel、连续本地 append、leader suffix 合并和冲突后缀截断。
- 相同 index/term 但 payload 不同被视为不变量破坏并拒绝，避免静默分叉。
- 5 个 RaftLog 测试全部通过。

### Checkpoint 4.2：选举与日志复制状态机已保存并验证

- 保存随机化选举计时、Candidate 自投票、多数派当选和高 term 立即降级。
- RequestVote 实现每 term 一票及 last-log-term/index 新旧比较。
- AppendEntries 实现心跳重置、prev-log 匹配、冲突 term/index 提示和 commit 传播。
- Leader 保存 nextIndex/matchIndex，忽略过期响应并按 conflict hint 快速回退。
- 仅当前 term 且多数派已复制的条目可直接推进 commit index。
- 单节点集群无需 RPC 即可提交 no-op 和 command。
- 10 个 RaftNode 算法测试全部通过。

### Checkpoint 4.3：三节点确定性集群测试已保存并验证

- 保存无 socket 测试传输队列，可同步投递 RequestVote、AppendEntries 及其响应。
- node1 在差异化超时下当选，no-op 复制后通过心跳向全部 Follower 传播 commit。
- 临时丢弃发往 node3 的 command，node1+node2 多数派仍可提交。
- 恢复投递后 node3 自动补齐日志，三个节点最终拥有相同 term/index/command 和 commit index。
- 三节点选举、分区期间提交与落后节点追赶测试通过。

### Checkpoint 4.4：模块边界与阶段门禁完成

- 将日志新旧与 quorum 规则固化到 `LeaderElection` 模块。
- 将 conflict hint 回退与 current-term commit 计算固化到 `LogReplication` 模块。
- 增加旧 term 条目不能仅凭副本数直接提交的回归测试。
- Debug 构建成功，45/45 测试通过，总耗时 0.09 秒。
- Release 构建成功，45/45 测试通过，总耗时 0.07 秒。
- Raft 源码通过 IDE lint 与严格编译告警检查。

### 已知限制

- Phase 4 使用 typed RPC 和确定性测试传输，尚未实现 Peer TCP codec/连接管理。
- current term、votedFor 和 Raft log 尚未接入磁盘适配器；真实发送 RPC 前必须增加持久化屏障。
- 目前为固定成员集群，不支持 joint consensus 成员变更。
- 尚未实现 InstallSnapshot、PreVote、CheckQuorum 和 Leader transfer。
- commit index 尚未驱动 KV 状态机，客户端写请求仍走 Phase 3 单机路径。

### 下一步

等待 Phase 4 确认。确认后进入 Phase 5，实现 command codec、StateMachine、按序应用
committed entry、客户端等待提交结果，以及 Raft 持久状态门禁。

## 2026-07-13 16:32 (UTC+8) — Phase 5 启动

### 目标

将客户端写命令编码为 Raft log entry，仅在 commit index 推进后按序应用到 KVStore，
并为 Leader 本地请求关联最终状态机结果。

### Checkpoint 5.0：应用边界确定

- SET/DELETE 作为 opaque command 进入 Raft；GET 不写日志。
- StateMachine 只应用 `(last_applied, commit_index]`，no-op 只推进 applied index。
- 命令携带 client id/request id，重复请求返回缓存结果但不重复修改 KVStore。
- 多节点提交等待通过 pending index 跟踪；单节点 Leader 可同步完成。

### Checkpoint 5.1：Command Codec 与 StateMachine 已保存并验证

- 保存版本化 SET/DELETE command codec，包含 client id、request id 和有界 key/value。
- 保存 strict next-index 应用规则；乱序、gap、损坏 command 均不会推进 lastApplied。
- no-op 仅推进 lastApplied，不修改 KVStore。
- 保存按 client id 的最近响应缓存：相同 request id 返回原结果，旧 request id 被拒绝。
- SET、DELETE、NOT_FOUND、重复请求和 stale request 共 7 个测试全部通过。

### Checkpoint 5.2：RaftKVService 已保存并验证

- 保存 RaftNode 与 StateMachine 组合层，commit index 推进后循环应用全部连续 entry。
- Leader submit 返回 NotLeader/Pending/Applied/Invalid，并用 log index 关联异步完成结果。
- 三节点测试确认未达到多数派前所有 KVStore 均不变化；多数派提交后 Leader 才产生结果。
- 后续 heartbeat 将 commit 传播给 Follower，三个 KVStore 最终收敛。
- 单节点 Leader 在 submit 返回前完成日志提交与状态机应用。

### Checkpoint 5.3：网络请求链路与持久化门禁已保存

- 客户端协议增加稳定 client id；TcpClient 默认生成非零进程客户端身份。
- Server 支持注入应用 RequestHandler，保留 Phase 3 standalone handler 兼容路径。
- 真实 TCP 测试确认 SET 经 Client → Server → Raft log → commit → StateMachine 后返回 `OK`。
- 保存 `RaftPersistence` 同步屏障；term、vote、no-op 和 command 变更均在返回相关 RPC 前保存。
- 持久化失败抛出 fatal error，阻止依赖未持久状态的 RPC 离开 Raft Core。

### Checkpoint 5.4：阶段门禁完成

- Debug 构建成功，58/58 测试通过，总耗时 0.12 秒。
- Release 构建成功，58/58 测试通过，总耗时 0.10 秒。
- Phase 5 源码通过 IDE lint 与严格编译告警检查。

### 已知限制

- `RaftPersistence` 已强制 save barrier，但尚无磁盘 load/save 实现；Phase 6 必须补齐恢复。
- 多节点 pending 请求尚未接入网络连接的异步响应队列，当前通过 log index 查询完成结果。
- Follower 只返回 NotLeader 语义，尚未在客户端协议中携带 Leader endpoint。
- GET 当前只读取 applied state，未实现 ReadIndex，因此多节点下不能宣称线性一致。
- DELETE 已进入 replicated command codec，但 Phase 2 客户端 wire message 尚未增加 DELETE 类型。

### 下一步

等待 Phase 5 确认。确认后进入 Phase 6，实现 Peer RPC codec、节点间 TCP 传输、真实三进程
集群、Leader 重定向、异步客户端完成响应和故障测试。

## 2026-07-13 16:53 (UTC+8) — Phase 6 启动

### 目标

实现 Client IO、Peer IO、Raft 事件三线程真实节点运行时，完成 Raft 磁盘恢复、Peer TCP、
异步写响应、Leader 重定向、线性一致读和三进程故障测试。

### Checkpoint 6.0：实施顺序确定

- 先完成可加载的 Raft 持久状态，再开放真实 Peer RPC，避免运行时发送未持久化状态。
- Client 与 Peer 使用独立监听端口和线程；Raft 状态只由事件线程修改。
- GET 只由 Leader 经多数派 read barrier 后返回，Follower 仅提供 Leader hint。

### Checkpoint 6.1：Raft 持久恢复已保存

- 新增 `FileRaftPersistence`：独立文件头、完整状态记录、CRC32C、`fdatasync` 和独占锁。
- `load()` 返回最后完整镜像；只截断不完整尾记录，checksum 或格式损坏会停止启动。
- 持久镜像包含 term、vote、完整日志和已知 commit index。
- `RaftLog::restore` 与 `RaftNode` 执行二次语义校验；恢复角色固定为 Follower。
- `RaftKVService` 启动时只重放 committed prefix，未确认后缀保持不可见。
- Debug 构建成功，61/61 测试通过，耗时 0.13 秒。

### Checkpoint 6.2：Peer 协议与传输已保存

- 新增独立 Raft wire magic/version，完整覆盖 RequestVote、AppendEntries 及响应。
- frame 限制 2 MiB、单 command 1 MiB、单批 256 entries；半包保持在连接输入缓冲。
- 新增有界跨线程队列与 epoll `PeerTransport`，管理监听、持久出站连接、身份绑定和重连。
- Peer IO 仅编解码和搬运 typed RPC，不直接访问 `RaftNode`。
- Debug 构建成功，65/65 测试通过，耗时 0.14 秒。

### Checkpoint 6.3：三线程 NodeService 已保存

- 新增 `NodeService`，统一拥有 Client Server、PeerTransport、RaftKVService、KVStore 和
  Raft journal。
- Client IO、Peer IO、Raft event 分属三个 joinable `std::thread`；只有 Raft 线程修改
  consensus 与 state-machine 状态。
- 单调时钟驱动逻辑 tick；磁盘/应用 fatal error 会设置停止状态并唤醒 Client epoll。
- 新增 `dkv_node`，成员参数同时配置 client/peer IPv4 端口，数据目录与 standalone WAL
  目标完全分离。

### Checkpoint 6.4：客户端提交语义已保存

- Client Server 使用 connection id 跨有界队列保留 pending 请求，commit/apply 后异步响应。
- wire protocol 增加 DELETE、NOT_LEADER、SERVER_BUSY、UNAVAILABLE 和结构化 Leader endpoint。
- `TcpClient` 在固定总 deadline/attempt 上限内以同一 client/request id 跟随 Leader hint，
  并可轮询配置的集群 endpoint；CLI 增加 DELETE。
- Leader 变化时未完成请求返回 NOT_LEADER；旧日志若稍后提交，去重状态保证同 id 重试安全。

### Checkpoint 6.5：线性一致 GET 已保存

- Leader 仅在本 term no-op 已提交后接受读，创建带唯一 context 的 AppendEntries quorum probe。
- 成功响应按 term/context 去重计票，达到多数派并满足 `lastApplied >= readIndex` 后读取 KVStore。
- term/role 改变返回 NOT_LEADER；多数派超时返回 UNAVAILABLE，旧 Leader 不会返回本地旧值。
- read context 已加入独立 Peer codec，并由 Follower 在相关 AppendEntries response 中原样返回。

### Checkpoint 6.6：真实集群阶段门禁完成

- 新增真实三进程测试：选举、Follower 重定向、pending SET、多数派故障写入、Leader 宕机
  重选、全节点停止与 durable restart。
- 隔离旧 Leader 后 GET 等待 read quorum 超时并返回 UNAVAILABLE，不泄露本地旧值。
- Raft journal 测试补齐完整记录 checksum 损坏拒绝；不把 corruption 当成 torn tail。
- Debug 构建成功，73/73 测试通过，总耗时 11.62 秒。
- Release 构建成功，73/73 测试通过，总耗时 11.33 秒。
- IDE lint 无新增诊断；Debug/Release 严格编译选项无新增 warning。

### Phase 6 已知限制

- 尚未实现 InstallSnapshot、日志压缩、成员变更、mTLS/节点认证。
- Raft journal 当前追加完整状态镜像，恢复简单但写放大与磁盘增长需要 Phase 7 优化。
- Client response queue 使用 10 ms polling；后续可通过 eventfd 降低空闲延迟。
- 未覆盖磁盘空间耗尽、`fdatasync` 卡顿、双向网络分区矩阵和恶意 Peer 流量。

### 下一步

Phase 7 先建立可复现基准，再评估增量 journal + snapshot、group commit、read batching、
eventfd completion 唤醒和 epoll ET；任何优化不得削弱本阶段故障门禁。

## 2026-07-13 17:21 (UTC+8) — Phase 7 启动

### Checkpoint 7.0：性能基线目标

- 首轮只测量真实三节点 TCP 的串行 SET 与线性一致 GET，避免在没有数据前修改热路径。
- 固定集群规模、操作数、payload、durability 和统计方法，输出 throughput、P50、P99。
- 第一项候选优化为 Client completion 的 eventfd 精确唤醒；优化前后复用同一 benchmark。

### Checkpoint 7.1：Client completion eventfd 已保存

- 新增 `dkv_cluster_benchmark`，在真实三节点 TCP/三线程环境输出 SET/GET throughput、
  P50 和 P99。
- 基线 100 次串行操作：SET 6.68 ops/s、P50 151244 us；GET 8.46 ops/s、
  P50 119666 us。
- `Server::notifyResponses()` 复用 wake eventfd；Raft thread 入队完成响应后立即唤醒
  Client epoll，移除异步模式固定 10 ms polling。
- 优化后：SET 8.22 ops/s、P50 122257 us；GET 9.42 ops/s、P50 105800 us。
- Debug 73/73 通过（10.65 秒），Release 73/73 通过（10.20 秒），无新增 lint/编译告警。

### Checkpoint 7.2：增量 Raft journal 已保存

- 保留原 full record 兼容读取，新增 checksummed delta record：metadata + truncate index +
  changed suffix。
- conflict replacement、metadata-only save 和普通 append 都不再重复序列化到磁盘的历史
  日志；每条记录仍独立 `fdatasync`，不放松持久化屏障。
- 100 条 128-byte command 的格式级累计大小从 758728 bytes 降至 19878 bytes，
  write amplification 减少 97.38%。
- 新增 suffix replacement、线性文件增长和 torn delta tail 测试。
- 端到端吞吐未观察到提升，说明当前主瓶颈是同步次数而非 payload 大小；已如实记录。
- Debug 76/76 通过（10.16 秒），Release 76/76 通过（10.30 秒），无新增 lint/编译告警。

### Checkpoint 7.3：AppendEntries 持久屏障合并已保存

- 同一个 AppendEntries 内的 higher term、日志 suffix 和 commit index 变更合并为一次
  save/`fdatasync`，所有 return path 仍在响应前完成 barrier。
- 没有采用不安全的定时延后刷盘；任何 persistence failure 继续阻止 RPC response。
- 新增单次 barrier 同时覆盖 term/log/commit 的 RecordingPersistence 测试。
- 100 SET 两轮均值：6.64 → 7.55 ops/s（+13.7%），P50 156796 → 135171 us
  （-13.8%），P99 248820 → 200091 us（-19.6%）。
- Debug 77/77 通过（10.49 秒），Release 77/77 通过（10.36 秒），无新增 lint/编译告警。

### Checkpoint 7.4：零等待 ReadIndex batching 已保存

- 同一次 Raft event-loop drain 已到达的线性 GET 共享一个 read context 和 quorum probe。
- 放弃会损害本机延迟的固定 2 ms 等待窗口；默认只合并已经排队的请求，不人为延迟首读。
- 新增 read request/barrier 原子计数与 16 并发读集成测试。
- 80 GET / 16 并发：99.40 → 113.59 ops/s（+14.3%），barrier 80 → 18
  （-77.5%），P50 -3.2%，P99 -2.4%。
- Debug 78/78 通过（12.11 秒），Release 78/78 通过（12.20 秒），无新增 lint/编译告警。

### Checkpoint 7.5：Raft journal 原子 compaction 已保存

- `FileRaftPersistenceOptions` 新增 delta record 数和文件字节双阈值，默认 1024 / 64 MiB。
- 达到阈值后写入 checksummed full image 临时文件，执行 file `fdatasync`、atomic rename、
  parent-directory `fsync`，再把独占锁切换到新 inode。
- 新增测试覆盖多轮 compaction、文件大小上限、完整状态恢复、锁转移和临时文件清理。
- 明确边界：该功能限制 delta replay，不替代 Snapshot/log prefix truncation。
- Debug 79/79 通过（12.05 秒），Release 79/79 通过（11.87 秒），无新增 lint/编译告警。

### Checkpoint 8.1：StateMachine Snapshot 状态镜像已保存

- `StateMachineSnapshot` 现在同时捕获 applied index/term、确定性排序的 KV entries 和
  client-session 去重表，避免恢复后重复执行已确认请求。
- `KVStore` 新增一致性导出和原子全量替换；恢复先完整校验 metadata、session 和重复 key，
  再修改在线状态，非法镜像不会造成部分覆盖。
- 新增 snapshot/restore 后 KV 和 exactly-once 语义保持、非法 metadata 不修改在线状态测试。
- 当前仅完成内存状态镜像边界；checksummed 原子文件发布、Raft log offset 化、
  InstallSnapshot RPC 将按后续 checkpoint 分步实现。
- Debug 81/81 通过（11.98 秒），Release 81/81 通过（11.98 秒），无新增编译告警。

### Checkpoint 8.2：Snapshot 原子磁盘发布已保存

- 新增 `FileSnapshotStore`，使用独立 magic/version、big-endian 长度字段和 CRC32C 保存
  applied boundary、二进制 KV entries 与 client sessions。
- 发布顺序固定为同目录 temporary write → `fdatasync` → atomic rename → parent-directory
  `fsync`；只有完整 durable publication 才返回成功，临时文件在失败路径清理。
- 解码在分配和复制前限制 256 MiB payload、条目数量及 key/value/response 长度，并拒绝
  checksum、重复 key/client、非法 status、截断和 trailing bytes。
- 新增 5 个测试覆盖无文件、二进制 round-trip、连续原子替换、checksum corruption 和
  发布前语义校验。
- 当前 Snapshot 尚未接入 NodeService 启动恢复，也未触发 Raft log prefix truncation。
- Debug 86/86 通过（11.44 秒），Release 86/86 通过（11.96 秒），无新增编译告警。

### Checkpoint 8.3：Snapshot 启动恢复与 journal 边界校验已保存

- `NodeService` 在构造 Raft/KV service 前加载 `state.snapshot`；文件损坏立即拒绝启动，
  不再静默回退到仅 journal replay。
- `RaftKVService` 只接受 `last_included_index <= durable commit_index` 且 journal 对应
  index 的 term 与 `last_included_term` 完全一致的 Snapshot。
- 边界确认后先恢复 KV/session/applied index，再只重放 Snapshot 之后的 committed suffix，
  保持 exactly-once session 语义并跳过已快照命令。
- 新增测试覆盖 Snapshot + committed suffix 恢复、term mismatch 拒绝，以及真实
  `NodeService` 对损坏 Snapshot 的 fail-closed 启动行为。
- 当前 journal 仍保留完整前缀；下一 checkpoint 将把 `RaftLog` 改为 absolute-index
  offset 模型，为安全 prefix truncation 做准备。
- Debug 89/89 通过（11.52 秒），Release 89/89 通过（12.03 秒），无新增编译告警。

### Checkpoint 8.4：RaftLog absolute-index offset 模型已保存

- `RaftLog` 的 vector position 不再等于 absolute log index；`entries_[0]` 现在是可前移的
  `(snapshot_index, snapshot_term)` metadata boundary，新节点仍从 `(0,0)` 开始。
- `termAt`、`entryAt`、`entriesFrom`、suffix replacement 和 term boundary 查询统一通过
  absolute-index → vector-offset 映射，`lastIndex` 直接读取最后 entry 的绝对索引。
- 新增 `compactTo`：仅在本地存在且 term 匹配时删除前缀，将边界 command 替换为 no-op
  metadata；失败不会改变原日志。
- Leader conflict backtracking 的最小 `nextIndex` 改为 compacted boundary 之后，Follower
  对 boundary 之前的 `previous_log_index` 返回可识别的 conflict floor。
- 新增 4 个测试覆盖 offset restore、前缀压缩、压缩后 suffix replacement 和 conflict
  clamp。
- 当前 `FileRaftPersistence` 尚未接受 offset image，尚未在生产路径调用 `compactTo`。
- Debug 93/93 通过（11.80 秒），Release 93/93 通过（11.89 秒），无新增编译告警。
