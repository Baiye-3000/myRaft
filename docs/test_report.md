# DistributedKV 测试报告

## Phase 0

- 范围：CMake 工程骨架。
- 单元测试：无，本阶段没有业务实现。
- 环境：Linux x86_64，GCC 8.5.0，CMake。
- 命令：`cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build`。
- 构建验证：通过。
- 结论：Phase 0 工程配置有效，可以进入后续开发阶段。

后续每个阶段将在此记录环境、命令、测试数量、结果、失败分析和已知覆盖缺口。

## Phase 1

- 范围：线程安全内存 KVStore。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：构建成功，6/6 测试通过，耗时 0.04 秒。
- Release：构建成功，6/6 测试通过，耗时 0.02 秒。

覆盖场景：

1. 新 key 插入和查询。
2. 已有 key 覆盖且条目数不增长。
3. 缺失 key 返回 `std::nullopt`。
4. 删除已有 key 及重复删除。
5. 空 key/value 的存储语义。
6. 8 个线程并发写入和读取共 4000 个独立 key。

覆盖缺口：

- 尚未使用 ThreadSanitizer 动态检测数据竞争。
- 尚未执行长时间压力测试及内存分配失败注入。

结论：Phase 1 功能、Debug/Release 构建和测试门禁全部通过。

## Phase 2

- 范围：二进制协议、Connection、epoll Server、TCP Client 和命令行程序。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：最终构建成功，19/19 测试通过，耗时 0.05 秒。
- Release：构建成功，19/19 测试通过，耗时 0.03 秒。
- 手工 smoke test：`SET name tom` → `OK`，`GET name` → `tom`，
  `GET missing` → `NOT_FOUND`。

新增覆盖：

1. 请求/响应编码往返。
2. TCP 半包和粘包增量解析。
3. 错误 magic 与超限 key 拒绝。
4. Connection 非阻塞读写、EOF 和输出背压。
5. 真实 loopback TCP SET/GET 链路。
6. 缺失 key 的协议状态。
7. 8 个并发 TCP 客户端。
8. eventfd 有序关闭服务端。

覆盖缺口：

- 尚未进行慢客户端、连接耗尽和大 value 压力测试。
- 尚未注入 send/recv 短写之外的系统调用故障。
- 尚未执行 ASan/UBSan/TSan。

结论：Phase 2 功能、可执行程序、Debug/Release 构建和测试门禁全部通过。

## Phase 3

- 范围：版本化 WAL、CRC32C、文件锁、尾部恢复、同步策略与服务端重启恢复。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：构建成功，25/25 测试通过，耗时 0.06 秒。
- Release：构建成功，25/25 测试通过，耗时 0.05 秒。
- 手工重启：首次 SET 返回 `OK`；停止并重启服务端后 GET 返回 `tom`。

新增覆盖：

1. SET/REMOVE 多记录顺序重放。
2. 不完整 WAL 尾部检测、截断与同步。
3. 完整记录 checksum 损坏拒绝恢复。
4. 同一路径并发 writer 文件锁。
5. 未执行恢复前禁止 append。
6. WAL-first 服务端 SET 路径。
7. 销毁并重建 WAL、KVStore、Server 后通过真实 TCP 读取确认值。

覆盖缺口：

- 未模拟真实断电和磁盘控制器写缓存行为。
- 未注入 ENOSPC、EIO、fdatasync 失败及部分 pwrite。
- 尚未执行 sanitizer 和长时间并发写入压力测试。

结论：Phase 3 功能、恢复语义、Debug/Release 构建和测试门禁全部通过。

## Phase 4

- 范围：RaftLog、LeaderElection、LogReplication、RaftNode 和确定性三节点传输。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：构建成功，45/45 测试通过，耗时 0.09 秒。
- Release：构建成功，45/45 测试通过，耗时 0.07 秒。
- Raft 专项：20/20 测试通过。

新增覆盖：

1. 连续日志 append、幂等 suffix、冲突截断和非法间隙拒绝。
2. 每 term 一票、候选日志新旧比较和多数派计算。
3. Follower/Candidate/Leader 角色迁移及高 term 降级。
4. 心跳重置和随机化选举计时。
5. nextIndex/matchIndex、冲突快速回退及过期响应处理。
6. 当前 term 多数派提交规则和单节点立即提交。
7. 三节点 Leader 选举、单 Follower 丢包、多数派提交及恢复追赶。

覆盖缺口：

- 未接入真实 Peer TCP 传输、乱序网络调度和多进程测试。
- 未执行 Raft 持久状态崩溃恢复。
- 未覆盖 Snapshot、成员变更及极端 term/index 耗尽。

结论：Phase 4 Raft 核心算法、Debug/Release 构建和确定性测试门禁全部通过。

## Phase 5

- 范围：KV command codec、StateMachine、去重、RaftKVService、网络请求组合和持久化屏障。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：构建成功，58/58 测试通过，耗时 0.12 秒。
- Release：构建成功，58/58 测试通过，耗时 0.10 秒。

新增覆盖：

1. SET/DELETE command 二进制往返与输入限制。
2. no-op、SET、DELETE、NOT_FOUND 和严格应用顺序。
3. 相同 request 去重与 stale request 拒绝。
4. Follower 拒绝 submit 和单节点同步提交。
5. 三节点未提交不可见、多数派提交后 Leader 应用、Follower 最终收敛。
6. term/vote/log 持久化 barrier 调用及失败传播。
7. 真实 TCP SET 经 Raft commit 后才返回并可 GET。
8. client id 在网络协议中的编码往返。

覆盖缺口：

- 尚无 RaftPersistentState 磁盘恢复实现。
- 尚无真实 Peer TCP、多进程 Leader 切换和客户端异步等待测试。
- 尚未验证多节点线性一致 GET。

结论：Phase 5 Raft/KV 组合、Debug/Release 构建和测试门禁全部通过。

## Phase 6 — Checkpoint 6.1

- 范围：Raft journal save/load、尾部恢复、严格日志恢复和 committed state 重建。
- 环境：Linux x86_64，GCC 8.5.0，GoogleTest 1.17.0。
- Debug：构建成功，61/61 测试通过，耗时 0.13 秒。

新增覆盖：

1. 多个完整状态镜像恢复最后一份 durable state。
2. 不完整最终记录被安全截断，前一份状态不丢失。
3. 重启只应用 committed prefix，未提交后缀不可见。

待本阶段最终门禁补充：checksum 损坏测试、Release 全量和真实三进程恢复。

## Phase 6 — Checkpoint 6.2

- 范围：Peer Raft codec、半包、协议上限和 epoll 连接管理。
- Debug：构建成功，65/65 测试通过，耗时 0.14 秒。

新增覆盖：

1. AppendEntries 元数据、二进制 command 和 source NodeId 往返。
2. 不完整 frame 等待后续字节，不提前消费。
3. 超量 entry batch 在发送前拒绝。
4. AppendEntries 冲突响应的关联字段完整往返。

待集成测试覆盖：Peer TCP 重连、队列背压和三节点双向 RPC。

## Phase 6 — Checkpoint 6.3–6.5

- 范围：三线程 NodeService、异步 pending、DELETE、Leader hint/retry 和 ReadIndex barrier。
- Debug：真实 TCP 三节点 `node_service_test` 2/2 通过，耗时 3.78 秒。

新增覆盖：

1. 三个 NodeService 实例经独立 client/peer TCP 端口完成选举。
2. SET 在多数派 commit/apply 后响应，GET 经 quorum barrier 返回。
3. DELETE 复制后返回 OK，后续线性读返回 NOT_FOUND。
4. Follower 停止时多数派继续写；重启节点加载 journal 并重新加入。
5. DELETE request 与结构化 Leader hint 协议往返。
6. Raft read context 只由 Leader 发出并由 Follower response 关联返回。

## Phase 6 最终门禁

- Debug：73/73 测试通过，11.62 秒。
- Release：73/73 测试通过，11.33 秒。
- 真实三线程 TCP：2/2 场景通过。
- 真实三进程故障：2/2 场景通过。
- IDE lint：无新增诊断。
- GCC 严格选项：Debug/Release 无新增 warning。

三进程覆盖：

1. 自动选举及任意 Follower 的结构化 Leader hint 重定向。
2. SET/GET/DELETE、多数派 commit 后响应和 ReadIndex quorum。
3. 单节点停止后多数派继续写，节点重启后日志追赶。
4. Leader 终止后重新选举并保留已提交值。
5. 全节点终止/重启后恢复新旧 term 已提交数据。
6. 隔离旧 Leader 后本地有值仍因 read quorum timeout 拒绝读取。

持久化覆盖补齐：

- 最后完整镜像恢复、torn tail 截断、checksum corruption 拒绝、只重放 committed prefix。

未覆盖：

- InstallSnapshot/日志压缩、动态成员变更、跨机网络延迟分布和完整分区矩阵。
- 磁盘满、I/O 长时间卡顿、进程恰在每个 syscall 边界崩溃。
- TLS/Peer 密钥认证与恶意流量测试。

结论：Phase 6 计划内真实集群运行时、客户端语义、线性一致读和核心故障恢复门禁通过。

## Phase 7 — Checkpoint 7.1

- 变更：Client completion 从 10 ms polling 改为 eventfd 精确唤醒。
- Debug：73/73 通过，10.65 秒。
- Release：73/73 通过，10.20 秒。
- Benchmark：Release 三节点、100 SET + 100 linearizable GET 完成。
- 正确性：持久化、Leader 重选、旧 Leader 读隔离和全节点恢复测试全部保持通过。
- 静态检查：新增 benchmark 与修改文件无 lint 或严格编译告警。

## Phase 7 — Checkpoint 7.2

- 变更：Raft journal 从重复完整镜像改为 full base + incremental suffix delta。
- Debug：76/76 通过，10.16 秒。
- Release：76/76 通过，10.30 秒。
- 新增：suffix replacement 恢复、100-entry 线性增长上限、torn delta tail 回退。
- 兼容性：既有 full record magic/version 继续读取，delta 使用独立 record magic。
- 安全性：full/delta checksum、长度上限、同步 barrier 和 committed-prefix 重建保持不变。
- 性能结论：格式级写入量减少 97.38%；端到端延迟未改善，不作吞吐收益声明。

## Phase 7 — Checkpoint 7.3

- 变更：单个 AppendEntries 内 term/log/commit mutation 共用一次 durable barrier。
- Debug：77/77 通过，10.49 秒。
- Release：77/77 通过，10.36 秒。
- 新增：验证 response 前仅一次 save 即包含 term、entry 和 commit index。
- 性能：100 SET 两轮均值吞吐 +13.7%，P50 -13.8%，P99 -19.6%。
- 安全性：所有早退/冲突/成功路径保持 response-before-durability 禁止规则。

## Phase 7 — Checkpoint 7.4

- 变更：同一 Raft loop 已排队的 GET 共用 ReadIndex quorum barrier，不增加固定等待。
- Debug：78/78 通过，12.11 秒。
- Release：78/78 通过，12.20 秒。
- 新增：16 个并发线性读全部返回正确值，且 barrier 数严格少于 request 数。
- 性能：80 GET / 16 并发吞吐 +14.3%，quorum probes -77.5%，P50/P99 均未回退。
- 故障语义：batch 在 term change、step-down、timeout 时整体失败，旧 Leader 读测试保持通过。

## Phase 7 — Checkpoint 7.5

- 变更：按 delta count/file bytes 阈值原子 compact Raft journal。
- Debug：79/79 通过，12.05 秒。
- Release：79/79 通过，11.87 秒。
- 新增：多轮 compaction 后恢复完整 entries、文件大小受限、新 inode 保持独占锁。
- 发布顺序：temporary fdatasync → rename → parent fsync → fd/lock handoff。
- 未覆盖：各个 compaction syscall 的进程崩溃注入和磁盘空间耗尽。

## Phase 8 — Checkpoint 8.1

- 变更：StateMachine 可创建和恢复包含 KV、applied boundary 与 client sessions 的镜像。
- Debug：81/81 通过，11.98 秒。
- Release：81/81 通过，11.98 秒。
- 新增：恢复后 KV 内容、lastApplied 和重复请求缓存结果保持一致。
- 原子性：非法 index/term 镜像在修改 KVStore 前失败，在线数据和 applied index 不变。
- 未覆盖：Snapshot 文件 checksum/崩溃发布、日志前缀截断和 InstallSnapshot 网络恢复。

## Phase 8 — Checkpoint 8.2

- 变更：新增 checksummed、版本化、原子发布的 `FileSnapshotStore`。
- Debug：86/86 通过，11.44 秒。
- Release：86/86 通过，11.96 秒。
- 新增：无文件返回 nullopt、含 NUL 字节数据 round-trip、两代镜像替换、payload checksum
  损坏拒绝、重复 key 在发布前拒绝。
- 持久化顺序：temporary write → fdatasync → rename → parent fsync；成功后无临时文件。
- 未覆盖：syscall 逐点 crash injection、磁盘耗尽、NodeService 重启恢复及 journal
  boundary 协调。

## Phase 8 — Checkpoint 8.3

- 变更：NodeService 启动加载 Snapshot，RaftKVService 校验 journal/commit boundary 后
  恢复并重放 committed suffix。
- Debug：89/89 通过，11.52 秒。
- Release：89/89 通过，12.03 秒。
- 新增：index 2 Snapshot 恢复后仅应用 index 3；boundary term mismatch 拒绝构造；
  损坏 `state.snapshot` 导致真实 NodeService fail closed。
- 保持：既有三节点选举、持久重启、线性读和多进程故障测试全部通过。
- 未覆盖：截断 journal 后的启动、周期 Snapshot 生成及 InstallSnapshot。

## Phase 8 — Checkpoint 8.4

- 变更：RaftLog 改为 retained boundary + absolute-index offset，新增安全 `compactTo`。
- Debug：93/93 通过，11.80 秒。
- Release：93/93 通过，11.89 秒。
- 新增：index 100 boundary restore/lookup/append、index 2 前缀压缩及失败原子性、
  index 50 后缀冲突替换、Leader backtrack 不越过 index 40 boundary。
- 保持：零边界旧日志行为以及全套集群、持久化、线性读测试全部通过。
- 未覆盖：offset journal round-trip、生产路径 prefix truncation 和 InstallSnapshot。

## Phase 8 — Checkpoint 8.5

- 变更：Raft journal full/delta codec 与 `RaftNode` load 支持非零 Snapshot boundary。
- Debug：96/96 通过，12.77 秒。
- Release：96/96 通过，12.64 秒。
- 新增：index 100 full image round-trip、index 50 delta suffix、Snapshot + index 2
  compacted journal 服务重启恢复。
- 修复：`commit_index` 与 absolute `lastIndex` 比较，避免压缩日志被启动拒绝。
- 未覆盖：InstallSnapshot RPC。

## Phase 8 — Checkpoint 8.6

- 变更：`RaftKVService::tryPublishSnapshot` + `NodeService` 周期触发；boundary 变化时
  journal 写 full image。
- Debug：99/99 通过，12.00 秒。
- Release：99/99 通过，12.62 秒。
- 新增：阈值门控 Snapshot 发布与 log 前缀截断、发布后 snapshot+journal 重启恢复、
  boundary 变化 full image round-trip。
- 保持：三节点选举、持久重启、线性读、多进程故障及 offset journal 测试全部通过。
- 未覆盖：InstallSnapshot RPC、落后 Follower 被动安装 Snapshot。

## Phase 8 — Checkpoint 8.7.1

- 变更：`InstallSnapshotRequest/Response` 类型与 `RaftProtocol` wire type 5/6 编解码。
- Debug：102/102 通过，12.80 秒。
- 新增：InstallSnapshot 分块 round-trip、超大 chunk 拒绝、response round-trip。
- 未覆盖：Leader 发送、Follower 安装、NodeService 完整接线（8.7.2–8.7.4）。

## Phase 8 — Checkpoint 8.7.2

- 变更：InstallSnapshot 发送/接收核心逻辑；Follower 用 `replaceLogWithSnapshotBoundary`。
- Debug：107/107 通过，12.53 秒。
- 新增：snapshot 分块 IO、Leader 向落后 peer 发送、Follower 安装追上、boundary 压缩。
- 保持：8.7.1 协议测试及全套集群/持久化测试通过。
- 未覆盖：`node_service_test` 三节点截断后 InstallSnapshot 端到端门禁（8.7.4）。

## Phase 8 — Checkpoint 8.7.4

- 变更：三节点 TCP 集成测试；InstallSnapshot 安装/重启门禁；PeerTransport pending 与
  offset 重置修复。
- Debug：108/108 通过，15.07 秒。
- 新增：`LaggingNodeInstallsSnapshotAfterJournalCompaction`。
- 保持：8.7.2 单元测试及全套集群/持久化测试通过。
- Phase 8 Snapshot/InstallSnapshot 收尾完成。

## Phase 9.2 — Joint consensus 核心

- 变更：RaftNode 支持单步 ConfChange、joint 双多数派投票/提交、提交后自动退出 joint，
  并动态刷新成员资格与复制目标。
- 新增：`RaftNodeTest.CommitsJointMembershipChangeWithBothMajorities`，覆盖 3→4、重复
  propose 拒绝和最终稳定配置。
- Debug：113/113 通过。
- 未覆盖：Learner/catch-up、运行期 PeerTransport/NodeService 更新、Admin RPC、故障分区。

## Phase 9 — 后续计划

计划文档：[phase9_membership_plan.md](phase9_membership_plan.md)。

预期新增测试：

- 配置编解码与持久化 round-trip。
- Joint consensus 确定性单测（3→4→3、双多数派 commit、单步拒绝）。
- Learner catch-up 与 Snapshot 安装后入组。
- `node_service_test` 在线 ADD/REMOVE。
- `cluster_process_test` 扩容中 Leader 切换、分区、崩溃恢复（≥6 场景）。
- `dkv_admin` 端到端脚本门禁。

完成 Phase 9 后，「未覆盖」中的动态成员变更项应移入已覆盖列表。

## Phase 9.3 — Learner/catch-up 核心

- 变更：Learner 可接收日志与 Snapshot，不参与选举和 commit quorum；ADD 前必须追平
  `commitIndex`。
- 新增：`RaftNodeTest.LearnerCatchesUpBeforeMembershipChange`。
- 专项测试：Learner 追赶、禁止投票、追平后 ADD 均通过。
- 未覆盖：Learner 持久化、PeerTransport 动态 endpoint、NodeService 在线管理、Admin RPC。

## Phase 9.4 — PeerTransport/NodeService 运行期接线

- 变更：PeerTransport 使用线程安全更新队列在 epoll 线程执行 add/remove；NodeService 根据
  stable/joint 配置同步 endpoint 和连接集合；线性读同步采用 joint 双 quorum。
- 回归：Debug 全量 `114/114` 通过。
- 未覆盖：Admin RPC、`dkv_admin`、在线启动新进程的 endpoint 发现与故障分区门禁。

## Phase 9.5 — Admin 工具与在线扩缩容

- 变更：wire protocol v2 增加 ADD/REMOVE/LIST_MEMBERS；`dkv_admin` 支持 Leader hint；
  `dkv_node --learner` 安全启动待加入节点。
- 新增：2 个协议 round-trip 测试及
  `NodeServiceTest.AddsAndRemovesOnlineMember` 真实 TCP 3→4→3 测试。
- Debug：117/117 通过，16.90 秒。
- Release：117/117 通过，17.29 秒。
- 保持：Snapshot/InstallSnapshot、持久重启、线性读、Leader 重选举测试无回归。
- 未覆盖：成员变更中 Leader 宕机、网络分区、进程崩溃恢复矩阵（Phase 9.6）。

## Phase 9.6 — 故障门禁

- 新增真实进程测试：在线扩容并经新节点访问、四成员 Leader 故障、移除节点隔离、
  扩容配置和 KV 全量重启恢复、pre-joint/committed-joint Leader 崩溃恢复、仅旧配置
  多数派存活时禁止提交。成员变更相关真实进程场景共 7 个。
- 新增确定性测试：joint Leader 切换后恢复退出条目；仅旧配置多数派无法提交 ADD；
  未提交 joint 条目被覆盖后回滚临时配置；新成员 append joint 后立即成为 voter。
- 新增持久化故障注入：torn delta 后 stable/joint 配置保持最后完整状态。
- 手工门禁：`scripts/expand_cluster.sh` 完成 3→4→3，扩容前 KV 数据保持可读。
- Debug：129/129 通过，42.07 秒。
- Release：129/129 通过，40.63 秒。
- Phase 9 动态成员变更测试矩阵完成；剩余非目标为批量变更、自动发现、mTLS/ACL 和
  多 Raft Group。

## Phase 10.1 — Admin 成员操作幂等化

- 变更：wire protocol v3 为 ADD/REMOVE 增加非零 operation id；ConfChange joint/exit
  传播同一 ID；Raft 持久化 active 与最近一次 completed 操作。
- 新增 5 个确定性用例：ADD/REMOVE ID 与非法字段、legacy ConfChange 解码、成员操作状态
  落盘、joint/exit ID 传播和完成状态重启恢复。
- 扩展真实 TCP 用例：completed ADD/REMOVE 重放返回 `OK_REPLAYED`，ID 与载荷冲突返回
  `OPERATION_ID_REUSED`。
- 扩展真实进程用例：committed-joint Leader 崩溃后以相同 ID 向新 Leader 重试；全量重启
  后重放最近 completed ADD。
- Debug：134/134 通过，19.19 秒。
- Release：134/134 通过，19.30 秒，无新增编译告警。
