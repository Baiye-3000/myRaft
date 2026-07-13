# DistributedKV 优化记录

性能优化从 Phase 7 开始，以可重复的基准数据为依据。任何优化必须记录测试环境、负载、
数据集、优化前后吞吐与延迟，以及对正确性和复杂度的影响。

## 候选方向

- 网络：epoll ET、批量读写、减少复制、连接背压。
- 存储：WAL group commit、预分配、Snapshot 写入优化。
- Raft：扩大安全批次、流水复制、read barrier 合并、心跳抑制。
- 线程：有界 worker pool、任务批处理和线程亲和性评估。

## Phase 6 基线边界

Phase 6 优先完成正确性门禁，尚未宣称性能优化：

- 每次 Raft persistent-state 变化追加完整镜像并 `fdatasync`，写放大明显。
- 每个线性读创建独立 quorum context，尚未合并同一轮 heartbeat 内的读。
- Client completion 由 10 ms epoll timeout 检查响应队列；后续可改为 eventfd 精确唤醒。
- Peer 未连接队列和 Connection 字节缓冲均有硬上限，避免故障期间无限内存增长。

Phase 7 必须先建立吞吐、P50/P99 延迟、WAL sync 延迟和恢复时间基线，再实施 group commit、
增量 journal/snapshot、read batching 或 ET epoll。

## Checkpoint 7.1：Client completion eventfd

测试环境与负载：

- Release 构建，Linux x86_64，本机 loopback 三节点。
- 每个节点运行 Client IO、Peer IO、Raft 三线程。
- 串行执行 100 次 SET（64-byte value），随后对相同 key 执行 100 次线性一致 GET。
- Raft journal 保持每次状态变化 `fdatasync`，没有降低 durability。

优化前（Client IO 最多等待 10 ms polling timeout 才发现 completion）：

- SET：6.68 ops/s，P50 151244 us，P99 224640 us。
- GET：8.46 ops/s，P50 119666 us，P99 180802 us。

优化后（Raft thread 入队 response 后写 eventfd，Client epoll 立即唤醒）：

- SET：8.22 ops/s，P50 122257 us，P99 196625 us。
- GET：9.42 ops/s，P50 105800 us，P99 166933 us。

变化：

- SET throughput +23.1%，P50 -19.2%，P99 -12.5%。
- GET throughput +11.3%，P50 -11.6%，P99 -7.7%。

结论：移除固定 completion polling 延迟有稳定的方向性收益，且不改变 Raft/durability
语义。当前数字是单次本机基线，不作为跨机器容量结论；后续 benchmark 将增加重复轮次、
并发度和方差输出。

## Checkpoint 7.2：增量 Raft journal

优化前，每次 term/vote/commit/log 变化都追加完整 `RaftPersistentState`，日志持续增长时
累计写入量为 O(n²)。优化后：

- 第一条记录和显式新基线仍使用 full record。
- 后续记录保存 term、vote、commit index、共同前缀 truncate index 和变化 suffix。
- conflict replacement 可截断旧后缀再追加新 entries；metadata-only 更新不重复写日志。
- full/delta 分别使用独立 record magic，共用 CRC32C、长度限制、torn-tail 修复和
  `fdatasync` barrier；旧 full-only 文件无需迁移即可读取。

固定 100 次 append、每条 command 128 bytes 的格式级测试：

- 旧 full-image 累计文件大小：758728 bytes。
- 新 full + delta 累计文件大小：19878 bytes。
- 写入与文件增长减少 97.38%，由二次增长降为线性增长。

端到端 100 SET benchmark 重复两次得到 6.36/6.92 ops/s，没有观察到延迟收益，且低于
Checkpoint 7.1 的单次 8.22 ops/s。原因是每次状态变化仍执行相同次数的 `fdatasync`，
payload 缩小不等于同步次数减少；本机单次数据也存在明显波动。因此本检查点只确认
write amplification 改善，不宣称吞吐提升。后续 group commit 必须在不破坏 Raft
持久化先行规则的前提下减少 sync 次数。

## Checkpoint 7.3：AppendEntries barrier coalescing

不能简单把同步 `RaftPersistence::save()` 改成定时刷盘：RequestVote/AppendEntries 响应在
返回 Peer IO 前必须依赖 durable term、vote 和 log，否则崩溃后可能违反 Raft safety。
本检查点采用请求边界内的安全合并：

- `handleAppendEntries()` 累积 term、log suffix、commit index 的 mutation。
- 所有成功和冲突返回路径统一经过 `finish()` persistence barrier。
- 一个 RPC 即使同时推进 term、写入 entries 和 commit，也只执行一次 save/`fdatasync`。
- barrier 仍严格发生在 response 返回之前；失败继续抛出 fatal error，不发送响应。

100 次串行 SET，Checkpoint 7.2 优化前重复两轮平均值：

- throughput 6.64 ops/s，P50 156796 us，P99 248820 us。

合并后重复两轮平均值：

- throughput 7.55 ops/s，P50 135171 us，P99 200091 us。
- throughput +13.7%，P50 -13.8%，P99 -19.6%。

GET 数据波动较大且不经过 journal mutation，不归因于本优化。进一步跨 RPC group commit
需要异步 durability completion 和批次内 response barrier，不能在当前同步接口上通过
延迟 `fdatasync` 冒险实现。

## Checkpoint 7.4：零等待 ReadIndex batching

最初尝试 2 ms 固定聚合窗口：80 GET / 16 并发时 barrier 从 80 降到 6，但 throughput
从 99.40 降到 89.24 ops/s，P99 从 196795 增至 234296 us。固定等待在本机低延迟环境
得不偿失，因此没有作为默认策略。

最终实现只合并 Raft event loop 单次 drain 已经到达的 GET，不增加定时等待：

- 同批请求共享 term、read index、context、quorum acknowledgements 和 deadline。
- 多数派确认后，每个请求仍使用自己的 key/connection/request id 独立完成。
- term/role 变化和 timeout 对整个 batch 一致失败，不允许部分请求绕过 barrier。
- 暴露原子 `read_requests/read_barriers` counters，用于验证聚合比例。

80 GET / 16 并发对比：

- 未批处理：99.40 ops/s，P50 153673 us，P99 196795 us，80 barriers。
- 零等待批处理：113.59 ops/s，P50 148704 us，P99 192104 us，18 barriers。
- throughput +14.3%，P50 -3.2%，P99 -2.4%，Peer quorum probes 减少 77.5%。

## Checkpoint 7.5：Raft journal 原子 compaction

增量 delta 将增长从二次降为线性，但如果永久保留所有 delta，文件大小和启动 replay 时间
仍会无限增长。`FileRaftPersistenceOptions` 现在提供双阈值：

- 默认最多 1024 个 delta records。
- 默认最大 journal 文件 64 MiB。

任一阈值达到后，在持久化线程内生成只含 header + 最新 full image 的临时文件，依次执行：

1. 写入完整 checksummed image 并 `fdatasync` 临时文件。
2. `rename` 原子替换 journal 路径。
3. `fsync` 父目录确保 rename durable。
4. 将独占锁和后续 append 切换到新 inode。

测试使用每 4 个 delta compaction，连续追加 10 条 128-byte command 后文件保持低于
5000 bytes；重启恢复完整状态，旧 inode 锁正确转移，临时文件不残留。

该优化限制 delta replay 长度，但 full image 仍包含完整 Raft log。真正让长期空间与恢复
时间独立于历史长度仍需要 StateMachine snapshot + Raft log prefix truncation；频繁
compaction 还会增加 full-image 写入，因此生产阈值必须结合日志大小和恢复目标调优。

## StateMachine Snapshot 基础（Checkpoint 8.1）

状态机现在能够在单一 applied boundary 导出完整 KV 数据与 client-session 去重状态，并
原子恢复。该步骤本身不减少磁盘占用，因此不声明吞吐或空间收益；它建立了安全前缀截断
必须依赖的状态边界。后续只有在镜像完成 checksum 校验、file sync、atomic rename 和
directory sync 后，Raft journal 才能删除 `last_included_index` 之前的 entries。

Checkpoint 8.2 已完成上述 durable publication：Snapshot 使用 256 MiB 总上限和字段级
上限避免损坏长度导致无界分配，并通过 CRC32C 与原子替换缩短恢复路径。当前仍不声明空间
收益，因为 journal 尚未依据 `last_included_index` 删除前缀；下一性能检查点应比较固定
历史长度下压缩前后的 journal bytes 与启动 replay 时间。

Checkpoint 8.3 已将 Snapshot 接入启动恢复：匹配 journal boundary 后，状态机跳过
Snapshot 已覆盖的 committed entries，只重放后缀。当前 journal 解码本身仍扫描完整历史，
所以这只减少 command apply 工作，不足以限制总启动时间；必须完成 absolute-index log 与
持久化前缀截断后再测量恢复收益。

Checkpoint 8.4 已完成内存日志的 absolute-index offset 和安全 `compactTo`，从算法层面
解除 vector 大小随历史 index 增长的约束。该能力尚未接入持久化和自动触发，因此本
checkpoint 不声明磁盘空间或恢复时间收益；下一步需让 full/delta journal codec 接受
非零 boundary，并验证 compacted journal 重启。
