# DistributedKV

DistributedKV 是一个从零设计的 C++17 分布式 Key-Value 存储系统。系统将使用
Raft 在多个节点间复制命令，并提供 `SET`、`GET`、`DELETE` 三类客户端操作。

本项目参考 Raft 论文及公开实现的工程思想，但独立设计协议、模块边界、持久化格式
和并发模型。

## 当前状态

已完成 **Phase 9：动态成员变更**。系统具备独立 Client/Peer TCP 端口、三线程节点
运行时、Raft journal 恢复、Leader 重定向、异步 commit 响应、DELETE 和多数派确认的
线性一致 GET。三进程测试覆盖 Leader 重选、少数节点故障、隔离 Leader 禁止读和全节点
持久化重启。Phase 7 已建立三节点性能基线，并完成 Client completion eventfd 精确唤醒。
Raft journal 已采用兼容旧格式的 full base + incremental suffix delta，append-heavy
格式级写入量降低 97.38%；单个 AppendEntries 的 term/log/commit durability barrier
已安全合并，100 SET 基准吞吐进一步提升 13.7%。零等待 ReadIndex batching 将 16 并发
GET 的 quorum probes 减少 77.5%，吞吐提升 14.3%。
Raft journal 支持按 delta 数量/文件大小阈值执行 checksummed full-image 原子 compaction，
限制增量记录回放长度。

## 已实现能力

- C++17 与 CMake 构建
- 基于 Linux epoll 的自研 TCP 通信层
- 自定义长度前缀二进制协议
- WAL/Raft journal 与内存 KV 表
- Raft Leader 选举、日志复制和故障恢复
- GoogleTest 单元测试与集群故障测试
- Joint consensus 在线单节点扩容/缩容、Learner 追赶与 `dkv_admin`

## 目录

```text
DistributedKV/
├── CMakeLists.txt
├── docs/
├── src/
│   ├── client/
│   ├── network/
│   ├── raft/
│   └── storage/
└── tests/
```

详细设计见 [架构文档](docs/architecture.md)，开发时间线见
[开发日志](docs/development_log.md)。Phase 9 动态成员变更计划见
[phase9_membership_plan.md](docs/phase9_membership_plan.md)，实现复盘与剩余风险见
[phase9_retrospective.md](docs/phase9_retrospective.md)。

## 构建与测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
```

Release 三节点基准：

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --target dkv_cluster_benchmark
./build-release/dkv_cluster_benchmark 100
```

## 运行三节点集群

```bash
MEMBERS="1,127.0.0.1,7101,127.0.0.1,8101 2,127.0.0.1,7102,127.0.0.1,8102 3,127.0.0.1,7103,127.0.0.1,8103"
./build/dkv_node 1 ./data/node1 $MEMBERS
./build/dkv_node 2 ./data/node2 $MEMBERS
./build/dkv_node 3 ./data/node3 $MEMBERS
```

以上三条节点命令分别在三个终端运行。客户端可连接任一存活节点：

```bash
./build/dkv_client 127.0.0.1 7102
```

客户端命令：

```text
SET name tom
GET name
DELETE name
QUIT
```

Phase 3 单机模式仍保留为 `dkv_server`，其命令 WAL 不与 Raft journal 混用。

## 在线扩容与缩容

先以 Learner 模式启动待加入的 node 4。其启动参数包含旧集群和自身 endpoint，但在
joint consensus 提交前不会发起选举或投票：

```bash
MEMBERS4="$MEMBERS 4,127.0.0.1,7104,127.0.0.1,8104"
./build/dkv_node 4 ./data/node4 --learner $MEMBERS4
./build/dkv_admin 127.0.0.1 7101 add-node 4 127.0.0.1 7104 127.0.0.1 8104
./build/dkv_admin 127.0.0.1 7101 show-members
```

`add-node` 会等待 Learner 追平，并在进入、退出 joint consensus 后返回。缩容同样只在
新稳定配置提交后返回：

```bash
./build/dkv_admin 127.0.0.1 7101 remove-node 4
```

ADD/REMOVE 可连接任一成员；Follower 会返回结构化 Leader endpoint，Admin 客户端自动
重试。CLI 会在发送前输出 `operation-id`；响应丢失时，将该 ID 作为命令最后一个参数
重试即可，例如 `remove-node 4 123456`。同一 ID 只能绑定完全相同的操作载荷。
每次只能增删一个节点，变更进行中会拒绝其他管理请求。

完整的本机 3→4→3 演示可直接运行：

```bash
./scripts/expand_cluster.sh ./build
```
