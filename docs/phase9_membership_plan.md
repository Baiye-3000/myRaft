# Phase 9：动态成员变更计划

> 状态：已于 2026-07-15 完成。Debug/Release 129/129 测试及 3→4→3 演示脚本通过。

本计划依据 Raft 论文 Section 6（Cluster Membership Changes）与 Figure 13 的
**joint consensus** 两阶段方法，为 DistributedKV 设计可在线扩容/缩容的成员变更能力。

## 1. 背景与目标

### 1.1 当前限制

- 成员列表在 `dkv_node` 启动时通过命令行静态注入。
- `RaftNode::NodeConfig.peers`、`PeerTransport` 端点表、`NodeService` quorum 均来自
  启动参数，**运行期不可变**。
- 单独启动新节点或只修改部分节点的 `MEMBERS` 会导致双 Leader、错误 quorum、复制目标
  不一致，**不安全**。

### 1.2 Phase 9 目标

在**不破坏既有 KV 正确性**的前提下，实现：

| 能力 | 说明 |
|------|------|
| 在线加节点 | 3 节点运行中安全扩到 4 节点，数据继续可写可读 |
| 在线删节点 | 4 节点安全缩到 3 节点 |
| 持久化成员配置 | 重启后成员与线上一致，不依赖启动参数覆盖 |
| 单步变更 | 每次仅增或删 **1** 个节点（论文推荐，避免组合爆炸） |
| 故障容忍 | Leader 切换、进程崩溃、网络分区下仍满足 Raft 安全不变量 |

### 1.3 前置依赖

Phase 9 开始前，建议 Phase 8 完成以下能力（当前 8.5 已就绪，8.6 待做）：

- Snapshot 发布 + journal 前缀截断接入 `NodeService`（新节点 catch-up 依赖 Snapshot +
  增量日志，否则全量复制成本过高）。
- `InstallSnapshot` RPC（落后新成员必须通过 Snapshot 安装追上状态机）。

若 8.6/8.7 未完成，Phase 9 的 catch-up 路径可先用**全日志复制**作为临时方案，但
门禁测试须在 Snapshot 路径就绪后补全。

---

## 2. Raft 论文核心约束

### 2.1 为什么不能直接改配置

任意时刻若旧配置与新配置的节点各自选举，可能同时出现两个 Leader。成员变更本身必须：

1. 作为 **普通 Raft 日志条目** 提交；
2. 在过渡态使用 **联合配置** \(C_{old,new}\)；
3. 对任何决策，**同时**获得 \(C_{old}\) 与 \(C_{new}\) 的多数派确认。

### 2.2 两阶段流程（论文 Figure 13）

```text
阶段 A — 进入联合配置
  Leader 追加 ConfChange(C_old → C_old,new)
  复制并提交
  所有节点进入 joint 模式：quorum = majority(C_old) ∩ majority(C_new) 的论文等价实现
       （实现上：对 commit 同时检查 old 与 new 两侧是否均达多数）

阶段 B — 退出到纯新配置
  Leader 追加 ConfChange(C_old,new → C_new)
  复制并提交
  所有节点切换到 C_new，quorum 仅按新成员数计算
```

### 2.3 单步变更规则

- 一次 ConfChange **只增加 1 个或删除 1 个** server（论文 Section 6.2）。
- 从 \(C_{old}\) 到 \(C_{new}\) 若相差超过 1 个节点，必须拆成多轮单步变更。
- 新加入节点在成为投票成员前，应先以 **Learner / 非投票** 身份复制日志（可选优化；
  首版可直接进入 joint，但 catch-up 必须在 ConfChange 提交前由 Leader 保证「足够新」）。

### 2.4 与 KV 状态机的关系

- ConfChange 是 **Raft 内部条目**，不写入 `KVStore`，由 `RaftNode` 在 `apply` 路径旁路
  处理。
- 已提交的 KV 命令顺序不受成员变更影响；成员变更只改变**后续**选举与复制的参与者集合。

---

## 3. 架构设计

### 3.1 新增类型

```text
ClusterMember {
  node_id
  client_host, client_port   // 客户端接入
  peer_host, peer_port       // Raft peer 接入
}

ClusterConfiguration {
  config_id          // 单调递增，用于去重与恢复
  members[]          // 含自身
}

ConfChangeType { kAddNode, kRemoveNode }

ConfChangeEntry {
  type
  member             // ADD 时完整端点；REMOVE 时仅 node_id 有效
  joint              // true = 进入联合配置；false = 退出到纯新配置
  target_config      // 变更后的完整成员快照（便于恢复与审计）
}
```

### 3.2 配置状态机（RaftNode 内部）

每个节点维护：

| 字段 | 含义 |
|------|------|
| `stable_config_` | 当前已提交并生效的配置 \(C_{stable}\) |
| `joint_config_` | 可选；联合配置 \(C_{old,new}\) 的并集 |
| `pending_change_` | Leader 已 propose、尚未 commit 的变更 |

状态转换：

```text
STABLE(C_old)
  --[ADD/REMOVE propose, joint=true]--> JOINT(C_old,new)   [pending]
  --[commit joint entry]-------------> JOINT(C_old,new)   [stable joint active]
  --[propose joint=false, C_new]-----> STABLE(C_new)      [pending]
  --[commit exit joint]--------------> STABLE(C_new)
```

### 3.3 Quorum 计算

```text
若在 JOINT 模式：
  old_quorum = majority(stable_config)
  new_quorum = majority(target_config 或 joint union)
  一次 commit 需要：old_side_ack >= old_quorum AND new_side_ack >= new_quorum

若在 STABLE 模式：
  quorum = majority(stable_config)
```

与现有 `LeaderElection::quorumSize` 统一封装为 `ConfigurationQuorum` 辅助类。

### 3.4 复制与连接

- **Leader** 在 propose ConfChange 后，对 `target_config` 中所有 peer（含新节点）发送
  `AppendEntries`。
- **PeerTransport** 新增 `addPeer(endpoint)` / `removePeer(node_id)`，在配置 commit 后
  由 Raft 事件线程调用（不阻塞 commit 路径）。
- 新节点进程可提前启动，但以 **Learner** 或「未入配置」模式仅接收复制；待 ConfChange
  提交后再参与投票（首版可在 Admin 触发 ADD 前要求新节点已在线并完成 catch-up）。

### 3.5 持久化

- `RaftPersistentState` 增加 `cluster_config` 与 `joint_config`（可选）字段。
- journal 编码与 Phase 8 offset model 兼容；配置变更不单独写文件。
- 启动时：**以持久化配置为准**，命令行 `MEMBERS` 降级为「仅用于首次 bootstrap」或
  「与持久化配置一致性校验」。

### 3.6 管理入口

首版提供 **运维命令**（二选一或都要）：

1. `dkv_admin add-node ...` / `remove-node ...` — 连接 Leader，提交 ConfChange。
2. HTTP-less 二进制 admin RPC（复用 peer 端口或独立 admin 端口）。

客户端 `dkv_client` 在成员变更后需能发现新端点：

- 短期：运维更新客户端 endpoints 列表；
- Phase 9 末期：`LIST_MEMBERS` 或 Leader 重定向携带完整成员 hints。

---

## 4. 分阶段交付（Checkpoint）

每完成一个 Checkpoint：编码 → 单测/集成测 → 更新 `development_log.md` /
`test_report.md` → Debug/Release 全量通过。

### Checkpoint 9.1：配置模型与持久化

**目标**：成员配置可编码、落盘、重启恢复。

**工作项**：

- 定义 `ClusterConfiguration`、`ConfChangeEntry`、`EntryType::kConfChange`。
- 扩展 `FileRaftPersistence` 编解码 `cluster_config`（及可选 `joint_config`）。
- `RaftNode` 构造时加载持久化配置；与 `NodeConfig` 合并策略文档化。
- 单测：配置 round-trip、损坏拒绝、与 offset journal 共存。

**门禁**：配置相关单测通过；既有 96 项回归不退化。

---

### Checkpoint 9.2：Joint consensus 核心

**目标**：Raft 层实现论文两阶段成员变更状态机（不含网络与 Admin）。

**工作项**：

- `RaftNode::proposeConfChange(Add|Remove)`：仅 Leader、每次一步、无并发 pending。
- 实现 JOINT 模式下 `advanceCommitIndex` 的双多数派判定。
- `applyConfChange`：更新 `stable_config_` / `joint_config_`，重算 `peers` 列表。
- 选举与投票：候选者日志比较使用配置内成员资格；非成员拒绝投票。
- 确定性单测：内存传输模拟 3→4→3 全路径、Leader 切换、重复 propose 拒绝。

**门禁**：≥8 个 Raft 确定性 conf change 测试；不变量：任意 term 至多一个 Leader。

---

### Checkpoint 9.3：Catch-up 与 Learner（新节点加入）

**目标**：新节点在成为投票成员前数据足够新。

**工作项**：

- 定义 Learner 角色（不参与选举、不计入 commit quorum，但接收复制）。
- Leader 在 ADD 的 joint 阶段前，先将新节点追至 `matchIndex >= commitIndex`（或安装
  Snapshot）。
- 依赖 Phase 8 `InstallSnapshot`（若未完成则本 checkpoint 用全日志追赶，并标注技术债）。
- 单测：落后新节点追赶、Snapshot 安装后参与 joint commit。

**门禁**：新节点追赶测试通过；落后节点不投票直至 catch-up 完成。

---

### Checkpoint 9.4：PeerTransport 与 NodeService 运行期接线

**目标**：配置提交后，真实 TCP 连接与三线程运行时同步更新。

**工作项**：

- `PeerTransport::addPeer` / `removePeer`：epoll 动态注册/注销、drain 旧连接。
- `NodeService` 监听 `RaftKVService` 配置变更回调，更新 quorum 计数与 client 重定向表。
- 进程内集成测试：3 节点集群运行中 Admin 触发 ADD，第 4 节点收到复制并参与 joint。

**门禁**：`node_service_test` 新增在线 ADD 用例；无死锁、无重复连接泄漏。

---

### Checkpoint 9.5：Admin 工具与客户端发现

**目标**：运维可操作的扩容/缩容命令。

**工作项**：

- `dkv_admin`：`add-node id,client-host,client-port,peer-host,peer-port`、
  `remove-node id`、`show-members`。
- Admin 请求仅 Leader 执行；Follower 返回 `NOT_LEADER` + hint。
- `dkv_client`：支持从响应中合并新成员 endpoints（可选配置刷新文件）。
- 文档：扩容/缩容操作手册与回滚策略。

**门禁**：手工脚本 `scripts/expand_cluster.sh` 可演示 3→4；缩容 4→3 后数据完整。

---

### Checkpoint 9.6：故障与分区门禁测试

**目标**：论文级安全场景覆盖。

**工作项**：

- 多进程测试 `cluster_process_test` 扩展：
  - 扩容中 Leader 宕机 → 新 Leader 继续完成或安全阻塞未完成变更；
  - 扩容中旧配置分区 → 不能单独 commit 仅旧 quorum 的变更；
  - 缩容中被移除节点隔离 → 集群仍可用；
  - 各阶段进程崩溃重启 → 配置与 KV 一致。
- 故障注入：ConfChange 条目半截持久化、torn journal + 配置恢复。

**门禁**：新增 ≥6 个集群级 conf change 测试；Debug/Release 全量通过；文档记录已知限制。

---

## 5. 测试矩阵

| 场景 | 类型 | 期望 |
|------|------|------|
| 3→4 在线 ADD | 多进程 | joint 提交后 4 节点 quorum，KV 不丢 |
| 4→3 REMOVE | 多进程 | 被删节点不再投票，数据可读 |
| 连续 ADD 两次 | 单测 | 第二次须等第一次完全退出 joint |
| 并发双 ADD | 单测 | 第二个 propose 拒绝 |
| ADD 非 Leader | Admin | NOT_LEADER |
| 新节点未 catch-up | 集成 | ADD 阻塞或保持 Learner |
| Leader 崩溃于 joint | 多进程 | 新 Leader 恢复变更状态机 |
| 分区旧多数 | 多进程 | 不能提交仅旧侧多数的 conf entry |
| 持久化重启 | 集成 | 配置与 Phase 8 snapshot 一致 |
| 客户端写后扩容 | E2E | 线性一致读仍正确 |

---

## 6. 安全不变量（评审清单）

实现与 Code Review 必须始终满足：

1. **Election Safety**：任一 term 最多一个 Leader（在各自生效配置下）。
2. **Leader Completeness**：已提交条目出现在所有更高 term Leader 的日志中。
3. **State Machine Safety**：所有节点按相同顺序 apply 相同 KV 命令。
4. **Config Safety**：已提交的 ConfChange 不会在未经过 joint 的情况下直接切换 quorum。
5. **Single-step**：单次变更最多改动 1 个成员。
6. **No double vote**：联合配置前后，节点 ID 不重复、不投双重票。

---

## 7. 非目标（Phase 9 不做）

- 一次变更多个节点（批量 ADD/REMOVE）。
- 跨数据中心自动发现（DNS、Service Mesh）。
- 自动负载均衡与读写分离。
- 成员变更期间的在线密钥轮换 / mTLS（可列 Phase 10）。
- 分片与多 Raft Group。

---

## 8. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Joint 实现复杂导致双 Leader | 先确定性单测，再多进程；严格双 quorum 单元测试 |
| 新节点拖慢 commit | Learner 追赶 + Snapshot；ADD 前 gate |
| 动态连接 epoll 竞态 | 配置变更仅在 Raft 线程 apply；Peer IO 线程队列化 |
| 持久化格式演进 | `config_id` + 版本号；旧 journal 可读 |
| 与 Phase 8 Snapshot 耦合 | 9.3 明确依赖；可先日志追赶后补 Snapshot 测试 |

---

## 9. 阶段门禁（Phase 9 完成定义）

满足以下全部条件方可宣告 Phase 9 完成：

1. Debug / Release 构建成功，全量测试通过（含新增 conf change 测试）。
2. 演示脚本完成 3→4 扩容与 4→3 缩容，KV 数据校验通过。
3. `architecture.md`、`development_log.md`、`test_report.md` 已更新。
4. 明确记录：仍不支持的操作（批量变更、mTLS 等）。
5. 代码评审确认第 6 节不变量均有测试覆盖。

---

## 10. 建议实施顺序（总览）

```text
Phase 8.6/8.7 收尾（Snapshot 截断 + InstallSnapshot）
    ↓
9.1 配置持久化
    ↓
9.2 Joint consensus（Raft 核心）
    ↓
9.3 Catch-up / Learner
    ↓
9.4 PeerTransport + NodeService
    ↓
9.5 dkv_admin + 客户端发现
    ↓
9.6 故障分区门禁测试 → Phase 9 完成
```

---

## 11. 参考

- Diego Ongaro, John Ousterhout, *In Search of an Understandable Consensus Algorithm
  (Extended Version)*, Section 6 — Cluster Membership Changes.
- 本项目：`docs/architecture.md` §13–14、`src/raft/raft_node.cpp`（quorum/peers）、
  `src/server/node_service.cpp`（静态 members）。
