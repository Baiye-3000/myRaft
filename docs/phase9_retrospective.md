# Phase 9 动态成员变更复盘

## 1. 结论

Phase 9 已实现单步 3→4→3 在线成员变更，包括配置持久化、joint consensus、Learner
追赶、动态 PeerTransport、Admin 协议和进程级故障恢复。

最终门禁包含 7 个成员变更相关真实 TCP/多进程场景：

1. 在线加入第四节点并经新节点读写。
2. 四成员稳定配置下 Leader 宕机后继续写入。
3. 移除 node 4 并隔离该进程后继续读写。
4. 四成员配置与 KV 数据全进程重启恢复。
5. Learner 追平、joint propose 前 Leader 崩溃并原地重启。
6. joint 已提交、exit 未提交时 Leader 崩溃，新 Leader 完成变更，旧 Leader 重启收敛。
7. joint 已 propose 后只保留旧配置多数派，确认新配置多数派不足时不能提交。

Debug/Release 全量测试和 `scripts/expand_cluster.sh` 3→4→3 演示共同构成完成门禁。

## 2. 最终状态机

```text
STABLE(C_old)
  |
  | register learner + catch up to commitIndex
  v
LEARNER_CAUGHT_UP
  |
  | append C_old,new (configuration takes effect locally on append)
  v
JOINT_ACTIVE(C_old, C_new)
  |
  | commit requires majority(C_old) AND majority(C_new)
  | append/commit C_new
  v
STABLE(C_new)
```

这里最重要的区别是：

- 配置条目在本地日志 **append 后就参与选举和 quorum 判断**；
- `commitIndex` 决定该配置是否不可回退；
- 未提交配置若被新 Leader 的日志覆盖，必须回滚本地临时配置；
- 已提交 joint 配置若缺少 exit 条目，新 Leader 必须重建并继续完成转换。

只维护“当前持久化配置”而不区分追加、提交和覆盖，会在 Leader 切换时产生幽灵配置或
选举死锁。

## 3. 故障测试发现的问题

### 3.1 新 Leader 不会退出已提交 joint

初版只在原 Leader 提交 joint 时自动追加 `C_new`。若它在两步之间宕机，新 Leader 能
恢复 joint 状态，却不会生成退出条目，集群永久停留在双 quorum。

修复：新 Leader 上任时扫描未提交日志，复用已有 exit；若不存在，则从 stable/joint
配置的单步差异重建 exit 条目。

### 3.2 未提交 joint 被覆盖后仍残留

Leader propose 时会立即激活 joint。若该条目尚未提交就被更高 term Leader 覆盖，本地
joint 状态也必须消失。否则该节点未来再次成为 Leader 时，可能恢复一个从未提交的成员
变更。

修复：记录 pending ConfChange 的日志位置和内容；AppendEntries 覆盖该条目时同步恢复
stable peers，并清除 pending/joint。

### 3.3 新节点在 joint 期间仍是 Learner

确定性测试没有暴露这一问题，因为原 Leader 始终存活。真实进程测试在 joint commit 后
杀死 Leader，剩余旧节点选举需要新配置多数派，但 node 4 仍拒绝投票，造成死锁。

修复：joint 条目一旦 append 到 node 4，本地身份立即从 Learner 切为 joint voter；若
条目后来被覆盖，再随临时 joint 一起回滚。

### 3.4 成员查询存在短暂视图差异

Admin 成功响应来自 Leader 的稳定配置，但随后 `LIST_MEMBERS` 可能连接到尚未收到最新
heartbeat 的 Follower，短暂返回上一版配置。这不是配置回退，但运维工具不能把一次
Follower 查询当作全局传播屏障。

处理：测试和演示脚本采用有界收敛等待；响应仍保持结构化 `config` endpoint 列表。

## 4. 安全不变量与证据

| 不变量 | 实现约束 | 主要测试 |
|---|---|---|
| Election Safety | stable/joint 成员资格限制投票；term 内单票 | `RaftNodeTest` 投票测试、joint Leader 崩溃测试 |
| Leader Completeness | 候选者日志新旧比较；配置也是 Raft 日志 | `NewLeaderRecoversCommittedJointExit`、全量重启 |
| State Machine Safety | ConfChange 不进入 KVStore，应用索引仍连续 | 在线扩缩容前后 KV 读写测试 |
| Config Safety | joint 同时检查 old/new 多数派 | `OldMajorityAloneCannotCommitJointChange` |
| Config Rollback | 未提交 joint 被覆盖时恢复 stable | `RollsBackOverwrittenUncommittedJointChange` |
| Single-step | ADD/REMOVE 每次仅允许一个节点 | proposal 校验与并发 Admin 拒绝 |
| Learner Safety | 追平前不投票；append joint 后才成为 voter | Learner 单测、pre-joint/joint 崩溃测试 |

## 5. 测试方法总结

单靠确定性 Raft 测试不足以证明运行时接线正确。Phase 9 最有效的组合是：

- 纯 Raft 测试验证 quorum 数学、日志覆盖和状态转换；
- 持久化测试验证 stable/joint 与 torn journal 恢复；
- NodeService 测试验证三线程和动态连接；
- 多进程测试验证真实 socket、进程死亡、重新选举和原地重启；
- 演示脚本验证用户实际执行的 CLI 工作流。

三个测试专用暂停点通过 `DKV_TEST_MEMBERSHIP_PAUSE_*` 环境变量启用：

- `learner-caught-up`
- `joint-proposed`
- `joint-committed`

暂停点默认关闭，只用于把异步流程稳定停在可注入 `SIGKILL` 或节点隔离的位置。相比依赖
固定 sleep，它能证明测试命中的确切协议阶段，也显著减少偶发失败。

## 6. 运维语义

- 新节点必须先用 `dkv_node --learner` 启动。
- `add-node` 仅在 Learner 追平且最终稳定配置提交后返回成功。
- `remove-node` 返回成功后，被移除节点不再参与后续 quorum。
- Admin CLI 在发送前打印 operation id；请求丢失响应时，以相同命令和相同 ID 重试。
- 配置传播是最终收敛的；Follower 的 `show-members` 不是线性一致读。

## 7. 仍然存在的限制

- 只支持单节点 ADD/REMOVE，不支持批量配置变更。
- Admin ADD/REMOVE 已按持久化 operation id 幂等：active 操作可在 Leader 切换后重新绑定，
  最近一次 completed 操作可在进程重启后返回 `OK_REPLAYED`。当前仅保存最近一条 completed
  记录；后续成员变更完成后，更早 ID 不再保证可重放。
- 故障门禁覆盖进程停止形成的对称不可达和 quorum 不足，尚未覆盖单向丢包、消息重排、
  长时间延迟等完整链路分区矩阵。
- 仅支持显式 IPv4 endpoint，不包含 DNS/服务发现、mTLS、ACL 或节点身份认证。
- 不支持多 Raft Group、分片和跨 Group 事务。

这些限制不破坏当前单 Group、受信网络、单步变更模型下的安全性，但在生产化之前必须
作为独立阶段处理。

## 8. 后续建议

1. 增加可编程链路代理，覆盖单向分区、延迟、重复和乱序，而不依赖进程停止近似网络故障。
2. 将成员配置纳入 Snapshot 元数据，缩短仅靠日志恢复配置历史的路径。
3. 按运维审计窗口把 completed operation 从单条扩展为有界历史。
4. 增加节点身份认证和 mTLS，避免未授权 peer/Admin 参与共识。
5. 在引入多 Raft Group 前，把当前配置状态机抽成可独立模型检查的模块。
