#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "raft/cluster_config.h"
#include "raft/raft_log.h"
#include "raft/raft_persistence.h"
#include "raft/types.h"

namespace distributed_kv::raft {

struct NodeConfig {
  NodeId node_id{0};
  std::vector<NodeId> peers;
  std::optional<ClusterConfiguration> bootstrap_cluster;
  std::uint64_t election_timeout_min_ms{150};
  std::uint64_t election_timeout_max_ms{300};
  std::uint64_t heartbeat_interval_ms{50};
  std::size_t maximum_entries_per_append{64};
  std::uint64_t random_seed{1};
  // Runtime-only non-voting replicas. They receive replication but never
  // contribute to elections or commit quorums.
  std::vector<NodeId> learners;
  bool learner{false};
};

/**
 * Deterministic, single-threaded Raft consensus state machine.
 *
 * RaftNode never performs network or wall-clock I/O. Callers advance logical
 * time and deliver typed RPCs, then send returned OutboundRpc messages.
 */
class RaftNode final {
 public:
  /**
   * Validates configuration and creates a term-zero follower.
   *
   * Input: fixed cluster membership, timer limits, and optional synchronous
   * persistence barrier that outlives the node.
   * Output: initialized node; invalid configuration throws invalid_argument.
   * Thread safety: construction requires exclusive access.
   */
  explicit RaftNode(NodeConfig config,
                    RaftPersistence* persistence = nullptr);

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;
  RaftNode(RaftNode&&) = delete;
  RaftNode& operator=(RaftNode&&) = delete;

  /**
   * Advances logical time and emits election or heartbeat RPCs.
   *
   * Input: elapsed monotonic milliseconds.
   * Output: zero or more outbound RPCs.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> tick(std::uint64_t elapsed_ms);

  /**
   * Processes a RequestVote request.
   *
   * Input: candidate request.
   * Output: response reflecting current term and one-vote/log freshness rules.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] RequestVoteResponse handleRequestVote(
      const RequestVoteRequest& request);

  /**
   * Processes one peer's RequestVote response.
   *
   * Input: responding peer and response.
   * Output: initial AppendEntries RPCs if this response creates a quorum.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> handleRequestVoteResponse(
      NodeId source, const RequestVoteResponse& response);

  /**
   * Processes an AppendEntries request and mutates follower log/commit state.
   *
   * Input: leader request.
   * Output: success or conflict-hint response.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] AppendEntriesResponse handleAppendEntries(
      const AppendEntriesRequest& request);

  /**
   * Processes one follower's AppendEntries response.
   *
   * Input: responding peer and correlated response metadata.
   * Output: retry or next-batch RPCs when needed.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> handleAppendEntriesResponse(
      NodeId source, const AppendEntriesResponse& response);

  /**
   * Appends a client command when this node is Leader.
   *
   * Input: opaque state-machine command bytes.
   * Output: accepted index and replication RPCs, or rejected result.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] ProposeResult propose(std::string command);

  [[nodiscard]] ProposeResult proposeConfChange(ConfChangeType type,
                                                 ClusterMember member,
                                                 std::uint64_t operation_id = 0);

  [[nodiscard]] bool addLearner(NodeId learner_id);
  [[nodiscard]] bool learnerCaughtUp(NodeId learner_id) const noexcept;

  /**
   * Creates quorum probes tagged for one linearizable read.
   *
   * Input: non-zero opaque read context.
   * Output: current-term AppendEntries probes, empty when not Leader.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> makeReadBarrier(
      std::uint64_t read_context) const;

  /**
   * Returns the current role.
   *
   * Input: none.
   * Output: Follower, Candidate, or Leader.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] Role role() const noexcept;

  /**
   * Returns the current term.
   *
   * Input: none.
   * Output: current monotonically increasing term.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] Term currentTerm() const noexcept;

  /**
   * Returns the currently known Leader.
   *
   * Input: none.
   * Output: Leader id or std::nullopt.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::optional<NodeId> leaderId() const noexcept;

  /**
   * Returns the highest committed log index.
   *
   * Input: none.
   * Output: commit index.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] LogIndex commitIndex() const noexcept;

  /**
   * Returns the durable stable cluster configuration when one is known.
   */
  [[nodiscard]] const std::optional<ClusterConfiguration>& clusterConfig()
      const noexcept;

  /**
   * Returns the optional joint cluster configuration during membership change.
   */
  [[nodiscard]] const std::optional<ClusterConfiguration>& jointConfig()
      const noexcept;

  [[nodiscard]] const std::optional<MembershipOperation>&
  activeMembershipOperation() const noexcept;

  [[nodiscard]] const std::optional<MembershipOperation>&
  completedMembershipOperation() const noexcept;

  /**
   * Exposes immutable log inspection for application/testing integration.
   *
   * Input: none.
   * Output: borrowed log reference.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] const RaftLog& log() const noexcept;

  /**
   * Drops log prefixes through boundary and persists the compacted journal.
   *
   * Input: an applied absolute index and its expected term.
   * Output: true after in-memory compaction and durable save; false leaves
   * the log unchanged.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] bool compactLogPrefix(LogIndex boundary_index,
                                      Term boundary_term,
                                      std::string& error);

  /**
   * Reports whether a follower replication cursor is behind the local
   * snapshot boundary and therefore needs InstallSnapshot.
   */
  [[nodiscard]] bool peerNeedsSnapshot(NodeId peer) const;

  /**
   * Returns the configured local node id.
   */
  [[nodiscard]] NodeId nodeId() const noexcept;

  /**
   * Accepts a valid Leader heartbeat term and id, updating follower state.
   */
  [[nodiscard]] bool acknowledgeLeader(Term term, NodeId leader_id);

  /**
   * Replaces the log through a snapshot boundary and advances commit index.
   */
  [[nodiscard]] bool applySnapshotBoundary(LogIndex boundary_index,
                                           Term boundary_term,
                                           std::string& error);

  /**
   * Replaces the entire log with one snapshot boundary entry.
   */
  [[nodiscard]] bool replaceLogWithSnapshotBoundary(
      LogIndex boundary_index, Term boundary_term, std::string& error);

  /**
   * Records successful snapshot installation for one follower.
   */
  void onInstallSnapshotSuccess(NodeId peer,
                                LogIndex last_included_index);

  /**
   * Steps down when a peer reports a newer term.
   */
  void observeHigherTerm(Term term);

 private:
  /**
   * Starts a new election and emits vote requests or single-node leadership.
   *
   * Input: none.
   * Output: vote or initial replication RPCs.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> startElection();

  /**
   * Transitions to follower, updating term/vote when term increases.
   *
   * Input: observed term and optional Leader.
   * Output: follower state with reset election timer.
   * Thread safety: owning Raft event thread only.
   */
  void becomeFollower(Term term, std::optional<NodeId> leader);

  /**
   * Transitions Candidate to Leader and appends a no-op entry.
   *
   * Input: none.
   * Output: initial replication RPCs.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> becomeLeader();

  /**
   * Builds one AppendEntries from a follower's nextIndex.
   *
   * Input: configured peer.
   * Output: correlated replication RPC.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] OutboundRpc makeAppendEntries(NodeId peer) const;

  /**
   * Builds AppendEntries for every peer.
   *
   * Input: none.
   * Output: one RPC per peer.
   * Thread safety: owning Raft event thread only.
   */
  [[nodiscard]] std::vector<OutboundRpc> replicateToAll() const;

  /**
   * Advances Leader commit index using current-term majority replication.
   *
   * Input: current matchIndex map.
   * Output: monotonically advanced commit index.
   * Thread safety: owning Raft event thread only.
   */
  void advanceCommitIndex();

  /**
   * Selects a new deterministic pseudo-random election timeout.
   *
   * Input: internal PRNG state and configured range.
   * Output: updated timeout and PRNG state.
   * Thread safety: owning Raft event thread only.
   */
  void resetElectionTimer() noexcept;

  /**
   * Tests fixed-cluster membership.
   *
   * Input: node id.
   * Output: true for self or configured peer.
   * Thread safety: configuration is immutable after construction.
   */
  [[nodiscard]] bool isMember(NodeId node_id) const noexcept;
  [[nodiscard]] bool isVotingMember(NodeId node_id) const noexcept;

  /**
   * Returns votes/replicas required for a strict majority.
   *
   * Input: fixed membership size.
   * Output: quorum count.
   * Thread safety: configuration is immutable.
   */
  [[nodiscard]] std::size_t quorumSize() const noexcept;

  [[nodiscard]] bool hasElectionQuorum() const noexcept;
  [[nodiscard]] bool hasReplicationQuorum(LogIndex index) const noexcept;
  void applyCommittedConfiguration();
  void applyConfigurationEntry(const ConfChangeEntry& entry);
  void recoverJointExitEntry();
  [[nodiscard]] std::optional<ConfChangeEntry> makeJointExitEntry() const;
  [[nodiscard]] std::vector<NodeId> configurationMemberIds(
      const ClusterConfiguration& config) const;

  /**
   * Saves current term, vote and complete log or throws runtime_error.
   *
   * Input: current safety-critical state.
   * Output: durable barrier completion.
   * Thread safety: owning Raft event thread only.
   */
  void persistOrThrow();

  NodeConfig config_;
  RaftPersistence* persistence_;
  Role role_{Role::kFollower};
  Term current_term_{0};
  std::optional<NodeId> voted_for_;
  std::optional<NodeId> leader_id_;
  RaftLog log_;
  LogIndex commit_index_{0};
  std::uint64_t election_elapsed_ms_{0};
  std::uint64_t heartbeat_elapsed_ms_{0};
  std::uint64_t election_timeout_ms_{0};
  std::uint64_t random_state_{1};
  std::unordered_set<NodeId> votes_received_;
  std::unordered_map<NodeId, LogIndex> next_index_;
  std::unordered_map<NodeId, LogIndex> match_index_;
  std::optional<ClusterConfiguration> cluster_config_;
  std::optional<ClusterConfiguration> joint_config_;
  std::optional<MembershipOperation> active_membership_operation_;
  std::optional<MembershipOperation> completed_membership_operation_;
  LogIndex configuration_applied_index_{0};
  std::optional<LogIndex> pending_conf_change_index_;
};

}  // namespace distributed_kv::raft
