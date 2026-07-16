#pragma once

#include <optional>
#include <string>
#include <vector>

#include "raft/cluster_config.h"
#include "raft/types.h"

namespace distributed_kv::raft {

struct RaftPersistentState {
  Term current_term{0};
  std::optional<NodeId> voted_for;
  std::vector<LogEntry> entries;
  LogIndex commit_index{0};
  std::optional<ClusterConfiguration> cluster_config;
  std::optional<ClusterConfiguration> joint_config;
  std::optional<MembershipOperation> active_membership_operation;
  std::optional<MembershipOperation> completed_membership_operation;
};

/**
 * Synchronous persistence barrier for Raft safety-critical state.
 *
 * Implementations must not return true until current_term, voted_for and log
 * entries are durable as one recoverable state. RaftNode invokes save before
 * returning RPCs that depend on newly-mutated persistent state.
 */
class RaftPersistence {
 public:
  /**
   * Releases implementation resources.
   *
   * Input/output: implementation-defined.
   * Thread safety: no save may race with destruction.
   */
  virtual ~RaftPersistence() = default;

  /**
   * Loads the latest complete persistent-state image.
   *
   * Input: writable state and error.
   * Output: true with term-zero empty state when no image exists.
   * Thread safety: called during node construction before concurrent access.
   */
  [[nodiscard]] virtual bool load(RaftPersistentState& state,
                                  std::string& error) {
    state = RaftPersistentState{};
    error.clear();
    return true;
  }

  /**
   * Atomically saves one complete Raft persistent-state image.
   *
   * Input: immutable state and writable error.
   * Output: true only after the state is durable.
   * Thread safety: called by the owning Raft event thread.
   */
  [[nodiscard]] virtual bool save(const RaftPersistentState& state,
                                  std::string& error) = 0;
};

}  // namespace distributed_kv::raft
