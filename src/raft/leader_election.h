#pragma once

#include <cstddef>

#include "raft/raft_log.h"
#include "raft/types.h"

namespace distributed_kv::raft {

/**
 * Stateless election rules shared by RaftNode and deterministic tests.
 */
class LeaderElection final {
 public:
  LeaderElection() = delete;

  /**
   * Compares a candidate's last log position with the local log.
   *
   * Input: candidate last index/term and local log.
   * Output: true when the candidate is at least as up to date.
   * Thread safety: safe while the supplied log is immutable.
   */
  [[nodiscard]] static bool isCandidateLogUpToDate(
      LogIndex candidate_last_index, Term candidate_last_term,
      const RaftLog& local_log) noexcept;

  /**
   * Calculates the strict-majority size for a fixed cluster.
   *
   * Input: positive cluster member count.
   * Output: floor(member_count / 2) + 1.
   * Thread safety: stateless.
   */
  [[nodiscard]] static std::size_t quorumSize(
      std::size_t member_count) noexcept;

  /**
   * Tests whether a vote/replication count reaches a strict majority.
   *
   * Input: granted count and fixed member count.
   * Output: true when count reaches quorum.
   * Thread safety: stateless.
   */
  [[nodiscard]] static bool hasQuorum(std::size_t granted_count,
                                      std::size_t member_count) noexcept;
};

}  // namespace distributed_kv::raft
