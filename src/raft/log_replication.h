#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "raft/raft_log.h"
#include "raft/types.h"

namespace distributed_kv::raft {

/**
 * Stateless Leader-side calculations for conflict repair and commit.
 */
class LogReplication final {
 public:
  LogReplication() = delete;

  /**
   * Calculates nextIndex from a follower's conflict hint.
   *
   * Input: Leader log, follower conflict index and optional conflict term.
   * Output: clamped next index after the retained boundary and no later than
   * leader.lastIndex()+1.
   * Thread safety: safe while the supplied log is immutable.
   */
  [[nodiscard]] static LogIndex nextIndexAfterConflict(
      const RaftLog& leader_log, LogIndex conflict_index,
      std::optional<Term> conflict_term) noexcept;

  /**
   * Calculates the highest current-term index replicated by a majority.
   *
   * Input: Leader log/term, current commit, follower match indexes, and total
   * member count. The Leader itself is counted implicitly.
   * Output: monotonically non-decreasing safe commit index.
   * Thread safety: safe while all supplied state is immutable.
   */
  [[nodiscard]] static LogIndex calculateCommitIndex(
      const RaftLog& leader_log, Term current_term,
      LogIndex current_commit,
      const std::vector<LogIndex>& follower_match_indexes,
      std::size_t member_count) noexcept;
};

}  // namespace distributed_kv::raft
