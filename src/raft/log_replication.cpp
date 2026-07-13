#include "raft/log_replication.h"

#include <algorithm>
#include <limits>

#include "raft/leader_election.h"

namespace distributed_kv::raft {

LogIndex LogReplication::nextIndexAfterConflict(
    const RaftLog& leader_log, LogIndex conflict_index,
    std::optional<Term> conflict_term) noexcept {
  LogIndex candidate = conflict_index;
  if (conflict_term.has_value()) {
    const auto local_last = leader_log.lastIndexOfTerm(*conflict_term);
    if (local_last.has_value()) {
      candidate = *local_last + 1U;
    }
  }
  const LogIndex minimum =
      leader_log.firstIndex() == std::numeric_limits<LogIndex>::max()
          ? leader_log.firstIndex()
          : leader_log.firstIndex() + 1U;
  candidate = std::max(minimum, candidate);
  return std::min(candidate, leader_log.lastIndex() + 1U);
}

LogIndex LogReplication::calculateCommitIndex(
    const RaftLog& leader_log, Term current_term,
    LogIndex current_commit,
    const std::vector<LogIndex>& follower_match_indexes,
    std::size_t member_count) noexcept {
  for (LogIndex candidate = leader_log.lastIndex();
       candidate > current_commit; --candidate) {
    if (leader_log.termAt(candidate) !=
        std::optional<Term>(current_term)) {
      continue;
    }

    std::size_t replicated = 1;
    for (const LogIndex match_index : follower_match_indexes) {
      if (match_index >= candidate) {
        ++replicated;
      }
    }
    if (LeaderElection::hasQuorum(replicated, member_count)) {
      return candidate;
    }
  }
  return current_commit;
}

}  // namespace distributed_kv::raft
