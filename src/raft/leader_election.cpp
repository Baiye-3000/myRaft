#include "raft/leader_election.h"

namespace distributed_kv::raft {

bool LeaderElection::isCandidateLogUpToDate(
    LogIndex candidate_last_index, Term candidate_last_term,
    const RaftLog& local_log) noexcept {
  return candidate_last_term > local_log.lastTerm() ||
         (candidate_last_term == local_log.lastTerm() &&
          candidate_last_index >= local_log.lastIndex());
}

std::size_t LeaderElection::quorumSize(
    std::size_t member_count) noexcept {
  return member_count / 2U + 1U;
}

bool LeaderElection::hasQuorum(std::size_t granted_count,
                               std::size_t member_count) noexcept {
  return granted_count >= quorumSize(member_count);
}

}  // namespace distributed_kv::raft
