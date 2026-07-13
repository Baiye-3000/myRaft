#include "raft/raft_node.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "raft/leader_election.h"
#include "raft/log_replication.h"

namespace distributed_kv::raft {
namespace {

// Input: unsigned timer and delta. Output: saturated sum.
// Thread safety: stateless.
std::uint64_t saturatingAdd(std::uint64_t value, std::uint64_t delta) {
  if (delta > std::numeric_limits<std::uint64_t>::max() - value) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return value + delta;
}

}  // namespace

RaftNode::RaftNode(NodeConfig config, RaftPersistence* persistence)
    : config_(std::move(config)),
      persistence_(persistence),
      random_state_(config_.random_seed) {
  if (config_.node_id == 0 || config_.election_timeout_min_ms == 0 ||
      config_.election_timeout_min_ms >
          config_.election_timeout_max_ms ||
      config_.heartbeat_interval_ms == 0 ||
      config_.heartbeat_interval_ms >= config_.election_timeout_min_ms ||
      config_.maximum_entries_per_append == 0) {
    throw std::invalid_argument("invalid Raft node configuration");
  }

  std::unordered_set<NodeId> members;
  members.insert(config_.node_id);
  for (const NodeId peer : config_.peers) {
    if (peer == 0 || !members.insert(peer).second) {
      throw std::invalid_argument("Raft membership contains duplicate id");
    }
  }
  if (random_state_ == 0) {
    random_state_ = config_.node_id;
  }
  if (persistence_ != nullptr) {
    RaftPersistentState state;
    std::string error;
    if (!persistence_->load(state, error)) {
      throw std::runtime_error(
          error.empty() ? "Raft persistent-state load failed" : error);
    }
    const bool is_initial = state.current_term == 0 &&
                            !state.voted_for.has_value() &&
                            state.entries.empty() &&
                            state.commit_index == 0;
    if (!is_initial) {
      if (state.current_term == 0 ||
          (state.voted_for.has_value() && !isMember(*state.voted_for)) ||
          state.commit_index >= state.entries.size() ||
          !log_.restore(state.entries, error)) {
        throw std::runtime_error(
            error.empty() ? "invalid Raft persistent state" : error);
      }
      const auto entry_after_current_term =
          std::find_if(state.entries.begin(), state.entries.end(),
                       [&state](const LogEntry& entry) {
                         return entry.term > state.current_term;
                       });
      if (entry_after_current_term != state.entries.end()) {
        throw std::runtime_error(
            "persistent log contains a future-term entry");
      }
      current_term_ = state.current_term;
      voted_for_ = state.voted_for;
      commit_index_ = state.commit_index;
    }
  }
  resetElectionTimer();
}

std::vector<OutboundRpc> RaftNode::tick(std::uint64_t elapsed_ms) {
  if (role_ == Role::kLeader) {
    heartbeat_elapsed_ms_ =
        saturatingAdd(heartbeat_elapsed_ms_, elapsed_ms);
    if (heartbeat_elapsed_ms_ >= config_.heartbeat_interval_ms) {
      heartbeat_elapsed_ms_ = 0;
      return replicateToAll();
    }
    return {};
  }

  election_elapsed_ms_ =
      saturatingAdd(election_elapsed_ms_, elapsed_ms);
  if (election_elapsed_ms_ >= election_timeout_ms_) {
    return startElection();
  }
  return {};
}

RequestVoteResponse RaftNode::handleRequestVote(
    const RequestVoteRequest& request) {
  RequestVoteResponse response{current_term_, request.term, false};
  if (!isMember(request.candidate_id) || request.term < current_term_) {
    return response;
  }
  bool persistent_state_changed = false;
  if (request.term > current_term_) {
    becomeFollower(request.term, std::nullopt);
    persistent_state_changed = true;
  }

  const bool candidate_is_up_to_date =
      LeaderElection::isCandidateLogUpToDate(
          request.last_log_index, request.last_log_term, log_);
  const bool can_vote =
      !voted_for_.has_value() || *voted_for_ == request.candidate_id;
  if (can_vote && candidate_is_up_to_date) {
    persistent_state_changed =
        persistent_state_changed || !voted_for_.has_value() ||
        *voted_for_ != request.candidate_id;
    voted_for_ = request.candidate_id;
    election_elapsed_ms_ = 0;
    resetElectionTimer();
    response.vote_granted = true;
  }
  if (persistent_state_changed) {
    persistOrThrow();
  }
  response.term = current_term_;
  return response;
}

std::vector<OutboundRpc> RaftNode::handleRequestVoteResponse(
    NodeId source, const RequestVoteResponse& response) {
  if (!isMember(source) || source == config_.node_id) {
    return {};
  }
  if (response.term > current_term_) {
    becomeFollower(response.term, std::nullopt);
    persistOrThrow();
    return {};
  }
  if (role_ != Role::kCandidate || response.term != current_term_ ||
      response.election_term != current_term_ || !response.vote_granted) {
    return {};
  }

  votes_received_.insert(source);
  if (votes_received_.size() >= quorumSize()) {
    return becomeLeader();
  }
  return {};
}

AppendEntriesResponse RaftNode::handleAppendEntries(
    const AppendEntriesRequest& request) {
  AppendEntriesResponse response{
      current_term_,
      false,
      request.previous_log_index,
      request.entries.size(),
      0,
      log_.lastIndex() + 1U,
      std::nullopt,
      request.read_context,
  };
  bool persistent_state_changed = false;
  const auto finish = [this, &response, &persistent_state_changed] {
    response.term = current_term_;
    if (persistent_state_changed) {
      persistOrThrow();
    }
    return response;
  };
  if (!isMember(request.leader_id) || request.term < current_term_) {
    return finish();
  }

  const bool term_increased = request.term > current_term_;
  if (term_increased || role_ != Role::kFollower) {
    becomeFollower(request.term, request.leader_id);
    persistent_state_changed = term_increased;
  } else {
    leader_id_ = request.leader_id;
    election_elapsed_ms_ = 0;
    resetElectionTimer();
  }
  if (request.previous_log_index > log_.lastIndex()) {
    response.conflict_index = log_.lastIndex() + 1U;
    return finish();
  }
  const auto previous_term = log_.termAt(request.previous_log_index);
  if (!previous_term.has_value() ||
      *previous_term != request.previous_log_term) {
    response.conflict_term = previous_term;
    response.conflict_index =
        previous_term.has_value()
            ? log_.firstIndexOfTerm(*previous_term)
                  .value_or(log_.firstIndex() + 1U)
            : (request.previous_log_index < log_.firstIndex()
                   ? log_.firstIndex() + 1U
                   : log_.lastIndex() + 1U);
    return finish();
  }

  std::string error;
  if (!log_.appendFrom(request.previous_log_index, request.entries, error)) {
    response.conflict_index = log_.lastIndex() + 1U;
    return finish();
  }
  if (!request.entries.empty()) {
    persistent_state_changed = true;
  }

  if (request.leader_commit > commit_index_) {
    const LogIndex previous_commit = commit_index_;
    commit_index_ = std::min(request.leader_commit, log_.lastIndex());
    if (commit_index_ != previous_commit) {
      persistent_state_changed = true;
    }
  }
  response.success = true;
  response.match_index =
      request.entries.empty() ? request.previous_log_index
                              : request.entries.back().index;
  response.conflict_index = response.match_index + 1U;
  response.conflict_term.reset();
  return finish();
}

std::vector<OutboundRpc> RaftNode::handleAppendEntriesResponse(
    NodeId source, const AppendEntriesResponse& response) {
  if (!isMember(source) || source == config_.node_id) {
    return {};
  }
  if (response.term > current_term_) {
    becomeFollower(response.term, std::nullopt);
    persistOrThrow();
    return {};
  }
  if (role_ != Role::kLeader || response.term != current_term_) {
    return {};
  }

  auto next = next_index_.find(source);
  auto matched = match_index_.find(source);
  if (next == next_index_.end() || matched == match_index_.end()) {
    return {};
  }
  if (response.success) {
    if (response.match_index < response.request_previous_log_index ||
        response.match_index > log_.lastIndex() ||
        response.request_entry_count >
            std::numeric_limits<LogIndex>::max() -
                response.request_previous_log_index ||
        response.match_index !=
            response.request_previous_log_index +
                static_cast<LogIndex>(response.request_entry_count)) {
      return {};
    }
    matched->second = std::max(matched->second, response.match_index);
    next->second = std::max(next->second, response.match_index + 1U);
    advanceCommitIndex();
    if (next->second <= log_.lastIndex()) {
      return {makeAppendEntries(source)};
    }
    return {};
  }

  if (next->second == 0 ||
      response.request_previous_log_index != next->second - 1U ||
      matched->second > response.request_previous_log_index) {
    return {};
  }
  next->second = LogReplication::nextIndexAfterConflict(
      log_, response.conflict_index, response.conflict_term);
  return {makeAppendEntries(source)};
}

ProposeResult RaftNode::propose(std::string command) {
  if (role_ != Role::kLeader) {
    return {};
  }
  const LogIndex index =
      log_.append(current_term_, EntryType::kCommand, std::move(command));
  persistOrThrow();
  advanceCommitIndex();
  return ProposeResult{true, index, replicateToAll()};
}

std::vector<OutboundRpc> RaftNode::makeReadBarrier(
    std::uint64_t read_context) const {
  if (role_ != Role::kLeader || read_context == 0) {
    return {};
  }
  std::vector<OutboundRpc> outbound = replicateToAll();
  for (OutboundRpc& rpc : outbound) {
    auto* append = std::get_if<AppendEntriesRequest>(&rpc.payload);
    if (append != nullptr) {
      append->read_context = read_context;
    }
  }
  return outbound;
}

Role RaftNode::role() const noexcept { return role_; }

Term RaftNode::currentTerm() const noexcept { return current_term_; }

std::optional<NodeId> RaftNode::leaderId() const noexcept {
  return leader_id_;
}

LogIndex RaftNode::commitIndex() const noexcept { return commit_index_; }

const RaftLog& RaftNode::log() const noexcept { return log_; }

std::vector<OutboundRpc> RaftNode::startElection() {
  if (current_term_ == std::numeric_limits<Term>::max()) {
    return {};
  }
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = config_.node_id;
  leader_id_.reset();
  votes_received_.clear();
  votes_received_.insert(config_.node_id);
  election_elapsed_ms_ = 0;
  resetElectionTimer();
  persistOrThrow();

  if (votes_received_.size() >= quorumSize()) {
    return becomeLeader();
  }

  std::vector<OutboundRpc> outbound;
  outbound.reserve(config_.peers.size());
  for (const NodeId peer : config_.peers) {
    outbound.push_back(OutboundRpc{
        peer,
        RequestVoteRequest{
            current_term_,
            config_.node_id,
            log_.lastIndex(),
            log_.lastTerm(),
        },
    });
  }
  return outbound;
}

void RaftNode::becomeFollower(Term term, std::optional<NodeId> leader) {
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.reset();
  }
  role_ = Role::kFollower;
  leader_id_ = leader;
  votes_received_.clear();
  next_index_.clear();
  match_index_.clear();
  heartbeat_elapsed_ms_ = 0;
  election_elapsed_ms_ = 0;
  resetElectionTimer();
}

std::vector<OutboundRpc> RaftNode::becomeLeader() {
  role_ = Role::kLeader;
  leader_id_ = config_.node_id;
  votes_received_.clear();
  heartbeat_elapsed_ms_ = 0;

  const LogIndex initial_next = log_.lastIndex() + 1U;
  next_index_.clear();
  match_index_.clear();
  for (const NodeId peer : config_.peers) {
    next_index_[peer] = initial_next;
    match_index_[peer] = 0;
  }
  const LogIndex no_op_index =
      log_.append(current_term_, EntryType::kNoOp, "");
  static_cast<void>(no_op_index);
  persistOrThrow();
  advanceCommitIndex();
  return replicateToAll();
}

OutboundRpc RaftNode::makeAppendEntries(NodeId peer) const {
  const auto found = next_index_.find(peer);
  const LogIndex next =
      found == next_index_.end() ? log_.lastIndex() + 1U : found->second;
  const LogIndex previous = next - 1U;
  return OutboundRpc{
      peer,
      AppendEntriesRequest{
          current_term_,
          config_.node_id,
          previous,
          log_.termAt(previous).value_or(0),
          log_.entriesFrom(next, config_.maximum_entries_per_append),
          commit_index_,
      },
  };
}

std::vector<OutboundRpc> RaftNode::replicateToAll() const {
  std::vector<OutboundRpc> outbound;
  outbound.reserve(config_.peers.size());
  for (const NodeId peer : config_.peers) {
    outbound.push_back(makeAppendEntries(peer));
  }
  return outbound;
}

void RaftNode::advanceCommitIndex() {
  if (role_ != Role::kLeader) {
    return;
  }
  std::vector<LogIndex> follower_matches;
  follower_matches.reserve(match_index_.size());
  for (const auto& peer_match : match_index_) {
    follower_matches.push_back(peer_match.second);
  }
  const LogIndex previous_commit = commit_index_;
  commit_index_ = LogReplication::calculateCommitIndex(
      log_, current_term_, commit_index_, follower_matches,
      config_.peers.size() + 1U);
  if (commit_index_ != previous_commit) {
    persistOrThrow();
  }
}

void RaftNode::resetElectionTimer() noexcept {
  random_state_ ^= random_state_ << 13U;
  random_state_ ^= random_state_ >> 7U;
  random_state_ ^= random_state_ << 17U;
  if (random_state_ == 0) {
    random_state_ = 0x9e3779b97f4a7c15ULL;
  }
  const std::uint64_t span =
      config_.election_timeout_max_ms -
      config_.election_timeout_min_ms;
  election_timeout_ms_ =
      config_.election_timeout_min_ms +
      random_state_ % (span + 1U);
}

bool RaftNode::isMember(NodeId node_id) const noexcept {
  return node_id == config_.node_id ||
         std::find(config_.peers.begin(), config_.peers.end(), node_id) !=
             config_.peers.end();
}

std::size_t RaftNode::quorumSize() const noexcept {
  return LeaderElection::quorumSize(config_.peers.size() + 1U);
}

void RaftNode::persistOrThrow() {
  if (persistence_ == nullptr) {
    return;
  }
  RaftPersistentState state{
      current_term_,
      voted_for_,
      log_.persistentEntries(),
      commit_index_,
  };
  std::string error;
  if (!persistence_->save(state, error)) {
    throw std::runtime_error(
        error.empty() ? "Raft persistence barrier failed" : error);
  }
}

}  // namespace distributed_kv::raft
