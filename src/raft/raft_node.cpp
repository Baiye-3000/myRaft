#include "raft/raft_node.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "raft/cluster_config.h"
#include "raft/leader_election.h"
#include "raft/log_replication.h"

namespace distributed_kv::raft {
namespace {

void applyPeersFromConfiguration(const ClusterConfiguration& config,
                                 NodeId self, std::vector<NodeId>& peers) {
  peers.clear();
  peers.reserve(config.members.size());
  for (const ClusterMember& member : config.members) {
    if (member.node_id != self) {
      peers.push_back(member.node_id);
    }
  }
}

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
  for (const NodeId learner : config_.learners) {
    if (learner == 0 || !members.insert(learner).second) {
      throw std::invalid_argument("Raft learner membership contains duplicate id");
    }
  }
  if (random_state_ == 0) {
    random_state_ = config_.node_id;
  }

  std::optional<RaftPersistentState> loaded;
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
                            state.commit_index == 0 &&
                            !state.cluster_config.has_value() &&
                            !state.joint_config.has_value() &&
                            !state.active_membership_operation.has_value() &&
                            !state.completed_membership_operation.has_value();
    if (!is_initial) {
      if (state.current_term == 0 || state.entries.empty() ||
          state.commit_index > state.entries.back().index ||
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
    loaded = std::move(state);
  }

  if (loaded.has_value() && loaded->cluster_config.has_value()) {
    std::string error;
    if (!validateClusterConfiguration(*loaded->cluster_config,
                                      0, error)) {
      throw std::runtime_error(error.empty()
                                 ? "invalid persisted cluster configuration"
                                 : error);
    }
    cluster_config_ = loaded->cluster_config;
    joint_config_ = loaded->joint_config;
    config_.learner = !std::any_of(
        cluster_config_->members.begin(), cluster_config_->members.end(),
        [this](const ClusterMember& member) {
          return member.node_id == config_.node_id;
        });
    applyPeersFromConfiguration(*cluster_config_, config_.node_id,
                                config_.peers);
    if (joint_config_.has_value()) {
      for (const NodeId id : configurationMemberIds(*joint_config_)) {
        if (std::find(config_.peers.begin(), config_.peers.end(), id) ==
            config_.peers.end() && id != config_.node_id) {
          config_.peers.push_back(id);
        }
      }
    }
    configuration_applied_index_ = commit_index_;
  } else if (config_.bootstrap_cluster.has_value()) {
    std::string error;
    if (!validateClusterConfiguration(*config_.bootstrap_cluster,
                                      config_.learner ? 0 : config_.node_id,
                                      error)) {
      throw std::invalid_argument(error.empty()
                                      ? "invalid bootstrap cluster configuration"
                                      : error);
    }
    cluster_config_ = config_.bootstrap_cluster;
    applyPeersFromConfiguration(*cluster_config_, config_.node_id,
                                config_.peers);
  }

  if (loaded.has_value()) {
    std::string error;
    if ((loaded->active_membership_operation.has_value() &&
         !validateMembershipOperation(
             *loaded->active_membership_operation, error)) ||
        (loaded->completed_membership_operation.has_value() &&
         !validateMembershipOperation(
             *loaded->completed_membership_operation, error))) {
      throw std::runtime_error(error.empty()
                                   ? "invalid persisted membership operation"
                                   : error);
    }
    active_membership_operation_ = loaded->active_membership_operation;
    completed_membership_operation_ =
        loaded->completed_membership_operation;
    if (active_membership_operation_.has_value() &&
        !joint_config_.has_value()) {
      throw std::runtime_error(
          "active membership operation has no joint configuration");
    }
  }

  if (loaded.has_value() && loaded->voted_for.has_value() &&
      !isMember(*loaded->voted_for)) {
    throw std::runtime_error("persistent vote is outside cluster membership");
  }

  if (loaded.has_value() && joint_config_.has_value()) {
    for (LogIndex index = commit_index_ + 1U; index <= log_.lastIndex();
         ++index) {
      const auto entry = log_.entryAt(index);
      if (!entry.has_value() || entry->type != EntryType::kConfChange) continue;
      ConfChangeEntry change;
      std::string error;
      if (decodeConfChangeEntry(entry->command, change, error) &&
          change.target_config == *joint_config_) {
        pending_conf_change_index_ = index;
        if (change.operation_id != 0 &&
            !active_membership_operation_.has_value()) {
          active_membership_operation_ = MembershipOperation{
              change.operation_id, change.type, change.member};
        }
        break;
      }
    }
  }

  resetElectionTimer();
}

std::vector<OutboundRpc> RaftNode::tick(std::uint64_t elapsed_ms) {
  if (config_.learner) return {};
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
  if (config_.learner || !isVotingMember(request.candidate_id) ||
      request.term < current_term_) {
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
  if (!isVotingMember(source) || source == config_.node_id) {
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
  if (hasElectionQuorum()) {
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
  if (!isVotingMember(request.leader_id) || request.term < current_term_) {
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
  std::optional<std::string> pending_command;
  bool pending_was_uncommitted_joint = false;
  if (pending_conf_change_index_.has_value() &&
      *pending_conf_change_index_ > commit_index_) {
    const auto pending = log_.entryAt(*pending_conf_change_index_);
    if (pending.has_value() && pending->type == EntryType::kConfChange) {
      ConfChangeEntry change;
      std::string decode_error;
      if (decodeConfChangeEntry(pending->command, change, decode_error)) {
        pending_command = pending->command;
        pending_was_uncommitted_joint = change.joint;
      }
    }
  }
  if (!log_.appendFrom(request.previous_log_index, request.entries, error)) {
    response.conflict_index = log_.lastIndex() + 1U;
    return finish();
  }
  if (!request.entries.empty()) {
    persistent_state_changed = true;
  }
  if (pending_command.has_value()) {
    const auto retained = log_.entryAt(*pending_conf_change_index_);
    if (!retained.has_value() || retained->command != *pending_command) {
      if (pending_was_uncommitted_joint) {
        joint_config_.reset();
        active_membership_operation_.reset();
        if (cluster_config_.has_value()) {
          applyPeersFromConfiguration(*cluster_config_, config_.node_id,
                                      config_.peers);
        }
      }
      pending_conf_change_index_.reset();
      persistent_state_changed = true;
    }
  }
  for (const LogEntry& appended : request.entries) {
    if (appended.type != EntryType::kConfChange ||
        appended.index <= commit_index_) {
      continue;
    }
    ConfChangeEntry change;
    std::string decode_error;
    if (decodeConfChangeEntry(appended.command, change, decode_error) &&
        change.joint) {
      applyConfigurationEntry(change);
      pending_conf_change_index_ = appended.index;
      persistent_state_changed = true;
    }
  }

  if (request.leader_commit > commit_index_) {
    const LogIndex previous_commit = commit_index_;
    commit_index_ = std::min(request.leader_commit, log_.lastIndex());
    if (commit_index_ != previous_commit) {
      applyCommittedConfiguration();
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

ProposeResult RaftNode::proposeConfChange(ConfChangeType type,
                                          ClusterMember member,
                                          std::uint64_t operation_id) {
  if (role_ != Role::kLeader || !cluster_config_.has_value() ||
      pending_conf_change_index_.has_value()) {
    return {};
  }
  // The exit entry is generated after the joint entry commits; callers must
  // not start a second membership change while joint consensus is active.
  if (joint_config_.has_value()) return {};
  if (type != ConfChangeType::kAddNode && type != ConfChangeType::kRemoveNode) {
    return {};
  }
  if (operation_id != 0) {
    const MembershipOperation operation{operation_id, type, member};
    std::string operation_error;
    if (!validateMembershipOperation(operation, operation_error) ||
        active_membership_operation_.has_value()) {
      return {};
    }
  }
  ClusterConfiguration target =
      joint_config_.has_value() ? *joint_config_ : *cluster_config_;
  if (target.config_id == std::numeric_limits<std::uint64_t>::max()) return {};
  ++target.config_id;
  const auto found = std::find_if(
      target.members.begin(), target.members.end(),
      [&member](const ClusterMember& candidate) {
        return candidate.node_id == member.node_id;
      });
  if (type == ConfChangeType::kAddNode) {
    if (member.node_id == 0 || found != target.members.end() ||
        !learnerCaughtUp(member.node_id)) return {};
    target.members.push_back(member);
    std::sort(target.members.begin(), target.members.end(),
              [](const ClusterMember& left, const ClusterMember& right) {
                return left.node_id < right.node_id;
              });
  } else {
    if (found == target.members.end() || member.node_id == config_.node_id ||
        target.members.size() <= 1U) return {};
    target.members.erase(found);
  }
  std::string error;
  if (!validateClusterConfiguration(target, config_.node_id, error)) return {};
  ConfChangeEntry change{type, !joint_config_.has_value(), member, target,
                         operation_id};
  if (joint_config_.has_value()) {
    // A second entry exits joint consensus and must describe the new stable set.
    change.joint = false;
  } else {
    joint_config_ = target;
    if (operation_id != 0) {
      active_membership_operation_ =
          MembershipOperation{operation_id, type, member};
    }
    std::vector<NodeId> ids = configurationMemberIds(*cluster_config_);
    for (const NodeId id : configurationMemberIds(target)) {
      if (std::find(ids.begin(), ids.end(), id) == ids.end()) ids.push_back(id);
    }
    config_.peers.clear();
    for (const NodeId id : ids) {
      if (id != config_.node_id) config_.peers.push_back(id);
    }
    config_.learners.erase(
        std::remove(config_.learners.begin(), config_.learners.end(),
                    member.node_id),
        config_.learners.end());
  }
  std::string encoded;
  if (!encodeConfChangeEntry(change, encoded, error)) return {};
  const LogIndex index =
      log_.append(current_term_, EntryType::kConfChange, std::move(encoded));
  pending_conf_change_index_ = index;
  persistOrThrow();
  advanceCommitIndex();
  return ProposeResult{true, index, replicateToAll()};
}

bool RaftNode::addLearner(NodeId learner_id) {
  if (role_ != Role::kLeader || learner_id == 0 ||
      learner_id == config_.node_id || isMember(learner_id)) {
    return false;
  }
  config_.learners.push_back(learner_id);
  next_index_[learner_id] = std::max<LogIndex>(1U, log_.firstIndex());
  match_index_[learner_id] = 0;
  return true;
}

bool RaftNode::learnerCaughtUp(NodeId learner_id) const noexcept {
  if (std::find(config_.learners.begin(), config_.learners.end(), learner_id) ==
      config_.learners.end()) {
    return false;
  }
  const auto found = match_index_.find(learner_id);
  return found != match_index_.end() && found->second >= commit_index_;
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

const std::optional<ClusterConfiguration>& RaftNode::clusterConfig()
    const noexcept {
  return cluster_config_;
}

const std::optional<ClusterConfiguration>& RaftNode::jointConfig()
    const noexcept {
  return joint_config_;
}

const std::optional<MembershipOperation>&
RaftNode::activeMembershipOperation() const noexcept {
  return active_membership_operation_;
}

const std::optional<MembershipOperation>&
RaftNode::completedMembershipOperation() const noexcept {
  return completed_membership_operation_;
}

const RaftLog& RaftNode::log() const noexcept { return log_; }

bool RaftNode::compactLogPrefix(LogIndex boundary_index,
                                  Term boundary_term,
                                  std::string& error) {
  if (!log_.compactTo(boundary_index, boundary_term, error)) {
    return false;
  }
  persistOrThrow();
  error.clear();
  return true;
}

bool RaftNode::peerNeedsSnapshot(NodeId peer) const {
  if (role_ != Role::kLeader) {
    return false;
  }
  const auto next = next_index_.find(peer);
  const auto matched = match_index_.find(peer);
  if (next == next_index_.end() || matched == match_index_.end()) {
    return false;
  }
  if (next->second <= log_.firstIndex()) {
    return true;
  }
  return log_.firstIndex() > 0 &&
         matched->second < log_.firstIndex();
}

NodeId RaftNode::nodeId() const noexcept { return config_.node_id; }

bool RaftNode::acknowledgeLeader(Term term, NodeId leader_id) {
  if (!isMember(leader_id) || term < current_term_) {
    return false;
  }
  bool persistent_state_changed = false;
  if (term > current_term_) {
    becomeFollower(term, leader_id);
    persistent_state_changed = true;
  } else if (role_ != Role::kFollower) {
    becomeFollower(term, leader_id);
  } else {
    leader_id_ = leader_id;
    election_elapsed_ms_ = 0;
    resetElectionTimer();
  }
  if (persistent_state_changed) {
    persistOrThrow();
  }
  return true;
}

bool RaftNode::applySnapshotBoundary(LogIndex boundary_index,
                                     Term boundary_term,
                                     std::string& error) {
  if (!log_.compactTo(boundary_index, boundary_term, error)) {
    return false;
  }
  if (commit_index_ < boundary_index) {
    commit_index_ = std::min(boundary_index, log_.lastIndex());
  }
  persistOrThrow();
  error.clear();
  return true;
}

bool RaftNode::replaceLogWithSnapshotBoundary(LogIndex boundary_index,
                                              Term boundary_term,
                                              std::string& error) {
  const std::vector<LogEntry> entries{
      LogEntry{boundary_index, boundary_term, EntryType::kNoOp, ""},
  };
  if (!log_.restore(entries, error)) {
    return false;
  }
  if (commit_index_ < boundary_index) {
    commit_index_ = boundary_index;
  }
  persistOrThrow();
  error.clear();
  return true;
}

void RaftNode::onInstallSnapshotSuccess(NodeId peer,
                                        LogIndex last_included_index) {
  if (role_ != Role::kLeader || last_included_index > log_.lastIndex()) {
    return;
  }
  const auto next = next_index_.find(peer);
  const auto matched = match_index_.find(peer);
  if (next == next_index_.end() || matched == match_index_.end()) {
    return;
  }
  matched->second = std::max(matched->second, last_included_index);
  next->second = std::max(next->second, last_included_index + 1U);
  advanceCommitIndex();
}

void RaftNode::observeHigherTerm(Term term) {
  if (term > current_term_) {
    becomeFollower(term, std::nullopt);
    persistOrThrow();
  }
}

std::vector<OutboundRpc> RaftNode::startElection() {
  if (config_.learner) return {};
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

  if (hasElectionQuorum()) {
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
  for (const NodeId learner : config_.learners) {
    next_index_[learner] = initial_next;
    match_index_[learner] = 0;
  }
  const LogIndex no_op_index =
      log_.append(current_term_, EntryType::kNoOp, "");
  static_cast<void>(no_op_index);
  recoverJointExitEntry();
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
  outbound.reserve(config_.peers.size() + config_.learners.size());
  for (const NodeId peer : config_.peers) {
    outbound.push_back(makeAppendEntries(peer));
  }
  for (const NodeId learner : config_.learners) {
    outbound.push_back(makeAppendEntries(learner));
  }
  return outbound;
}

void RaftNode::advanceCommitIndex() {
  if (role_ != Role::kLeader) {
    return;
  }
  const LogIndex previous_commit = commit_index_;
  for (LogIndex candidate = log_.lastIndex(); candidate > commit_index_;
       --candidate) {
    if (log_.termAt(candidate) != std::optional<Term>(current_term_) ||
        !hasReplicationQuorum(candidate)) {
      continue;
    }
    commit_index_ = candidate;
    break;
  }
  if (commit_index_ != previous_commit) {
    applyCommittedConfiguration();
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
             config_.peers.end() ||
         std::find(config_.learners.begin(), config_.learners.end(), node_id) !=
             config_.learners.end();
}

bool RaftNode::isVotingMember(NodeId node_id) const noexcept {
  if (node_id == config_.node_id) return !config_.learner;
  return std::find(config_.peers.begin(), config_.peers.end(), node_id) !=
         config_.peers.end();
}

std::size_t RaftNode::quorumSize() const noexcept {
  return LeaderElection::quorumSize(config_.peers.size() + 1U);
}

std::vector<NodeId> RaftNode::configurationMemberIds(
    const ClusterConfiguration& config) const {
  std::vector<NodeId> ids;
  ids.reserve(config.members.size());
  for (const ClusterMember& member : config.members) ids.push_back(member.node_id);
  return ids;
}

bool RaftNode::hasElectionQuorum() const noexcept {
  if (!cluster_config_.has_value() || !joint_config_.has_value()) {
    return votes_received_.size() >= quorumSize();
  }
  auto count = [this](const ClusterConfiguration& config) {
    std::size_t granted = 0;
    for (const ClusterMember& member : config.members) {
      if (votes_received_.count(member.node_id) != 0U) ++granted;
    }
    return granted;
  };
  return LeaderElection::hasQuorum(count(*cluster_config_),
                                   cluster_config_->members.size()) &&
         LeaderElection::hasQuorum(count(*joint_config_),
                                   joint_config_->members.size());
}

bool RaftNode::hasReplicationQuorum(LogIndex index) const noexcept {
  auto replicated = [this, index](NodeId id) {
    if (id == config_.node_id) return log_.lastIndex() >= index;
    const auto found = match_index_.find(id);
    return found != match_index_.end() && found->second >= index;
  };
  if (!cluster_config_.has_value() || !joint_config_.has_value()) {
    std::size_t count = replicated(config_.node_id) ? 1U : 0U;
    for (const NodeId peer : config_.peers) {
      if (replicated(peer)) ++count;
    }
    return LeaderElection::hasQuorum(count, config_.peers.size() + 1U);
  }
  auto count = [&replicated](const ClusterConfiguration& config) {
    std::size_t result = 0;
    for (const ClusterMember& member : config.members) {
      if (replicated(member.node_id)) ++result;
    }
    return result;
  };
  return LeaderElection::hasQuorum(count(*cluster_config_),
                                   cluster_config_->members.size()) &&
         LeaderElection::hasQuorum(count(*joint_config_),
                                   joint_config_->members.size());
}

void RaftNode::applyConfigurationEntry(const ConfChangeEntry& entry) {
  if (entry.joint) {
    joint_config_ = entry.target_config;
    if (entry.operation_id != 0) {
      active_membership_operation_ = MembershipOperation{
          entry.operation_id, entry.type, entry.member};
    }
    std::vector<NodeId> ids = configurationMemberIds(*cluster_config_);
    for (const NodeId id : configurationMemberIds(entry.target_config)) {
      if (std::find(ids.begin(), ids.end(), id) == ids.end()) ids.push_back(id);
    }
    config_.peers.clear();
    for (const NodeId id : ids) {
      if (id != config_.node_id) config_.peers.push_back(id);
    }
    if (std::any_of(entry.target_config.members.begin(),
                    entry.target_config.members.end(),
                    [this](const ClusterMember& member) {
                      return member.node_id == config_.node_id;
                    })) {
      config_.learner = false;
    }
    pending_conf_change_index_.reset();
    if (role_ == Role::kLeader) {
      const auto exit = makeJointExitEntry();
      if (!exit.has_value()) {
        throw std::runtime_error("joint configuration has invalid delta");
      }
      std::string encoded;
      std::string error;
      if (!encodeConfChangeEntry(*exit, encoded, error)) {
        throw std::runtime_error(error.empty() ? "failed to encode joint exit" : error);
      }
      pending_conf_change_index_ =
          log_.append(current_term_, EntryType::kConfChange, std::move(encoded));
    }
  } else {
    cluster_config_ = entry.target_config;
    joint_config_.reset();
    applyPeersFromConfiguration(*cluster_config_, config_.node_id, config_.peers);
    config_.learner = !std::any_of(
        cluster_config_->members.begin(), cluster_config_->members.end(),
        [this](const ClusterMember& member) {
          return member.node_id == config_.node_id;
        });
    if (entry.operation_id != 0) {
      completed_membership_operation_ = MembershipOperation{
          entry.operation_id, entry.type, entry.member};
      active_membership_operation_.reset();
    }
  }
  if (!entry.joint) pending_conf_change_index_.reset();
}

std::optional<ConfChangeEntry> RaftNode::makeJointExitEntry() const {
  if (!cluster_config_.has_value() || !joint_config_.has_value()) {
    return std::nullopt;
  }
  const auto findMember = [](const ClusterConfiguration& configuration,
                             NodeId id) -> const ClusterMember* {
    const auto found = std::find_if(
        configuration.members.begin(), configuration.members.end(),
        [id](const ClusterMember& member) { return member.node_id == id; });
    return found == configuration.members.end() ? nullptr : &*found;
  };

  if (joint_config_->members.size() == cluster_config_->members.size() + 1U) {
    for (const ClusterMember& member : joint_config_->members) {
      if (findMember(*cluster_config_, member.node_id) == nullptr) {
        return ConfChangeEntry{
            ConfChangeType::kAddNode, false, member, *joint_config_,
            active_membership_operation_.has_value()
                ? active_membership_operation_->operation_id
                : 0};
      }
    }
  } else if (cluster_config_->members.size() ==
             joint_config_->members.size() + 1U) {
    for (const ClusterMember& member : cluster_config_->members) {
      if (findMember(*joint_config_, member.node_id) == nullptr) {
        return ConfChangeEntry{
            ConfChangeType::kRemoveNode, false,
            ClusterMember{member.node_id, {}, 0, {}, 0}, *joint_config_,
            active_membership_operation_.has_value()
                ? active_membership_operation_->operation_id
                : 0};
      }
    }
  }
  return std::nullopt;
}

void RaftNode::recoverJointExitEntry() {
  if (!joint_config_.has_value()) return;
  for (LogIndex index = configuration_applied_index_ + 1U;
       index <= log_.lastIndex(); ++index) {
    const auto entry = log_.entryAt(index);
    if (!entry.has_value() || entry->type != EntryType::kConfChange) continue;
    ConfChangeEntry change;
    std::string error;
    if (decodeConfChangeEntry(entry->command, change, error) && !change.joint &&
        change.target_config == *joint_config_) {
      pending_conf_change_index_ = index;
      return;
    }
  }

  const auto exit = makeJointExitEntry();
  if (!exit.has_value()) {
    throw std::runtime_error("joint configuration has invalid delta");
  }
  std::string encoded;
  std::string error;
  if (!encodeConfChangeEntry(*exit, encoded, error)) {
    throw std::runtime_error(error.empty() ? "failed to recover joint exit"
                                           : error);
  }
  pending_conf_change_index_ =
      log_.append(current_term_, EntryType::kConfChange, std::move(encoded));
}

void RaftNode::applyCommittedConfiguration() {
  while (configuration_applied_index_ < commit_index_) {
    const LogIndex index = configuration_applied_index_ + 1U;
    const auto entry = log_.entryAt(index);
    if (!entry.has_value()) break;
    if (entry->type == EntryType::kConfChange) {
      ConfChangeEntry change;
      std::string error;
      if (!decodeConfChangeEntry(entry->command, change, error)) {
        throw std::runtime_error(error.empty() ? "invalid committed conf change" : error);
      }
      applyConfigurationEntry(change);
    }
    configuration_applied_index_ = index;
  }
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
      cluster_config_,
      joint_config_,
      active_membership_operation_,
      completed_membership_operation_,
  };
  std::string error;
  if (!persistence_->save(state, error)) {
    throw std::runtime_error(
        error.empty() ? "Raft persistence barrier failed" : error);
  }
}

}  // namespace distributed_kv::raft
