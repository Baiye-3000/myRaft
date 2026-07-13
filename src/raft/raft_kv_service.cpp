#include "raft/raft_kv_service.h"

#include <stdexcept>
#include <utility>

#include "storage/kv_store.h"

namespace distributed_kv::raft {

RaftKVService::RaftKVService(NodeConfig config, storage::KVStore& store,
                             RaftPersistence* persistence,
                             const StateMachineSnapshot* snapshot)
    : raft_node_(std::move(config), persistence), state_machine_(store) {
  std::string error;
  if (snapshot != nullptr) {
    if (snapshot->last_included_index > raft_node_.commitIndex()) {
      throw std::runtime_error(
          "snapshot boundary is ahead of durable commit index");
    }
    const auto boundary_term =
        raft_node_.log().termAt(snapshot->last_included_index);
    if (!boundary_term.has_value() ||
        *boundary_term != snapshot->last_included_term) {
      throw std::runtime_error(
          "snapshot boundary does not match durable Raft log");
    }
    if (!state_machine_.restore(*snapshot, error)) {
      throw std::runtime_error("failed to restore state snapshot: " + error);
    }
  }
  if (!applyCommitted(error)) {
    throw std::runtime_error("failed to rebuild committed state: " + error);
  }
}

std::vector<OutboundRpc> RaftKVService::tick(std::uint64_t elapsed_ms,
                                             std::string& error) {
  auto outbound = raft_node_.tick(elapsed_ms);
  if (!applyCommitted(error)) {
    return {};
  }
  return outbound;
}

RequestVoteResponse RaftKVService::handleRequestVote(
    const RequestVoteRequest& request) {
  return raft_node_.handleRequestVote(request);
}

std::vector<OutboundRpc> RaftKVService::handleRequestVoteResponse(
    NodeId source, const RequestVoteResponse& response,
    std::string& error) {
  auto outbound =
      raft_node_.handleRequestVoteResponse(source, response);
  if (!applyCommitted(error)) {
    return {};
  }
  return outbound;
}

AppendEntriesResponse RaftKVService::handleAppendEntries(
    const AppendEntriesRequest& request, std::string& error) {
  AppendEntriesResponse response =
      raft_node_.handleAppendEntries(request);
  if (!applyCommitted(error)) {
    response.success = false;
  }
  return response;
}

std::vector<OutboundRpc> RaftKVService::handleAppendEntriesResponse(
    NodeId source, const AppendEntriesResponse& response,
    std::string& error) {
  auto outbound =
      raft_node_.handleAppendEntriesResponse(source, response);
  if (!applyCommitted(error)) {
    return {};
  }
  return outbound;
}

SubmitResult RaftKVService::submit(const KVCommand& command,
                                   std::string& error) {
  std::string encoded;
  if (!KVCommandCodec::encode(command, encoded, error)) {
    return SubmitResult{SubmitStatus::kInvalid, 0, std::nullopt, {}};
  }

  ProposeResult proposal = raft_node_.propose(std::move(encoded));
  if (!proposal.accepted) {
    error.clear();
    return SubmitResult{
        SubmitStatus::kNotLeader, 0, std::nullopt,
        std::move(proposal.outbound),
    };
  }

  pending_local_.insert(proposal.index);
  if (!applyCommitted(error)) {
    return SubmitResult{
        SubmitStatus::kInvalid, proposal.index, std::nullopt, {},
    };
  }

  const auto completed = completed_local_.find(proposal.index);
  if (completed != completed_local_.end()) {
    ApplyResult result = std::move(completed->second);
    completed_local_.erase(completed);
    return SubmitResult{
        SubmitStatus::kApplied,
        proposal.index,
        std::move(result),
        std::move(proposal.outbound),
    };
  }

  error.clear();
  return SubmitResult{
      SubmitStatus::kPending,
      proposal.index,
      std::nullopt,
      std::move(proposal.outbound),
  };
}

std::optional<ApplyResult> RaftKVService::takeResult(LogIndex index) {
  const auto completed = completed_local_.find(index);
  if (completed == completed_local_.end()) {
    return std::nullopt;
  }
  ApplyResult result = std::move(completed->second);
  completed_local_.erase(completed);
  return result;
}

std::optional<std::string> RaftKVService::getApplied(
    const std::string& key) const {
  return state_machine_.get(key);
}

const RaftNode& RaftKVService::raftNode() const noexcept {
  return raft_node_;
}

LogIndex RaftKVService::lastApplied() const noexcept {
  return state_machine_.lastApplied();
}

bool RaftKVService::applyCommitted(std::string& error) {
  while (state_machine_.lastApplied() < raft_node_.commitIndex()) {
    const LogIndex next = state_machine_.lastApplied() + 1U;
    const auto entry = raft_node_.log().entryAt(next);
    if (!entry.has_value()) {
      error = "committed Raft entry is absent from local log";
      return false;
    }

    const auto result = state_machine_.apply(*entry, error);
    if (!error.empty()) {
      return false;
    }
    if (result.has_value() && pending_local_.erase(next) != 0U) {
      completed_local_.insert_or_assign(next, *result);
    }
  }
  error.clear();
  return true;
}

}  // namespace distributed_kv::raft
