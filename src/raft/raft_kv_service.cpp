#include "raft/raft_kv_service.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "storage/kv_store.h"

namespace distributed_kv::raft {
namespace {

constexpr std::size_t kSnapshotChunkSize = 64U * 1024U;

}  // namespace

RaftKVService::RaftKVService(NodeConfig config, storage::KVStore& store,
                             RaftPersistence* persistence,
                             const StateMachineSnapshot* snapshot,
                             FileSnapshotStore* snapshot_store)
    : raft_node_(std::move(config), persistence),
      state_machine_(store),
      last_snapshot_index_(snapshot != nullptr
                               ? snapshot->last_included_index
                               : 0),
      snapshot_store_(snapshot_store) {
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

std::vector<OutboundRpc> RaftKVService::finalizeOutbound(
    std::vector<OutboundRpc> outbound, std::string& error) {
  if (snapshot_store_ == nullptr) {
    error.clear();
    return outbound;
  }
  return completeReplication(std::move(outbound), *snapshot_store_, error);
}

std::vector<OutboundRpc> RaftKVService::tick(std::uint64_t elapsed_ms,
                                             std::string& error) {
  auto outbound = raft_node_.tick(elapsed_ms);
  if (!applyCommitted(error)) {
    return {};
  }
  return finalizeOutbound(std::move(outbound), error);
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
  return finalizeOutbound(std::move(outbound), error);
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
  return finalizeOutbound(std::move(outbound), error);
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
    auto outbound =
        finalizeOutbound(std::move(proposal.outbound), error);
    if (!error.empty()) {
      return SubmitResult{
          SubmitStatus::kInvalid, proposal.index, std::nullopt, {},
      };
    }
    return SubmitResult{
        SubmitStatus::kApplied,
        proposal.index,
        std::move(result),
        std::move(outbound),
    };
  }

  error.clear();
  auto outbound = finalizeOutbound(std::move(proposal.outbound), error);
  if (!error.empty()) {
    return SubmitResult{
        SubmitStatus::kInvalid, proposal.index, std::nullopt, {},
    };
  }
  return SubmitResult{
      SubmitStatus::kPending,
      proposal.index,
      std::nullopt,
      std::move(outbound),
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

ProposeResult RaftKVService::proposeConfChange(ConfChangeType type,
                                               ClusterMember member,
                                               std::uint64_t operation_id) {
  return raft_node_.proposeConfChange(type, std::move(member), operation_id);
}

bool RaftKVService::addLearner(NodeId learner_id) {
  return raft_node_.addLearner(learner_id);
}

bool RaftKVService::learnerCaughtUp(NodeId learner_id) const noexcept {
  return raft_node_.learnerCaughtUp(learner_id);
}

LogIndex RaftKVService::lastApplied() const noexcept {
  return state_machine_.lastApplied();
}

bool RaftKVService::tryPublishSnapshot(
    FileSnapshotStore& store, std::size_t entry_threshold,
    SnapshotPublishResult& result, std::string& error) {
  result = SnapshotPublishResult{};
  if (entry_threshold == 0) {
    error.clear();
    return true;
  }

  const LogIndex last_applied = state_machine_.lastApplied();
  if (last_applied <= last_snapshot_index_ ||
      last_applied - last_snapshot_index_ < entry_threshold) {
    error.clear();
    return true;
  }

  const auto boundary_term = raft_node_.log().termAt(last_applied);
  if (!boundary_term.has_value()) {
    error = "cannot snapshot without term at last applied index";
    return false;
  }

  const StateMachineSnapshot image =
      state_machine_.snapshot(*boundary_term);
  if (!store.save(image, error)) {
    return false;
  }
  if (!raft_node_.compactLogPrefix(last_applied, *boundary_term, error)) {
    return false;
  }

  last_snapshot_index_ = last_applied;
  result.performed = true;
  result.boundary_index = last_applied;
  result.boundary_term = *boundary_term;
  error.clear();
  return true;
}

LogIndex RaftKVService::lastSnapshotIndex() const noexcept {
  return last_snapshot_index_;
}

std::vector<OutboundRpc> RaftKVService::completeReplication(
    std::vector<OutboundRpc> outbound, FileSnapshotStore& store,
    std::string& error) {
  std::vector<OutboundRpc> result;
  result.reserve(outbound.size());
  for (OutboundRpc& rpc : outbound) {
    if (std::holds_alternative<AppendEntriesRequest>(rpc.payload) &&
        raft_node_.peerNeedsSnapshot(rpc.destination)) {
      LeaderSnapshotSend& send = snapshot_sends_[rpc.destination];
      const LogIndex boundary = raft_node_.log().firstIndex();
      const auto boundary_term = raft_node_.log().termAt(boundary);
      if (boundary_term.has_value() &&
          send.last_included_index == boundary &&
          send.last_included_term == *boundary_term) {
        send.offset = 0;
      }
      const auto replacement =
          makeInstallSnapshotRpc(rpc.destination, store, error);
      if (!error.empty()) {
        return {};
      }
      if (replacement.has_value()) {
        result.push_back(std::move(*replacement));
        continue;
      }
    }
    result.push_back(std::move(rpc));
  }
  error.clear();
  return result;
}

std::optional<OutboundRpc> RaftKVService::makeInstallSnapshotRpc(
    NodeId peer, FileSnapshotStore& store, std::string& error) {
  if (!raft_node_.peerNeedsSnapshot(peer)) {
    error.clear();
    return std::nullopt;
  }
  const LogIndex boundary = raft_node_.log().firstIndex();
  const auto boundary_term = raft_node_.log().termAt(boundary);
  if (!boundary_term.has_value()) {
    error = "leader snapshot boundary term is absent";
    return std::nullopt;
  }
  std::optional<StateMachineSnapshot> metadata;
  if (!store.load(metadata, error) || !metadata.has_value() ||
      metadata->last_included_index != boundary ||
      metadata->last_included_term != *boundary_term) {
    error.clear();
    return std::nullopt;
  }

  LeaderSnapshotSend& send = snapshot_sends_[peer];
  if (send.last_included_index != boundary ||
      send.last_included_term != *boundary_term) {
    send = LeaderSnapshotSend{boundary, *boundary_term, 0};
  }

  std::string chunk;
  bool eof = false;
  if (!store.readBytes(send.offset, kSnapshotChunkSize, chunk, eof, error)) {
    return std::nullopt;
  }
  send.offset += static_cast<std::uint64_t>(chunk.size());

  InstallSnapshotRequest request;
  request.term = raft_node_.currentTerm();
  request.leader_id = raft_node_.nodeId();
  request.last_included_index = boundary;
  request.last_included_term = *boundary_term;
  request.offset =
      send.offset - static_cast<std::uint64_t>(chunk.size());
  request.done = eof;
  request.data = std::move(chunk);
  error.clear();
  return OutboundRpc{peer, request};
}

InstallSnapshotResponse RaftKVService::handleInstallSnapshot(
    const InstallSnapshotRequest& request, FileSnapshotStore& store,
    std::string& error) {
  InstallSnapshotResponse response{raft_node_.currentTerm(), false};
  if (request.term < raft_node_.currentTerm()) {
    response.term = raft_node_.currentTerm();
    error.clear();
    return response;
  }
  if (!raft_node_.acknowledgeLeader(request.term, request.leader_id)) {
    response.term = raft_node_.currentTerm();
    error.clear();
    return response;
  }
  if (request.last_included_index < raft_node_.log().firstIndex() &&
      state_machine_.lastApplied() >= request.last_included_index) {
    response.success = true;
    error.clear();
    return response;
  }

  if (request.offset == 0) {
    if (snapshot_receiver_.has_value()) {
      snapshot_receiver_->cancel();
    }
    snapshot_receiver_.emplace(store.path());
  } else if (!snapshot_receiver_.has_value()) {
    error = "snapshot receive is not initialized";
    return response;
  }

  if (!snapshot_receiver_->appendChunk(
          request.last_included_index, request.last_included_term,
          request.offset, request.data, request.done, error)) {
    snapshot_receiver_->cancel();
    snapshot_receiver_.reset();
    return response;
  }

  if (!request.done) {
    response.success = true;
    error.clear();
    return response;
  }

  std::optional<StateMachineSnapshot> installed;
  if (!snapshot_receiver_->finishAndLoad(installed, error) ||
      !installed.has_value()) {
    snapshot_receiver_.reset();
    return response;
  }
  snapshot_receiver_.reset();

  if (!state_machine_.restore(*installed, error)) {
    return response;
  }
  if (!raft_node_.replaceLogWithSnapshotBoundary(
          installed->last_included_index, installed->last_included_term,
          error)) {
    return response;
  }
  last_snapshot_index_ = installed->last_included_index;
  if (!applyCommitted(error)) {
    response.success = false;
    return response;
  }

  response.success = true;
  error.clear();
  return response;
}

std::vector<OutboundRpc> RaftKVService::handleInstallSnapshotResponse(
    NodeId source, const InstallSnapshotResponse& response,
    FileSnapshotStore& store, std::string& error) {
  raft_node_.observeHigherTerm(response.term);
  if (raft_node_.role() != Role::kLeader ||
      response.term != raft_node_.currentTerm()) {
    error.clear();
    return {};
  }

  const auto send = snapshot_sends_.find(source);
  if (send == snapshot_sends_.end()) {
    error.clear();
    return {};
  }

  if (response.success) {
    std::uint64_t file_size = 0;
    if (!store.fileSize(file_size, error)) {
      return {};
    }
    if (send->second.offset < file_size) {
      const auto next = makeInstallSnapshotRpc(source, store, error);
      if (!error.empty()) {
        return {};
      }
      if (!next.has_value()) {
        error.clear();
        return {};
      }
      return {*next};
    }
    const LogIndex installed = send->second.last_included_index;
    snapshot_sends_.erase(send);
    raft_node_.onInstallSnapshotSuccess(source, installed);
    const AppendEntriesResponse continuation{
        raft_node_.currentTerm(),
        true,
        installed,
        0,
        installed,
        installed + 1U,
        std::nullopt,
        0,
    };
    return handleAppendEntriesResponse(source, continuation, error);
  }

  send->second.offset = 0;
  const auto retry = makeInstallSnapshotRpc(source, store, error);
  if (!error.empty()) {
    return {};
  }
  if (!retry.has_value()) {
    error.clear();
    return {};
  }
  return {*retry};
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
