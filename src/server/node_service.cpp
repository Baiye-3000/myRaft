#include "server/node_service.h"

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

#include "raft/cluster_config.h"
#include "raft/kv_command.h"

namespace distributed_kv::server {
namespace {

const ClusterNodeEndpoint& selfEndpoint(
    const NodeServiceConfig& config) {
  const auto found =
      std::find_if(config.members.begin(), config.members.end(),
                   [&config](const ClusterNodeEndpoint& member) {
                     return member.node_id == config.node_id;
                   });
  if (found == config.members.end()) {
    throw std::invalid_argument("node id is absent from membership");
  }
  return *found;
}

std::vector<network::MemberEndpoint> memberEndpoints(
    const raft::RaftNode& node) {
  std::vector<network::MemberEndpoint> result;
  if (!node.clusterConfig().has_value()) return result;
  result.reserve(node.clusterConfig()->members.size());
  for (const raft::ClusterMember& member : node.clusterConfig()->members) {
    result.push_back(network::MemberEndpoint{
        member.node_id, member.client_host, member.client_port,
        member.peer_host, member.peer_port});
  }
  return result;
}

raft::MembershipOperation membershipOperation(
    const network::Request& request) {
  const bool adding =
      request.type == network::MessageType::kAddNodeRequest;
  return raft::MembershipOperation{
      request.operation_id,
      adding ? raft::ConfChangeType::kAddNode
             : raft::ConfChangeType::kRemoveNode,
      adding ? raft::ClusterMember{request.node_id, request.client_host,
                                   request.client_port, request.peer_host,
                                   request.peer_port}
             : raft::ClusterMember{request.node_id, {}, 0, {}, 0}};
}

}  // namespace

raft::NodeConfig NodeService::makeRaftConfig(
    const NodeServiceConfig& config) {
  raft::NodeConfig result;
  result.node_id = config.node_id;
  result.election_timeout_min_ms = config.election_timeout_min_ms;
  result.election_timeout_max_ms = config.election_timeout_max_ms;
  result.heartbeat_interval_ms = config.heartbeat_interval_ms;
  result.random_seed = config.node_id * 0x9e3779b97f4a7c15ULL;
  raft::ClusterConfiguration bootstrap;
  bootstrap.config_id = 1;
  bootstrap.members.reserve(config.members.size());
  for (const ClusterNodeEndpoint& member : config.members) {
    if (config.learner && member.node_id == config.node_id) continue;
    bootstrap.members.push_back(raft::ClusterMember{
        member.node_id, member.client_host, member.client_port,
        member.peer_host, member.peer_port});
  }
  std::sort(bootstrap.members.begin(), bootstrap.members.end(),
            [](const raft::ClusterMember& left,
               const raft::ClusterMember& right) {
              return left.node_id < right.node_id;
            });
  result.bootstrap_cluster = std::move(bootstrap);
  result.learner = config.learner;
  for (const ClusterNodeEndpoint& member : config.members) {
    if (member.node_id != config.node_id) {
      result.peers.push_back(member.node_id);
    }
  }
  return result;
}

network::ServerConfig NodeService::makeServerConfig(
    const NodeServiceConfig& config) {
  const ClusterNodeEndpoint& self = selfEndpoint(config);
  network::ServerConfig result;
  result.bind_address = self.client_host;
  result.port = self.client_port;
  return result;
}

network::PeerTransportConfig NodeService::makePeerConfig(
    const NodeServiceConfig& config) {
  const ClusterNodeEndpoint& self = selfEndpoint(config);
  network::PeerTransportConfig result;
  result.node_id = config.node_id;
  result.bind_host = self.peer_host;
  result.bind_port = self.peer_port;
  result.fault_directory = config.peer_fault_directory;
  for (const ClusterNodeEndpoint& member : config.members) {
    if (member.node_id != config.node_id) {
      result.peers.push_back(network::PeerEndpoint{
          member.node_id, member.peer_host, member.peer_port});
    }
  }
  return result;
}

std::optional<raft::StateMachineSnapshot> NodeService::loadSnapshot(
    const raft::FileSnapshotStore& store) {
  std::optional<raft::StateMachineSnapshot> snapshot;
  std::string error;
  if (!store.load(snapshot, error)) {
    throw std::runtime_error("failed to load state snapshot: " + error);
  }
  return snapshot;
}

NodeService::NodeService(NodeServiceConfig config)
    : config_(std::move(config)),
      persistence_(config_.data_directory + "/raft.wal"),
      snapshot_store_(config_.data_directory + "/state.snapshot"),
      snapshot_(loadSnapshot(snapshot_store_)),
      raft_service_(makeRaftConfig(config_), store_, &persistence_,
                    snapshot_.has_value() ? &*snapshot_ : nullptr,
                    &snapshot_store_),
      client_requests_(config_.queue_capacity),
      client_responses_(config_.queue_capacity),
      peer_inbound_(config_.queue_capacity),
      peer_outbound_(config_.queue_capacity),
      client_server_(makeServerConfig(config_), client_requests_,
                     client_responses_),
      peer_transport_(makePeerConfig(config_), peer_inbound_,
                      peer_outbound_) {
  if (config_.data_directory.empty() || config_.members.empty() ||
      config_.queue_capacity == 0 || config_.read_timeout_ms == 0) {
    throw std::invalid_argument("invalid NodeService configuration");
  }
  if (config_.learner && config_.members.size() < 2U) {
    throw std::invalid_argument("learner requires an existing voter endpoint");
  }
  if (config_.membership_pause_stage.empty() !=
      config_.membership_pause_directory.empty()) {
    throw std::invalid_argument("membership pause configuration is incomplete");
  }
}

NodeService::~NodeService() { stop(); }

bool NodeService::start(std::string& error) {
  if (client_thread_.joinable() || peer_thread_.joinable() ||
      raft_thread_.joinable()) {
    error = "NodeService is already started";
    return false;
  }
  stop_requested_.store(false);
  if (!client_server_.start(error)) {
    return false;
  }
  if (!peer_transport_.start(error)) {
    client_server_.stop();
    return false;
  }
  client_thread_ = std::thread([this] {
    std::string thread_error;
    if (!client_server_.run(thread_error) && !stop_requested_.load()) {
      setFatal(std::move(thread_error));
    }
  });
  peer_thread_ = std::thread([this] {
    std::string thread_error;
    if (!peer_transport_.run(stop_requested_, thread_error) &&
        !stop_requested_.load()) {
      setFatal(std::move(thread_error));
    }
  });
  raft_thread_ = std::thread([this] { runRaft(); });
  error.clear();
  return true;
}

void NodeService::stop() noexcept {
  stop_requested_.store(true);
  client_server_.stop();
  client_requests_.close();
  peer_inbound_.close();
  if (client_thread_.joinable() &&
      client_thread_.get_id() != std::this_thread::get_id()) {
    client_thread_.join();
  }
  if (peer_thread_.joinable() &&
      peer_thread_.get_id() != std::this_thread::get_id()) {
    peer_thread_.join();
  }
  if (raft_thread_.joinable() &&
      raft_thread_.get_id() != std::this_thread::get_id()) {
    raft_thread_.join();
  }
  peer_outbound_.close();
  client_responses_.close();
  peer_transport_.close();
}

bool NodeService::wait(std::string& error) {
  if (client_thread_.joinable()) {
    client_thread_.join();
  }
  if (peer_thread_.joinable()) {
    peer_thread_.join();
  }
  if (raft_thread_.joinable()) {
    raft_thread_.join();
  }
  std::lock_guard<std::mutex> lock(fatal_mutex_);
  error = fatal_error_;
  return error.empty();
}

void NodeService::runRaft() noexcept {
  try {
    auto previous = std::chrono::steady_clock::now();
    while (!stop_requested_.load()) {
      std::string error;
      processPeerMessages(error);
      if (!error.empty()) {
        throw std::runtime_error(error);
      }
      syncPeerTransport();
      processClientRequests(error);
      if (!error.empty()) {
        throw std::runtime_error(error);
      }
      startReadBatchIfReady();
      const auto now = std::chrono::steady_clock::now();
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - previous);
      if (elapsed.count() > 0) {
        previous = now;
        sendRpcs(raft_service_.tick(
            static_cast<std::uint64_t>(elapsed.count()), error));
        if (!error.empty()) {
          throw std::runtime_error(error);
        }
      }
      completePending();
      processMembershipChange();
      raft::SnapshotPublishResult snapshot_result;
      if (!raft_service_.tryPublishSnapshot(
              snapshot_store_, config_.snapshot_entry_threshold,
              snapshot_result, error)) {
        throw std::runtime_error(error);
      }
      if (snapshot_result.performed) {
        snapshot_ = loadSnapshot(snapshot_store_);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  } catch (const std::exception& exception) {
    setFatal(exception.what());
  } catch (...) {
    setFatal("unknown Raft event-thread failure");
  }
}

void NodeService::processPeerMessages(std::string& error) {
  network::RaftMessage message;
  while (peer_inbound_.popFor(message, std::chrono::milliseconds(0))) {
    std::visit(
        [this, &message, &error](const auto& rpc) {
          using Type = std::decay_t<decltype(rpc)>;
          if constexpr (std::is_same_v<Type,
                                       raft::RequestVoteRequest>) {
            if (rpc.candidate_id != message.source) {
              return;
            }
            const raft::RequestVoteResponse response =
                raft_service_.handleRequestVote(rpc);
            static_cast<void>(peer_outbound_.tryPush(network::PeerSend{
                message.source, raft::RpcPayload{response}}));
          } else if constexpr (std::is_same_v<
                                   Type, raft::RequestVoteResponse>) {
            sendRpcs(raft_service_.handleRequestVoteResponse(
                message.source, rpc, error));
          } else if constexpr (std::is_same_v<
                                   Type, raft::AppendEntriesRequest>) {
            if (rpc.leader_id != message.source) {
              return;
            }
            const raft::AppendEntriesResponse response =
                raft_service_.handleAppendEntries(rpc, error);
            static_cast<void>(peer_outbound_.tryPush(network::PeerSend{
                message.source, raft::RpcPayload{response}}));
          } else if constexpr (std::is_same_v<
                                   Type, raft::InstallSnapshotRequest>) {
            if (rpc.leader_id != message.source) {
              return;
            }
            const raft::InstallSnapshotResponse response =
                raft_service_.handleInstallSnapshot(rpc, snapshot_store_,
                                                    error);
            static_cast<void>(peer_outbound_.tryPush(network::PeerSend{
                message.source, raft::RpcPayload{response}}));
          } else if constexpr (std::is_same_v<
                                   Type, raft::InstallSnapshotResponse>) {
            sendRpcs(raft_service_.handleInstallSnapshotResponse(
                message.source, rpc, snapshot_store_, error));
          } else {
            if (rpc.success && rpc.read_context != 0) {
              const auto read = pending_reads_.find(rpc.read_context);
              if (read != pending_reads_.end() &&
                  read->second.term ==
                      raft_service_.raftNode().currentTerm()) {
                read->second.acknowledgements.insert(message.source);
              }
            }
            sendRpcs(raft_service_.handleAppendEntriesResponse(
                message.source, rpc, error));
          }
        },
        message.payload);
    if (!error.empty()) {
      return;
    }
  }
  error.clear();
}

void NodeService::processClientRequests(std::string& error) {
  network::ClientRequestEvent event;
  while (client_requests_.popFor(event, std::chrono::milliseconds(0))) {
    if (event.request.type == network::MessageType::kListMembersRequest) {
      sendClient(event.connection_id,
                 network::Response{event.request.request_id,
                                   network::StatusCode::kOk, "OK", {}, 0,
                                   memberEndpoints(raft_service_.raftNode())});
      continue;
    }
    if (event.request.type == network::MessageType::kAddNodeRequest ||
        event.request.type == network::MessageType::kRemoveNodeRequest) {
      const raft::RaftNode& node = raft_service_.raftNode();
      if (node.role() != raft::Role::kLeader) {
        sendClient(event.connection_id,
                   notLeaderResponse(event.request.request_id));
        continue;
      }
      const raft::MembershipOperation operation =
          membershipOperation(event.request);
      if (node.completedMembershipOperation().has_value() &&
          node.completedMembershipOperation()->operation_id ==
              operation.operation_id) {
        if (*node.completedMembershipOperation() == operation) {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kOk,
                                       "OK_REPLAYED", {}, 0,
                                       memberEndpoints(node)});
        } else {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kInvalidRequest,
                                       "OPERATION_ID_REUSED"});
        }
        continue;
      }
      if (node.activeMembershipOperation().has_value()) {
        if (node.activeMembershipOperation()->operation_id !=
            operation.operation_id) {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kServerBusy,
                                       "MEMBERSHIP_CHANGE_IN_PROGRESS"});
        } else if (!(*node.activeMembershipOperation() == operation)) {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kInvalidRequest,
                                       "OPERATION_ID_REUSED"});
        } else {
          pending_membership_change_ = PendingMembershipChange{
              event.connection_id, std::move(event.request), 0};
        }
        continue;
      }
      if (pending_membership_change_.has_value()) {
        const raft::MembershipOperation pending_operation =
            membershipOperation(pending_membership_change_->request);
        if (pending_operation.operation_id == operation.operation_id &&
            pending_operation == operation) {
          pending_membership_change_->connection_id = event.connection_id;
          pending_membership_change_->request = std::move(event.request);
        } else if (pending_operation.operation_id == operation.operation_id) {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kInvalidRequest,
                                       "OPERATION_ID_REUSED"});
        } else {
          sendClient(event.connection_id,
                     network::Response{event.request.request_id,
                                       network::StatusCode::kServerBusy,
                                       "MEMBERSHIP_CHANGE_IN_PROGRESS"});
        }
        continue;
      }
      if (node.jointConfig().has_value()) {
        sendClient(event.connection_id,
                   network::Response{event.request.request_id,
                                     network::StatusCode::kServerBusy,
                                     "MEMBERSHIP_CHANGE_IN_PROGRESS"});
        continue;
      }

      PendingMembershipChange pending;
      pending.connection_id = event.connection_id;
      pending.request = std::move(event.request);
      if (pending.request.type == network::MessageType::kAddNodeRequest) {
        const network::PeerEndpoint endpoint{
            pending.request.node_id, pending.request.peer_host,
            pending.request.peer_port};
        if (!raft_service_.addLearner(pending.request.node_id)) {
          sendClient(pending.connection_id,
                     network::Response{pending.request.request_id,
                                       network::StatusCode::kInvalidRequest,
                                       "ADD_NODE_REJECTED"});
          continue;
        }
        learner_peers_[pending.request.node_id] = endpoint;
      } else {
        const raft::ProposeResult proposed = raft_service_.proposeConfChange(
            raft::ConfChangeType::kRemoveNode,
            raft::ClusterMember{pending.request.node_id, {}, 0, {}, 0},
            pending.request.operation_id);
        if (!proposed.accepted) {
          sendClient(pending.connection_id,
                     network::Response{pending.request.request_id,
                                       network::StatusCode::kInvalidRequest,
                                       "REMOVE_NODE_REJECTED"});
          continue;
        }
        pending.log_index = proposed.index;
        sendRpcs(proposed.outbound);
      }
      pending_membership_change_ = std::move(pending);
      continue;
    }

    if (event.request.type == network::MessageType::kGetRequest) {
      const raft::RaftNode& node = raft_service_.raftNode();
      const auto committed_term = node.log().termAt(node.commitIndex());
      if (node.role() != raft::Role::kLeader) {
        sendClient(event.connection_id,
                   notLeaderResponse(event.request.request_id));
      } else if (!committed_term.has_value() ||
                 *committed_term != node.currentTerm()) {
        sendClient(event.connection_id,
                   network::Response{
                       event.request.request_id,
                       network::StatusCode::kUnavailable,
                       "LEADER_NOT_READY"});
      } else {
        if (deferred_reads_.empty()) {
          read_batch_started_ = std::chrono::steady_clock::now();
        }
        deferred_reads_.push_back(std::move(event));
        read_request_count_.fetch_add(1, std::memory_order_relaxed);
      }
      continue;
    }

    raft::KVCommandType command_type;
    if (event.request.type == network::MessageType::kSetRequest) {
      command_type = raft::KVCommandType::kSet;
    } else if (event.request.type ==
               network::MessageType::kDeleteRequest) {
      command_type = raft::KVCommandType::kDelete;
    } else {
      sendClient(event.connection_id,
                 network::Response{
                     event.request.request_id,
                     network::StatusCode::kInvalidRequest,
                     "unsupported request"});
      continue;
    }
    const raft::KVCommand command{
        command_type, event.request.client_id, event.request.request_id,
        event.request.key, event.request.value};
    raft::SubmitResult result = raft_service_.submit(command, error);
    sendRpcs(std::move(result.outbound));
    if (!error.empty()) {
      return;
    }
    if (result.status == raft::SubmitStatus::kNotLeader) {
      sendClient(event.connection_id,
                 notLeaderResponse(event.request.request_id));
    } else if (result.status == raft::SubmitStatus::kInvalid) {
      sendClient(event.connection_id,
                 network::Response{
                     event.request.request_id,
                     network::StatusCode::kInvalidRequest,
                     "INVALID_REQUEST"});
    } else if (result.status == raft::SubmitStatus::kApplied &&
               result.result.has_value()) {
      const raft::ApplyResult& applied = *result.result;
      sendClient(
          event.connection_id,
          network::Response{
              event.request.request_id,
              applied.status == raft::ApplyStatus::kOk
                  ? network::StatusCode::kOk
                  : (applied.status == raft::ApplyStatus::kNotFound
                         ? network::StatusCode::kNotFound
                         : network::StatusCode::kInvalidRequest),
              applied.payload});
    } else {
      pending_writes_[result.log_index] =
          PendingWrite{event.connection_id, std::move(event.request)};
    }
  }
  error.clear();
}

void NodeService::processMembershipChange() {
  if (!pending_membership_change_.has_value()) return;
  PendingMembershipChange& pending = *pending_membership_change_;
  const raft::RaftNode& node = raft_service_.raftNode();
  if (node.role() != raft::Role::kLeader) {
    learner_peers_.erase(pending.request.node_id);
    sendClient(pending.connection_id,
               notLeaderResponse(pending.request.request_id));
    pending_membership_change_.reset();
    return;
  }

  const bool adding =
      pending.request.type == network::MessageType::kAddNodeRequest;
  if (adding && !pending.log_index.has_value()) {
    if (!raft_service_.learnerCaughtUp(pending.request.node_id)) return;
    pauseAtMembershipStage("learner-caught-up");
    if (stop_requested_.load()) return;
    const raft::ClusterMember member{
        pending.request.node_id, pending.request.client_host,
        pending.request.client_port, pending.request.peer_host,
        pending.request.peer_port};
    const raft::ProposeResult proposed =
        raft_service_.proposeConfChange(raft::ConfChangeType::kAddNode, member,
                                        pending.request.operation_id);
    if (!proposed.accepted) {
      sendClient(pending.connection_id,
                 network::Response{pending.request.request_id,
                                   network::StatusCode::kInternalError,
                                   "ADD_NODE_PROPOSAL_FAILED"});
      pending_membership_change_.reset();
      return;
    }
    pending.log_index = proposed.index;
    pauseAtMembershipStage("joint-proposed");
    if (stop_requested_.load()) return;
    sendRpcs(proposed.outbound);
    return;
  }

  if (pending.log_index.has_value() && node.jointConfig().has_value() &&
      node.commitIndex() >= *pending.log_index) {
    pauseAtMembershipStage("joint-committed");
    if (stop_requested_.load()) return;
  }

  if (!pending.log_index.has_value() || node.jointConfig().has_value() ||
      !node.clusterConfig().has_value()) return;
  const bool present = std::any_of(
      node.clusterConfig()->members.begin(), node.clusterConfig()->members.end(),
      [&pending](const raft::ClusterMember& member) {
        return member.node_id == pending.request.node_id;
      });
  if (present != adding) return;

  learner_peers_.erase(pending.request.node_id);
  sendClient(pending.connection_id,
             network::Response{pending.request.request_id,
                               network::StatusCode::kOk, "OK", {}, 0,
                               memberEndpoints(node)});
  pending_membership_change_.reset();
}

void NodeService::pauseAtMembershipStage(const std::string& stage) {
  if (config_.membership_pause_stage != stage) return;
  const std::string prefix = config_.membership_pause_directory + "/node" +
                             std::to_string(config_.node_id) + "-" + stage;
  const std::string reached = prefix + ".reached";
  const int descriptor =
      ::open(reached.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (descriptor >= 0) static_cast<void>(::close(descriptor));
  const std::string resume = prefix + ".continue";
  while (!stop_requested_.load() && ::access(resume.c_str(), F_OK) != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
}

void NodeService::startReadBatchIfReady() {
  if (deferred_reads_.empty() || !read_batch_started_.has_value()) {
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  if (now - *read_batch_started_ <
      std::chrono::milliseconds(config_.read_batch_window_ms)) {
    return;
  }
  const raft::RaftNode& node = raft_service_.raftNode();
  const auto committed_term = node.log().termAt(node.commitIndex());
  if (node.role() != raft::Role::kLeader) {
    for (const auto& event : deferred_reads_) {
      sendClient(event.connection_id,
                 notLeaderResponse(event.request.request_id));
    }
    deferred_reads_.clear();
    read_batch_started_.reset();
    return;
  }
  if (!committed_term.has_value() ||
      *committed_term != node.currentTerm()) {
    for (const auto& event : deferred_reads_) {
      sendClient(event.connection_id,
                 network::Response{event.request.request_id,
                                   network::StatusCode::kUnavailable,
                                   "LEADER_NOT_READY"});
    }
    deferred_reads_.clear();
    read_batch_started_.reset();
    return;
  }
  std::uint64_t context = next_read_context_++;
  if (context == 0) {
    context = next_read_context_++;
  }
  PendingRead pending;
  pending.requests = std::move(deferred_reads_);
  pending.term = node.currentTerm();
  pending.read_index = node.commitIndex();
  pending.acknowledgements.insert(config_.node_id);
  pending.deadline =
      now + std::chrono::milliseconds(config_.read_timeout_ms);
  pending_reads_.emplace(context, std::move(pending));
  read_batch_started_.reset();
  read_barrier_count_.fetch_add(1, std::memory_order_relaxed);
  sendRpcs(node.makeReadBarrier(context));
}

void NodeService::completePending() {
  const raft::RaftNode& node = raft_service_.raftNode();
  for (auto iterator = pending_writes_.begin();
       iterator != pending_writes_.end();) {
    if (node.role() != raft::Role::kLeader) {
      sendClient(iterator->second.connection_id,
                 notLeaderResponse(iterator->second.request.request_id));
      iterator = pending_writes_.erase(iterator);
      continue;
    }
    const auto result = raft_service_.takeResult(iterator->first);
    if (!result.has_value()) {
      ++iterator;
      continue;
    }
    const network::StatusCode status =
        result->status == raft::ApplyStatus::kOk
            ? network::StatusCode::kOk
            : (result->status == raft::ApplyStatus::kNotFound
                   ? network::StatusCode::kNotFound
                   : network::StatusCode::kInvalidRequest);
    sendClient(iterator->second.connection_id,
               network::Response{iterator->second.request.request_id,
                                 status, result->payload});
    iterator = pending_writes_.erase(iterator);
  }

  const auto now = std::chrono::steady_clock::now();
  for (auto iterator = pending_reads_.begin();
       iterator != pending_reads_.end();) {
    PendingRead& read = iterator->second;
    if (node.role() != raft::Role::kLeader ||
        node.currentTerm() != read.term) {
      for (const auto& event : read.requests) {
        sendClient(event.connection_id,
                   notLeaderResponse(event.request.request_id));
      }
      iterator = pending_reads_.erase(iterator);
    } else if (now >= read.deadline) {
      for (const auto& event : read.requests) {
        sendClient(event.connection_id,
                   network::Response{event.request.request_id,
                                     network::StatusCode::kUnavailable,
                                     "READ_QUORUM_TIMEOUT"});
      }
      iterator = pending_reads_.erase(iterator);
    } else if (readQuorumReached(read) &&
               raft_service_.lastApplied() >= read.read_index) {
      for (const auto& event : read.requests) {
        const auto value =
            raft_service_.getApplied(event.request.key);
        sendClient(event.connection_id,
                   network::Response{
                       event.request.request_id,
                       value.has_value()
                           ? network::StatusCode::kOk
                           : network::StatusCode::kNotFound,
                       value.value_or("NOT_FOUND")});
      }
      iterator = pending_reads_.erase(iterator);
    } else {
      ++iterator;
    }
  }
}

void NodeService::syncPeerTransport() {
  std::unordered_map<raft::NodeId, network::PeerEndpoint> desired;
  const auto& node = raft_service_.raftNode();
  auto addConfiguration = [&desired, this](
                              const raft::ClusterConfiguration& configuration) {
    for (const raft::ClusterMember& member : configuration.members) {
      if (member.node_id != config_.node_id) {
        desired[member.node_id] = network::PeerEndpoint{
            member.node_id, member.peer_host, member.peer_port};
      }
    }
  };
  if (node.clusterConfig().has_value()) {
    addConfiguration(*node.clusterConfig());
    if (node.jointConfig().has_value()) addConfiguration(*node.jointConfig());
    config_.members.clear();
    for (const raft::ClusterMember& member : node.clusterConfig()->members) {
      config_.members.push_back(ClusterNodeEndpoint{
          member.node_id, member.client_host, member.client_port,
          member.peer_host, member.peer_port});
    }
  } else {
    for (const ClusterNodeEndpoint& member : config_.members) {
      if (member.node_id != config_.node_id) {
        desired[member.node_id] = network::PeerEndpoint{
            member.node_id, member.peer_host, member.peer_port};
      }
    }
  }
  for (const auto& learner : learner_peers_) {
    desired[learner.first] = learner.second;
  }
  for (const auto& item : desired) {
    const auto existing = transport_peers_.find(item.first);
    if (existing == transport_peers_.end() ||
        existing->second.host != item.second.host ||
        existing->second.port != item.second.port) {
      if (existing != transport_peers_.end()) {
        static_cast<void>(peer_transport_.removePeer(item.first));
      }
      if (peer_transport_.addPeer(item.second)) {
        transport_peers_[item.first] = item.second;
      }
    }
  }
  for (const auto& item : transport_peers_) {
    if (desired.find(item.first) == desired.end()) {
      static_cast<void>(peer_transport_.removePeer(item.first));
    }
  }
  for (auto iterator = transport_peers_.begin();
       iterator != transport_peers_.end();) {
    if (desired.find(iterator->first) == desired.end()) {
      iterator = transport_peers_.erase(iterator);
    } else {
      ++iterator;
    }
  }
}

bool NodeService::readQuorumReached(const PendingRead& read) const {
  const auto& node = raft_service_.raftNode();
  const auto count = [&read](const raft::ClusterConfiguration& configuration) {
    std::size_t granted = 0;
    for (const raft::ClusterMember& member : configuration.members) {
      if (read.acknowledgements.count(member.node_id) != 0U) ++granted;
    }
    return granted >= configuration.members.size() / 2U + 1U;
  };
  if (node.clusterConfig().has_value()) {
    if (!count(*node.clusterConfig())) return false;
    return !node.jointConfig().has_value() || count(*node.jointConfig());
  }
  return read.acknowledgements.size() >= config_.members.size() / 2U + 1U;
}

NodeServiceMetrics NodeService::metrics() const noexcept {
  return NodeServiceMetrics{
      read_request_count_.load(std::memory_order_relaxed),
      read_barrier_count_.load(std::memory_order_relaxed),
  };
}

void NodeService::sendRpcs(std::vector<raft::OutboundRpc> outbound) {
  for (raft::OutboundRpc& rpc : outbound) {
    static_cast<void>(peer_outbound_.tryPush(network::PeerSend{
        rpc.destination, std::move(rpc.payload)}));
  }
}

void NodeService::sendClient(std::uint64_t connection_id,
                             network::Response response) {
  if (client_responses_.tryPush(
          network::ClientResponseEvent{connection_id,
                                       std::move(response)})) {
    client_server_.notifyResponses();
  }
}

network::Response NodeService::notLeaderResponse(
    std::uint64_t request_id) const {
  network::Response response{request_id, network::StatusCode::kNotLeader,
                             "NOT_LEADER"};
  const auto leader = raft_service_.raftNode().leaderId();
  if (!leader.has_value()) {
    return response;
  }
  const auto endpoint =
      std::find_if(config_.members.begin(), config_.members.end(),
                   [&leader](const ClusterNodeEndpoint& member) {
                     return member.node_id == *leader;
                   });
  if (endpoint != config_.members.end()) {
    response.leader_host = endpoint->client_host;
    response.leader_port = endpoint->client_port;
  }
  return response;
}

void NodeService::setFatal(std::string error) noexcept {
  {
    std::lock_guard<std::mutex> lock(fatal_mutex_);
    if (fatal_error_.empty()) {
      fatal_error_ = error.empty() ? "node runtime failed" : std::move(error);
    }
  }
  stop_requested_.store(true);
  client_server_.stop();
  client_requests_.close();
  peer_inbound_.close();
}

}  // namespace distributed_kv::server
