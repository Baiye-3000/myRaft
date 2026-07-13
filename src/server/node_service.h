#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/bounded_queue.h"
#include "network/peer_transport.h"
#include "network/server.h"
#include "raft/file_raft_persistence.h"
#include "raft/file_snapshot_store.h"
#include "raft/raft_kv_service.h"
#include "storage/kv_store.h"

namespace distributed_kv::server {

struct ClusterNodeEndpoint {
  raft::NodeId node_id{0};
  std::string client_host;
  std::uint16_t client_port{0};
  std::string peer_host;
  std::uint16_t peer_port{0};
};

struct NodeServiceConfig {
  raft::NodeId node_id{0};
  std::string data_directory;
  std::vector<ClusterNodeEndpoint> members;
  std::uint64_t election_timeout_min_ms{300};
  std::uint64_t election_timeout_max_ms{600};
  std::uint64_t heartbeat_interval_ms{100};
  std::uint64_t read_timeout_ms{1000};
  std::uint64_t read_batch_window_ms{0};
  std::size_t queue_capacity{4096};
};

struct NodeServiceMetrics {
  std::uint64_t read_requests{0};
  std::uint64_t read_barriers{0};
};

/**
 * Owns the Client IO, Peer IO, and Raft event threads for one real node.
 */
class NodeService final {
 public:
  explicit NodeService(NodeServiceConfig config);
  ~NodeService();

  NodeService(const NodeService&) = delete;
  NodeService& operator=(const NodeService&) = delete;

  /**
   * Starts listeners and all three managed threads.
   *
   * Input: writable error. Output: true when all listeners are ready.
   * Thread safety: exclusive lifecycle call.
   */
  [[nodiscard]] bool start(std::string& error);

  /**
   * Requests orderly shutdown and joins all threads.
   *
   * Input/output: none. Thread safety: idempotent from lifecycle thread.
   */
  void stop() noexcept;

  /**
   * Joins worker threads and reports a fatal runtime error.
   *
   * Input: writable error. Output: true for an externally requested stop.
   * Thread safety: lifecycle thread only.
   */
  [[nodiscard]] bool wait(std::string& error);

  [[nodiscard]] bool stopped() const noexcept {
    return stop_requested_.load();
  }

  /**
   * Returns lock-free counters for read batching observability.
   *
   * Input: none. Output: point-in-time request/barrier counts.
   * Thread safety: safe from test and monitoring threads.
   */
  [[nodiscard]] NodeServiceMetrics metrics() const noexcept;

 private:
  struct PendingWrite {
    std::uint64_t connection_id{0};
    network::Request request;
  };

  struct PendingRead {
    std::vector<network::ClientRequestEvent> requests;
    raft::Term term{0};
    raft::LogIndex read_index{0};
    std::unordered_set<raft::NodeId> acknowledgements;
    std::chrono::steady_clock::time_point deadline;
  };

  static raft::NodeConfig makeRaftConfig(
      const NodeServiceConfig& config);
  static network::ServerConfig makeServerConfig(
      const NodeServiceConfig& config);
  static network::PeerTransportConfig makePeerConfig(
      const NodeServiceConfig& config);
  static std::optional<raft::StateMachineSnapshot> loadSnapshot(
      const raft::FileSnapshotStore& store);

  void runRaft() noexcept;
  void processPeerMessages(std::string& error);
  void processClientRequests(std::string& error);
  void startReadBatchIfReady();
  void completePending();
  void sendRpcs(std::vector<raft::OutboundRpc> outbound);
  void sendClient(std::uint64_t connection_id,
                  network::Response response);
  [[nodiscard]] network::Response notLeaderResponse(
      std::uint64_t request_id) const;
  void setFatal(std::string error) noexcept;

  NodeServiceConfig config_;
  storage::KVStore store_;
  raft::FileRaftPersistence persistence_;
  raft::FileSnapshotStore snapshot_store_;
  std::optional<raft::StateMachineSnapshot> snapshot_;
  raft::RaftKVService raft_service_;
  common::BoundedQueue<network::ClientRequestEvent> client_requests_;
  common::BoundedQueue<network::ClientResponseEvent> client_responses_;
  common::BoundedQueue<network::RaftMessage> peer_inbound_;
  common::BoundedQueue<network::PeerSend> peer_outbound_;
  network::Server client_server_;
  network::PeerTransport peer_transport_;
  std::unordered_map<raft::LogIndex, PendingWrite> pending_writes_;
  std::unordered_map<std::uint64_t, PendingRead> pending_reads_;
  std::vector<network::ClientRequestEvent> deferred_reads_;
  std::optional<std::chrono::steady_clock::time_point> read_batch_started_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<std::uint64_t> read_request_count_{0};
  std::atomic<std::uint64_t> read_barrier_count_{0};
  std::thread client_thread_;
  std::thread peer_thread_;
  std::thread raft_thread_;
  mutable std::mutex fatal_mutex_;
  std::string fatal_error_;
  std::uint64_t next_read_context_{1};
};

}  // namespace distributed_kv::server
