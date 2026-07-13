#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/bounded_queue.h"
#include "network/raft_protocol.h"

namespace distributed_kv::network {

struct PeerEndpoint {
  raft::NodeId node_id{0};
  std::string host;
  std::uint16_t port{0};
};

struct PeerSend {
  raft::NodeId destination{0};
  raft::RpcPayload payload;
};

struct PeerTransportConfig {
  raft::NodeId node_id{0};
  std::string bind_host{"127.0.0.1"};
  std::uint16_t bind_port{0};
  std::vector<PeerEndpoint> peers;
  int poll_timeout_ms{20};
};

/**
 * Single-threaded epoll transport for persistent Raft peer connections.
 *
 * The transport owns only byte I/O. Decoded RPCs and outbound requests cross
 * bounded queues; it never mutates Raft state.
 */
class PeerTransport final {
 public:
  PeerTransport(PeerTransportConfig config,
                common::BoundedQueue<RaftMessage>& inbound,
                common::BoundedQueue<PeerSend>& outbound);
  ~PeerTransport();

  PeerTransport(const PeerTransport&) = delete;
  PeerTransport& operator=(const PeerTransport&) = delete;

  /**
   * Opens the listener and epoll resources.
   *
   * Input: writable error. Output: true when ready to run.
   * Thread safety: call once before run.
   */
  [[nodiscard]] bool start(std::string& error);

  /**
   * Runs until stop becomes true or a fatal listener error occurs.
   *
   * Input: shared stop flag and writable error. Output: true on clean stop.
   * Thread safety: exactly one Peer IO thread.
   */
  [[nodiscard]] bool run(const std::atomic<bool>& stop, std::string& error);

  /**
   * Closes all descriptors.
   *
   * Input/output: none. Thread safety: call after run exits.
   */
  void close() noexcept;

 private:
  struct PeerConnection;

  [[nodiscard]] bool openListener(std::string& error);
  void drainOutbound();
  void connectMissingPeers();
  void acceptConnections();
  void handleEvent(int fd, std::uint32_t events);
  void processInput(PeerConnection& peer);
  void updateInterest(PeerConnection& peer);
  void closeConnection(int fd);

  PeerTransportConfig config_;
  common::BoundedQueue<RaftMessage>& inbound_;
  common::BoundedQueue<PeerSend>& outbound_;
  std::unordered_map<raft::NodeId, PeerEndpoint> endpoints_;
  std::unordered_map<int, std::unique_ptr<PeerConnection>> connections_;
  std::unordered_map<raft::NodeId, int> outbound_connections_;
  std::unordered_map<raft::NodeId,
                     std::vector<std::vector<std::uint8_t>>>
      pending_frames_;
  int listen_fd_{-1};
  int epoll_fd_{-1};
};

}  // namespace distributed_kv::network
