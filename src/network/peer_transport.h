#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
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
  std::string fault_directory;
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

  /** Queues an endpoint update for the owning epoll thread. */
  [[nodiscard]] bool addPeer(PeerEndpoint peer);

  /** Queues removal and connection draining for one peer. */
  [[nodiscard]] bool removePeer(raft::NodeId peer_id);

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
  void releaseDelayedFrames();
  void queueFrame(raft::NodeId destination,
                  std::vector<std::uint8_t> frame);
  [[nodiscard]] bool faultEnabled(raft::NodeId destination,
                                  const std::string& action) const;
  [[nodiscard]] std::chrono::milliseconds faultDelay(
      raft::NodeId destination) const;
  void markFaultReached(raft::NodeId destination,
                        const std::string& action) const;
  void connectMissingPeers();
  void acceptConnections();
  void handleEvent(int fd, std::uint32_t events);
  void processInput(PeerConnection& peer);
  void updateInterest(PeerConnection& peer);
  void closeConnection(int fd);
  void applyPeerUpdates();

  struct PeerUpdate {
    bool add{false};
    PeerEndpoint endpoint;
    raft::NodeId peer_id{0};
  };

  struct DelayedFrame {
    raft::NodeId destination{0};
    std::vector<std::uint8_t> frame;
    std::chrono::steady_clock::time_point release_at;
  };

  PeerTransportConfig config_;
  common::BoundedQueue<RaftMessage>& inbound_;
  common::BoundedQueue<PeerSend>& outbound_;
  std::unordered_map<raft::NodeId, PeerEndpoint> endpoints_;
  std::unordered_map<int, std::unique_ptr<PeerConnection>> connections_;
  std::unordered_map<raft::NodeId, int> outbound_connections_;
  std::unordered_map<raft::NodeId,
                     std::vector<std::vector<std::uint8_t>>>
      pending_frames_;
  std::vector<DelayedFrame> delayed_frames_;
  int listen_fd_{-1};
  int epoll_fd_{-1};
  mutable std::mutex update_mutex_;
  std::vector<PeerUpdate> pending_updates_;
};

}  // namespace distributed_kv::network
