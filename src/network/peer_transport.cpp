#include "network/peer_transport.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "network/connection.h"

namespace distributed_kv::network {
namespace {

constexpr int kMaximumEvents = 64;
constexpr std::size_t kMaximumPendingFramesPerPeer = 256;

bool makeAddress(const std::string& host, std::uint16_t port,
                 sockaddr_in& address) {
  address = sockaddr_in{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  return ::inet_pton(AF_INET, host.c_str(), &address.sin_addr) == 1;
}

}  // namespace

struct PeerTransport::PeerConnection {
  PeerConnection(int fd, bool is_connecting,
                 std::optional<raft::NodeId> identity)
      : connection(fd),
        connecting(is_connecting),
        peer_id(identity) {}

  Connection connection;
  bool connecting{false};
  std::optional<raft::NodeId> peer_id;
};

PeerTransport::PeerTransport(
    PeerTransportConfig config, common::BoundedQueue<RaftMessage>& inbound,
    common::BoundedQueue<PeerSend>& outbound)
    : config_(std::move(config)),
      inbound_(inbound),
      outbound_(outbound) {
  for (const PeerEndpoint& peer : config_.peers) {
    endpoints_.emplace(peer.node_id, peer);
  }
}

PeerTransport::~PeerTransport() { close(); }

bool PeerTransport::start(std::string& error) {
  if (config_.node_id == 0 || config_.bind_port == 0 ||
      config_.poll_timeout_ms <= 0 ||
      endpoints_.size() != config_.peers.size()) {
    error = "invalid PeerTransport configuration";
    return false;
  }
  for (const auto& item : endpoints_) {
    if (item.first == 0 || item.first == config_.node_id ||
        item.second.port == 0) {
      error = "invalid peer endpoint";
      return false;
    }
  }
  epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) {
    error = std::string("epoll_create1 failed: ") + std::strerror(errno);
    return false;
  }
  return openListener(error);
}

bool PeerTransport::openListener(std::string& error) {
  sockaddr_in address {};
  if (!makeAddress(config_.bind_host, config_.bind_port, address)) {
    error = "invalid peer bind address";
    return false;
  }
  listen_fd_ =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) {
    error = std::string("peer socket failed: ") + std::strerror(errno);
    return false;
  }
  int reuse = 1;
  if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                   sizeof(reuse)) != 0 ||
      ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0 ||
      ::listen(listen_fd_, SOMAXCONN) != 0) {
    error = std::string("peer listener setup failed: ") +
            std::strerror(errno);
    return false;
  }
  epoll_event event {};
  event.events = EPOLLIN;
  event.data.fd = listen_fd_;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &event) != 0) {
    error = std::string("failed to register peer listener: ") +
            std::strerror(errno);
    return false;
  }
  error.clear();
  return true;
}

bool PeerTransport::run(const std::atomic<bool>& stop, std::string& error) {
  if (listen_fd_ < 0 || epoll_fd_ < 0) {
    error = "PeerTransport is not started";
    return false;
  }
  std::array<epoll_event, kMaximumEvents> events{};
  while (!stop.load()) {
    drainOutbound();
    connectMissingPeers();
    const int count =
        ::epoll_wait(epoll_fd_, events.data(), kMaximumEvents,
                     config_.poll_timeout_ms);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      error = std::string("peer epoll_wait failed: ") +
              std::strerror(errno);
      return false;
    }
    for (int index = 0; index < count; ++index) {
      if (events[static_cast<std::size_t>(index)].data.fd == listen_fd_) {
        acceptConnections();
      } else {
        handleEvent(events[static_cast<std::size_t>(index)].data.fd,
                    events[static_cast<std::size_t>(index)].events);
      }
    }
  }
  error.clear();
  return true;
}

void PeerTransport::drainOutbound() {
  PeerSend send;
  while (outbound_.popFor(send, std::chrono::milliseconds(0))) {
    const auto endpoint = endpoints_.find(send.destination);
    if (endpoint == endpoints_.end()) {
      continue;
    }
    std::vector<std::uint8_t> frame;
    std::string error;
    if (!RaftProtocol::encode(config_.node_id, send.payload, frame, error)) {
      continue;
    }
    const auto connection = outbound_connections_.find(send.destination);
    if (connection == outbound_connections_.end()) {
      auto& pending = pending_frames_[send.destination];
      if (pending.size() < kMaximumPendingFramesPerPeer) {
        pending.push_back(std::move(frame));
      }
      continue;
    }
    const auto peer = connections_.find(connection->second);
    if (peer == connections_.end() ||
        !peer->second->connection.queueOutput(frame, error)) {
      closeConnection(connection->second);
      continue;
    }
    updateInterest(*peer->second);
  }
}

void PeerTransport::connectMissingPeers() {
  for (const auto& item : endpoints_) {
    if (outbound_connections_.count(item.first) != 0) {
      continue;
    }
    sockaddr_in address {};
    if (!makeAddress(item.second.host, item.second.port, address)) {
      continue;
    }
    const int fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      continue;
    }
    const int result =
        ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    if (result != 0 && errno != EINPROGRESS) {
      static_cast<void>(::close(fd));
      continue;
    }
    auto connection = std::make_unique<PeerConnection>(
        fd, result != 0, std::optional<raft::NodeId>(item.first));
    std::string error;
    auto pending = pending_frames_.find(item.first);
    if (pending != pending_frames_.end()) {
      for (const auto& frame : pending->second) {
        if (!connection->connection.queueOutput(frame, error)) {
          break;
        }
      }
      pending_frames_.erase(pending);
    }
    epoll_event event {};
    event.events = EPOLLIN | EPOLLRDHUP | EPOLLOUT;
    event.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
      continue;
    }
    outbound_connections_[item.first] = fd;
    connections_[fd] = std::move(connection);
  }
}

void PeerTransport::acceptConnections() {
  while (true) {
    const int fd = ::accept4(listen_fd_, nullptr, nullptr,
                             SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      return;
    }
    epoll_event event {};
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
      static_cast<void>(::close(fd));
      continue;
    }
    connections_[fd] =
        std::make_unique<PeerConnection>(fd, false, std::nullopt);
  }
}

void PeerTransport::handleEvent(int fd, std::uint32_t events) {
  auto found = connections_.find(fd);
  if (found == connections_.end()) {
    return;
  }
  PeerConnection& peer = *found->second;
  if ((events & static_cast<std::uint32_t>(
                    EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U) {
    closeConnection(fd);
    return;
  }
  if (peer.connecting &&
      (events & static_cast<std::uint32_t>(EPOLLOUT)) != 0U) {
    int socket_error = 0;
    socklen_t length = sizeof(socket_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0 ||
        socket_error != 0) {
      closeConnection(fd);
      return;
    }
    peer.connecting = false;
  }
  std::string error;
  if ((events & static_cast<std::uint32_t>(EPOLLIN)) != 0U) {
    const ConnectionResult result = peer.connection.readAvailable(error);
    if (result != ConnectionResult::kReady) {
      closeConnection(fd);
      return;
    }
    processInput(peer);
    if (connections_.count(fd) == 0) {
      return;
    }
  }
  if ((events & static_cast<std::uint32_t>(EPOLLOUT)) != 0U &&
      !peer.connecting) {
    const ConnectionResult result = peer.connection.flushOutput(error);
    if (result != ConnectionResult::kReady) {
      closeConnection(fd);
      return;
    }
  }
  updateInterest(peer);
}

void PeerTransport::processInput(PeerConnection& peer) {
  while (true) {
    RaftMessage message;
    std::string error;
    const RaftDecodeStatus status = RaftProtocol::tryDecode(
        peer.connection.inputBuffer(), message, error);
    if (status == RaftDecodeStatus::kNeedMoreData) {
      return;
    }
    if (status == RaftDecodeStatus::kError ||
        endpoints_.count(message.source) == 0 ||
        (peer.peer_id.has_value() && *peer.peer_id != message.source) ||
        !inbound_.tryPush(std::move(message))) {
      closeConnection(peer.connection.fd());
      return;
    }
    peer.peer_id = message.source;
  }
}

void PeerTransport::updateInterest(PeerConnection& peer) {
  epoll_event event {};
  event.events = EPOLLIN | EPOLLRDHUP;
  if (peer.connecting || peer.connection.wantsWrite()) {
    event.events |= EPOLLOUT;
  }
  event.data.fd = peer.connection.fd();
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, peer.connection.fd(), &event) !=
      0) {
    closeConnection(peer.connection.fd());
  }
}

void PeerTransport::closeConnection(int fd) {
  auto found = connections_.find(fd);
  if (found == connections_.end()) {
    return;
  }
  for (auto iterator = outbound_connections_.begin();
       iterator != outbound_connections_.end();) {
    if (iterator->second == fd) {
      iterator = outbound_connections_.erase(iterator);
    } else {
      ++iterator;
    }
  }
  static_cast<void>(::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
  connections_.erase(found);
}

void PeerTransport::close() noexcept {
  connections_.clear();
  outbound_connections_.clear();
  if (listen_fd_ >= 0) {
    static_cast<void>(::close(listen_fd_));
    listen_fd_ = -1;
  }
  if (epoll_fd_ >= 0) {
    static_cast<void>(::close(epoll_fd_));
    epoll_fd_ = -1;
  }
}

}  // namespace distributed_kv::network
