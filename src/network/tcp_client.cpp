#include "network/tcp_client.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <random>
#include <utility>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace distributed_kv::network {
namespace {

class ScopedDescriptor final {
 public:
  // Input: owned descriptor. Output: RAII descriptor wrapper.
  // Thread safety: construction requires exclusive ownership.
  explicit ScopedDescriptor(int descriptor) noexcept
      : descriptor_(descriptor) {}

  // Input: none. Output: closes the owned descriptor.
  // Thread safety: no operation may race with destruction.
  ~ScopedDescriptor() {
    if (descriptor_ >= 0) {
      ::close(descriptor_);
    }
  }

  ScopedDescriptor(const ScopedDescriptor&) = delete;
  ScopedDescriptor& operator=(const ScopedDescriptor&) = delete;

  // Input: none. Output: borrowed descriptor.
  // Thread safety: caller must synchronize descriptor use.
  [[nodiscard]] int get() const noexcept { return descriptor_; }

 private:
  int descriptor_;
};

// Input: process entropy, monotonic time and process id. Output: nonzero client
// identity suitable for request deduplication. Thread safety: local state only.
std::uint64_t generateClientId() {
  std::random_device entropy;
  const std::uint64_t random_bits =
      (static_cast<std::uint64_t>(entropy()) << 32U) ^
      static_cast<std::uint64_t>(entropy());
  const auto ticks = std::chrono::steady_clock::now()
                         .time_since_epoch()
                         .count();
  std::uint64_t client_id =
      random_bits ^ static_cast<std::uint64_t>(ticks) ^
      static_cast<std::uint64_t>(::getpid());
  if (client_id == 0) {
    client_id = 1;
  }
  return client_id;
}

std::uint64_t generateOperationId() { return generateClientId(); }

}  // namespace

TcpClient::TcpClient(ClientConfig config) : config_(std::move(config)) {
  if (config_.client_id == 0) {
    config_.client_id = generateClientId();
  }
}

bool TcpClient::set(const std::string& key, const std::string& value,
                    Response& response, std::string& error) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  return execute(Request{MessageType::kSetRequest, request_id, key, value,
                         config_.client_id},
                 response, error);
}

bool TcpClient::get(const std::string& key, Response& response,
                    std::string& error) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  return execute(Request{MessageType::kGetRequest, request_id, key, "",
                         config_.client_id},
                 response, error);
}

bool TcpClient::remove(const std::string& key, Response& response,
                       std::string& error) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  return execute(Request{MessageType::kDeleteRequest, request_id, key, "",
                         config_.client_id},
                 response, error);
}

bool TcpClient::addNode(const MemberEndpoint& member, Response& response,
                        std::string& error, std::uint64_t operation_id) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  Request request;
  request.type = MessageType::kAddNodeRequest;
  request.request_id = request_id;
  request.client_id = config_.client_id;
  request.operation_id =
      operation_id == 0 ? generateOperationId() : operation_id;
  request.node_id = member.node_id;
  request.client_host = member.client_host;
  request.client_port = member.client_port;
  request.peer_host = member.peer_host;
  request.peer_port = member.peer_port;
  return execute(request, response, error);
}

bool TcpClient::removeNode(std::uint64_t node_id, Response& response,
                           std::string& error, std::uint64_t operation_id) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  Request request;
  request.type = MessageType::kRemoveNodeRequest;
  request.request_id = request_id;
  request.client_id = config_.client_id;
  request.operation_id =
      operation_id == 0 ? generateOperationId() : operation_id;
  request.node_id = node_id;
  return execute(request, response, error);
}

bool TcpClient::listMembers(Response& response, std::string& error) {
  const std::uint64_t request_id =
      next_request_id_.fetch_add(1, std::memory_order_relaxed);
  Request request;
  request.type = MessageType::kListMembersRequest;
  request.request_id = request_id;
  request.client_id = config_.client_id;
  return execute(request, response, error);
}

bool TcpClient::execute(const Request& request, Response& response,
                        std::string& error) const {
  std::vector<std::uint8_t> encoded;
  if (!Protocol::encodeRequest(request, encoded, error)) {
    return false;
  }

  if (config_.timeout.count() <= 0 || config_.maximum_attempts == 0) {
    error = "client timeout and attempt limit must be positive";
    return false;
  }
  std::vector<ClientEndpoint> endpoints = config_.endpoints;
  if (endpoints.empty()) {
    endpoints.push_back(ClientEndpoint{config_.host, config_.port});
  }
  const Deadline deadline = std::chrono::steady_clock::now() + config_.timeout;
  ClientEndpoint endpoint = endpoints.front();
  std::size_t fallback_index = 0;
  for (std::size_t attempt = 0; attempt < config_.maximum_attempts;
       ++attempt) {
    const int socket_fd = connectSocket(endpoint, deadline, error);
    if (socket_fd >= 0) {
      const ScopedDescriptor socket(socket_fd);
      if (sendAll(socket.get(), encoded, deadline, error) &&
          receiveResponse(socket.get(), deadline, response, error)) {
        if (response.request_id != request.request_id) {
          error = "response request id does not match request";
          return false;
        }
        if (response.status != StatusCode::kNotLeader) {
          error.clear();
          return true;
        }
        if (!response.leader_host.empty()) {
          endpoint =
              ClientEndpoint{response.leader_host, response.leader_port};
          continue;
        }
      }
    }
    fallback_index = (fallback_index + 1U) % endpoints.size();
    endpoint = endpoints[fallback_index];
    if (std::chrono::steady_clock::now() >= deadline) {
      break;
    }
  }
  if (error.empty()) {
    error = "cluster request exhausted retry limit";
  }
  return false;
}

int TcpClient::connectSocket(const ClientEndpoint& endpoint,
                             Deadline deadline, std::string& error) {
  if (endpoint.port == 0) {
    error = "client endpoint port must be positive";
    return -1;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(endpoint.port);
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &address.sin_addr) != 1) {
    error = "server host must be a valid IPv4 address";
    return -1;
  }

  const int socket_fd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) {
    error = std::string("socket failed: ") + std::strerror(errno);
    return -1;
  }

  const int result =
      ::connect(socket_fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address));
  if (result == 0) {
    error.clear();
    return socket_fd;
  }
  if (errno != EINPROGRESS ||
      !waitFor(socket_fd, POLLOUT, deadline, error)) {
    if (errno != EINPROGRESS && error.empty()) {
      error = std::string("connect failed: ") + std::strerror(errno);
    }
    ::close(socket_fd);
    return -1;
  }

  int socket_error = 0;
  socklen_t error_length = sizeof(socket_error);
  if (::getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error,
                   &error_length) != 0 ||
      socket_error != 0) {
    const int failure = socket_error != 0 ? socket_error : errno;
    error = std::string("connect failed: ") + std::strerror(failure);
    ::close(socket_fd);
    return -1;
  }

  error.clear();
  return socket_fd;
}

bool TcpClient::sendAll(int socket_fd,
                        const std::vector<std::uint8_t>& bytes,
                        Deadline deadline, std::string& error) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t sent =
        ::send(socket_fd, bytes.data() + offset, bytes.size() - offset,
               MSG_NOSIGNAL);
    if (sent > 0) {
      offset += static_cast<std::size_t>(sent);
      continue;
    }
    if (sent < 0 && errno == EINTR) {
      continue;
    }
    if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (!waitFor(socket_fd, POLLOUT, deadline, error)) {
        return false;
      }
      continue;
    }
    error = sent == 0 ? "connection closed while sending"
                      : std::string("send failed: ") + std::strerror(errno);
    return false;
  }
  error.clear();
  return true;
}

bool TcpClient::receiveResponse(int socket_fd, Deadline deadline,
                                Response& response, std::string& error) {
  std::vector<std::uint8_t> buffer;
  std::array<std::uint8_t, 16U * 1024U> chunk{};
  while (true) {
    Frame frame;
    const DecodeStatus decode_status =
        Protocol::tryDecode(buffer, frame, error);
    if (decode_status == DecodeStatus::kComplete) {
      return Protocol::decodeResponse(frame, response, error);
    }
    if (decode_status == DecodeStatus::kError) {
      return false;
    }

    const ssize_t received =
        ::recv(socket_fd, chunk.data(), chunk.size(), 0);
    if (received > 0) {
      const auto count = static_cast<std::size_t>(received);
      if (count > 2U * 1024U * 1024U - buffer.size()) {
        error = "response buffer limit exceeded";
        return false;
      }
      buffer.insert(buffer.end(), chunk.begin(),
                    chunk.begin() +
                        static_cast<std::array<std::uint8_t,
                                               16U * 1024U>::difference_type>(
                            count));
      continue;
    }
    if (received == 0) {
      error = "server closed before sending a complete response";
      return false;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      if (!waitFor(socket_fd, POLLIN, deadline, error)) {
        return false;
      }
      continue;
    }
    error = std::string("recv failed: ") + std::strerror(errno);
    return false;
  }
}

bool TcpClient::waitFor(int socket_fd, short requested_events,
                        Deadline deadline, std::string& error) {
  while (true) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      error = "network operation timed out";
      return false;
    }
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    const auto bounded =
        std::min<std::int64_t>(remaining.count(),
                               std::numeric_limits<int>::max());
    const int timeout_ms = static_cast<int>(bounded);

    pollfd descriptor{socket_fd, requested_events, 0};
    const int result = ::poll(&descriptor, 1, timeout_ms);
    if (result > 0 && (descriptor.revents & requested_events) != 0) {
      error.clear();
      return true;
    }
    if (result > 0) {
      error = "socket closed or failed while waiting for readiness";
      return false;
    }
    if (result == 0) {
      error = "network operation timed out";
      return false;
    }
    if (errno != EINTR) {
      error = std::string("poll failed: ") + std::strerror(errno);
      return false;
    }
  }
}

}  // namespace distributed_kv::network
