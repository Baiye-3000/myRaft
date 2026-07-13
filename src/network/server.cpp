#include "network/server.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <utility>

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "storage/kv_store.h"
#include "storage/wal.h"

namespace distributed_kv::network {
namespace {

constexpr int kMaximumEvents = 64;

// Input: owned descriptor by reference. Output: closed descriptor reset to -1.
// Thread safety: caller must exclusively own the descriptor.
void closeDescriptor(int& descriptor) noexcept {
  if (descriptor >= 0) {
    ::close(descriptor);
    descriptor = -1;
  }
}

// Input: epoll fd, target fd, event mask and error output. Output: registration
// success. Thread safety: caller serializes epoll_ctl operations.
bool addEpollDescriptor(int epoll_fd, int descriptor, std::uint32_t events,
                        std::string& error) {
  epoll_event event{};
  event.events = events;
  event.data.fd = descriptor;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, descriptor, &event) != 0) {
    error = std::string("epoll_ctl add failed: ") + std::strerror(errno);
    return false;
  }
  return true;
}

}  // namespace

Server::Server(ServerConfig config, storage::KVStore& store,
               storage::WAL* wal)
    : config_(std::move(config)),
      store_(&store),
      wal_(wal),
      request_handler_() {}

Server::Server(ServerConfig config, RequestHandler request_handler)
    : config_(std::move(config)),
      store_(nullptr),
      wal_(nullptr),
      request_handler_(std::move(request_handler)) {}

Server::Server(ServerConfig config,
               common::BoundedQueue<ClientRequestEvent>& requests,
               common::BoundedQueue<ClientResponseEvent>& responses)
    : config_(std::move(config)),
      store_(nullptr),
      wal_(nullptr),
      request_handler_(),
      request_queue_(&requests),
      response_queue_(&responses) {}

Server::~Server() {
  stop();
  closeResources();
}

bool Server::start(std::string& error) {
  if (listen_fd_ >= 0) {
    error = "server is already started";
    return false;
  }
  if (config_.listen_backlog <= 0 || config_.maximum_connections == 0) {
    error = "server limits must be positive";
    return false;
  }

  int listener =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listener < 0) {
    error = std::string("socket failed: ") + std::strerror(errno);
    return false;
  }

  const int reuse_address = 1;
  if (::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                   sizeof(reuse_address)) != 0) {
    error = std::string("setsockopt failed: ") + std::strerror(errno);
    closeDescriptor(listener);
    return false;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(config_.port);
  if (::inet_pton(AF_INET, config_.bind_address.c_str(),
                  &address.sin_addr) != 1) {
    error = "bind address must be a valid IPv4 address";
    closeDescriptor(listener);
    return false;
  }
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0 ||
      ::listen(listener, config_.listen_backlog) != 0) {
    error = std::string("bind/listen failed: ") + std::strerror(errno);
    closeDescriptor(listener);
    return false;
  }

  socklen_t address_length = sizeof(address);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address),
                    &address_length) != 0) {
    error = std::string("getsockname failed: ") + std::strerror(errno);
    closeDescriptor(listener);
    return false;
  }

  int epoll_descriptor = ::epoll_create1(EPOLL_CLOEXEC);
  int wake_descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (epoll_descriptor < 0 || wake_descriptor < 0 ||
      !addEpollDescriptor(epoll_descriptor, listener, EPOLLIN, error) ||
      !addEpollDescriptor(epoll_descriptor, wake_descriptor, EPOLLIN, error)) {
    if (error.empty()) {
      error = std::string("epoll/eventfd creation failed: ") +
              std::strerror(errno);
    }
    closeDescriptor(wake_descriptor);
    closeDescriptor(epoll_descriptor);
    closeDescriptor(listener);
    return false;
  }

  listen_fd_ = listener;
  epoll_fd_ = epoll_descriptor;
  wake_fd_ = wake_descriptor;
  bound_port_ = ntohs(address.sin_port);
  stop_requested_.store(false, std::memory_order_release);
  error.clear();
  return true;
}

bool Server::run(std::string& error) {
  if (epoll_fd_ < 0) {
    error = "server must be started before run";
    return false;
  }
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    error = "server event loop is already running";
    return false;
  }

  std::array<epoll_event, kMaximumEvents> events{};
  while (!stop_requested_.load(std::memory_order_acquire)) {
    drainResponses();
    const int ready =
        ::epoll_wait(epoll_fd_, events.data(), kMaximumEvents, -1);
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      error = std::string("epoll_wait failed: ") + std::strerror(errno);
      running_.store(false, std::memory_order_release);
      return false;
    }

    for (int index = 0; index < ready; ++index) {
      const int descriptor = events[static_cast<std::size_t>(index)].data.fd;
      if (descriptor == wake_fd_) {
        std::uint64_t wake_count = 0;
        while (::read(wake_fd_, &wake_count, sizeof(wake_count)) < 0 &&
               errno == EINTR) {
        }
        continue;
      }
      if (descriptor == listen_fd_) {
        if (!acceptConnections(error)) {
          running_.store(false, std::memory_order_release);
          return false;
        }
        continue;
      }
      if (!handleConnection(
              descriptor,
              events[static_cast<std::size_t>(index)].events)) {
        closeConnection(descriptor);
      }
    }
  }

  running_.store(false, std::memory_order_release);
  error.clear();
  return true;
}

void Server::stop() noexcept {
  stop_requested_.store(true, std::memory_order_release);
  notifyResponses();
}

void Server::notifyResponses() noexcept {
  if (wake_fd_ < 0) {
    return;
  }
  const std::uint64_t wake_count = 1;
  ssize_t result = 0;
  do {
    result = ::write(wake_fd_, &wake_count, sizeof(wake_count));
  } while (result < 0 && errno == EINTR);
}

std::uint16_t Server::boundPort() const noexcept { return bound_port_; }

bool Server::acceptConnections(std::string& error) {
  while (true) {
    const int client_fd =
        ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (client_fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        error.clear();
        return true;
      }
      error = std::string("accept4 failed: ") + std::strerror(errno);
      return false;
    }

    if (connections_.size() >= config_.maximum_connections) {
      ::close(client_fd);
      continue;
    }
    if (!addEpollDescriptor(epoll_fd_, client_fd, EPOLLIN | EPOLLRDHUP,
                            error)) {
      ::close(client_fd);
      return false;
    }
    connections_.emplace(client_fd, std::make_unique<Connection>(client_fd));
    const std::uint64_t connection_id = next_connection_id_++;
    connection_ids_[client_fd] = connection_id;
    connection_fds_[connection_id] = client_fd;
  }
}

bool Server::handleConnection(int client_fd, std::uint32_t events) {
  const auto found = connections_.find(client_fd);
  if (found == connections_.end()) {
    return false;
  }
  Connection& connection = *found->second;
  std::string error;

  if ((events & EPOLLIN) != 0U) {
    if (connection.readAvailable(error) != ConnectionResult::kReady ||
        !processInput(client_fd, connection)) {
      return false;
    }
  }
  if (connection.wantsWrite() &&
      connection.flushOutput(error) != ConnectionResult::kReady) {
    return false;
  }
  if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U) {
    return false;
  }

  epoll_event event{};
  event.events = EPOLLIN | EPOLLRDHUP;
  if (connection.wantsWrite()) {
    event.events |= EPOLLOUT;
  }
  event.data.fd = client_fd;
  return ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &event) == 0;
}

bool Server::processInput(int client_fd, Connection& connection) {
  while (true) {
    Frame frame;
    std::string error;
    const DecodeStatus status =
        Protocol::tryDecode(connection.inputBuffer(), frame, error);
    if (status == DecodeStatus::kNeedMoreData) {
      return true;
    }
    if (status == DecodeStatus::kError) {
      return false;
    }

    Request request;
    if (!Protocol::decodeRequest(frame, request, error)) {
      return false;
    }
    if (request_queue_ != nullptr) {
      const auto identity = connection_ids_.find(client_fd);
      if (identity == connection_ids_.end()) {
        return false;
      }
      if (request_queue_->tryPush(
              ClientRequestEvent{identity->second, std::move(request)})) {
        continue;
      }
      std::vector<std::uint8_t> busy;
      if (!Protocol::encodeResponse(
              Response{frame.request_id, StatusCode::kServerBusy,
                       "SERVER_BUSY"},
              busy, error) ||
          !connection.queueOutput(busy, error)) {
        return false;
      }
      continue;
    }
    std::vector<std::uint8_t> encoded;
    if (!Protocol::encodeResponse(dispatch(request), encoded, error) ||
        !connection.queueOutput(encoded, error)) {
      return false;
    }
  }
}

void Server::drainResponses() {
  if (response_queue_ == nullptr) {
    return;
  }
  ClientResponseEvent event;
  while (response_queue_->popFor(event, std::chrono::milliseconds(0))) {
    const auto descriptor = connection_fds_.find(event.connection_id);
    if (descriptor == connection_fds_.end()) {
      continue;
    }
    const auto connection = connections_.find(descriptor->second);
    if (connection == connections_.end()) {
      continue;
    }
    std::vector<std::uint8_t> encoded;
    std::string error;
    if (!Protocol::encodeResponse(event.response, encoded, error) ||
        !connection->second->queueOutput(encoded, error)) {
      closeConnection(descriptor->second);
      continue;
    }
    epoll_event interest {};
    interest.events = EPOLLIN | EPOLLRDHUP | EPOLLOUT;
    interest.data.fd = descriptor->second;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, descriptor->second,
                    &interest) != 0) {
      closeConnection(descriptor->second);
    }
  }
}

Response Server::dispatch(const Request& request) const {
  try {
    if (request_handler_) {
      return request_handler_(request);
    }
    if (store_ == nullptr) {
      return Response{request.request_id, StatusCode::kInternalError,
                      "request handler is unavailable"};
    }
    if (request.type == MessageType::kSetRequest) {
      if (wal_ != nullptr) {
        std::string persistence_error;
        if (!wal_->appendSet(request.key, request.value,
                             persistence_error)) {
          return Response{request.request_id, StatusCode::kInternalError,
                          "persistence failed"};
        }
      }
      const bool inserted = store_->put(request.key, request.value);
      static_cast<void>(inserted);
      return Response{request.request_id, StatusCode::kOk, "OK"};
    }
    if (request.type == MessageType::kDeleteRequest) {
      if (wal_ != nullptr) {
        std::string persistence_error;
        if (!wal_->appendRemove(request.key, persistence_error)) {
          return Response{request.request_id, StatusCode::kInternalError,
                          "persistence failed"};
        }
      }
      const bool removed = store_->remove(request.key);
      return Response{request.request_id,
                      removed ? StatusCode::kOk : StatusCode::kNotFound,
                      removed ? "OK" : "NOT_FOUND"};
    }
    if (request.type == MessageType::kGetRequest) {
      const auto value = store_->get(request.key);
      if (!value.has_value()) {
        return Response{request.request_id, StatusCode::kNotFound,
                        "NOT_FOUND"};
      }
      return Response{request.request_id, StatusCode::kOk, *value};
    }
    return Response{request.request_id, StatusCode::kInvalidRequest,
                    "unsupported request"};
  } catch (const std::exception&) {
    return Response{request.request_id, StatusCode::kInternalError,
                    "storage operation failed"};
  } catch (...) {
    return Response{request.request_id, StatusCode::kInternalError,
                    "unknown storage failure"};
  }
}

void Server::closeConnection(int client_fd) noexcept {
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
  const auto identity = connection_ids_.find(client_fd);
  if (identity != connection_ids_.end()) {
    connection_fds_.erase(identity->second);
    connection_ids_.erase(identity);
  }
  connections_.erase(client_fd);
}

void Server::closeResources() noexcept {
  connections_.clear();
  connection_ids_.clear();
  connection_fds_.clear();
  closeDescriptor(wake_fd_);
  closeDescriptor(epoll_fd_);
  closeDescriptor(listen_fd_);
  bound_port_ = 0;
}

}  // namespace distributed_kv::network
