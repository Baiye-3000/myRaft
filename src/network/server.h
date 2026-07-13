#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/bounded_queue.h"
#include "network/connection.h"
#include "network/protocol.h"

namespace distributed_kv::storage {
class KVStore;
class WAL;
}

namespace distributed_kv::network {

struct ServerConfig {
  std::string bind_address{"127.0.0.1"};
  std::uint16_t port{7000};
  int listen_backlog{128};
  std::size_t maximum_connections{1024};
};

using RequestHandler = std::function<Response(const Request&)>;

struct ClientRequestEvent {
  std::uint64_t connection_id{0};
  Request request;
};

struct ClientResponseEvent {
  std::uint64_t connection_id{0};
  Response response;
};

/**
 * Single-event-loop, level-triggered epoll TCP server for client KV requests.
 *
 * start() acquires resources, run() owns all Connection operations, and stop()
 * wakes run() through eventfd. KVStore provides its own data synchronization.
 */
class Server final {
 public:
  /**
   * Creates an unstarted server using non-owning storage dependencies.
   *
   * Input: immutable configuration copy, store that outlives the server, and
   * optional already-open/recovered WAL. A null WAL selects transient mode.
   * Output: an unstarted server.
   * Thread safety: construction requires exclusive access.
   */
  Server(ServerConfig config, storage::KVStore& store,
         storage::WAL* wal = nullptr);

  /**
   * Creates an unstarted server with an application request handler.
   *
   * Input: immutable configuration and callable that outlives active requests.
   * Output: unstarted server dispatching every decoded request to the callable.
   * Thread safety: handler executes only on the Server event-loop thread.
   */
  Server(ServerConfig config, RequestHandler request_handler);

  /**
   * Creates an asynchronous server joined to bounded Raft-thread queues.
   *
   * Input: config plus queues that outlive the server.
   * Output: requests retain connection identity until a response is posted.
   * Thread safety: queues provide cross-thread synchronization.
   */
  Server(ServerConfig config,
         common::BoundedQueue<ClientRequestEvent>& requests,
         common::BoundedQueue<ClientResponseEvent>& responses);

  /**
   * Requests shutdown and releases all owned descriptors.
   *
   * Input/output: none.
   * Thread safety: run() must have returned before destruction begins.
   */
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  Server(Server&&) = delete;
  Server& operator=(Server&&) = delete;

  /**
   * Binds the listener and creates epoll/eventfd resources.
   *
   * Input: writable error text.
   * Output: true on success; false without leaking partially-created fds.
   * Thread safety: call once before run(), with no concurrent operations.
   */
  [[nodiscard]] bool start(std::string& error);

  /**
   * Runs the blocking epoll event loop until stop() is requested.
   *
   * Input: writable fatal-error text.
   * Output: true for orderly shutdown, false for a fatal event-loop error.
   * Thread safety: at most one run() call; stop() may run concurrently.
   */
  [[nodiscard]] bool run(std::string& error);

  /**
   * Requests an orderly event-loop shutdown.
   *
   * Input: none.
   * Output: none; repeated calls are safe.
   * Thread safety: safe to call concurrently with run().
   */
  void stop() noexcept;

  /**
   * Wakes the event loop after an asynchronous response is queued.
   *
   * Input/output: none; a saturated eventfd is already considered signaled.
   * Thread safety: safe from the Raft completion thread.
   */
  void notifyResponses() noexcept;

  /**
   * Returns the bound TCP port, including an OS-assigned ephemeral port.
   *
   * Input: none.
   * Output: bound port, or zero before successful start().
   * Thread safety: call after start() and before destruction.
   */
  [[nodiscard]] std::uint16_t boundPort() const noexcept;

 private:
  /**
   * Accepts all pending sockets and registers them with epoll.
   *
   * Input: writable fatal-error text.
   * Output: false only when accept/epoll fails unexpectedly.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] bool acceptConnections(std::string& error);

  /**
   * Processes readiness for one client and refreshes its epoll interest.
   *
   * Input: client fd and epoll event mask.
   * Output: true to retain the connection, false to close it.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] bool handleConnection(int client_fd, std::uint32_t events);

  /**
   * Decodes and dispatches all complete requests currently buffered.
   *
   * Input: one event-loop-owned connection.
   * Output: true when input is valid and responses were queued.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] bool processInput(int client_fd, Connection& connection);

  void drainResponses();

  /**
   * Executes one validated request against KVStore.
   *
   * Input: decoded request.
   * Output: response carrying the same request id.
   * Thread safety: event-loop thread; KVStore synchronizes data access.
   */
  [[nodiscard]] Response dispatch(const Request& request) const;

  /**
   * Removes a client from epoll and releases its fd.
   *
   * Input: registered client fd.
   * Output: none.
   * Thread safety: event-loop thread only.
   */
  void closeConnection(int client_fd) noexcept;

  /**
   * Closes all server descriptors and client connections.
   *
   * Input/output: none beyond owned resources.
   * Thread safety: no concurrent run() operation.
   */
  void closeResources() noexcept;

  ServerConfig config_;
  storage::KVStore* store_;
  storage::WAL* wal_;
  RequestHandler request_handler_;
  common::BoundedQueue<ClientRequestEvent>* request_queue_{nullptr};
  common::BoundedQueue<ClientResponseEvent>* response_queue_{nullptr};
  int listen_fd_{-1};
  int epoll_fd_{-1};
  int wake_fd_{-1};
  std::uint16_t bound_port_{0};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
  std::unordered_map<int, std::uint64_t> connection_ids_;
  std::unordered_map<std::uint64_t, int> connection_fds_;
  std::uint64_t next_connection_id_{1};
};

}  // namespace distributed_kv::network
