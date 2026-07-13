#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "network/protocol.h"

namespace distributed_kv::network {

struct ClientEndpoint {
  std::string host;
  std::uint16_t port{0};
};

struct ClientConfig {
  std::string host{"127.0.0.1"};
  std::uint16_t port{7000};
  std::chrono::milliseconds timeout{2000};
  std::uint64_t client_id{0};
  std::vector<ClientEndpoint> endpoints;
  std::size_t maximum_attempts{5};
};

/**
 * Synchronous request client using non-blocking sockets with bounded waits.
 *
 * Each operation uses a fresh TCP connection in Phase 2. This keeps retry and
 * response-correlation semantics explicit; connection reuse is a later
 * optimization.
 */
class TcpClient final {
 public:
  /**
   * Creates a client from a copied endpoint and timeout configuration.
   *
   * Input: client configuration.
   * Output: initialized client with request ids starting at one.
   * Thread safety: construction requires exclusive access.
   */
  explicit TcpClient(ClientConfig config);

  /**
   * Sends a SET request and waits for its correlated response.
   *
   * Input: key, value, writable response and error.
   * Output: true when a valid response is received, including server errors.
   * Thread safety: safe for concurrent calls.
   */
  [[nodiscard]] bool set(const std::string& key, const std::string& value,
                         Response& response, std::string& error);

  /**
   * Sends a GET request and waits for its correlated response.
   *
   * Input: key, writable response and error.
   * Output: true when a valid response is received, including NOT_FOUND.
   * Thread safety: safe for concurrent calls.
   */
  [[nodiscard]] bool get(const std::string& key, Response& response,
                         std::string& error);

  [[nodiscard]] bool remove(const std::string& key, Response& response,
                            std::string& error);

  /**
   * Executes one pre-built request over a new bounded-lifetime connection.
   *
   * Input: request, writable response and error.
   * Output: true only for a valid response with a matching request id.
   * Thread safety: safe for concurrent calls.
   */
  [[nodiscard]] bool execute(const Request& request, Response& response,
                             std::string& error) const;

 private:
  using Deadline = std::chrono::steady_clock::time_point;

  /**
   * Creates and completes a non-blocking IPv4 TCP connection.
   *
   * Input: absolute deadline and writable error.
   * Output: owned fd for the caller, or -1 on error.
   * Thread safety: no shared mutable state.
   */
  [[nodiscard]] static int connectSocket(const ClientEndpoint& endpoint,
                                         Deadline deadline,
                                         std::string& error);

  /**
   * Sends an entire encoded frame before the deadline.
   *
   * Input: socket, bytes, deadline and writable error.
   * Output: true when all bytes are sent.
   * Thread safety: caller exclusively owns the socket.
   */
  [[nodiscard]] static bool sendAll(int socket_fd,
                                    const std::vector<std::uint8_t>& bytes,
                                    Deadline deadline, std::string& error);

  /**
   * Receives and decodes exactly one response before the deadline.
   *
   * Input: socket, deadline, writable response and error.
   * Output: true for a complete valid response.
   * Thread safety: caller exclusively owns the socket.
   */
  [[nodiscard]] static bool receiveResponse(int socket_fd, Deadline deadline,
                                            Response& response,
                                            std::string& error);

  /**
   * Waits for a requested poll event while preserving an absolute deadline.
   *
   * Input: socket, poll mask, deadline and writable error.
   * Output: true when requested readiness occurs.
   * Thread safety: no shared mutable state.
   */
  [[nodiscard]] static bool waitFor(int socket_fd, short requested_events,
                                    Deadline deadline, std::string& error);

  ClientConfig config_;
  std::atomic<std::uint64_t> next_request_id_{1};
};

}  // namespace distributed_kv::network
