#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace distributed_kv::network {

enum class ConnectionResult {
  kReady,
  kPeerClosed,
  kError,
};

/**
 * Owns one non-blocking TCP socket and its incremental I/O buffers.
 *
 * Connection is intentionally not internally synchronized. A Server assigns a
 * connection to one epoll event-loop thread for its entire lifetime.
 */
class Connection final {
 public:
  /**
   * Takes ownership of an already-created non-blocking socket descriptor.
   *
   * Input: valid socket descriptor.
   * Output: a connection owning the descriptor.
   * Thread safety: construction requires exclusive access.
   */
  explicit Connection(int socket_fd) noexcept;

  /**
   * Closes the owned socket descriptor.
   *
   * Input/output: none beyond releasing the fd.
   * Thread safety: no operation may race with destruction.
   */
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  Connection(Connection&&) = delete;
  Connection& operator=(Connection&&) = delete;

  /**
   * Returns the socket descriptor without transferring ownership.
   *
   * Input: none.
   * Output: owned fd.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] int fd() const noexcept;

  /**
   * Reads from the socket until EAGAIN, EOF, or an error occurs.
   *
   * Input: writable error text.
   * Output: result and bytes appended to inputBuffer().
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] ConnectionResult readAvailable(std::string& error);

  /**
   * Returns the mutable incremental receive buffer for protocol decoding.
   *
   * Input: none.
   * Output: reference valid for this connection's lifetime.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] std::vector<std::uint8_t>& inputBuffer() noexcept;

  /**
   * Appends one complete encoded frame to the pending output.
   *
   * Input: bytes to copy and writable error text.
   * Output: true when queued, false when the connection backpressure limit
   * would be exceeded.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] bool queueOutput(const std::vector<std::uint8_t>& bytes,
                                 std::string& error);

  /**
   * Writes queued bytes until drained or the socket returns EAGAIN.
   *
   * Input: writable error text.
   * Output: ready, peer-closed, or error result.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] ConnectionResult flushOutput(std::string& error);

  /**
   * Reports whether EPOLLOUT interest is currently required.
   *
   * Input: none.
   * Output: true while unsent bytes remain.
   * Thread safety: event-loop thread only.
   */
  [[nodiscard]] bool wantsWrite() const noexcept;

 private:
  static constexpr std::size_t kMaximumBufferedBytes = 2U * 1024U * 1024U;

  int socket_fd_;
  std::vector<std::uint8_t> input_;
  std::vector<std::uint8_t> output_;
  std::size_t output_offset_{0};
};

}  // namespace distributed_kv::network
