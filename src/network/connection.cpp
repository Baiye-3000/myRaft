#include "network/connection.h"

#include <cerrno>
#include <cstring>

#include <sys/socket.h>
#include <unistd.h>

namespace distributed_kv::network {
namespace {

constexpr std::size_t kReadChunkSize = 16U * 1024U;

// Input: errno value from socket I/O. Output: whether the peer is gone.
// Thread safety: stateless.
bool isPeerCloseError(int error_number) {
  return error_number == EPIPE || error_number == ECONNRESET ||
         error_number == ENOTCONN;
}

}  // namespace

Connection::Connection(int socket_fd) noexcept : socket_fd_(socket_fd) {}

Connection::~Connection() {
  if (socket_fd_ >= 0) {
    ::close(socket_fd_);
  }
}

int Connection::fd() const noexcept { return socket_fd_; }

ConnectionResult Connection::readAvailable(std::string& error) {
  std::uint8_t chunk[kReadChunkSize];
  while (true) {
    const ssize_t bytes_read =
        ::recv(socket_fd_, chunk, sizeof(chunk), 0);
    if (bytes_read > 0) {
      const auto count = static_cast<std::size_t>(bytes_read);
      if (count > kMaximumBufferedBytes - input_.size()) {
        error = "connection input buffer limit exceeded";
        return ConnectionResult::kError;
      }
      input_.insert(input_.end(), chunk, chunk + count);
      continue;
    }
    if (bytes_read == 0) {
      return ConnectionResult::kPeerClosed;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      error.clear();
      return ConnectionResult::kReady;
    }
    if (isPeerCloseError(errno)) {
      return ConnectionResult::kPeerClosed;
    }
    error = std::string("recv failed: ") + std::strerror(errno);
    return ConnectionResult::kError;
  }
}

std::vector<std::uint8_t>& Connection::inputBuffer() noexcept {
  return input_;
}

bool Connection::queueOutput(const std::vector<std::uint8_t>& bytes,
                             std::string& error) {
  if (output_offset_ > 0) {
    output_.erase(
        output_.begin(),
        output_.begin() +
            static_cast<std::vector<std::uint8_t>::difference_type>(
                output_offset_));
    output_offset_ = 0;
  }
  if (bytes.size() > kMaximumBufferedBytes - output_.size()) {
    error = "connection output buffer limit exceeded";
    return false;
  }
  output_.insert(output_.end(), bytes.begin(), bytes.end());
  error.clear();
  return true;
}

ConnectionResult Connection::flushOutput(std::string& error) {
  while (output_offset_ < output_.size()) {
    const std::size_t remaining = output_.size() - output_offset_;
    const ssize_t bytes_written =
        ::send(socket_fd_, output_.data() + output_offset_, remaining,
               MSG_NOSIGNAL);
    if (bytes_written > 0) {
      output_offset_ += static_cast<std::size_t>(bytes_written);
      continue;
    }
    if (bytes_written == 0) {
      return ConnectionResult::kPeerClosed;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      error.clear();
      return ConnectionResult::kReady;
    }
    if (isPeerCloseError(errno)) {
      return ConnectionResult::kPeerClosed;
    }
    error = std::string("send failed: ") + std::strerror(errno);
    return ConnectionResult::kError;
  }

  output_.clear();
  output_offset_ = 0;
  error.clear();
  return ConnectionResult::kReady;
}

bool Connection::wantsWrite() const noexcept {
  return output_offset_ < output_.size();
}

}  // namespace distributed_kv::network
