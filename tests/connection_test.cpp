#include "network/connection.h"

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace distributed_kv::network {
namespace {

class ConnectionTest : public ::testing::Test {
 protected:
  // Creates a non-blocking local socket pair for deterministic I/O tests.
  void SetUp() override {
    ASSERT_EQ(
        ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                     sockets_.data()),
        0);
  }

  // Closes descriptors not transferred to a Connection instance.
  void TearDown() override {
    for (int& socket_fd : sockets_) {
      if (socket_fd >= 0) {
        ::close(socket_fd);
        socket_fd = -1;
      }
    }
  }

  // Transfers ownership of the first socket to a Connection.
  std::unique_ptr<Connection> makeConnection() {
    auto connection = std::make_unique<Connection>(sockets_[0]);
    sockets_[0] = -1;
    return connection;
  }

  std::array<int, 2> sockets_{{-1, -1}};
};

// Verifies non-blocking reads drain currently available bytes.
TEST_F(ConnectionTest, ReadsAvailableData) {
  auto connection = makeConnection();
  const std::array<std::uint8_t, 4> sent{{1, 2, 3, 4}};
  ASSERT_EQ(::send(sockets_[1], sent.data(), sent.size(), MSG_NOSIGNAL),
            static_cast<ssize_t>(sent.size()));

  std::string error;
  EXPECT_EQ(connection->readAvailable(error), ConnectionResult::kReady);
  EXPECT_EQ(connection->inputBuffer(),
            std::vector<std::uint8_t>(sent.begin(), sent.end()));
}

// Verifies queued bytes are written exactly once and fully drained.
TEST_F(ConnectionTest, FlushesQueuedOutput) {
  auto connection = makeConnection();
  const std::vector<std::uint8_t> expected{9, 8, 7, 6};
  std::string error;
  ASSERT_TRUE(connection->queueOutput(expected, error)) << error;

  EXPECT_TRUE(connection->wantsWrite());
  EXPECT_EQ(connection->flushOutput(error), ConnectionResult::kReady);
  EXPECT_FALSE(connection->wantsWrite());

  std::array<std::uint8_t, 4> received{};
  ASSERT_EQ(::recv(sockets_[1], received.data(), received.size(), 0),
            static_cast<ssize_t>(received.size()));
  EXPECT_EQ(std::vector<std::uint8_t>(received.begin(), received.end()),
            expected);
}

// Verifies orderly peer shutdown is distinguished from an I/O failure.
TEST_F(ConnectionTest, ReportsPeerClosure) {
  auto connection = makeConnection();
  ASSERT_EQ(::close(sockets_[1]), 0);
  sockets_[1] = -1;

  std::string error;
  EXPECT_EQ(connection->readAvailable(error),
            ConnectionResult::kPeerClosed);
}

// Verifies output backpressure rejects unbounded queued data.
TEST_F(ConnectionTest, EnforcesOutputBufferLimit) {
  auto connection = makeConnection();
  const std::vector<std::uint8_t> oversized(2U * 1024U * 1024U + 1U, 0);
  std::string error;

  EXPECT_FALSE(connection->queueOutput(oversized, error));
  EXPECT_EQ(error, "connection output buffer limit exceeded");
}

}  // namespace
}  // namespace distributed_kv::network
