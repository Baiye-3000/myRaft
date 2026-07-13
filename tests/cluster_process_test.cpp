#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "network/tcp_client.h"

#ifndef DKV_NODE_PATH
#error "DKV_NODE_PATH must name the dkv_node executable"
#endif

namespace distributed_kv::server {
namespace {

struct ProcessMember {
  std::uint64_t id{0};
  std::uint16_t client_port{0};
  std::uint16_t peer_port{0};
  std::string directory;
};

std::uint16_t allocatePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
      0) {
    static_cast<void>(::close(fd));
    return 0;
  }
  socklen_t length = sizeof(address);
  static_cast<void>(
      ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length));
  static_cast<void>(::close(fd));
  return ntohs(address.sin_port);
}

class ClusterProcessTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-process-XXXXXX";
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* root = ::mkdtemp(directory_template.data());
    ASSERT_NE(root, nullptr);
    root_ = root;
    for (std::uint64_t id = 1; id <= 3; ++id) {
      ProcessMember member{id, allocatePort(), allocatePort(),
                           root_ + "/node" + std::to_string(id)};
      ASSERT_NE(member.client_port, 0);
      ASSERT_NE(member.peer_port, 0);
      ASSERT_EQ(::mkdir(member.directory.c_str(), 0750), 0);
      members_.push_back(std::move(member));
    }
  }

  void TearDown() override {
    stopAll();
    for (const ProcessMember& member : members_) {
      static_cast<void>(
          ::unlink((member.directory + "/raft.wal").c_str()));
      static_cast<void>(::rmdir(member.directory.c_str()));
    }
    static_cast<void>(::rmdir(root_.c_str()));
  }

  std::string memberSpec(const ProcessMember& member) const {
    return std::to_string(member.id) + ",127.0.0.1," +
           std::to_string(member.client_port) + ",127.0.0.1," +
           std::to_string(member.peer_port);
  }

  void startNode(std::size_t index) {
    std::array<std::string, 3> specifications{
        memberSpec(members_[0]), memberSpec(members_[1]),
        memberSpec(members_[2])};
    const std::string id = std::to_string(members_[index].id);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      ::execl(DKV_NODE_PATH, DKV_NODE_PATH, id.c_str(),
              members_[index].directory.c_str(),
              specifications[0].c_str(), specifications[1].c_str(),
              specifications[2].c_str(), static_cast<char*>(nullptr));
      _exit(127);
    }
    if (children_.size() < members_.size()) {
      children_.resize(members_.size(), -1);
    }
    children_[index] = child;
  }

  void startAll() {
    for (std::size_t index = 0; index < members_.size(); ++index) {
      startNode(index);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  }

  void stopNode(std::size_t index) {
    if (index >= children_.size() || children_[index] <= 0) {
      return;
    }
    static_cast<void>(::kill(children_[index], SIGTERM));
    static_cast<void>(::waitpid(children_[index], nullptr, 0));
    children_[index] = -1;
  }

  void stopAll() {
    for (std::size_t index = 0; index < children_.size(); ++index) {
      stopNode(index);
    }
  }

  network::TcpClient client() const {
    network::ClientConfig config;
    config.timeout = std::chrono::milliseconds(6000);
    config.maximum_attempts = 200;
    for (const ProcessMember& member : members_) {
      config.endpoints.push_back(
          network::ClientEndpoint{"127.0.0.1", member.client_port});
    }
    return network::TcpClient(std::move(config));
  }

  std::size_t discoverLeader() const {
    for (std::size_t index = 0; index < members_.size(); ++index) {
      if (index < children_.size() && children_[index] <= 0) {
        continue;
      }
      network::ClientConfig config;
      config.host = "127.0.0.1";
      config.port = members_[index].client_port;
      config.timeout = std::chrono::milliseconds(1000);
      config.maximum_attempts = 1;
      network::TcpClient direct(std::move(config));
      network::Response response;
      std::string error;
      static_cast<void>(direct.get("leader-probe", response, error));
      if (response.status == network::StatusCode::kOk ||
          response.status == network::StatusCode::kNotFound) {
        return index;
      }
    }
    return members_.size();
  }

  std::string root_;
  std::vector<ProcessMember> members_;
  std::vector<pid_t> children_;
};

// Exercises process failure, re-election, and all-node durable recovery.
TEST_F(ClusterProcessTest, ReelectsAndRecoversCommittedData) {
  startAll();
  auto cluster = client();
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("durable", "before", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, members_.size());
  const std::size_t follower = (leader + 1U) % members_.size();
  network::ClientConfig redirect_config;
  redirect_config.host = "127.0.0.1";
  redirect_config.port = members_[follower].client_port;
  redirect_config.timeout = std::chrono::milliseconds(3000);
  redirect_config.maximum_attempts = 3;
  network::TcpClient redirected(std::move(redirect_config));
  ASSERT_TRUE(redirected.get("durable", response, error)) << error;
  EXPECT_EQ(response.payload, "before");

  stopNode(leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  ASSERT_TRUE(cluster.set("after", "reelection", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  ASSERT_TRUE(cluster.get("durable", response, error)) << error;
  EXPECT_EQ(response.payload, "before");

  stopAll();
  startAll();
  ASSERT_TRUE(cluster.get("after", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  EXPECT_EQ(response.payload, "reelection");
}

// Verifies an isolated old Leader cannot satisfy a read from local state.
TEST_F(ClusterProcessTest, IsolatedLeaderReadTimesOut) {
  startAll();
  auto cluster = client();
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("guarded", "value", response, error)) << error;
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, members_.size());
  for (std::size_t index = 0; index < members_.size(); ++index) {
    if (index != leader) {
      stopNode(index);
    }
  }

  network::ClientConfig direct_config;
  direct_config.host = "127.0.0.1";
  direct_config.port = members_[leader].client_port;
  direct_config.timeout = std::chrono::milliseconds(2500);
  direct_config.maximum_attempts = 1;
  network::TcpClient direct(std::move(direct_config));
  ASSERT_TRUE(direct.get("guarded", response, error)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kUnavailable);
  EXPECT_EQ(response.payload, "READ_QUORUM_TIMEOUT");
}

}  // namespace
}  // namespace distributed_kv::server
