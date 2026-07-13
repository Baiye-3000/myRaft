#include "server/node_service.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "network/tcp_client.h"

namespace distributed_kv::server {
namespace {

std::uint16_t reservePort() {
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
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) !=
      0) {
    static_cast<void>(::close(fd));
    return 0;
  }
  const std::uint16_t port = ntohs(address.sin_port);
  static_cast<void>(::close(fd));
  return port;
}

class NodeServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-cluster-XXXXXX";
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    root_ = created;
    for (raft::NodeId id = 1; id <= 3; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      ASSERT_EQ(::mkdir(directory.c_str(), 0750), 0);
      members_.push_back(ClusterNodeEndpoint{
          id, "127.0.0.1", reservePort(), "127.0.0.1", reservePort()});
      ASSERT_NE(members_.back().client_port, 0);
      ASSERT_NE(members_.back().peer_port, 0);
    }
  }

  void TearDown() override {
    for (auto& node : nodes_) {
      if (node != nullptr) {
        node->stop();
      }
    }
    nodes_.clear();
    for (raft::NodeId id = 1; id <= 3; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      static_cast<void>(::unlink((directory + "/raft.wal").c_str()));
      static_cast<void>(::unlink((directory + "/state.snapshot").c_str()));
      static_cast<void>(::unlink((directory + "/state.snapshot.tmp").c_str()));
      static_cast<void>(::rmdir(directory.c_str()));
    }
    static_cast<void>(::rmdir(root_.c_str()));
  }

  std::unique_ptr<NodeService> makeNode(raft::NodeId id) {
    NodeServiceConfig config;
    config.node_id = id;
    config.data_directory =
        root_ + "/node" + std::to_string(id);
    config.members = members_;
    config.election_timeout_min_ms = 150 + id * 40;
    config.election_timeout_max_ms = 250 + id * 40;
    config.heartbeat_interval_ms = 50;
    config.read_timeout_ms = 1000;
    return std::make_unique<NodeService>(std::move(config));
  }

  network::TcpClient makeClient() const {
    network::ClientConfig config;
    config.timeout = std::chrono::milliseconds(5000);
    config.maximum_attempts = 100;
    for (const auto& member : members_) {
      config.endpoints.push_back(
          network::ClientEndpoint{member.client_host, member.client_port});
    }
    return network::TcpClient(std::move(config));
  }

  void startAll() {
    for (raft::NodeId id = 1; id <= 3; ++id) {
      auto node = makeNode(id);
      std::string error;
      ASSERT_TRUE(node->start(error)) << error;
      nodes_.push_back(std::move(node));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(900));
  }

  std::string root_;
  std::vector<ClusterNodeEndpoint> members_;
  std::vector<std::unique_ptr<NodeService>> nodes_;
};

// Exercises real TCP election, pending commit, redirect, ReadIndex, and DELETE.
TEST_F(NodeServiceTest, ThreeNodesServeReplicatedOperations) {
  startAll();
  auto client = makeClient();
  network::Response response;
  std::string error;
  ASSERT_TRUE(client.set("name", "tom", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  ASSERT_TRUE(client.get("name", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  EXPECT_EQ(response.payload, "tom");
  ASSERT_TRUE(client.remove("name", response, error)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
  ASSERT_TRUE(client.get("name", response, error)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kNotFound);
}

// Verifies a quorum remains writable and a restarted node reloads its journal.
TEST_F(NodeServiceTest, SurvivesNodeStopAndPersistentRestart) {
  startAll();
  auto client = makeClient();
  network::Response response;
  std::string error;
  ASSERT_TRUE(client.set("before", "one", response, error)) << error;

  nodes_[2]->stop();
  nodes_[2].reset();
  ASSERT_TRUE(client.set("during", "two", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);

  nodes_[2] = makeNode(3);
  ASSERT_TRUE(nodes_[2]->start(error)) << error;
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  ASSERT_TRUE(client.get("during", response, error)) << error;
  EXPECT_EQ(response.payload, "two");
}

// Verifies startup fails closed instead of ignoring a corrupt snapshot.
TEST_F(NodeServiceTest, RejectsCorruptSnapshotDuringConstruction) {
  const std::string path = root_ + "/node1/state.snapshot";
  const int fd =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  const std::string corrupt = "not-a-snapshot";
  ASSERT_EQ(::write(fd, corrupt.data(), corrupt.size()),
            static_cast<ssize_t>(corrupt.size()));
  ASSERT_EQ(::close(fd), 0);

  EXPECT_THROW(static_cast<void>(makeNode(1)), std::runtime_error);
}

// Verifies concurrent GETs share fewer quorum barriers than read requests.
TEST_F(NodeServiceTest, BatchesConcurrentLinearizableReads) {
  startAll();
  auto writer = makeClient();
  network::Response response;
  std::string error;
  ASSERT_TRUE(writer.set("shared", "value", response, error)) << error;

  constexpr std::size_t kReaderCount = 16;
  std::atomic<std::size_t> ready{0};
  std::atomic<bool> begin{false};
  std::atomic<std::size_t> failures{0};
  std::vector<std::thread> readers;
  readers.reserve(kReaderCount);
  for (std::size_t index = 0; index < kReaderCount; ++index) {
    readers.emplace_back([this, &ready, &begin, &failures] {
      auto client = makeClient();
      ready.fetch_add(1);
      while (!begin.load()) {
        std::this_thread::yield();
      }
      network::Response read_response;
      std::string read_error;
      if (!client.get("shared", read_response, read_error) ||
          read_response.status != network::StatusCode::kOk ||
          read_response.payload != "value") {
        failures.fetch_add(1);
      }
    });
  }
  while (ready.load() != kReaderCount) {
    std::this_thread::yield();
  }
  begin.store(true);
  for (auto& reader : readers) {
    reader.join();
  }
  ASSERT_EQ(failures.load(), 0U);

  NodeServiceMetrics total;
  for (const auto& node : nodes_) {
    const NodeServiceMetrics metrics = node->metrics();
    total.read_requests += metrics.read_requests;
    total.read_barriers += metrics.read_barriers;
  }
  EXPECT_EQ(total.read_requests, kReaderCount);
  EXPECT_LT(total.read_barriers, total.read_requests);
}

}  // namespace
}  // namespace distributed_kv::server
