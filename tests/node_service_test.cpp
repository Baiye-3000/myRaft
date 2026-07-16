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
#include <unordered_set>
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
    std::unordered_set<std::uint16_t> allocated_ports;
    const auto uniquePort = [&allocated_ports] {
      std::uint16_t port = 0;
      do {
        port = reservePort();
      } while (port != 0 && !allocated_ports.insert(port).second);
      return port;
    };
    for (raft::NodeId id = 1; id <= 4; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      ASSERT_EQ(::mkdir(directory.c_str(), 0750), 0);
      ClusterNodeEndpoint endpoint{
          id, "127.0.0.1", uniquePort(), "127.0.0.1", uniquePort()};
      ASSERT_NE(endpoint.client_port, 0);
      ASSERT_NE(endpoint.peer_port, 0);
      if (id <= 3) {
        members_.push_back(endpoint);
      } else {
        learner_endpoint_ = endpoint;
      }
    }
  }

  void TearDown() override {
    for (auto& node : nodes_) {
      if (node != nullptr) {
        node->stop();
      }
    }
    nodes_.clear();
    for (raft::NodeId id = 1; id <= 4; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      static_cast<void>(::unlink((directory + "/raft.wal").c_str()));
      static_cast<void>(::unlink((directory + "/state.snapshot").c_str()));
      static_cast<void>(::unlink((directory + "/state.snapshot.tmp").c_str()));
      static_cast<void>(::unlink((directory + "/state.snapshot.recv.tmp").c_str()));
      static_cast<void>(::rmdir(directory.c_str()));
    }
    static_cast<void>(::rmdir(root_.c_str()));
  }

  std::unique_ptr<NodeService> makeNode(raft::NodeId id,
                                        std::size_t snapshot_threshold = 64,
                                        bool late_joiner = false) {
    NodeServiceConfig config;
    config.node_id = id;
    config.data_directory =
        root_ + "/node" + std::to_string(id);
    config.members = members_;
    if (late_joiner) {
      config.election_timeout_min_ms = 3000;
      config.election_timeout_max_ms = 5000;
    } else {
      config.election_timeout_min_ms = 150 + id * 40;
      config.election_timeout_max_ms = 250 + id * 40;
    }
    config.heartbeat_interval_ms = 50;
    config.read_timeout_ms = 1000;
    config.snapshot_entry_threshold = snapshot_threshold;
    return std::make_unique<NodeService>(std::move(config));
  }

  void startNodes(const std::vector<raft::NodeId>& ids,
                  std::size_t snapshot_threshold = 64) {
    for (const raft::NodeId id : ids) {
      auto node = makeNode(id, snapshot_threshold);
      std::string error;
      ASSERT_TRUE(node->start(error)) << error;
      nodes_.push_back(std::move(node));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(900));
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

  network::TcpClient makeClientForNode(raft::NodeId id) const {
    const auto endpoint =
        std::find_if(members_.begin(), members_.end(),
                     [id](const ClusterNodeEndpoint& member) {
                       return member.node_id == id;
                     });
    EXPECT_NE(endpoint, members_.end());
    network::ClientConfig config;
    config.timeout = std::chrono::milliseconds(5000);
    config.maximum_attempts = 20;
    config.endpoints.push_back(
        network::ClientEndpoint{endpoint->client_host, endpoint->client_port});
    return network::TcpClient(std::move(config));
  }

  void startAll() { startNodes({1, 2, 3}); }

  bool pathExists(const std::string& path) const {
    return ::access(path.c_str(), F_OK) == 0;
  }

  std::string root_;
  std::vector<ClusterNodeEndpoint> members_;
  ClusterNodeEndpoint learner_endpoint_;
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

// Verifies Admin RPC drives learner catch-up and joint consensus in both
// directions while the cluster remains available.
TEST_F(NodeServiceTest, AddsAndRemovesOnlineMember) {
  startAll();
  NodeServiceConfig learner_config;
  learner_config.node_id = learner_endpoint_.node_id;
  learner_config.data_directory = root_ + "/node4";
  learner_config.members = members_;
  learner_config.members.push_back(learner_endpoint_);
  learner_config.learner = true;
  learner_config.election_timeout_min_ms = 3000;
  learner_config.election_timeout_max_ms = 5000;
  learner_config.heartbeat_interval_ms = 50;
  auto learner = std::make_unique<NodeService>(std::move(learner_config));
  std::string error;
  ASSERT_TRUE(learner->start(error)) << error;
  nodes_.push_back(std::move(learner));

  auto admin = makeClient();
  network::Response response;
  const network::MemberEndpoint member{
      learner_endpoint_.node_id, learner_endpoint_.client_host,
      learner_endpoint_.client_port, learner_endpoint_.peer_host,
      learner_endpoint_.peer_port};
  constexpr std::uint64_t kAddOperation = 91001;
  ASSERT_TRUE(admin.addNode(member, response, error, kAddOperation)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk) << response.payload;
  ASSERT_EQ(response.members.size(), 4U);
  ASSERT_TRUE(admin.addNode(member, response, error, kAddOperation)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
  EXPECT_EQ(response.payload, "OK_REPLAYED");
  network::MemberEndpoint conflicting = member;
  ++conflicting.client_port;
  ASSERT_TRUE(admin.addNode(conflicting, response, error, kAddOperation))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kInvalidRequest);
  EXPECT_EQ(response.payload, "OPERATION_ID_REUSED");

  network::ClientConfig direct_config;
  direct_config.timeout = std::chrono::milliseconds(5000);
  direct_config.maximum_attempts = 20;
  direct_config.endpoints.push_back(network::ClientEndpoint{
      learner_endpoint_.client_host, learner_endpoint_.client_port});
  network::TcpClient learner_client(std::move(direct_config));
  ASSERT_TRUE(learner_client.set("expanded", "yes", response, error)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);

  constexpr std::uint64_t kRemoveOperation = 91002;
  ASSERT_TRUE(admin.removeNode(learner_endpoint_.node_id, response, error,
                               kRemoveOperation))
      << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk) << response.payload;
  ASSERT_EQ(response.members.size(), 3U);
  ASSERT_TRUE(admin.removeNode(learner_endpoint_.node_id, response, error,
                               kRemoveOperation)) << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
  EXPECT_EQ(response.payload, "OK_REPLAYED");
  for (std::size_t attempt = 0; attempt < 50U; ++attempt) {
    ASSERT_TRUE(admin.listMembers(response, error)) << error;
    if (response.members.size() == 3U) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(response.members.size(), 3U);
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

// Verifies a late-starting node receives InstallSnapshot after journal compaction.
TEST_F(NodeServiceTest, LaggingNodeInstallsSnapshotAfterJournalCompaction) {
  constexpr std::size_t kSnapshotThreshold = 2;

  {
    auto paused = makeNode(3, kSnapshotThreshold, true);
    std::string error;
    ASSERT_TRUE(paused->start(error)) << error;
    paused->stop();
  }
  static_cast<void>(::unlink((root_ + "/node3/raft.wal").c_str()));
  static_cast<void>(::unlink((root_ + "/node3/state.snapshot").c_str()));

  startNodes({1, 2}, kSnapshotThreshold);

  auto client = makeClient();
  network::Response response;
  std::string error;
  ASSERT_TRUE(client.set("alpha", "one", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);
  ASSERT_TRUE(client.set("beta", "two", response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk);

  const std::string leader_snapshot =
      root_ + "/node1/state.snapshot";
  const std::string alternate_snapshot =
      root_ + "/node2/state.snapshot";
  for (int attempt = 0; attempt < 40; ++attempt) {
    if (pathExists(leader_snapshot) || pathExists(alternate_snapshot)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(pathExists(leader_snapshot) ||
              pathExists(alternate_snapshot));

  auto late_node = makeNode(3, kSnapshotThreshold, true);
  ASSERT_TRUE(late_node->start(error)) << error;
  nodes_.push_back(std::move(late_node));

  const std::string node3_snapshot = root_ + "/node3/state.snapshot";
  for (int attempt = 0; attempt < 80; ++attempt) {
    if (pathExists(node3_snapshot)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  ASSERT_TRUE(pathExists(node3_snapshot));

  auto node3_client = makeClientForNode(3);
  ASSERT_TRUE(node3_client.get("alpha", response, error)) << error;
  EXPECT_EQ(response.payload, "one");

  nodes_[2]->stop();
  nodes_[2].reset();
  auto recovered = makeNode(3, kSnapshotThreshold, true);
  ASSERT_TRUE(recovered->start(error)) << error;
  nodes_[2] = std::move(recovered);
  std::this_thread::sleep_for(std::chrono::milliseconds(900));

  auto restarted_client = makeClientForNode(3);
  ASSERT_TRUE(restarted_client.get("alpha", response, error)) << error;
  EXPECT_EQ(response.payload, "one");
  ASSERT_TRUE(restarted_client.get("beta", response, error)) << error;
  EXPECT_EQ(response.payload, "two");
}

}  // namespace
}  // namespace distributed_kv::server
