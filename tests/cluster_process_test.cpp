#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
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
    std::unordered_set<std::uint16_t> allocated_ports;
    const auto uniquePort = [&allocated_ports] {
      std::uint16_t port = 0;
      do {
        port = allocatePort();
      } while (port != 0 && !allocated_ports.insert(port).second);
      return port;
    };
    for (std::uint64_t id = 1; id <= 4; ++id) {
      ProcessMember member{id, uniquePort(), uniquePort(),
                           root_ + "/node" + std::to_string(id)};
      ASSERT_NE(member.client_port, 0);
      ASSERT_NE(member.peer_port, 0);
      ASSERT_EQ(::mkdir(member.directory.c_str(), 0750), 0);
      members_.push_back(std::move(member));
    }
  }

  void TearDown() override {
    stopAll();
    for (std::uint64_t id = 1; id <= 4; ++id) {
      for (const std::string& stage :
           {"learner-caught-up", "joint-proposed", "joint-committed"}) {
        static_cast<void>(::unlink(
            (root_ + "/node" + std::to_string(id) + "-" + stage +
             ".reached")
                .c_str()));
        static_cast<void>(::unlink(
            (root_ + "/node" + std::to_string(id) + "-" + stage +
             ".continue")
                .c_str()));
      }
    }
    for (const ProcessMember& source : members_) {
      for (const ProcessMember& destination : members_) {
        if (source.id == destination.id) continue;
        for (const std::string& action : {"drop", "delay", "duplicate"}) {
          const std::string path =
              peerFaultPath(source.id, destination.id, action);
          static_cast<void>(::unlink(path.c_str()));
          static_cast<void>(::unlink((path + ".reached").c_str()));
        }
      }
    }
    for (const ProcessMember& member : members_) {
      static_cast<void>(
          ::unlink((member.directory + "/raft.wal").c_str()));
      static_cast<void>(
          ::unlink((member.directory + "/state.snapshot").c_str()));
      static_cast<void>(
          ::unlink((member.directory + "/state.snapshot.tmp").c_str()));
      static_cast<void>(
          ::unlink((member.directory + "/state.snapshot.recv.tmp").c_str()));
      static_cast<void>(::rmdir(member.directory.c_str()));
    }
    static_cast<void>(::rmdir(root_.c_str()));
  }

  std::string memberSpec(const ProcessMember& member) const {
    return std::to_string(member.id) + ",127.0.0.1," +
           std::to_string(member.client_port) + ",127.0.0.1," +
           std::to_string(member.peer_port);
  }

  void startNode(std::size_t index, bool learner = false) {
    const std::string id = std::to_string(members_[index].id);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      if (!membership_pause_stage_.empty()) {
        static_cast<void>(::setenv("DKV_TEST_MEMBERSHIP_PAUSE_STAGE",
                                   membership_pause_stage_.c_str(), 1));
        static_cast<void>(::setenv("DKV_TEST_MEMBERSHIP_PAUSE_DIRECTORY",
                                   root_.c_str(), 1));
      }
      if (!peer_fault_directory_.empty()) {
        static_cast<void>(::setenv("DKV_TEST_PEER_FAULT_DIRECTORY",
                                   peer_fault_directory_.c_str(), 1));
      }
      std::vector<std::string> arguments{
          DKV_NODE_PATH, id, members_[index].directory};
      if (learner) arguments.push_back("--learner");
      const std::size_t member_count = learner ? members_.size() : 3U;
      for (std::size_t member = 0; member < member_count; ++member) {
        arguments.push_back(memberSpec(members_[member]));
      }
      std::vector<char*> raw;
      raw.reserve(arguments.size() + 1U);
      for (std::string& argument : arguments) raw.push_back(argument.data());
      raw.push_back(nullptr);
      ::execv(DKV_NODE_PATH, raw.data());
      _exit(127);
    }
    if (children_.size() < members_.size()) {
      children_.resize(members_.size(), -1);
    }
    children_[index] = child;
  }

  void startAll() {
    for (std::size_t index = 0; index < 3U; ++index) {
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

  void killNode(std::size_t index) {
    if (index >= children_.size() || children_[index] <= 0) return;
    static_cast<void>(::kill(children_[index], SIGKILL));
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

  network::TcpClient directClient(std::size_t index,
                                  std::size_t maximum_attempts = 1) const {
    network::ClientConfig config;
    config.host = "127.0.0.1";
    config.port = members_[index].client_port;
    config.timeout = std::chrono::milliseconds(10000);
    config.maximum_attempts = maximum_attempts;
    return network::TcpClient(std::move(config));
  }

  bool waitForStage(std::size_t index, const std::string& stage) const {
    const std::string marker =
        root_ + "/node" + std::to_string(members_[index].id) + "-" +
        stage + ".reached";
    for (std::size_t attempt = 0; attempt < 500U; ++attempt) {
      if (::access(marker.c_str(), F_OK) == 0) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }

  void resumeStage(std::size_t index, const std::string& stage) const {
    const std::string marker =
        root_ + "/node" + std::to_string(members_[index].id) + "-" +
        stage + ".continue";
    const int descriptor =
        ::open(marker.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    ASSERT_GE(descriptor, 0);
    ASSERT_EQ(::close(descriptor), 0);
  }

  std::string peerFaultPath(std::uint64_t source, std::uint64_t destination,
                            const std::string& action) const {
    return root_ + "/peer-" + std::to_string(source) + "-to-" +
           std::to_string(destination) + "." + action;
  }

  void enablePeerFault(std::size_t source, std::size_t destination,
                       const std::string& action,
                       const std::string& value = {}) const {
    const std::string path = peerFaultPath(
        members_[source].id, members_[destination].id, action);
    static_cast<void>(::unlink((path + ".reached").c_str()));
    const int descriptor =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    ASSERT_GE(descriptor, 0);
    if (!value.empty()) {
      ASSERT_EQ(::write(descriptor, value.data(), value.size()),
                static_cast<ssize_t>(value.size()));
    }
    ASSERT_EQ(::close(descriptor), 0);
  }

  void disablePeerFault(std::size_t source, std::size_t destination,
                        const std::string& action) const {
    ASSERT_EQ(::unlink(peerFaultPath(members_[source].id,
                                    members_[destination].id,
                                    action).c_str()), 0);
  }

  bool waitForPeerFault(std::size_t source, std::size_t destination,
                        const std::string& action) const {
    const std::string reached =
        peerFaultPath(members_[source].id, members_[destination].id,
                      action) + ".reached";
    for (std::size_t attempt = 0; attempt < 500U; ++attempt) {
      if (::access(reached.c_str(), F_OK) == 0) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  }

  bool waitForMemberCount(network::TcpClient& admin,
                          std::size_t expected) const {
    for (std::size_t attempt = 0; attempt < 200U; ++attempt) {
      network::Response response;
      std::string error;
      if (admin.listMembers(response, error) &&
          response.members.size() == expected) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
  }

  network::MemberEndpoint adminMember(std::size_t index) const {
    return network::MemberEndpoint{
        members_[index].id, "127.0.0.1", members_[index].client_port,
        "127.0.0.1", members_[index].peer_port};
  }

  void addFourthNode(network::TcpClient& admin) {
    startNode(3, true);
    network::Response response;
    std::string error;
    ASSERT_TRUE(admin.addNode(adminMember(3), response, error)) << error;
    ASSERT_EQ(response.status, network::StatusCode::kOk) << response.payload;
    ASSERT_EQ(response.members.size(), 4U);
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
  std::string membership_pause_stage_;
  std::string peer_fault_directory_;
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

TEST_F(ClusterProcessTest, OneWayLeaderPartitionElectsSafeReplacement) {
  peer_fault_directory_ = root_;
  startAll();
  auto cluster = client();
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("one-way-before", "committed", response, error))
      << error;
  const std::size_t old_leader = discoverLeader();
  ASSERT_LT(old_leader, 3U);
  std::vector<std::size_t> followers;
  for (std::size_t index = 0; index < 3U; ++index) {
    if (index == old_leader) continue;
    followers.push_back(index);
    enablePeerFault(old_leader, index, "drop");
  }
  for (const std::size_t follower : followers) {
    ASSERT_TRUE(waitForPeerFault(old_leader, follower, "drop"));
  }

  ASSERT_TRUE(cluster.set("one-way-after", "new-leader", response, error))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk) << response.payload;
  for (const std::size_t follower : followers) {
    disablePeerFault(old_leader, follower, "drop");
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  ASSERT_TRUE(cluster.get("one-way-before", response, error)) << error;
  EXPECT_EQ(response.payload, "committed");
  ASSERT_TRUE(cluster.get("one-way-after", response, error)) << error;
  EXPECT_EQ(response.payload, "new-leader");
}

TEST_F(ClusterProcessTest, DelayedAndDuplicateFramesConverge) {
  peer_fault_directory_ = root_;
  startAll();
  auto cluster = client();
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, 3U);
  std::vector<std::size_t> followers;
  for (std::size_t index = 0; index < 3U; ++index) {
    if (index != leader) followers.push_back(index);
  }
  enablePeerFault(leader, followers[0], "duplicate");
  enablePeerFault(leader, followers[1], "delay", "700");

  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("fault-sequence", "first", response, error))
      << error;
  ASSERT_TRUE(waitForPeerFault(leader, followers[0], "duplicate"));
  ASSERT_TRUE(waitForPeerFault(leader, followers[1], "delay"));
  disablePeerFault(leader, followers[0], "duplicate");
  disablePeerFault(leader, followers[1], "delay");
  ASSERT_TRUE(cluster.set("fault-sequence", "second", response, error))
      << error;
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  ASSERT_TRUE(cluster.get("fault-sequence", response, error)) << error;
  EXPECT_EQ(response.payload, "second");
}

// Verifies a real fourth process catches up as a Learner before joining.
TEST_F(ClusterProcessTest, ExpandsOnlineAndServesThroughNewMember) {
  startAll();
  auto cluster = client();
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("before-expand", "durable", response, error))
      << error;
  addFourthNode(cluster);

  network::ClientConfig direct_config;
  direct_config.host = "127.0.0.1";
  direct_config.port = members_[3].client_port;
  direct_config.timeout = std::chrono::milliseconds(6000);
  direct_config.maximum_attempts = 10;
  network::TcpClient through_new_member(std::move(direct_config));
  ASSERT_TRUE(through_new_member.get("before-expand", response, error))
      << error;
  EXPECT_EQ(response.payload, "durable");
  ASSERT_TRUE(through_new_member.set("after-expand", "four", response, error))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
}

// Verifies a four-voter configuration elects a replacement after Leader loss.
TEST_F(ClusterProcessTest, ExpandedClusterSurvivesLeaderFailure) {
  startAll();
  auto cluster = client();
  addFourthNode(cluster);
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, members_.size());
  stopNode(leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("post-expand-failover", "ok", response, error))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
}

// Verifies the removed process is no longer needed by commit or read quorums.
TEST_F(ClusterProcessTest, RemovedNodeCanBeIsolated) {
  startAll();
  auto cluster = client();
  addFourthNode(cluster);
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.removeNode(members_[3].id, response, error)) << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk) << response.payload;
  ASSERT_EQ(response.members.size(), 3U);
  stopNode(3);

  ASSERT_TRUE(cluster.set("after-remove", "available", response, error))
      << error;
  ASSERT_TRUE(cluster.get("after-remove", response, error)) << error;
  EXPECT_EQ(response.payload, "available");
}

// Verifies the committed four-member configuration and KV state survive a
// complete process shutdown, including voter promotion on the former Learner.
TEST_F(ClusterProcessTest, ExpandedConfigurationSurvivesFullRestart) {
  startAll();
  auto cluster = client();
  startNode(3, true);
  constexpr std::uint64_t kOperationId = 92001;
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.addNode(adminMember(3), response, error, kOperationId))
      << error;
  ASSERT_EQ(response.status, network::StatusCode::kOk) << response.payload;
  ASSERT_TRUE(cluster.set("expanded-restart", "preserved", response, error))
      << error;
  stopAll();
  for (std::size_t index = 0; index < members_.size(); ++index) {
    startNode(index, index == 3U);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1400));

  ASSERT_TRUE(cluster.listMembers(response, error)) << error;
  EXPECT_EQ(response.members.size(), 4U);
  ASSERT_TRUE(cluster.addNode(adminMember(3), response, error, kOperationId))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
  EXPECT_EQ(response.payload, "OK_REPLAYED");
  ASSERT_TRUE(cluster.get("expanded-restart", response, error)) << error;
  EXPECT_EQ(response.payload, "preserved");
}

// Crashes the real Leader after learner catch-up but before the joint entry is
// proposed. The old three-voter configuration must remain writable.
TEST_F(ClusterProcessTest, LeaderCrashBeforeJointProposalLeavesOldConfig) {
  membership_pause_stage_ = "learner-caught-up";
  startAll();
  startNode(3, true);
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, 3U);
  auto direct = directClient(leader);
  network::Response admin_response;
  std::string admin_error;
  bool admin_completed = false;
  std::thread request([&] {
    admin_completed =
        direct.addNode(adminMember(3), admin_response, admin_error);
  });
  const bool reached = waitForStage(leader, "learner-caught-up");
  killNode(leader);
  request.join();
  ASSERT_TRUE(reached);
  EXPECT_FALSE(admin_completed);

  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  auto cluster = client();
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("pre-joint-crash", "available", response, error))
      << error;
  EXPECT_TRUE(waitForMemberCount(cluster, 3U));
  startNode(leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  auto restarted = directClient(leader, 20);
  EXPECT_TRUE(waitForMemberCount(restarted, 3U));
}

// Crashes the real Leader after the joint entry commits but before C_new is
// committed. A replacement Leader must finish the transition to four voters.
TEST_F(ClusterProcessTest, LeaderCrashAfterJointCommitCompletesChange) {
  membership_pause_stage_ = "joint-committed";
  startAll();
  startNode(3, true);
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, 3U);
  auto direct = directClient(leader);
  network::Response admin_response;
  std::string admin_error;
  bool admin_completed = false;
  constexpr std::uint64_t kOperationId = 92002;
  std::thread request([&] {
    admin_completed =
        direct.addNode(adminMember(3), admin_response, admin_error,
                       kOperationId);
  });
  const bool reached = waitForStage(leader, "joint-committed");
  killNode(leader);
  request.join();
  ASSERT_TRUE(reached);
  EXPECT_FALSE(admin_completed);

  for (std::size_t index = 0; index < members_.size(); ++index) {
    if (index != leader) resumeStage(index, "joint-committed");
  }
  auto cluster = client();
  network::Response retry_response;
  std::string retry_error;
  ASSERT_TRUE(cluster.addNode(adminMember(3), retry_response, retry_error,
                              kOperationId)) << retry_error;
  EXPECT_EQ(retry_response.status, network::StatusCode::kOk)
      << retry_response.payload;
  const bool converged = waitForMemberCount(cluster, 4U);
  if (!converged) {
    for (std::size_t index = 0; index < members_.size(); ++index) {
      if (index < children_.size() && children_[index] <= 0) continue;
      auto inspect = directClient(index);
      network::Response inspection;
      std::string inspection_error;
      const bool received = inspect.listMembers(inspection, inspection_error);
      ADD_FAILURE() << "node " << members_[index].id
                    << " list received=" << received
                    << " members=" << inspection.members.size()
                    << " error=" << inspection_error;
    }
  }
  ASSERT_TRUE(converged);
  network::Response response;
  std::string error;
  ASSERT_TRUE(cluster.set("joint-crash", "recovered", response, error))
      << error;
  EXPECT_EQ(response.status, network::StatusCode::kOk);
  startNode(leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  auto restarted = directClient(leader, 20);
  EXPECT_TRUE(waitForMemberCount(restarted, 4U));
}

// Keeps only an old-configuration majority after the joint entry is proposed.
// Without a new-configuration majority the membership request must not finish.
TEST_F(ClusterProcessTest, OldMajorityAloneCannotCommitJointChange) {
  membership_pause_stage_ = "joint-proposed";
  startAll();
  startNode(3, true);
  const std::size_t leader = discoverLeader();
  ASSERT_LT(leader, 3U);
  auto direct = directClient(leader);
  network::Response admin_response;
  std::string admin_error;
  std::atomic<bool> admin_finished{false};
  std::thread request([&] {
    static_cast<void>(
        direct.addNode(adminMember(3), admin_response, admin_error));
    admin_finished.store(true);
  });
  const bool reached = waitForStage(leader, "joint-proposed");
  if (!reached) {
    killNode(leader);
    request.join();
  }
  ASSERT_TRUE(reached);

  const std::size_t isolated_old = (leader + 1U) % 3U;
  stopNode(isolated_old);
  stopNode(3);
  resumeStage(leader, "joint-proposed");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  EXPECT_FALSE(admin_finished.load());

  killNode(leader);
  request.join();
}

}  // namespace
}  // namespace distributed_kv::server
