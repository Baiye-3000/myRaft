#include "raft/file_raft_persistence.h"
#include "raft/file_snapshot_store.h"
#include "raft/raft_kv_service.h"
#include "storage/kv_store.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

class FixedPersistence final : public RaftPersistence {
 public:
  explicit FixedPersistence(RaftPersistentState state)
      : state_(std::move(state)) {}

  bool load(RaftPersistentState& state, std::string& error) override {
    state = state_;
    error.clear();
    return true;
  }

  bool save(const RaftPersistentState& state,
            std::string& error) override {
    state_ = state;
    error.clear();
    return true;
  }

 private:
  RaftPersistentState state_;
};

std::string encodedSet(std::uint64_t client_id,
                       std::uint64_t request_id, std::string key,
                       std::string value) {
  std::string encoded;
  std::string error;
  EXPECT_TRUE(KVCommandCodec::encode(
      KVCommand{KVCommandType::kSet, client_id, request_id,
                std::move(key), std::move(value)},
      encoded, error))
      << error;
  return encoded;
}

// Verifies a Follower rejects writes without changing local state.
TEST(RaftKVServiceTest, FollowerRejectsSubmission) {
  storage::KVStore store;
  RaftKVService service(
      NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1}, store);
  std::string error;

  const SubmitResult result = service.submit(
      KVCommand{KVCommandType::kSet, 10, 1, "name", "tom"}, error);

  EXPECT_EQ(result.status, SubmitStatus::kNotLeader);
  EXPECT_EQ(store.get("name"), std::nullopt);
  EXPECT_EQ(service.raftNode().log().lastIndex(), 0U);
}

// Verifies a single-node Leader commits and applies before returning.
TEST(RaftKVServiceTest, SingleNodeSubmissionAppliesSynchronously) {
  storage::KVStore store;
  RaftKVService service(
      NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store);
  std::string error;
  EXPECT_TRUE(service.tick(100, error).empty()) << error;
  ASSERT_EQ(service.raftNode().role(), Role::kLeader);
  ASSERT_EQ(service.lastApplied(), 1U);

  const SubmitResult result = service.submit(
      KVCommand{KVCommandType::kSet, 10, 1, "name", "tom"}, error);

  ASSERT_EQ(result.status, SubmitStatus::kApplied) << error;
  ASSERT_TRUE(result.result.has_value());
  EXPECT_EQ(result.result->payload, "OK");
  EXPECT_EQ(store.get("name"), std::optional<std::string>("tom"));
  EXPECT_EQ(service.lastApplied(), 2U);
}

// Verifies startup restores a snapshot then replays its committed log suffix.
TEST(RaftKVServiceTest, RestoresSnapshotAndReplaysCommittedSuffix) {
  FixedPersistence persistence(RaftPersistentState{
      4,
      std::nullopt,
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 2, EntryType::kNoOp, ""},
          LogEntry{2, 3, EntryType::kCommand,
                   encodedSet(10, 1, "name", "tom")},
          LogEntry{3, 4, EntryType::kCommand,
                   encodedSet(11, 1, "city", "shanghai")},
      },
      3,
  });
  StateMachineSnapshot snapshot;
  snapshot.last_included_index = 2;
  snapshot.last_included_term = 3;
  snapshot.entries = {{"name", "tom"}};
  snapshot.sessions = {
      SnapshotSession{10, 1, ApplyStatus::kOk, "OK"}};
  storage::KVStore store;

  RaftKVService service(
      NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store,
      &persistence, &snapshot);

  EXPECT_EQ(service.lastApplied(), 3U);
  EXPECT_EQ(store.get("name"), std::optional<std::string>("tom"));
  EXPECT_EQ(store.get("city"),
            std::optional<std::string>("shanghai"));
}

// Verifies a snapshot cannot be paired with a different durable log boundary.
TEST(RaftKVServiceTest, RejectsSnapshotWithMismatchedLogBoundary) {
  FixedPersistence persistence(RaftPersistentState{
      3,
      std::nullopt,
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 3, EntryType::kNoOp, ""},
      },
      1,
  });
  StateMachineSnapshot snapshot;
  snapshot.last_included_index = 1;
  snapshot.last_included_term = 2;
  storage::KVStore store;

  EXPECT_THROW(
      RaftKVService(
          NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store,
          &persistence, &snapshot),
      std::runtime_error);
  EXPECT_EQ(store.size(), 0U);
}

struct ServiceEnvelope {
  NodeId source{0};
  OutboundRpc rpc;
};

class ServiceCluster {
 public:
  /**
   * Creates three Raft/KV services with isolated stores and staggered timers.
   *
   * Input: none.
   * Output: deterministic three-member service cluster.
   * Thread safety: test thread only.
   */
  ServiceCluster() {
    addMember(1, {2, 3}, 100);
    addMember(2, {1, 3}, 200);
    addMember(3, {1, 2}, 300);
  }

  /**
   * Returns one mutable service by id.
   *
   * Input: configured node id.
   * Output: borrowed service reference.
   * Thread safety: test thread only.
   */
  RaftKVService& service(NodeId node_id) {
    return *members_.at(node_id)->service;
  }

  /**
   * Returns one node's applied KVStore.
   *
   * Input: configured node id.
   * Output: borrowed store reference.
   * Thread safety: test thread only.
   */
  storage::KVStore& store(NodeId node_id) {
    return members_.at(node_id)->store;
  }

  /**
   * Delivers all generated RPC reactions except an optional dropped target.
   *
   * Input: source id, initial RPCs and dropped destination.
   * Output: consensus and application side effects on recipient services.
   * Thread safety: test thread only.
   */
  void deliver(NodeId source, std::vector<OutboundRpc> initial,
               NodeId dropped_destination = 0) {
    std::deque<ServiceEnvelope> queue;
    for (auto& rpc : initial) {
      queue.push_back(ServiceEnvelope{source, std::move(rpc)});
    }

    std::size_t steps = 0;
    while (!queue.empty()) {
      ASSERT_LT(steps++, 1000U);
      ServiceEnvelope envelope = std::move(queue.front());
      queue.pop_front();
      if (envelope.rpc.destination == dropped_destination) {
        continue;
      }

      RaftKVService& destination = service(envelope.rpc.destination);
      RaftKVService& origin = service(envelope.source);
      std::vector<OutboundRpc> follow_up;
      std::string error;
      if (const auto* vote =
              std::get_if<RequestVoteRequest>(&envelope.rpc.payload)) {
        const RequestVoteResponse response =
            destination.handleRequestVote(*vote);
        follow_up = origin.handleRequestVoteResponse(
            envelope.rpc.destination, response, error);
      } else if (const auto* append =
                     std::get_if<AppendEntriesRequest>(
                         &envelope.rpc.payload)) {
        const AppendEntriesResponse response =
            destination.handleAppendEntries(*append, error);
        ASSERT_TRUE(error.empty()) << error;
        follow_up = origin.handleAppendEntriesResponse(
            envelope.rpc.destination, response, error);
      } else {
        ADD_FAILURE() << "unexpected response payload in service queue";
      }
      ASSERT_TRUE(error.empty()) << error;

      for (auto& rpc : follow_up) {
        queue.push_back(
            ServiceEnvelope{envelope.source, std::move(rpc)});
      }
    }
  }

 private:
  struct Member {
    storage::KVStore store;
    std::unique_ptr<RaftKVService> service;
  };

  /**
   * Adds one member while preserving KVStore address stability.
   *
   * Input: node id, peers and fixed election timeout.
   * Output: member inserted into cluster map.
   * Thread safety: constructor thread only.
   */
  void addMember(NodeId node_id, std::vector<NodeId> peers,
                 std::uint64_t election_timeout) {
    auto member = std::make_unique<Member>();
    member->service = std::make_unique<RaftKVService>(
        NodeConfig{node_id, std::move(peers), std::nullopt, election_timeout,
                   election_timeout, 20, 64, node_id},
        member->store);
    members_.emplace(node_id, std::move(member));
  }

  std::unordered_map<NodeId, std::unique_ptr<Member>> members_;
};

// Verifies KV remains unchanged until majority commit, then converges everywhere.
TEST(RaftKVServiceTest, AppliesOnlyAfterMajorityCommit) {
  ServiceCluster cluster;
  std::string error;
  auto election = cluster.service(1).tick(100, error);
  ASSERT_TRUE(error.empty()) << error;
  cluster.deliver(1, std::move(election));
  ASSERT_EQ(cluster.service(1).raftNode().role(), Role::kLeader);

  auto heartbeat = cluster.service(1).tick(20, error);
  cluster.deliver(1, std::move(heartbeat));
  ASSERT_EQ(cluster.service(1).lastApplied(), 1U);

  SubmitResult submission = cluster.service(1).submit(
      KVCommand{KVCommandType::kSet, 50, 1, "name", "tom"}, error);
  ASSERT_EQ(submission.status, SubmitStatus::kPending) << error;
  EXPECT_EQ(cluster.store(1).get("name"), std::nullopt);
  EXPECT_EQ(cluster.store(2).get("name"), std::nullopt);

  cluster.deliver(1, std::move(submission.outbound), 3);
  const auto completed =
      cluster.service(1).takeResult(submission.log_index);
  ASSERT_TRUE(completed.has_value());
  EXPECT_EQ(completed->status, ApplyStatus::kOk);
  EXPECT_EQ(cluster.store(1).get("name"),
            std::optional<std::string>("tom"));
  EXPECT_EQ(cluster.store(2).get("name"), std::nullopt);
  EXPECT_EQ(cluster.store(3).get("name"), std::nullopt);

  heartbeat = cluster.service(1).tick(20, error);
  cluster.deliver(1, std::move(heartbeat));
  heartbeat = cluster.service(1).tick(20, error);
  cluster.deliver(1, std::move(heartbeat));

  for (NodeId node_id : {1U, 2U, 3U}) {
    EXPECT_EQ(cluster.store(node_id).get("name"),
              std::optional<std::string>("tom"));
    EXPECT_EQ(cluster.service(node_id).lastApplied(), 2U);
  }
}

class SnapshotCompactionFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-snapshot-XXXXXX";
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    directory_ = created;
    journal_path_ = directory_ + "/raft.wal";
    snapshot_path_ = directory_ + "/state.snapshot";
  }

  void TearDown() override {
    static_cast<void>(::unlink(journal_path_.c_str()));
    static_cast<void>(::unlink((journal_path_ + ".leader").c_str()));
    static_cast<void>(::unlink((journal_path_ + ".follower").c_str()));
    static_cast<void>(::unlink((journal_path_ + ".compact.tmp").c_str()));
    static_cast<void>(::unlink(snapshot_path_.c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".leader").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".leader.tmp").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".leader.recv.tmp").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".follower").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".follower.tmp").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".follower.recv.tmp").c_str()));
    static_cast<void>(::unlink((snapshot_path_ + ".tmp").c_str()));
    static_cast<void>(::rmdir(directory_.c_str()));
  }

  std::string directory_;
  std::string journal_path_;
  std::string snapshot_path_;
};

// Verifies threshold-gated snapshot publication compacts the durable journal.
TEST_F(SnapshotCompactionFixture, PublishesSnapshotAndCompactsJournal) {
  storage::KVStore store;
  FileRaftPersistence persistence(journal_path_);
  FileSnapshotStore snapshot_store(snapshot_path_);
  RaftKVService service(NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store,
                        &persistence);
  std::string error;
  ASSERT_TRUE(service.tick(100, error).empty()) << error;
  ASSERT_EQ(service.lastApplied(), 1U);

  SnapshotPublishResult result;
  ASSERT_TRUE(service.tryPublishSnapshot(snapshot_store, 2, result, error))
      << error;
  EXPECT_FALSE(result.performed);

  SubmitResult first = service.submit(
      KVCommand{KVCommandType::kSet, 10, 1, "name", "tom"}, error);
  ASSERT_EQ(first.status, SubmitStatus::kApplied) << error;
  EXPECT_EQ(service.lastApplied(), 2U);

  ASSERT_TRUE(service.tryPublishSnapshot(snapshot_store, 2, result, error))
      << error;
  ASSERT_TRUE(result.performed);
  EXPECT_EQ(result.boundary_index, 2U);
  EXPECT_EQ(service.lastSnapshotIndex(), 2U);
  EXPECT_EQ(service.raftNode().log().firstIndex(), 2U);
  EXPECT_EQ(service.raftNode().log().lastIndex(), 2U);
  EXPECT_EQ(service.raftNode().log().persistentEntries().size(), 1U);

  std::optional<StateMachineSnapshot> loaded;
  ASSERT_TRUE(snapshot_store.load(loaded, error)) << error;
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->last_included_index, 2U);
  ASSERT_EQ(loaded->entries.size(), 1U);
  EXPECT_EQ(loaded->entries.front().first, "name");
  EXPECT_EQ(loaded->entries.front().second, "tom");
}

// Verifies a published snapshot plus compacted journal rebuild on restart.
TEST_F(SnapshotCompactionFixture, RestartsFromPublishedSnapshot) {
  storage::KVStore store;
  FileRaftPersistence persistence(journal_path_);
  FileSnapshotStore snapshot_store(snapshot_path_);
  {
    RaftKVService service(NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store,
                          &persistence);
    std::string error;
    ASSERT_TRUE(service.tick(100, error).empty()) << error;
    for (std::uint64_t index = 1; index <= 3; ++index) {
      SubmitResult submission = service.submit(
          KVCommand{KVCommandType::kSet, 20 + index, index,
                    "key" + std::to_string(index),
                    "value" + std::to_string(index)},
          error);
      ASSERT_EQ(submission.status, SubmitStatus::kApplied) << error;
    }
    SnapshotPublishResult result;
    ASSERT_TRUE(service.tryPublishSnapshot(snapshot_store, 2, result, error))
        << error;
    ASSERT_TRUE(result.performed);
  }

  std::optional<StateMachineSnapshot> snapshot;
  std::string error;
  ASSERT_TRUE(snapshot_store.load(snapshot, error)) << error;
  ASSERT_TRUE(snapshot.has_value());

  storage::KVStore recovered_store;
  RaftKVService recovered(NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1},
                          recovered_store, &persistence, &*snapshot);
  EXPECT_EQ(recovered.lastApplied(), snapshot->last_included_index);
  EXPECT_EQ(recovered.getApplied("key1"),
            std::optional<std::string>("value1"));
  EXPECT_EQ(recovered.getApplied("key2"),
            std::optional<std::string>("value2"));
  EXPECT_EQ(recovered.getApplied("key3"),
            std::optional<std::string>("value3"));
  EXPECT_EQ(recovered.raftNode().log().firstIndex(),
            snapshot->last_included_index);
}

// Verifies Leaders emit InstallSnapshot when a follower lags the boundary.
TEST_F(SnapshotCompactionFixture, LeaderSendsInstallSnapshotToLaggingPeer) {
  storage::KVStore leader_store;
  FileRaftPersistence leader_wal(journal_path_ + ".leader");
  FileSnapshotStore leader_snapshot(snapshot_path_ + ".leader");
  RaftKVService leader(NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1},
                       leader_store, &leader_wal, nullptr,
                       &leader_snapshot);

  storage::KVStore caught_up_store;
  RaftKVService caught_up(NodeConfig{2, {1, 3}, std::nullopt, 100, 100, 20, 64, 2},
                          caught_up_store);

  std::string error;
  static_cast<void>(leader.tick(100, error));
  static_cast<void>(leader.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}, error));
  ASSERT_EQ(leader.raftNode().role(), Role::kLeader);

  SubmitResult write = leader.submit(
      KVCommand{KVCommandType::kSet, 30, 1, "name", "tom"}, error);
  ASSERT_EQ(write.status, SubmitStatus::kPending) << error;
  std::deque<OutboundRpc> replication(
      write.outbound.begin(), write.outbound.end());
  std::size_t replication_steps = 0;
  while (!replication.empty()) {
    ASSERT_LT(replication_steps++, 32U);
    OutboundRpc rpc = std::move(replication.front());
    replication.pop_front();
    if (rpc.destination != 2) {
      continue;
    }
    const auto& append =
        std::get<AppendEntriesRequest>(rpc.payload);
    const AppendEntriesResponse response =
        caught_up.handleAppendEntries(append, error);
    ASSERT_TRUE(error.empty()) << error;
    const std::vector<OutboundRpc> follow_up =
        leader.handleAppendEntriesResponse(2, response, error);
    ASSERT_TRUE(error.empty()) << error;
    for (const OutboundRpc& next : follow_up) {
      replication.push_back(next);
    }
  }
  ASSERT_TRUE(leader.takeResult(write.log_index).has_value());

  SnapshotPublishResult published;
  ASSERT_TRUE(
      leader.tryPublishSnapshot(leader_snapshot, 1, published, error))
      << error;
  ASSERT_TRUE(published.performed);

  std::vector<OutboundRpc> outbound = leader.tick(20, error);
  ASSERT_TRUE(error.empty()) << error;
  ASSERT_EQ(outbound.size(), 2U);
  const auto install = std::find_if(
      outbound.begin(), outbound.end(),
      [](const OutboundRpc& rpc) {
        return rpc.destination == 3U &&
               std::holds_alternative<InstallSnapshotRequest>(rpc.payload);
      });
  EXPECT_NE(install, outbound.end());
}

// Verifies a lagging follower installs the Leader snapshot and catches up.
TEST_F(SnapshotCompactionFixture, FollowerCatchesUpViaInstallSnapshot) {
  const std::string leader_snap_path = snapshot_path_ + ".leader2";
  const std::string leader_wal_path = journal_path_ + ".leader2";
  const std::string follower_snap_path = snapshot_path_ + ".follower2";
  const std::string follower_wal_path = journal_path_ + ".follower2";

  storage::KVStore leader_store;
  FileRaftPersistence leader_wal(leader_wal_path);
  FileSnapshotStore leader_snapshot(leader_snap_path);
  RaftKVService leader(NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1},
                       leader_store, &leader_wal, nullptr,
                       &leader_snapshot);

  storage::KVStore caught_up_store;
  RaftKVService caught_up(NodeConfig{2, {1, 3}, std::nullopt, 100, 100, 20, 64, 2},
                          caught_up_store);

  std::string error;
  static_cast<void>(leader.tick(100, error));
  static_cast<void>(leader.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}, error));
  ASSERT_EQ(leader.raftNode().role(), Role::kLeader);

  SubmitResult write = leader.submit(
      KVCommand{KVCommandType::kSet, 30, 1, "name", "tom"}, error);
  ASSERT_EQ(write.status, SubmitStatus::kPending) << error;
  std::deque<OutboundRpc> replication(
      write.outbound.begin(), write.outbound.end());
  std::size_t replication_steps = 0;
  while (!replication.empty()) {
    ASSERT_LT(replication_steps++, 32U);
    OutboundRpc rpc = std::move(replication.front());
    replication.pop_front();
    if (rpc.destination != 2) {
      continue;
    }
    const auto& append =
        std::get<AppendEntriesRequest>(rpc.payload);
    const AppendEntriesResponse response =
        caught_up.handleAppendEntries(append, error);
    ASSERT_TRUE(error.empty()) << error;
    const std::vector<OutboundRpc> follow_up =
        leader.handleAppendEntriesResponse(2, response, error);
    ASSERT_TRUE(error.empty()) << error;
    for (const OutboundRpc& next : follow_up) {
      replication.push_back(next);
    }
  }
  ASSERT_TRUE(leader.takeResult(write.log_index).has_value());

  SnapshotPublishResult published;
  ASSERT_TRUE(
      leader.tryPublishSnapshot(leader_snapshot, 1, published, error))
      << error;
  ASSERT_TRUE(published.performed);

  storage::KVStore lagging_store;
  FileRaftPersistence lagging_wal(follower_wal_path);
  FileSnapshotStore lagging_snapshot(follower_snap_path);
  RaftKVService lagging(NodeConfig{3, {1, 2}, std::nullopt, 100, 100, 20, 64, 3},
                        lagging_store, &lagging_wal, nullptr,
                        &lagging_snapshot);

  std::vector<OutboundRpc> pending = leader.tick(20, error);
  ASSERT_TRUE(error.empty()) << error;
  pending.erase(
      std::remove_if(pending.begin(), pending.end(),
                     [](const OutboundRpc& rpc) {
                       return rpc.destination != 3U;
                     }),
      pending.end());
  ASSERT_FALSE(pending.empty());
  ASSERT_TRUE(std::holds_alternative<InstallSnapshotRequest>(
      pending.front().payload));

  std::size_t steps = 0;
  while (!pending.empty()) {
    ASSERT_LT(steps++, 32U);
    std::vector<OutboundRpc> next;
    for (OutboundRpc& rpc : pending) {
      ASSERT_EQ(rpc.destination, 3U);
      const InstallSnapshotRequest request =
          std::get<InstallSnapshotRequest>(rpc.payload);
      const InstallSnapshotResponse response =
          lagging.handleInstallSnapshot(request, lagging_snapshot, error);
      ASSERT_TRUE(error.empty()) << error;
      next = leader.handleInstallSnapshotResponse(3, response,
                                                  leader_snapshot, error);
      ASSERT_TRUE(error.empty()) << error;
    }
    pending = std::move(next);
  }

  EXPECT_EQ(lagging.lastApplied(), published.boundary_index);
  EXPECT_EQ(lagging.getApplied("name"),
            std::optional<std::string>("tom"));
  EXPECT_EQ(lagging.raftNode().log().firstIndex(),
            published.boundary_index);
}

}  // namespace
}  // namespace distributed_kv::raft
