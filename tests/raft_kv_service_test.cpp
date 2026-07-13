#include "raft/raft_kv_service.h"
#include "storage/kv_store.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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
      NodeConfig{1, {2, 3}, 100, 100, 20, 64, 1}, store);
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
      NodeConfig{1, {}, 100, 100, 20, 64, 1}, store);
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
      NodeConfig{1, {}, 100, 100, 20, 64, 1}, store,
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
          NodeConfig{1, {}, 100, 100, 20, 64, 1}, store,
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
        NodeConfig{node_id, std::move(peers), election_timeout,
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

}  // namespace
}  // namespace distributed_kv::raft
