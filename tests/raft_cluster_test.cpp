#include "raft/raft_node.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

struct Envelope {
  NodeId source{0};
  OutboundRpc rpc;
};

class DeterministicCluster {
 public:
  /**
   * Creates three nodes with non-overlapping deterministic election timeouts.
   *
   * Input: none.
   * Output: term-zero three-node cluster.
   * Thread safety: test thread only.
   */
  DeterministicCluster() {
    nodes_.emplace(
        1, std::make_unique<RaftNode>(
               NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1}));
    nodes_.emplace(
        2, std::make_unique<RaftNode>(
               NodeConfig{2, {1, 3}, std::nullopt, 200, 200, 20, 64, 2}));
    nodes_.emplace(
        3, std::make_unique<RaftNode>(
               NodeConfig{3, {1, 2}, std::nullopt, 300, 300, 20, 64, 3}));
  }

  /**
   * Returns one mutable node for deterministic event injection.
   *
   * Input: configured node id.
   * Output: borrowed node reference.
   * Thread safety: test thread only.
   */
  RaftNode& node(NodeId node_id) { return *nodes_.at(node_id); }

  /**
   * Delivers requests and generated responses until the queue is empty.
   *
   * Input: originating node, initial RPCs, and optional dropped destination.
   * Output: all non-dropped protocol reactions applied.
   * Thread safety: test thread only.
   */
  void deliver(NodeId source, std::vector<OutboundRpc> initial,
               NodeId dropped_destination = 0) {
    std::deque<Envelope> queue;
    for (auto& rpc : initial) {
      queue.push_back(Envelope{source, std::move(rpc)});
    }

    std::size_t steps = 0;
    while (!queue.empty()) {
      ASSERT_LT(steps++, 1000U);
      Envelope envelope = std::move(queue.front());
      queue.pop_front();
      if (envelope.rpc.destination == dropped_destination) {
        continue;
      }

      RaftNode& destination = node(envelope.rpc.destination);
      RaftNode& origin = node(envelope.source);
      std::vector<OutboundRpc> follow_up;
      if (const auto* vote =
              std::get_if<RequestVoteRequest>(&envelope.rpc.payload)) {
        const RequestVoteResponse response =
            destination.handleRequestVote(*vote);
        follow_up = origin.handleRequestVoteResponse(
            envelope.rpc.destination, response);
      } else if (const auto* append =
                     std::get_if<AppendEntriesRequest>(
                         &envelope.rpc.payload)) {
        const AppendEntriesResponse response =
            destination.handleAppendEntries(*append);
        follow_up = origin.handleAppendEntriesResponse(
            envelope.rpc.destination, response);
      } else {
        ADD_FAILURE() << "cluster queue received an unexpected response RPC";
      }

      for (auto& rpc : follow_up) {
        queue.push_back(Envelope{envelope.source, std::move(rpc)});
      }
    }
  }

 private:
  std::unordered_map<NodeId, std::unique_ptr<RaftNode>> nodes_;
};

// Verifies election, majority commit, temporary loss, catch-up and equal logs.
TEST(RaftClusterTest, ElectsReplicatesAndCatchesUpFollower) {
  DeterministicCluster cluster;

  cluster.deliver(1, cluster.node(1).tick(100));
  ASSERT_EQ(cluster.node(1).role(), Role::kLeader);
  cluster.deliver(1, cluster.node(1).tick(20));
  EXPECT_EQ(cluster.node(1).commitIndex(), 1U);
  EXPECT_EQ(cluster.node(2).commitIndex(), 1U);
  EXPECT_EQ(cluster.node(3).commitIndex(), 1U);

  const ProposeResult proposal = cluster.node(1).propose("SET name tom");
  ASSERT_TRUE(proposal.accepted);
  cluster.deliver(1, proposal.outbound, 3);
  EXPECT_EQ(cluster.node(1).commitIndex(), 2U);
  EXPECT_EQ(cluster.node(2).log().lastIndex(), 2U);
  EXPECT_EQ(cluster.node(3).log().lastIndex(), 1U);

  cluster.deliver(1, cluster.node(1).tick(20));
  cluster.deliver(1, cluster.node(1).tick(20));

  for (NodeId node_id : {1U, 2U, 3U}) {
    const RaftNode& node = cluster.node(node_id);
    EXPECT_EQ(node.commitIndex(), 2U);
    ASSERT_EQ(node.log().lastIndex(), 2U);
    EXPECT_EQ(node.log().entryAt(2)->command, "SET name tom");
    EXPECT_EQ(node.log().entryAt(2)->term, 1U);
  }
}

}  // namespace
}  // namespace distributed_kv::raft
