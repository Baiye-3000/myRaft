#include "raft/raft_node.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

class RecordingPersistence final : public RaftPersistence {
 public:
  /**
   * Records or rejects one synchronous persistence barrier.
   *
   * Input: complete persistent state and writable error.
   * Output: configured success/failure.
   * Thread safety: test thread only.
   */
  bool save(const RaftPersistentState& state,
            std::string& error) override {
    if (fail) {
      error = "injected persistence failure";
      return false;
    }
    saved.push_back(state);
    error.clear();
    return true;
  }

  bool fail{false};
  std::vector<RaftPersistentState> saved;
};

/**
 * Creates a deterministic three-member node with a fixed election timeout.
 *
 * Input: local node id.
 * Output: configured RaftNode.
 * Thread safety: test thread only.
 */
RaftNode makeNode(NodeId node_id = 1) {
  std::vector<NodeId> peers{1, 2, 3};
  peers.erase(std::remove(peers.begin(), peers.end(), node_id), peers.end());
  return RaftNode(NodeConfig{
      node_id, peers, 100, 100, 20, 64, node_id,
  });
}

/**
 * Elects node 1 by delivering one granted peer vote.
 *
 * Input: term-zero node 1.
 * Output: initial Leader AppendEntries messages.
 * Thread safety: test thread only.
 */
std::vector<OutboundRpc> electNodeOne(RaftNode& node) {
  const auto vote_requests = node.tick(100);
  EXPECT_EQ(vote_requests.size(), 2U);
  EXPECT_EQ(node.role(), Role::kCandidate);
  return node.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true});
}

/**
 * Builds a successful response correlated to one AppendEntries request.
 *
 * Input: request accepted by a follower.
 * Output: success response with exact replicated-through index.
 * Thread safety: stateless.
 */
AppendEntriesResponse successfulResponse(
    const AppendEntriesRequest& request) {
  const LogIndex match =
      request.entries.empty() ? request.previous_log_index
                              : request.entries.back().index;
  return AppendEntriesResponse{
      request.term,
      true,
      request.previous_log_index,
      request.entries.size(),
      match,
      match + 1U,
      std::nullopt,
  };
}

// Verifies election timeout starts a new term and broadcasts RequestVote.
TEST(RaftNodeTest, StartsElectionAfterTimeout) {
  RaftNode node = makeNode();

  EXPECT_TRUE(node.tick(99).empty());
  const auto outbound = node.tick(1);

  EXPECT_EQ(node.role(), Role::kCandidate);
  EXPECT_EQ(node.currentTerm(), 1U);
  ASSERT_EQ(outbound.size(), 2U);
  const auto& request = std::get<RequestVoteRequest>(outbound[0].payload);
  EXPECT_EQ(request.term, 1U);
  EXPECT_EQ(request.candidate_id, 1U);
  EXPECT_EQ(request.last_log_index, 0U);
}

// Verifies a candidate becomes Leader only after a strict majority.
TEST(RaftNodeTest, BecomesLeaderAfterMajorityVote) {
  RaftNode node = makeNode();

  const auto append_requests = electNodeOne(node);

  EXPECT_EQ(node.role(), Role::kLeader);
  EXPECT_EQ(node.leaderId(), std::optional<NodeId>(1));
  ASSERT_EQ(node.log().lastIndex(), 1U);
  EXPECT_EQ(node.log().entryAt(1)->type, EntryType::kNoOp);
  ASSERT_EQ(append_requests.size(), 2U);
  EXPECT_TRUE(
      std::holds_alternative<AppendEntriesRequest>(
          append_requests[0].payload));
}

// Verifies one-vote-per-term and candidate-log freshness rules.
TEST(RaftNodeTest, GrantsAtMostOneVoteToUpToDateCandidate) {
  RaftNode node = makeNode();

  const auto first = node.handleRequestVote(
      RequestVoteRequest{1, 2, 0, 0});
  const auto second = node.handleRequestVote(
      RequestVoteRequest{1, 3, 0, 0});

  EXPECT_TRUE(first.vote_granted);
  EXPECT_FALSE(second.vote_granted);
  EXPECT_EQ(node.currentTerm(), 1U);
}

// Verifies stale candidate logs are rejected even in a newer term.
TEST(RaftNodeTest, RejectsCandidateWithStaleLog) {
  RaftNode node = makeNode();
  const AppendEntriesRequest append{
      2,
      2,
      0,
      0,
      {{1, 2, EntryType::kCommand, "newer"}},
      0,
  };
  ASSERT_TRUE(node.handleAppendEntries(append).success);

  const auto vote = node.handleRequestVote(
      RequestVoteRequest{3, 3, 10, 1});

  EXPECT_FALSE(vote.vote_granted);
  EXPECT_EQ(node.currentTerm(), 3U);
}

// Verifies follower conflict replacement and Leader commit propagation.
TEST(RaftNodeTest, AppendsAndCommitsLeaderEntries) {
  RaftNode node = makeNode();
  const AppendEntriesRequest initial{
      1,
      2,
      0,
      0,
      {
          {1, 1, EntryType::kCommand, "a"},
          {2, 1, EntryType::kCommand, "b"},
      },
      1,
  };
  ASSERT_TRUE(node.handleAppendEntries(initial).success);
  EXPECT_EQ(node.commitIndex(), 1U);

  const AppendEntriesRequest replacement{
      2,
      2,
      0,
      0,
      {{1, 2, EntryType::kCommand, "replacement"}},
      1,
  };
  ASSERT_TRUE(node.handleAppendEntries(replacement).success);

  EXPECT_EQ(node.log().lastIndex(), 1U);
  EXPECT_EQ(node.log().lastTerm(), 2U);
  EXPECT_EQ(node.log().entryAt(1)->command, "replacement");
}

// Verifies a Leader commits a current-term command after one follower ack.
TEST(RaftNodeTest, CommitsProposalAfterMajorityReplication) {
  RaftNode node = makeNode();
  const auto initial_replication = electNodeOne(node);
  const auto peer_two =
      std::find_if(initial_replication.begin(), initial_replication.end(),
                   [](const OutboundRpc& rpc) {
                     return rpc.destination == 2;
                   });
  ASSERT_NE(peer_two, initial_replication.end());
  const auto& no_op =
      std::get<AppendEntriesRequest>(peer_two->payload);
  EXPECT_TRUE(node.handleAppendEntriesResponse(
                       2, successfulResponse(no_op))
                  .empty());
  EXPECT_EQ(node.commitIndex(), 1U);

  const ProposeResult proposal = node.propose("SET x y");
  ASSERT_TRUE(proposal.accepted);
  ASSERT_EQ(proposal.index, 2U);
  const auto command_rpc =
      std::find_if(proposal.outbound.begin(), proposal.outbound.end(),
                   [](const OutboundRpc& rpc) {
                     return rpc.destination == 2;
                   });
  ASSERT_NE(command_rpc, proposal.outbound.end());
  const auto& command =
      std::get<AppendEntriesRequest>(command_rpc->payload);
  const auto follow_up =
      node.handleAppendEntriesResponse(2, successfulResponse(command));
  EXPECT_TRUE(follow_up.empty());

  EXPECT_EQ(node.commitIndex(), 2U);
}

// Verifies conflict hints move nextIndex directly and emit an immediate retry.
TEST(RaftNodeTest, UsesConflictHintForReplicationRetry) {
  RaftNode node = makeNode();
  const AppendEntriesRequest old_leader{
      1,
      2,
      0,
      0,
      {
          {1, 1, EntryType::kCommand, "a"},
          {2, 1, EntryType::kCommand, "b"},
      },
      0,
  };
  ASSERT_TRUE(node.handleAppendEntries(old_leader).success);
  const auto vote_requests = node.tick(100);
  ASSERT_EQ(vote_requests.size(), 2U);
  const auto initial = node.handleRequestVoteResponse(
      2, RequestVoteResponse{2, 2, true});
  ASSERT_EQ(node.role(), Role::kLeader);

  const auto peer_two =
      std::find_if(initial.begin(), initial.end(),
                   [](const OutboundRpc& rpc) {
                     return rpc.destination == 2;
                   });
  ASSERT_NE(peer_two, initial.end());
  const auto& sent = std::get<AppendEntriesRequest>(peer_two->payload);
  ASSERT_EQ(sent.previous_log_index, 2U);

  const AppendEntriesResponse failure{
      2, false, 2, sent.entries.size(), 0, 1, std::nullopt,
  };
  const auto retry = node.handleAppendEntriesResponse(2, failure);

  ASSERT_EQ(retry.size(), 1U);
  const auto& retried =
      std::get<AppendEntriesRequest>(retry[0].payload);
  EXPECT_EQ(retried.previous_log_index, 0U);
  ASSERT_FALSE(retried.entries.empty());
  EXPECT_EQ(retried.entries.front().index, 1U);
}

// Verifies a higher-term response immediately demotes a Leader.
TEST(RaftNodeTest, StepsDownForHigherTermResponse) {
  RaftNode node = makeNode();
  const auto initial = electNodeOne(node);
  ASSERT_EQ(node.role(), Role::kLeader);
  const auto& sent = std::get<AppendEntriesRequest>(initial[0].payload);

  const auto ignored = node.handleAppendEntriesResponse(
      initial[0].destination,
      AppendEntriesResponse{
          5, false, sent.previous_log_index, sent.entries.size(), 0, 1,
          std::nullopt,
      });
  EXPECT_TRUE(ignored.empty());

  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.currentTerm(), 5U);
}

// Verifies valid Leader traffic resets the follower election timer.
TEST(RaftNodeTest, HeartbeatResetsElectionTimeout) {
  RaftNode node = makeNode();
  EXPECT_TRUE(node.tick(90).empty());
  ASSERT_TRUE(node.handleAppendEntries(
                      AppendEntriesRequest{1, 2, 0, 0, {}, 0})
                  .success);

  EXPECT_TRUE(node.tick(90).empty());
  EXPECT_EQ(node.role(), Role::kFollower);
}

// Verifies term, vote and log are saved before dependent RPCs are returned.
TEST(RaftNodeTest, PersistsSafetyStateBeforeOutboundRpc) {
  RecordingPersistence persistence;
  RaftNode node(
      NodeConfig{1, {2, 3}, 100, 100, 20, 64, 1},
      &persistence);

  const auto votes = node.tick(100);
  ASSERT_EQ(votes.size(), 2U);
  ASSERT_EQ(persistence.saved.size(), 1U);
  EXPECT_EQ(persistence.saved.back().current_term, 1U);
  EXPECT_EQ(persistence.saved.back().voted_for,
            std::optional<NodeId>(1));

  const auto replication = node.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true});
  ASSERT_EQ(replication.size(), 2U);
  ASSERT_EQ(persistence.saved.size(), 2U);
  EXPECT_EQ(persistence.saved.back().entries.size(), 2U);
  EXPECT_EQ(persistence.saved.back().entries.back().type,
            EntryType::kNoOp);

  const ProposeResult proposal = node.propose("command");
  ASSERT_TRUE(proposal.accepted);
  ASSERT_EQ(persistence.saved.size(), 3U);
  EXPECT_EQ(persistence.saved.back().entries.back().command, "command");
}

// Verifies one AppendEntries response uses one barrier for term/log/commit.
TEST(RaftNodeTest, CoalescesAppendEntriesPersistenceBarrier) {
  RecordingPersistence persistence;
  RaftNode node(
      NodeConfig{1, {2, 3}, 100, 100, 20, 64, 1},
      &persistence);
  const AppendEntriesRequest request{
      2,
      2,
      0,
      0,
      {LogEntry{1, 2, EntryType::kCommand, "command"}},
      1,
  };
  const AppendEntriesResponse response =
      node.handleAppendEntries(request);
  ASSERT_TRUE(response.success);
  ASSERT_EQ(persistence.saved.size(), 1U);
  EXPECT_EQ(persistence.saved.back().current_term, 2U);
  EXPECT_EQ(persistence.saved.back().entries.size(), 2U);
  EXPECT_EQ(persistence.saved.back().commit_index, 1U);
}

// Verifies failed persistence suppresses RPC return by faulting the operation.
TEST(RaftNodeTest, PropagatesPersistenceFailure) {
  RecordingPersistence persistence;
  persistence.fail = true;
  RaftNode node(
      NodeConfig{1, {2, 3}, 100, 100, 20, 64, 1},
      &persistence);

  EXPECT_THROW(static_cast<void>(node.tick(100)), std::runtime_error);
}

// Verifies a single-member cluster commits without waiting for RPC responses.
TEST(RaftNodeTest, SingleNodeCommitsImmediately) {
  RaftNode node(NodeConfig{
      1, {}, 100, 100, 20, 64, 1,
  });

  EXPECT_TRUE(node.tick(100).empty());
  ASSERT_EQ(node.role(), Role::kLeader);
  EXPECT_EQ(node.commitIndex(), 1U);

  const ProposeResult proposal = node.propose("SET one 1");
  EXPECT_TRUE(proposal.accepted);
  EXPECT_TRUE(proposal.outbound.empty());
  EXPECT_EQ(node.commitIndex(), 2U);
}

// Verifies read barriers are emitted only by a Leader and preserve context.
TEST(RaftNodeTest, TagsReadBarrierProbesAndResponses) {
  RaftNode leader = makeNode(1);
  static_cast<void>(leader.tick(500));
  static_cast<void>(leader.handleRequestVoteResponse(
      2, RequestVoteResponse{leader.currentTerm(), leader.currentTerm(),
                             true}));
  ASSERT_EQ(leader.role(), Role::kLeader);
  const auto probes = leader.makeReadBarrier(42);
  ASSERT_EQ(probes.size(), 2U);
  for (const OutboundRpc& outbound : probes) {
    const auto& append =
        std::get<AppendEntriesRequest>(outbound.payload);
    EXPECT_EQ(append.read_context, 42U);
  }

  RaftNode follower = makeNode(2);
  AppendEntriesRequest request =
      std::get<AppendEntriesRequest>(probes.front().payload);
  const AppendEntriesResponse response =
      follower.handleAppendEntries(request);
  EXPECT_EQ(response.read_context, 42U);
}

}  // namespace
}  // namespace distributed_kv::raft
