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

  RaftPersistentState state_;

 private:
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
      node_id, peers, std::nullopt, 100, 100, 20, 64, node_id,
  });
}

ClusterConfiguration makeClusterConfiguration(std::size_t count = 3) {
  ClusterConfiguration config;
  config.config_id = 1;
  for (NodeId id = 1; id <= count; ++id) {
    config.members.push_back(ClusterMember{id, "127.0.0.1",
                                           static_cast<std::uint16_t>(7000 + id),
                                           "127.0.0.1",
                                           static_cast<std::uint16_t>(8000 + id)});
  }
  return config;
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
      NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1},
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
      NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1},
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
      NodeConfig{1, {2, 3}, std::nullopt, 100, 100, 20, 64, 1},
      &persistence);

  EXPECT_THROW(static_cast<void>(node.tick(100)), std::runtime_error);
}

// Verifies a single-member cluster commits without waiting for RPC responses.
TEST(RaftNodeTest, SingleNodeCommitsImmediately) {
  RaftNode node(NodeConfig{
      1, {}, std::nullopt, 100, 100, 20, 64, 1,
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

// Verifies snapshot boundaries compact the durable Raft log image.
TEST(RaftNodeTest, ApplySnapshotBoundaryCompactsLog) {
  FixedPersistence persistence(RaftPersistentState{
      2,
      std::nullopt,
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 1, EntryType::kNoOp, ""},
          LogEntry{2, 2, EntryType::kCommand, "tail"},
      },
      2,
  });
  RaftNode node(NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, &persistence);
  std::string error;
  ASSERT_TRUE(node.applySnapshotBoundary(2, 2, error)) << error;
  EXPECT_EQ(node.log().firstIndex(), 2U);
  EXPECT_EQ(node.log().lastIndex(), 2U);
  EXPECT_EQ(node.commitIndex(), 2U);
  ASSERT_EQ(persistence.state_.entries.size(), 1U);
  EXPECT_EQ(persistence.state_.entries.front().index, 2U);
}

// Verifies a 3-to-4 change needs both old and new majorities and exits joint
// consensus through the automatically generated second entry.
TEST(RaftNodeTest, CommitsJointMembershipChangeWithBothMajorities) {
  const ClusterConfiguration bootstrap = makeClusterConfiguration();
  RaftNode node(NodeConfig{1, {2, 3}, bootstrap, 100, 100, 20, 64, 1});
  static_cast<void>(node.tick(100));
  ASSERT_EQ(node.role(), Role::kCandidate);
  static_cast<void>(node.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}));
  ASSERT_EQ(node.role(), Role::kLeader);

  const ClusterMember new_member{4, "127.0.0.1", 7004, "127.0.0.1", 8004};
  ASSERT_TRUE(node.addLearner(4));
  const auto learner_sync = node.tick(20);
  const auto learner_rpc = std::find_if(
      learner_sync.begin(), learner_sync.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 4; });
  ASSERT_NE(learner_rpc, learner_sync.end());
  static_cast<void>(node.handleAppendEntriesResponse(
      4, successfulResponse(std::get<AppendEntriesRequest>(
             learner_rpc->payload))));
  const ProposeResult proposal =
      node.proposeConfChange(ConfChangeType::kAddNode, new_member);
  ASSERT_TRUE(proposal.accepted);
  EXPECT_FALSE(node.proposeConfChange(ConfChangeType::kAddNode, new_member)
                   .accepted);

  for (const OutboundRpc& rpc : proposal.outbound) {
    const auto& request = std::get<AppendEntriesRequest>(rpc.payload);
    static_cast<void>(node.handleAppendEntriesResponse(
        rpc.destination, successfulResponse(request)));
  }
  EXPECT_EQ(node.jointConfig()->members.size(), 4U);
  EXPECT_EQ(node.clusterConfig()->members.size(), 3U);
  EXPECT_TRUE(node.log().entryAt(node.commitIndex())->type ==
              EntryType::kConfChange);

  const auto exit_rpcs = node.tick(20);
  for (const OutboundRpc& rpc : exit_rpcs) {
    const auto& request = std::get<AppendEntriesRequest>(rpc.payload);
    static_cast<void>(node.handleAppendEntriesResponse(
        rpc.destination, successfulResponse(request)));
  }
  EXPECT_FALSE(node.jointConfig().has_value());
  ASSERT_TRUE(node.clusterConfig().has_value());
  EXPECT_EQ(node.clusterConfig()->members.size(), 4U);
}

// Verifies a Learner catches up without voting or contributing to commit.
TEST(RaftNodeTest, LearnerCatchesUpBeforeMembershipChange) {
  const ClusterConfiguration bootstrap = makeClusterConfiguration();
  RaftNode leader(NodeConfig{1, {2, 3}, bootstrap, 100, 100, 20, 64, 1});
  static_cast<void>(leader.tick(100));
  static_cast<void>(leader.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}));
  ASSERT_EQ(leader.role(), Role::kLeader);

  // Commit the Leader no-op with one voting follower.
  const auto initial = leader.tick(20);
  for (const OutboundRpc& rpc : initial) {
    if (rpc.destination == 2) {
      const auto& request = std::get<AppendEntriesRequest>(rpc.payload);
      static_cast<void>(leader.handleAppendEntriesResponse(
          2, successfulResponse(request)));
      break;
    }
  }
  ASSERT_EQ(leader.commitIndex(), 1U);

  ASSERT_TRUE(leader.addLearner(4));
  RaftNode learner(NodeConfig{4, {1}, std::nullopt, 100, 100, 20, 64, 4,
                               {}, true});
  const auto catch_up = leader.tick(20);
  auto learner_rpc = std::find_if(
      catch_up.begin(), catch_up.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 4; });
  ASSERT_NE(learner_rpc, catch_up.end());
  const auto& request = std::get<AppendEntriesRequest>(learner_rpc->payload);
  const AppendEntriesResponse response = learner.handleAppendEntries(request);
  ASSERT_TRUE(response.success);
  static_cast<void>(leader.handleAppendEntriesResponse(4, response));
  EXPECT_TRUE(leader.learnerCaughtUp(4));
  EXPECT_TRUE(learner.tick(1000).empty());
  EXPECT_FALSE(learner.handleRequestVote(
      RequestVoteRequest{2, 1, learner.log().lastIndex(), learner.log().lastTerm()})
                   .vote_granted);

  const ClusterMember member{4, "127.0.0.1", 7004, "127.0.0.1", 8004};
  EXPECT_TRUE(leader.proposeConfChange(ConfChangeType::kAddNode, member)
                  .accepted);
}

// Verifies a newly elected Leader reconstructs the missing C_new entry after
// the previous Leader committed joint consensus and failed before joint exit.
TEST(RaftNodeTest, NewLeaderRecoversCommittedJointExit) {
  const ClusterConfiguration stable = makeClusterConfiguration();
  ClusterConfiguration target = makeClusterConfiguration(4);
  target.config_id = 2;
  const ClusterMember added = target.members.back();
  std::string encoded;
  std::string error;
  ASSERT_TRUE(encodeConfChangeEntry(
      ConfChangeEntry{ConfChangeType::kAddNode, true, added, target},
      encoded, error)) << error;
  FixedPersistence persistence(RaftPersistentState{
      1,
      std::nullopt,
      {LogEntry{0, 0, EntryType::kNoOp, ""},
       LogEntry{1, 1, EntryType::kNoOp, ""},
       LogEntry{2, 1, EntryType::kConfChange, encoded}},
      2,
      stable,
      target,
  });
  RaftNode node(NodeConfig{2, {1, 3, 4}, stable, 100, 100, 20, 64, 2},
                &persistence);

  static_cast<void>(node.tick(100));
  static_cast<void>(node.handleRequestVoteResponse(
      1, RequestVoteResponse{2, 2, true}));
  const auto replication = node.handleRequestVoteResponse(
      3, RequestVoteResponse{2, 2, true});
  ASSERT_EQ(node.role(), Role::kLeader);
  ASSERT_EQ(node.log().lastIndex(), 4U);
  const auto exit_entry = node.log().entryAt(4);
  ASSERT_TRUE(exit_entry.has_value());
  ASSERT_EQ(exit_entry->type, EntryType::kConfChange);
  ConfChangeEntry exit;
  ASSERT_TRUE(decodeConfChangeEntry(exit_entry->command, exit, error)) << error;
  EXPECT_FALSE(exit.joint);
  EXPECT_EQ(exit.target_config, target);

  for (const OutboundRpc& rpc : replication) {
    if (rpc.destination == 1 || rpc.destination == 3) {
      static_cast<void>(node.handleAppendEntriesResponse(
          rpc.destination,
          successfulResponse(std::get<AppendEntriesRequest>(rpc.payload))));
    }
  }
  EXPECT_FALSE(node.jointConfig().has_value());
  ASSERT_TRUE(node.clusterConfig().has_value());
  EXPECT_EQ(*node.clusterConfig(), target);
}

// Verifies the old configuration's majority cannot commit an ADD unless the
// target configuration independently has a majority as well.
TEST(RaftNodeTest, JointAddRejectsOnlyOldConfigurationMajority) {
  const ClusterConfiguration bootstrap = makeClusterConfiguration();
  RaftNode leader(NodeConfig{1, {2, 3}, bootstrap, 100, 100, 20, 64, 1});
  static_cast<void>(leader.tick(100));
  static_cast<void>(leader.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}));
  ASSERT_EQ(leader.role(), Role::kLeader);
  const auto heartbeat = leader.tick(20);
  const auto voter = std::find_if(
      heartbeat.begin(), heartbeat.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 2; });
  ASSERT_NE(voter, heartbeat.end());
  static_cast<void>(leader.handleAppendEntriesResponse(
      2, successfulResponse(std::get<AppendEntriesRequest>(voter->payload))));

  ASSERT_TRUE(leader.addLearner(4));
  const auto catch_up = leader.tick(20);
  const auto learner = std::find_if(
      catch_up.begin(), catch_up.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 4; });
  ASSERT_NE(learner, catch_up.end());
  static_cast<void>(leader.handleAppendEntriesResponse(
      4, successfulResponse(std::get<AppendEntriesRequest>(learner->payload))));
  const ClusterMember member{4, "127.0.0.1", 7004, "127.0.0.1", 8004};
  const ProposeResult proposal =
      leader.proposeConfChange(ConfChangeType::kAddNode, member);
  ASSERT_TRUE(proposal.accepted);

  const auto old_voter = std::find_if(
      proposal.outbound.begin(), proposal.outbound.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 2; });
  ASSERT_NE(old_voter, proposal.outbound.end());
  static_cast<void>(leader.handleAppendEntriesResponse(
      2,
      successfulResponse(std::get<AppendEntriesRequest>(old_voter->payload))));
  EXPECT_LT(leader.commitIndex(), proposal.index);

  const auto new_voter = std::find_if(
      proposal.outbound.begin(), proposal.outbound.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 4; });
  ASSERT_NE(new_voter, proposal.outbound.end());
  static_cast<void>(leader.handleAppendEntriesResponse(
      4,
      successfulResponse(std::get<AppendEntriesRequest>(new_voter->payload))));
  EXPECT_GE(leader.commitIndex(), proposal.index);
}

// Verifies an uncommitted local joint configuration is discarded when a new
// Leader overwrites its ConfChange log entry.
TEST(RaftNodeTest, RollsBackOverwrittenUncommittedJointChange) {
  const ClusterConfiguration bootstrap = makeClusterConfiguration();
  RaftNode node(NodeConfig{1, {2, 3}, bootstrap, 100, 100, 20, 64, 1});
  static_cast<void>(node.tick(100));
  static_cast<void>(node.handleRequestVoteResponse(
      2, RequestVoteResponse{1, 1, true}));
  const auto heartbeat = node.tick(20);
  const auto voter = std::find_if(
      heartbeat.begin(), heartbeat.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 2; });
  ASSERT_NE(voter, heartbeat.end());
  static_cast<void>(node.handleAppendEntriesResponse(
      2, successfulResponse(std::get<AppendEntriesRequest>(voter->payload))));
  ASSERT_TRUE(node.addLearner(4));
  const auto catch_up = node.tick(20);
  const auto learner = std::find_if(
      catch_up.begin(), catch_up.end(),
      [](const OutboundRpc& rpc) { return rpc.destination == 4; });
  ASSERT_NE(learner, catch_up.end());
  static_cast<void>(node.handleAppendEntriesResponse(
      4, successfulResponse(std::get<AppendEntriesRequest>(learner->payload))));
  ASSERT_TRUE(node.proposeConfChange(
      ConfChangeType::kAddNode,
      ClusterMember{4, "127.0.0.1", 7004, "127.0.0.1", 8004}, 8100)
                  .accepted);
  ASSERT_TRUE(node.jointConfig().has_value());
  ASSERT_TRUE(node.activeMembershipOperation().has_value());

  const AppendEntriesResponse overwritten = node.handleAppendEntries(
      AppendEntriesRequest{2, 2, 1, 1,
                           {LogEntry{2, 2, EntryType::kNoOp, ""}}, 1});
  ASSERT_TRUE(overwritten.success);
  EXPECT_FALSE(node.jointConfig().has_value());
  EXPECT_FALSE(node.activeMembershipOperation().has_value());
  ASSERT_TRUE(node.clusterConfig().has_value());
  EXPECT_EQ(node.clusterConfig()->members.size(), 3U);

  static_cast<void>(node.tick(100));
  const auto leadership = node.handleRequestVoteResponse(
      2, RequestVoteResponse{3, 3, true});
  ASSERT_EQ(node.role(), Role::kLeader);
  EXPECT_EQ(node.log().entryAt(node.log().lastIndex())->type,
            EntryType::kNoOp);
  EXPECT_FALSE(node.jointConfig().has_value());
  EXPECT_FALSE(leadership.empty());
}

TEST(RaftNodeTest, PersistsIdempotentMembershipOperationThroughJointExit) {
  const ClusterConfiguration bootstrap = makeClusterConfiguration();
  FixedPersistence persistence(RaftPersistentState{});
  {
    RaftNode node(NodeConfig{1, {2, 3}, bootstrap, 100, 100, 20, 64, 1},
                  &persistence);
    static_cast<void>(node.tick(100));
    static_cast<void>(node.handleRequestVoteResponse(
        2, RequestVoteResponse{1, 1, true}));
    ASSERT_EQ(node.role(), Role::kLeader);
    ASSERT_TRUE(node.addLearner(4));
    const auto catch_up = node.tick(20);
    const auto learner = std::find_if(
        catch_up.begin(), catch_up.end(),
        [](const OutboundRpc& rpc) { return rpc.destination == 4; });
    ASSERT_NE(learner, catch_up.end());
    static_cast<void>(node.handleAppendEntriesResponse(
        4, successfulResponse(
               std::get<AppendEntriesRequest>(learner->payload))));
    const ClusterMember member{4, "127.0.0.1", 7004,
                               "127.0.0.1", 8004};
    const ProposeResult proposal = node.proposeConfChange(
        ConfChangeType::kAddNode, member, 8200);
    ASSERT_TRUE(proposal.accepted);
    ASSERT_TRUE(node.activeMembershipOperation().has_value());
    EXPECT_EQ(node.activeMembershipOperation()->operation_id, 8200U);
    ConfChangeEntry joint;
    std::string error;
    ASSERT_TRUE(decodeConfChangeEntry(
        node.log().entryAt(proposal.index)->command, joint, error)) << error;
    EXPECT_EQ(joint.operation_id, 8200U);

    for (const OutboundRpc& rpc : proposal.outbound) {
      static_cast<void>(node.handleAppendEntriesResponse(
          rpc.destination,
          successfulResponse(std::get<AppendEntriesRequest>(rpc.payload))));
    }
    const auto exit_entry = node.log().entryAt(node.log().lastIndex());
    ASSERT_TRUE(exit_entry.has_value());
    ConfChangeEntry exit;
    ASSERT_TRUE(decodeConfChangeEntry(exit_entry->command, exit, error))
        << error;
    EXPECT_FALSE(exit.joint);
    EXPECT_EQ(exit.operation_id, 8200U);

    const auto exit_rpcs = node.tick(20);
    for (const OutboundRpc& rpc : exit_rpcs) {
      static_cast<void>(node.handleAppendEntriesResponse(
          rpc.destination,
          successfulResponse(std::get<AppendEntriesRequest>(rpc.payload))));
    }
    EXPECT_FALSE(node.activeMembershipOperation().has_value());
    ASSERT_TRUE(node.completedMembershipOperation().has_value());
    EXPECT_EQ(node.completedMembershipOperation()->operation_id, 8200U);
  }

  RaftNode restarted(
      NodeConfig{1, {2, 3, 4}, bootstrap, 100, 100, 20, 64, 1},
      &persistence);
  ASSERT_TRUE(restarted.completedMembershipOperation().has_value());
  EXPECT_EQ(restarted.completedMembershipOperation()->operation_id, 8200U);
}

TEST(RaftNodeTest, DecodesLegacyConfChangeWithoutOperationId) {
  ClusterConfiguration target = makeClusterConfiguration(4);
  target.config_id = 2;
  std::string encoded;
  std::string error;
  ASSERT_TRUE(encodeConfChangeEntry(
      ConfChangeEntry{ConfChangeType::kAddNode, true,
                      target.members.back(), target, 8300},
      encoded, error)) << error;
  ASSERT_GE(encoded.size(), sizeof(std::uint64_t));
  encoded.resize(encoded.size() - sizeof(std::uint64_t));
  ConfChangeEntry decoded;
  ASSERT_TRUE(decodeConfChangeEntry(encoded, decoded, error)) << error;
  EXPECT_EQ(decoded.operation_id, 0U);
  EXPECT_EQ(decoded.target_config, target);
}

// Verifies an added Learner becomes a joint voter as soon as it appends the
// joint configuration, before the previous Leader announces its commit.
TEST(RaftNodeTest, LearnerVotesAfterAppendingJointConfiguration) {
  const ClusterConfiguration stable = makeClusterConfiguration();
  ClusterConfiguration target = makeClusterConfiguration(4);
  target.config_id = 2;
  std::string encoded;
  std::string error;
  ASSERT_TRUE(encodeConfChangeEntry(
      ConfChangeEntry{ConfChangeType::kAddNode, true,
                      target.members.back(), target},
      encoded, error)) << error;
  RaftNode learner(NodeConfig{4, {1, 2, 3}, stable, 100, 100, 20, 64, 4,
                               {}, true});
  const AppendEntriesResponse appended = learner.handleAppendEntries(
      AppendEntriesRequest{1, 1, 0, 0,
                           {LogEntry{1, 1, EntryType::kConfChange, encoded}},
                           0});
  ASSERT_TRUE(appended.success);
  ASSERT_TRUE(learner.jointConfig().has_value());

  const RequestVoteResponse vote = learner.handleRequestVote(
      RequestVoteRequest{2, 2, learner.log().lastIndex(),
                         learner.log().lastTerm()});
  EXPECT_TRUE(vote.vote_granted);
}

}  // namespace
}  // namespace distributed_kv::raft
