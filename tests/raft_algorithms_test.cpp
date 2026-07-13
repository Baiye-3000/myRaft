#include "raft/leader_election.h"
#include "raft/log_replication.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

// Verifies election quorum calculations for common fixed cluster sizes.
TEST(LeaderElectionTest, CalculatesStrictMajority) {
  EXPECT_EQ(LeaderElection::quorumSize(1), 1U);
  EXPECT_EQ(LeaderElection::quorumSize(3), 2U);
  EXPECT_EQ(LeaderElection::quorumSize(5), 3U);
  EXPECT_FALSE(LeaderElection::hasQuorum(2, 5));
  EXPECT_TRUE(LeaderElection::hasQuorum(3, 5));
}

// Verifies log freshness compares term before index as required by Raft.
TEST(LeaderElectionTest, ComparesCandidateLogFreshness) {
  RaftLog local;
  ASSERT_EQ(local.append(2, EntryType::kCommand, "local"), 1U);

  EXPECT_TRUE(LeaderElection::isCandidateLogUpToDate(1, 2, local));
  EXPECT_TRUE(LeaderElection::isCandidateLogUpToDate(0, 3, local));
  EXPECT_FALSE(LeaderElection::isCandidateLogUpToDate(100, 1, local));
}

// Verifies a conflict term present on Leader skips that entire local term.
TEST(LogReplicationTest, CalculatesConflictBacktrack) {
  RaftLog log;
  ASSERT_EQ(log.append(1, EntryType::kCommand, "a"), 1U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "b"), 2U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "c"), 3U);

  EXPECT_EQ(LogReplication::nextIndexAfterConflict(log, 1, 2), 4U);
  EXPECT_EQ(LogReplication::nextIndexAfterConflict(log, 2, 9), 2U);
  EXPECT_EQ(LogReplication::nextIndexAfterConflict(log, 0, std::nullopt),
            1U);
}

// Verifies conflict hints cannot move nextIndex behind a compacted boundary.
TEST(LogReplicationTest, ClampsBacktrackAfterCompactedBoundary) {
  RaftLog log;
  std::string error;
  ASSERT_TRUE(log.restore(
      {
          {40, 3, EntryType::kNoOp, ""},
          {41, 4, EntryType::kCommand, "a"},
          {42, 4, EntryType::kCommand, "b"},
      },
      error))
      << error;

  EXPECT_EQ(LogReplication::nextIndexAfterConflict(
                log, 1, std::nullopt),
            41U);
  EXPECT_EQ(LogReplication::nextIndexAfterConflict(log, 1, 4), 43U);
}

// Verifies old-term entries are not directly committed by replica count.
TEST(LogReplicationTest, CommitsOnlyCurrentTermDirectly) {
  RaftLog log;
  ASSERT_EQ(log.append(1, EntryType::kCommand, "old"), 1U);
  const std::vector<LogIndex> old_term_matches{1, 1};

  EXPECT_EQ(LogReplication::calculateCommitIndex(
                log, 2, 0, old_term_matches, 3),
            0U);

  ASSERT_EQ(log.append(2, EntryType::kNoOp, ""), 2U);
  const std::vector<LogIndex> current_term_matches{2, 0};
  EXPECT_EQ(LogReplication::calculateCommitIndex(
                log, 2, 0, current_term_matches, 3),
            2U);
}

}  // namespace
}  // namespace distributed_kv::raft
