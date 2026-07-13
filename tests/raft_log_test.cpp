#include "raft/raft_log.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

// Verifies local entries receive contiguous indexes after the sentinel.
TEST(RaftLogTest, AppendsLocalEntries) {
  RaftLog log;

  EXPECT_EQ(log.append(1, EntryType::kCommand, "set a 1"), 1U);
  EXPECT_EQ(log.append(1, EntryType::kCommand, "set b 2"), 2U);
  EXPECT_EQ(log.lastIndex(), 2U);
  EXPECT_EQ(log.lastTerm(), 1U);
  EXPECT_EQ(log.size(), 2U);
}

// Verifies a conflicting leader suffix truncates all later local entries.
TEST(RaftLogTest, ReplacesConflictingSuffix) {
  RaftLog log;
  ASSERT_EQ(log.append(1, EntryType::kCommand, "a"), 1U);
  ASSERT_EQ(log.append(1, EntryType::kCommand, "old-b"), 2U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "old-c"), 3U);

  const std::vector<LogEntry> incoming{
      {2, 3, EntryType::kCommand, "new-b"},
      {3, 3, EntryType::kCommand, "new-c"},
  };
  std::string error;
  ASSERT_TRUE(log.appendFrom(1, incoming, error)) << error;

  ASSERT_EQ(log.lastIndex(), 3U);
  EXPECT_EQ(log.termAt(2), std::optional<Term>(3));
  EXPECT_EQ(log.entryAt(3)->command, "new-c");
}

// Verifies replaying an identical suffix is idempotent.
TEST(RaftLogTest, AcceptsIdenticalSuffix) {
  RaftLog log;
  ASSERT_EQ(log.append(2, EntryType::kCommand, "a"), 1U);
  const std::vector<LogEntry> incoming{
      {1, 2, EntryType::kCommand, "a"},
  };
  std::string error;

  EXPECT_TRUE(log.appendFrom(0, incoming, error)) << error;
  EXPECT_EQ(log.size(), 1U);
}

// Verifies malformed non-contiguous leader entries are rejected atomically.
TEST(RaftLogTest, RejectsNonContiguousSuffix) {
  RaftLog log;
  const std::vector<LogEntry> incoming{
      {2, 1, EntryType::kCommand, "skipped-index"},
  };
  std::string error;

  EXPECT_FALSE(log.appendFrom(0, incoming, error));
  EXPECT_EQ(log.lastIndex(), 0U);
}

// Verifies conflict-term index helpers support accelerated leader backtracking.
TEST(RaftLogTest, FindsTermBoundaries) {
  RaftLog log;
  ASSERT_EQ(log.append(1, EntryType::kCommand, "a"), 1U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "b"), 2U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "c"), 3U);
  ASSERT_EQ(log.append(3, EntryType::kCommand, "d"), 4U);

  EXPECT_EQ(log.firstIndexOfTerm(2), std::optional<LogIndex>(2));
  EXPECT_EQ(log.lastIndexOfTerm(2), std::optional<LogIndex>(3));
  EXPECT_EQ(log.firstIndexOfTerm(9), std::nullopt);
}

// Verifies absolute lookups remain correct after restoring a compacted prefix.
TEST(RaftLogTest, RestoresAbsoluteIndexBoundary) {
  RaftLog log;
  std::string error;
  ASSERT_TRUE(log.restore(
      {
          {100, 4, EntryType::kNoOp, ""},
          {101, 5, EntryType::kCommand, "a"},
          {102, 5, EntryType::kCommand, "b"},
      },
      error))
      << error;

  EXPECT_EQ(log.firstIndex(), 100U);
  EXPECT_EQ(log.lastIndex(), 102U);
  EXPECT_EQ(log.size(), 2U);
  EXPECT_EQ(log.termAt(99), std::nullopt);
  EXPECT_EQ(log.termAt(100), std::optional<Term>(4));
  EXPECT_EQ(log.entryAt(101)->command, "a");
  EXPECT_EQ(log.entriesFrom(101, 8).size(), 2U);
  EXPECT_EQ(log.firstIndexOfTerm(5), std::optional<LogIndex>(101));
  EXPECT_EQ(log.lastIndexOfTerm(5), std::optional<LogIndex>(102));
  EXPECT_EQ(log.append(6, EntryType::kCommand, "c"), 103U);
}

// Verifies compaction retains boundary metadata and the following suffix.
TEST(RaftLogTest, CompactsPrefixToMetadataBoundary) {
  RaftLog log;
  ASSERT_EQ(log.append(1, EntryType::kCommand, "a"), 1U);
  ASSERT_EQ(log.append(2, EntryType::kCommand, "b"), 2U);
  ASSERT_EQ(log.append(3, EntryType::kCommand, "c"), 3U);
  std::string error;

  ASSERT_TRUE(log.compactTo(2, 2, error)) << error;
  EXPECT_EQ(log.firstIndex(), 2U);
  EXPECT_EQ(log.lastIndex(), 3U);
  EXPECT_EQ(log.size(), 1U);
  EXPECT_EQ(log.entryAt(1), std::nullopt);
  ASSERT_TRUE(log.entryAt(2).has_value());
  EXPECT_EQ(log.entryAt(2)->type, EntryType::kNoOp);
  EXPECT_TRUE(log.entryAt(2)->command.empty());
  EXPECT_EQ(log.entryAt(3)->command, "c");

  const auto before = log.persistentEntries();
  EXPECT_FALSE(log.compactTo(3, 99, error));
  EXPECT_EQ(log.persistentEntries(), before);
}

// Verifies suffix replacement uses vector offsets rather than absolute indexes.
TEST(RaftLogTest, ReplacesSuffixAfterCompactedBoundary) {
  RaftLog log;
  std::string error;
  ASSERT_TRUE(log.restore(
      {
          {50, 3, EntryType::kNoOp, ""},
          {51, 4, EntryType::kCommand, "old"},
          {52, 4, EntryType::kCommand, "old-tail"},
      },
      error))
      << error;

  ASSERT_TRUE(log.appendFrom(
      50, {{51, 5, EntryType::kCommand, "new"}}, error))
      << error;
  EXPECT_EQ(log.lastIndex(), 51U);
  EXPECT_EQ(log.entryAt(51)->command, "new");
}

}  // namespace
}  // namespace distributed_kv::raft
