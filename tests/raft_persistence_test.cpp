#include "raft/cluster_config.h"
#include "raft/file_raft_persistence.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "raft/kv_command.h"
#include "raft/raft_kv_service.h"
#include "storage/kv_store.h"

namespace distributed_kv::raft {
namespace {

class RaftPersistenceTest : public ::testing::Test {
 protected:
  // Creates one isolated journal path.
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-raft-XXXXXX";
    ASSERT_LT(prefix.size(), directory_template.size());
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    directory_ = created;
    path_ = directory_ + "/raft.wal";
  }

  // Removes test resources after journal handles close.
  void TearDown() override {
    static_cast<void>(::unlink(path_.c_str()));
    static_cast<void>(::unlink((path_ + ".compact.tmp").c_str()));
    static_cast<void>(::rmdir(directory_.c_str()));
  }

  std::string directory_;
  std::string path_;
};

RaftPersistentState sampleState() {
  return RaftPersistentState{
      3,
      NodeId{2},
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 2, EntryType::kNoOp, ""},
          LogEntry{2, 3, EntryType::kCommand, "command"},
      },
      1,
  };
}

ClusterConfiguration sampleClusterConfig() {
  ClusterConfiguration config;
  config.config_id = 1;
  config.members = {
      ClusterMember{1, "127.0.0.1", 5001, "127.0.0.1", 6001},
      ClusterMember{2, "127.0.0.1", 5002, "127.0.0.1", 6002},
      ClusterMember{3, "127.0.0.1", 5003, "127.0.0.1", 6003},
  };
  return config;
}

// Verifies complete state images survive close and preserve the latest image.
TEST_F(RaftPersistenceTest, SavesAndLoadsLatestState) {
  RaftPersistentState expected = sampleState();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
    expected.commit_index = 2;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.current_term, expected.current_term);
  EXPECT_EQ(actual.voted_for, expected.voted_for);
  EXPECT_EQ(actual.entries, expected.entries);
  EXPECT_EQ(actual.commit_index, expected.commit_index);
}

// Verifies conflicting suffixes recover through truncate-and-append deltas.
TEST_F(RaftPersistenceTest, RecoversIncrementalSuffixReplacement) {
  RaftPersistentState expected = sampleState();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
    expected.current_term = 4;
    expected.voted_for = NodeId{1};
    expected.entries[2] =
        LogEntry{2, 4, EntryType::kCommand, "replacement"};
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.current_term, expected.current_term);
  EXPECT_EQ(actual.voted_for, expected.voted_for);
  EXPECT_EQ(actual.entries, expected.entries);
  EXPECT_EQ(actual.commit_index, expected.commit_index);
}

// Verifies append-heavy journals grow linearly rather than rewriting history.
TEST_F(RaftPersistenceTest, AppendedEntriesUseBoundedDeltaRecords) {
  RaftPersistentState state{
      1,
      std::nullopt,
      {LogEntry{0, 0, EntryType::kNoOp, ""}},
      0,
  };
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(state, error)) << error;
    for (LogIndex index = 1; index <= 100; ++index) {
      state.entries.push_back(
          LogEntry{index, 1, EntryType::kCommand, std::string(128, 'x')});
      ASSERT_TRUE(persistence.save(state, error)) << error;
    }
  }
  struct stat status {};
  ASSERT_EQ(::stat(path_.c_str(), &status), 0);
  EXPECT_LT(status.st_size, static_cast<off_t>(50000));

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, state.entries);
}

// Verifies periodic atomic compaction bounds replay records and keeps state.
TEST_F(RaftPersistenceTest, CompactsDeltasIntoNewFullImage) {
  RaftPersistentState state{
      1,
      std::nullopt,
      {LogEntry{0, 0, EntryType::kNoOp, ""}},
      0,
  };
  {
    FileRaftPersistence persistence(FileRaftPersistenceOptions{
        path_, 4, 1024U * 1024U});
    std::string error;
    ASSERT_TRUE(persistence.save(state, error)) << error;
    for (LogIndex index = 1; index <= 10; ++index) {
      state.entries.push_back(
          LogEntry{index, 1, EntryType::kCommand, std::string(128, 'x')});
      ASSERT_TRUE(persistence.save(state, error)) << error;
    }
    struct stat status {};
    ASSERT_EQ(::stat(path_.c_str(), &status), 0);
    EXPECT_LT(status.st_size, static_cast<off_t>(5000));

    FileRaftPersistence competing(path_);
    RaftPersistentState ignored;
    EXPECT_FALSE(competing.load(ignored, error));
  }

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, state.entries);
  EXPECT_FALSE(::access((path_ + ".compact.tmp").c_str(), F_OK) == 0);
}

// Verifies an incomplete final record is discarded without losing prior state.
TEST_F(RaftPersistenceTest, TruncatesIncompleteTail) {
  const RaftPersistentState expected = sampleState();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  const int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
  ASSERT_GE(fd, 0);
  const std::array<unsigned char, 5> torn{{0xd6, 0x52, 0x46, 0x54, 0}};
  ASSERT_EQ(::write(fd, torn.data(), torn.size()),
            static_cast<ssize_t>(torn.size()));
  ASSERT_EQ(::close(fd), 0);

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, expected.entries);
}

// Verifies a torn delta is removed while its complete base image survives.
TEST_F(RaftPersistenceTest, TruncatesIncompleteDeltaTail) {
  const RaftPersistentState base = sampleState();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(base, error)) << error;
    RaftPersistentState next = base;
    next.current_term = 4;
    next.entries.push_back(
        LogEntry{3, 4, EntryType::kCommand, "new-command"});
    ASSERT_TRUE(persistence.save(next, error)) << error;
  }
  const int fd = ::open(path_.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  const off_t end = ::lseek(fd, 0, SEEK_END);
  ASSERT_GT(end, 8);
  ASSERT_EQ(::ftruncate(fd, end - 8), 0);
  ASSERT_EQ(::close(fd), 0);

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, base.entries);
  EXPECT_EQ(actual.current_term, base.current_term);
}

// Verifies a torn post-configuration delta preserves the last complete
// cluster and joint configuration image.
TEST_F(RaftPersistenceTest, TornDeltaPreservesJointConfiguration) {
  RaftPersistentState base = sampleState();
  base.cluster_config = sampleClusterConfig();
  ClusterConfiguration joint = sampleClusterConfig();
  joint.config_id = 2;
  joint.members.push_back(
      ClusterMember{4, "127.0.0.1", 5004, "127.0.0.1", 6004});
  base.joint_config = joint;
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(base, error)) << error;
    RaftPersistentState next = base;
    next.current_term = 4;
    next.entries.push_back(LogEntry{3, 4, EntryType::kNoOp, ""});
    ASSERT_TRUE(persistence.save(next, error)) << error;
  }
  const int fd = ::open(path_.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  const off_t end = ::lseek(fd, 0, SEEK_END);
  ASSERT_GT(end, 8);
  ASSERT_EQ(::ftruncate(fd, end - 8), 0);
  ASSERT_EQ(::close(fd), 0);

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.current_term, base.current_term);
  EXPECT_EQ(actual.entries, base.entries);
  EXPECT_EQ(actual.cluster_config, base.cluster_config);
  EXPECT_EQ(actual.joint_config, base.joint_config);
}

// Verifies complete but corrupted records are never treated as torn tails.
TEST_F(RaftPersistenceTest, RejectsChecksumCorruption) {
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(sampleState(), error)) << error;
  }
  const int fd = ::open(path_.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  const off_t end = ::lseek(fd, 0, SEEK_END);
  ASSERT_GT(end, 0);
  unsigned char byte = 0;
  ASSERT_EQ(::pread(fd, &byte, 1, end - 1), 1);
  byte ^= 0x80U;
  ASSERT_EQ(::pwrite(fd, &byte, 1, end - 1), 1);
  ASSERT_EQ(::close(fd), 0);

  FileRaftPersistence recovered(path_);
  RaftPersistentState state;
  std::string error;
  EXPECT_FALSE(recovered.load(state, error));
  EXPECT_NE(error.find("checksum"), std::string::npos);
}

// Verifies committed commands rebuild the state machine after restart.
TEST_F(RaftPersistenceTest, ServiceRebuildsOnlyCommittedPrefix) {
  KVCommand command{KVCommandType::kSet, 7, 9, "name", "tom"};
  std::string encoded;
  std::string error;
  ASSERT_TRUE(KVCommandCodec::encode(command, encoded, error)) << error;
  RaftPersistentState state{
      4,
      std::nullopt,
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 4, EntryType::kCommand, encoded},
          LogEntry{2, 4, EntryType::kCommand, encoded},
      },
      1,
  };
  FileRaftPersistence persistence(path_);
  ASSERT_TRUE(persistence.save(state, error)) << error;

  storage::KVStore store;
  NodeConfig config;
  config.node_id = 1;
  config.peers = {2, 3};
  RaftKVService service(config, store, &persistence);
  EXPECT_EQ(service.lastApplied(), 1U);
  ASSERT_TRUE(service.getApplied("name").has_value());
  EXPECT_EQ(*service.getApplied("name"), "tom");
}

// Verifies offset-boundary full images round-trip through the journal codec.
TEST_F(RaftPersistenceTest, RecoversOffsetBoundaryImage) {
  const RaftPersistentState expected{
      6,
      NodeId{1},
      {
          LogEntry{100, 4, EntryType::kNoOp, ""},
          LogEntry{101, 5, EntryType::kCommand, "alpha"},
          LogEntry{102, 6, EntryType::kCommand, "beta"},
      },
      102,
  };
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.current_term, expected.current_term);
  EXPECT_EQ(actual.voted_for, expected.voted_for);
  EXPECT_EQ(actual.entries, expected.entries);
  EXPECT_EQ(actual.commit_index, expected.commit_index);
}

// Verifies incremental deltas preserve non-zero absolute indexes.
TEST_F(RaftPersistenceTest, RecoversOffsetDeltaSuffix) {
  RaftPersistentState state{
      5,
      std::nullopt,
      {
          LogEntry{50, 3, EntryType::kNoOp, ""},
          LogEntry{51, 4, EntryType::kCommand, "old"},
      },
      51,
  };
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(state, error)) << error;
    state.entries[1] = LogEntry{51, 5, EntryType::kCommand, "new"};
    state.entries.push_back(
        LogEntry{52, 5, EntryType::kCommand, "tail"});
    state.commit_index = 52;
    ASSERT_TRUE(persistence.save(state, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, state.entries);
  EXPECT_EQ(actual.commit_index, state.commit_index);
}

// Verifies snapshot plus compacted journal rebuild committed state on restart.
TEST_F(RaftPersistenceTest, ServiceRebuildsFromCompactedJournal) {
  KVCommand command{KVCommandType::kSet, 7, 9, "city", "shanghai"};
  std::string encoded;
  std::string error;
  ASSERT_TRUE(KVCommandCodec::encode(command, encoded, error)) << error;
  RaftPersistentState state{
      4,
      std::nullopt,
      {
          LogEntry{2, 3, EntryType::kNoOp, ""},
          LogEntry{3, 4, EntryType::kCommand, encoded},
      },
      3,
  };
  FileRaftPersistence persistence(path_);
  ASSERT_TRUE(persistence.save(state, error)) << error;

  StateMachineSnapshot snapshot;
  snapshot.last_included_index = 2;
  snapshot.last_included_term = 3;
  snapshot.entries = {{"name", "tom"}};
  snapshot.sessions = {
      SnapshotSession{9, 1, ApplyStatus::kOk, "OK"}};

  storage::KVStore store;
  NodeConfig config;
  config.node_id = 1;
  config.peers = {2, 3};
  RaftKVService service(config, store, &persistence, &snapshot);
  EXPECT_EQ(service.lastApplied(), 3U);
  EXPECT_EQ(service.getApplied("name"), std::optional<std::string>("tom"));
  EXPECT_EQ(service.getApplied("city"),
            std::optional<std::string>("shanghai"));
}

// Verifies a changed log boundary is persisted as a full image, not a delta.
TEST_F(RaftPersistenceTest, PersistsBoundaryChangeAsFullImage) {
  RaftPersistentState initial{
      3,
      std::nullopt,
      {
          LogEntry{0, 0, EntryType::kNoOp, ""},
          LogEntry{1, 2, EntryType::kNoOp, ""},
          LogEntry{2, 3, EntryType::kCommand, "alpha"},
      },
      2,
  };
  RaftPersistentState compacted{
      3,
      std::nullopt,
      {
          LogEntry{2, 3, EntryType::kNoOp, ""},
      },
      2,
  };
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(initial, error)) << error;
    ASSERT_TRUE(persistence.save(compacted, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.entries, compacted.entries);
  EXPECT_EQ(actual.commit_index, compacted.commit_index);
}

// Verifies cluster configuration and joint configuration round-trip.
TEST_F(RaftPersistenceTest, SavesAndLoadsClusterConfiguration) {
  RaftPersistentState expected = sampleState();
  expected.cluster_config = sampleClusterConfig();
  ClusterConfiguration joint = sampleClusterConfig();
  joint.config_id = 2;
  joint.members.push_back(
      ClusterMember{4, "127.0.0.1", 5004, "127.0.0.1", 6004});
  expected.joint_config = joint;
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  ASSERT_TRUE(actual.cluster_config.has_value());
  ASSERT_TRUE(actual.joint_config.has_value());
  EXPECT_EQ(*actual.cluster_config, *expected.cluster_config);
  EXPECT_EQ(*actual.joint_config, *expected.joint_config);
}

// Verifies corrupted cluster configuration extensions are rejected.
TEST_F(RaftPersistenceTest, RejectsCorruptClusterConfiguration) {
  RaftPersistentState expected = sampleState();
  expected.cluster_config = sampleClusterConfig();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(expected, error)) << error;
  }
  const int fd = ::open(path_.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  const off_t end = ::lseek(fd, 0, SEEK_END);
  ASSERT_GT(end, 0);
  unsigned char byte = 0;
  const off_t corrupt_offset = end - 8;
  ASSERT_EQ(::pread(fd, &byte, 1, corrupt_offset), 1);
  byte ^= 0x01U;
  ASSERT_EQ(::pwrite(fd, &byte, 1, corrupt_offset), 1);
  ASSERT_EQ(::close(fd), 0);

  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  EXPECT_FALSE(recovered.load(actual, error));
  EXPECT_FALSE(error.empty());
}

// Verifies offset deltas preserve durable cluster configuration.
TEST_F(RaftPersistenceTest, RecoversOffsetDeltaPreservesClusterConfiguration) {
  RaftPersistentState state{
      5,
      std::nullopt,
      {
          LogEntry{50, 3, EntryType::kNoOp, ""},
          LogEntry{51, 4, EntryType::kCommand, "old"},
      },
      51,
  };
  state.cluster_config = sampleClusterConfig();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(state, error)) << error;
    state.entries[1] = LogEntry{51, 5, EntryType::kCommand, "new"};
    state.entries.push_back(
        LogEntry{52, 5, EntryType::kConfChange, "conf-change"});
    state.commit_index = 52;
    ASSERT_TRUE(persistence.save(state, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  ASSERT_TRUE(actual.cluster_config.has_value());
  EXPECT_EQ(*actual.cluster_config, *state.cluster_config);
  EXPECT_EQ(actual.entries, state.entries);
  EXPECT_EQ(actual.commit_index, state.commit_index);
}

// Verifies cluster configuration changes persist through full images.
TEST_F(RaftPersistenceTest, PersistsClusterConfigurationChanges) {
  RaftPersistentState initial = sampleState();
  initial.cluster_config = sampleClusterConfig();
  ClusterConfiguration updated = sampleClusterConfig();
  updated.config_id = 2;
  updated.members.push_back(
      ClusterMember{4, "127.0.0.1", 5004, "127.0.0.1", 6004});
  RaftPersistentState next = initial;
  next.cluster_config = updated;
  next.joint_config.reset();
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(initial, error)) << error;
    ASSERT_TRUE(persistence.save(next, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  ASSERT_TRUE(actual.cluster_config.has_value());
  EXPECT_EQ(*actual.cluster_config, updated);
  EXPECT_FALSE(actual.joint_config.has_value());
}

TEST_F(RaftPersistenceTest, PersistsMembershipOperationState) {
  RaftPersistentState state = sampleState();
  state.cluster_config = sampleClusterConfig();
  state.joint_config = sampleClusterConfig();
  state.joint_config->config_id = 2;
  state.active_membership_operation = MembershipOperation{
      7001, ConfChangeType::kAddNode,
      ClusterMember{4, "127.0.0.1", 5004, "127.0.0.1", 6004}};
  state.completed_membership_operation = MembershipOperation{
      7000, ConfChangeType::kRemoveNode,
      ClusterMember{5, {}, 0, {}, 0}};
  {
    FileRaftPersistence persistence(path_);
    std::string error;
    ASSERT_TRUE(persistence.save(state, error)) << error;
  }
  FileRaftPersistence recovered(path_);
  RaftPersistentState actual;
  std::string error;
  ASSERT_TRUE(recovered.load(actual, error)) << error;
  EXPECT_EQ(actual.active_membership_operation,
            state.active_membership_operation);
  EXPECT_EQ(actual.completed_membership_operation,
            state.completed_membership_operation);
}

}  // namespace
}  // namespace distributed_kv::raft
