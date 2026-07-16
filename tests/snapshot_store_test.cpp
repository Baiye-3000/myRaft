#include "raft/file_snapshot_store.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <optional>
#include <string>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

class SnapshotStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-snapshot-XXXXXX";
    ASSERT_LT(prefix.size(), directory_template.size());
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    directory_ = created;
    path_ = directory_ + "/state.snapshot";
  }

  void TearDown() override {
    static_cast<void>(::unlink(path_.c_str()));
    static_cast<void>(::unlink((path_ + ".tmp").c_str()));
    static_cast<void>(::rmdir(directory_.c_str()));
  }

  std::string directory_;
  std::string path_;
};

StateMachineSnapshot sampleSnapshot() {
  StateMachineSnapshot snapshot;
  snapshot.last_included_index = 12;
  snapshot.last_included_term = 4;
  snapshot.entries = {{"binary\0key", "value\0bytes"},
                      {"name", "tom"}};
  snapshot.entries[0].first.resize(10);
  snapshot.entries[0].second.resize(11);
  snapshot.sessions = {
      SnapshotSession{7, 9, ApplyStatus::kOk, "OK"},
      SnapshotSession{8, 3, ApplyStatus::kNotFound, "NOT_FOUND"},
  };
  return snapshot;
}

void expectEqual(const StateMachineSnapshot& actual,
                 const StateMachineSnapshot& expected) {
  EXPECT_EQ(actual.last_included_index, expected.last_included_index);
  EXPECT_EQ(actual.last_included_term, expected.last_included_term);
  EXPECT_EQ(actual.entries, expected.entries);
  ASSERT_EQ(actual.sessions.size(), expected.sessions.size());
  for (std::size_t index = 0; index < actual.sessions.size(); ++index) {
    EXPECT_EQ(actual.sessions[index].client_id,
              expected.sessions[index].client_id);
    EXPECT_EQ(actual.sessions[index].request_id,
              expected.sessions[index].request_id);
    EXPECT_EQ(actual.sessions[index].status, expected.sessions[index].status);
    EXPECT_EQ(actual.sessions[index].payload,
              expected.sessions[index].payload);
  }
}

TEST_F(SnapshotStoreTest, MissingSnapshotLoadsAsEmpty) {
  FileSnapshotStore store(path_);
  std::optional<StateMachineSnapshot> loaded = sampleSnapshot();
  std::string error;
  ASSERT_TRUE(store.load(loaded, error)) << error;
  EXPECT_FALSE(loaded.has_value());
}

TEST_F(SnapshotStoreTest, SavesAndLoadsBinarySnapshot) {
  const StateMachineSnapshot expected = sampleSnapshot();
  FileSnapshotStore store(path_);
  std::string error;
  ASSERT_TRUE(store.save(expected, error)) << error;

  std::optional<StateMachineSnapshot> actual;
  ASSERT_TRUE(store.load(actual, error)) << error;
  ASSERT_TRUE(actual.has_value());
  expectEqual(*actual, expected);
  EXPECT_EQ(::access((path_ + ".tmp").c_str(), F_OK), -1);
}

TEST_F(SnapshotStoreTest, AtomicallyReplacesPreviousSnapshot) {
  FileSnapshotStore store(path_);
  StateMachineSnapshot expected = sampleSnapshot();
  std::string error;
  ASSERT_TRUE(store.save(expected, error)) << error;
  expected.last_included_index = 20;
  expected.last_included_term = 6;
  expected.entries = {{"new", "state"}};
  ASSERT_TRUE(store.save(expected, error)) << error;

  std::optional<StateMachineSnapshot> actual;
  ASSERT_TRUE(store.load(actual, error)) << error;
  ASSERT_TRUE(actual.has_value());
  expectEqual(*actual, expected);
}

TEST_F(SnapshotStoreTest, RejectsChecksumCorruption) {
  FileSnapshotStore store(path_);
  std::string error;
  ASSERT_TRUE(store.save(sampleSnapshot(), error)) << error;
  const int fd = ::open(path_.c_str(), O_RDWR | O_CLOEXEC);
  ASSERT_GE(fd, 0);
  std::array<unsigned char, 1> byte{};
  ASSERT_EQ(::pread(fd, byte.data(), byte.size(), 24), 1);
  byte[0] ^= 0xffU;
  ASSERT_EQ(::pwrite(fd, byte.data(), byte.size(), 24), 1);
  ASSERT_EQ(::close(fd), 0);

  std::optional<StateMachineSnapshot> actual;
  EXPECT_FALSE(store.load(actual, error));
  EXPECT_NE(error.find("checksum"), std::string::npos);
}

TEST_F(SnapshotStoreTest, RejectsDuplicateStateBeforePublication) {
  StateMachineSnapshot invalid = sampleSnapshot();
  invalid.entries.push_back(invalid.entries.front());
  FileSnapshotStore store(path_);
  std::string error;
  EXPECT_FALSE(store.save(invalid, error));
  EXPECT_EQ(::access(path_.c_str(), F_OK), -1);
}

TEST_F(SnapshotStoreTest, ReadsSnapshotFileInChunks) {
  const StateMachineSnapshot expected = sampleSnapshot();
  FileSnapshotStore store(path_);
  std::string error;
  ASSERT_TRUE(store.save(expected, error)) << error;

  std::uint64_t size = 0;
  ASSERT_TRUE(store.fileSize(size, error)) << error;
  ASSERT_GT(size, 0U);

  std::string first;
  bool eof = false;
  ASSERT_TRUE(store.readBytes(0, 8, first, eof, error)) << error;
  EXPECT_FALSE(eof);
  EXPECT_EQ(first.size(), 8U);

  std::string second;
  ASSERT_TRUE(store.readBytes(first.size(), size, second, eof, error))
      << error;
  EXPECT_TRUE(eof);
  EXPECT_EQ(first.size() + second.size(), size);
}

TEST_F(SnapshotStoreTest, ReceiverPublishesChunkedSnapshot) {
  const StateMachineSnapshot expected = sampleSnapshot();
  FileSnapshotStore store(path_);
  std::string error;
  ASSERT_TRUE(store.save(expected, error)) << error;

  std::uint64_t size = 0;
  ASSERT_TRUE(store.fileSize(size, error)) << error;
  std::string bytes;
  bool eof = false;
  ASSERT_TRUE(store.readBytes(0, size, bytes, eof, error)) << error;
  ASSERT_TRUE(eof);

  SnapshotFileReceiver receiver(path_);
  const std::size_t split = bytes.size() / 2U;
  ASSERT_TRUE(receiver.appendChunk(
      expected.last_included_index, expected.last_included_term, 0,
      bytes.substr(0, split), false, error))
      << error;
  ASSERT_TRUE(receiver.appendChunk(
      expected.last_included_index, expected.last_included_term, split,
      bytes.substr(split), true, error))
      << error;

  std::optional<StateMachineSnapshot> loaded;
  ASSERT_TRUE(receiver.finishAndLoad(loaded, error)) << error;
  ASSERT_TRUE(loaded.has_value());
  expectEqual(*loaded, expected);
}

}  // namespace
}  // namespace distributed_kv::raft
