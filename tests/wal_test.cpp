#include "storage/wal.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace distributed_kv::storage {
namespace {

class WALTest : public ::testing::Test {
 protected:
  // Creates a unique directory and WAL path for one isolated test.
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-wal-XXXXXX";
    ASSERT_LT(prefix.size(), directory_template.size());
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());

    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    directory_ = created;
    wal_path_ = directory_ + "/storage.wal";
  }

  // Removes test files after all WAL handles have left their local scopes.
  void TearDown() override {
    ::unlink(wal_path_.c_str());
    ::rmdir(directory_.c_str());
  }

  /**
   * Opens and recovers a WAL using the requested synchronization policy.
   *
   * Input: sync policy.
   * Output: healthy WAL with an empty or recovered sequence.
   * Thread safety: fixture thread only.
   */
  std::unique_ptr<WAL> openWal(
      WalSyncPolicy sync_policy = WalSyncPolicy::kAlways) {
    auto wal = std::make_unique<WAL>(WalOptions{wal_path_, sync_policy});
    std::string error;
    EXPECT_TRUE(wal->open(error)) << error;
    std::vector<WalRecord> ignored_records;
    EXPECT_TRUE(wal->recover(ignored_records, error)) << error;
    return wal;
  }

  std::string directory_;
  std::string wal_path_;
};

// Verifies SET/REMOVE records survive close and preserve sequence ordering.
TEST_F(WALTest, AppendsAndRecoversRecords) {
  {
    auto wal = openWal();
    std::string error;
    ASSERT_TRUE(wal->appendSet("name", "tom", error)) << error;
    ASSERT_TRUE(wal->appendSet("city", "shanghai", error)) << error;
    ASSERT_TRUE(wal->appendRemove("name", error)) << error;
  }

  WAL recovered_wal(WalOptions{wal_path_, WalSyncPolicy::kAlways});
  std::string error;
  ASSERT_TRUE(recovered_wal.open(error)) << error;
  std::vector<WalRecord> records;
  ASSERT_TRUE(recovered_wal.recover(records, error)) << error;

  ASSERT_EQ(records.size(), 3U);
  EXPECT_EQ(records[0].sequence, 1U);
  EXPECT_EQ(records[0].operation, WalOperation::kSet);
  EXPECT_EQ(records[0].key, "name");
  EXPECT_EQ(records[0].value, "tom");
  EXPECT_EQ(records[2].sequence, 3U);
  EXPECT_EQ(records[2].operation, WalOperation::kRemove);
  EXPECT_EQ(records[2].key, "name");
  EXPECT_TRUE(records[2].value.empty());
}

// Verifies a torn final record is removed without losing earlier records.
TEST_F(WALTest, TruncatesIncompleteTailDuringRecovery) {
  {
    auto wal = openWal();
    std::string error;
    ASSERT_TRUE(wal->appendSet("name", "tom", error)) << error;
  }

  struct stat clean_status {};
  ASSERT_EQ(::stat(wal_path_.c_str(), &clean_status), 0);
  const int descriptor =
      ::open(wal_path_.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
  ASSERT_GE(descriptor, 0);
  const std::array<std::uint8_t, 5> torn_tail{{0xd1, 0x4b, 0x56, 0x52, 0}};
  ASSERT_EQ(::write(descriptor, torn_tail.data(), torn_tail.size()),
            static_cast<ssize_t>(torn_tail.size()));
  ASSERT_EQ(::close(descriptor), 0);

  WAL recovered_wal(WalOptions{wal_path_, WalSyncPolicy::kAlways});
  std::string error;
  ASSERT_TRUE(recovered_wal.open(error)) << error;
  std::vector<WalRecord> records;
  ASSERT_TRUE(recovered_wal.recover(records, error)) << error;
  ASSERT_EQ(records.size(), 1U);

  struct stat recovered_status {};
  ASSERT_EQ(::stat(wal_path_.c_str(), &recovered_status), 0);
  EXPECT_EQ(recovered_status.st_size, clean_status.st_size);
}

// Verifies complete records with invalid checksums are never silently applied.
TEST_F(WALTest, RejectsChecksumCorruption) {
  {
    auto wal = openWal();
    std::string error;
    ASSERT_TRUE(wal->appendSet("name", "tom", error)) << error;
  }

  const int descriptor = ::open(wal_path_.c_str(), O_RDWR | O_CLOEXEC);
  ASSERT_GE(descriptor, 0);
  constexpr off_t kFirstKeyByteOffset = 16 + 12 + 20;
  const std::uint8_t corrupted = 'N';
  ASSERT_EQ(::pwrite(descriptor, &corrupted, sizeof(corrupted),
                     kFirstKeyByteOffset),
            static_cast<ssize_t>(sizeof(corrupted)));
  ASSERT_EQ(::close(descriptor), 0);

  WAL recovered_wal(WalOptions{wal_path_, WalSyncPolicy::kAlways});
  std::string error;
  ASSERT_TRUE(recovered_wal.open(error)) << error;
  std::vector<WalRecord> records;
  EXPECT_FALSE(recovered_wal.recover(records, error));
  EXPECT_EQ(error, "WAL record checksum mismatch");
}

// Verifies the process-level file lock prevents a second writer.
TEST_F(WALTest, RejectsConcurrentWriter) {
  auto first = openWal();
  WAL second(WalOptions{wal_path_, WalSyncPolicy::kAlways});
  std::string error;

  EXPECT_FALSE(second.open(error));
  EXPECT_NE(error.find("failed to lock WAL"), std::string::npos);
}

// Verifies append is prohibited until existing records have been recovered.
TEST_F(WALTest, RequiresRecoveryBeforeAppend) {
  WAL wal(WalOptions{wal_path_, WalSyncPolicy::kAlways});
  std::string error;
  ASSERT_TRUE(wal.open(error)) << error;

  EXPECT_FALSE(wal.appendSet("name", "tom", error));
  EXPECT_EQ(error, "WAL must be open, recovered, and healthy before append");
}

}  // namespace
}  // namespace distributed_kv::storage
