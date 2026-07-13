#include "storage/kv_store.h"

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::storage {
namespace {

// Phase 1 regression coverage remains active in every later phase.
// Verifies that put inserts a new value and get returns an owned copy.
TEST(KVStoreTest, PutAndGetValue) {
  KVStore store;

  EXPECT_TRUE(store.put("name", "tom"));
  const auto value = store.get("name");

  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "tom");
  EXPECT_EQ(store.size(), 1U);
}

// Verifies that putting an existing key replaces, rather than duplicates, it.
TEST(KVStoreTest, PutReplacesExistingValue) {
  KVStore store;

  EXPECT_TRUE(store.put("name", "tom"));
  EXPECT_FALSE(store.put("name", "jerry"));

  EXPECT_EQ(store.get("name"), std::optional<std::string>("jerry"));
  EXPECT_EQ(store.size(), 1U);
}

// Verifies that an absent key is represented without a sentinel value.
TEST(KVStoreTest, GetMissingKeyReturnsNullopt) {
  KVStore store;

  EXPECT_EQ(store.get("missing"), std::nullopt);
}

// Verifies both successful removal and idempotent removal of an absent key.
TEST(KVStoreTest, RemoveReportsWhetherKeyExisted) {
  KVStore store;
  ASSERT_TRUE(store.put("name", "tom"));

  EXPECT_TRUE(store.remove("name"));
  EXPECT_FALSE(store.remove("name"));
  EXPECT_EQ(store.get("name"), std::nullopt);
  EXPECT_EQ(store.size(), 0U);
}

// Verifies that storage remains binary-safe with empty strings at this layer.
TEST(KVStoreTest, SupportsEmptyKeyAndValue) {
  KVStore store;

  EXPECT_TRUE(store.put("", ""));
  EXPECT_EQ(store.get(""), std::optional<std::string>(""));
}

// Verifies concurrent public operations on disjoint keys without data loss.
TEST(KVStoreTest, SupportsConcurrentReadersAndWriters) {
  constexpr std::size_t kThreadCount = 8;
  constexpr std::size_t kEntriesPerThread = 500;

  KVStore store;
  std::atomic<bool> observed_mismatch{false};
  std::vector<std::thread> workers;
  workers.reserve(kThreadCount);

  for (std::size_t thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    workers.emplace_back([thread_id, &store, &observed_mismatch] {
      for (std::size_t entry_id = 0; entry_id < kEntriesPerThread;
           ++entry_id) {
        const std::string key =
            std::to_string(thread_id) + ":" + std::to_string(entry_id);
        const std::string value = "value-" + std::to_string(entry_id);
        if (!store.put(key, value) || store.get(key) != value) {
          observed_mismatch.store(true, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& worker : workers) {
    worker.join();
  }

  EXPECT_FALSE(observed_mismatch.load(std::memory_order_relaxed));
  EXPECT_EQ(store.size(), kThreadCount * kEntriesPerThread);
}

}  // namespace
}  // namespace distributed_kv::storage
