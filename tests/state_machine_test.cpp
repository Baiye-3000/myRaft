#include "raft/kv_command.h"
#include "raft/state_machine.h"
#include "storage/kv_store.h"

#include <optional>
#include <string>
#include <utility>

#include <gtest/gtest.h>

namespace distributed_kv::raft {
namespace {

/**
 * Encodes a command for state-machine tests.
 *
 * Input: command fields.
 * Output: valid binary command or an empty string after test failure.
 * Thread safety: test thread only.
 */
std::string encodeCommand(KVCommandType type, std::uint64_t client_id,
                          std::uint64_t request_id, std::string key,
                          std::string value = "") {
  std::string encoded;
  std::string error;
  EXPECT_TRUE(KVCommandCodec::encode(
      KVCommand{type, client_id, request_id, std::move(key),
                std::move(value)},
      encoded, error))
      << error;
  return encoded;
}

// Verifies the deterministic command codec preserves all binary fields.
TEST(KVCommandTest, RoundTripsSetCommand) {
  const KVCommand original{
      KVCommandType::kSet, 7, 11, "name", "tom",
  };
  std::string encoded;
  std::string error;
  ASSERT_TRUE(KVCommandCodec::encode(original, encoded, error)) << error;

  KVCommand decoded;
  ASSERT_TRUE(KVCommandCodec::decode(encoded, decoded, error)) << error;
  EXPECT_EQ(decoded.type, original.type);
  EXPECT_EQ(decoded.client_id, original.client_id);
  EXPECT_EQ(decoded.request_id, original.request_id);
  EXPECT_EQ(decoded.key, original.key);
  EXPECT_EQ(decoded.value, original.value);
}

// Verifies DELETE rejects a value before entering the replicated log.
TEST(KVCommandTest, RejectsDeleteValue) {
  std::string encoded;
  std::string error;
  EXPECT_FALSE(KVCommandCodec::encode(
      KVCommand{KVCommandType::kDelete, 1, 1, "name", "unexpected"},
      encoded, error));
}

// Verifies no-op entries advance application order without changing KV data.
TEST(StateMachineTest, AppliesNoOp) {
  storage::KVStore store;
  StateMachine state_machine(store);
  std::string error;

  EXPECT_EQ(state_machine.apply(
                LogEntry{1, 1, EntryType::kNoOp, ""}, error),
            std::nullopt);
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(state_machine.lastApplied(), 1U);
  EXPECT_EQ(store.size(), 0U);
}

// Verifies SET and DELETE mutate KVStore only when explicitly applied.
TEST(StateMachineTest, AppliesSetAndDelete) {
  storage::KVStore store;
  StateMachine state_machine(store);
  std::string error;

  const auto set_result = state_machine.apply(
      LogEntry{1, 1, EntryType::kCommand,
               encodeCommand(KVCommandType::kSet, 10, 1, "name", "tom")},
      error);
  ASSERT_TRUE(set_result.has_value()) << error;
  EXPECT_EQ(store.get("name"), std::optional<std::string>("tom"));

  const auto delete_result = state_machine.apply(
      LogEntry{2, 1, EntryType::kCommand,
               encodeCommand(KVCommandType::kDelete, 10, 2, "name")},
      error);
  ASSERT_TRUE(delete_result.has_value()) << error;
  EXPECT_EQ(delete_result->status, ApplyStatus::kOk);
  EXPECT_EQ(store.get("name"), std::nullopt);
}

// Verifies an exact client retry returns the cached result without reapplying.
TEST(StateMachineTest, DeduplicatesRepeatedRequest) {
  storage::KVStore store;
  StateMachine state_machine(store);
  const std::string command =
      encodeCommand(KVCommandType::kDelete, 5, 1, "missing");
  std::string error;

  const auto first = state_machine.apply(
      LogEntry{1, 1, EntryType::kCommand, command}, error);
  const auto duplicate = state_machine.apply(
      LogEntry{2, 2, EntryType::kCommand, command}, error);

  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(duplicate.has_value());
  EXPECT_EQ(first->status, ApplyStatus::kNotFound);
  EXPECT_EQ(duplicate->status, ApplyStatus::kNotFound);
  EXPECT_TRUE(duplicate->duplicate);
  EXPECT_EQ(state_machine.lastApplied(), 2U);
}

// Verifies older request ids cannot overwrite newer client state.
TEST(StateMachineTest, RejectsStaleClientRequest) {
  storage::KVStore store;
  StateMachine state_machine(store);
  std::string error;

  ASSERT_TRUE(state_machine.apply(
      LogEntry{1, 1, EntryType::kCommand,
               encodeCommand(KVCommandType::kSet, 8, 2, "name", "new")},
      error));
  const auto stale = state_machine.apply(
      LogEntry{2, 1, EntryType::kCommand,
               encodeCommand(KVCommandType::kSet, 8, 1, "name", "old")},
      error);

  ASSERT_TRUE(stale.has_value());
  EXPECT_EQ(stale->status, ApplyStatus::kStaleRequest);
  EXPECT_EQ(store.get("name"), std::optional<std::string>("new"));
}

// Verifies gaps never partially advance state-machine application.
TEST(StateMachineTest, RejectsOutOfOrderEntry) {
  storage::KVStore store;
  StateMachine state_machine(store);
  std::string error;

  EXPECT_EQ(state_machine.apply(
                LogEntry{2, 1, EntryType::kNoOp, ""}, error),
            std::nullopt);
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(state_machine.lastApplied(), 0U);
}

// Verifies snapshots preserve KV data and exactly-once client responses.
TEST(StateMachineTest, SnapshotsAndRestoresDataAndSessions) {
  storage::KVStore source_store;
  StateMachine source(source_store);
  std::string error;
  ASSERT_TRUE(source.apply(
      LogEntry{1, 3, EntryType::kCommand,
               encodeCommand(KVCommandType::kSet, 9, 1, "name", "tom")},
      error));
  const StateMachineSnapshot image = source.snapshot(3);

  storage::KVStore restored_store;
  ASSERT_TRUE(restored_store.put("obsolete", "value"));
  StateMachine restored(restored_store);
  ASSERT_TRUE(restored.restore(image, error)) << error;
  EXPECT_EQ(restored.lastApplied(), 1U);
  EXPECT_EQ(restored.get("name"), std::optional<std::string>("tom"));
  EXPECT_EQ(restored.get("obsolete"), std::nullopt);

  const auto duplicate = restored.apply(
      LogEntry{2, 4, EntryType::kCommand,
               encodeCommand(KVCommandType::kSet, 9, 1, "name", "changed")},
      error);
  ASSERT_TRUE(duplicate.has_value()) << error;
  EXPECT_TRUE(duplicate->duplicate);
  EXPECT_EQ(restored.get("name"), std::optional<std::string>("tom"));
}

// Verifies malformed metadata cannot partially replace live state.
TEST(StateMachineTest, RejectsInvalidSnapshotWithoutMutation) {
  storage::KVStore store;
  ASSERT_TRUE(store.put("live", "value"));
  StateMachine state_machine(store);
  StateMachineSnapshot image;
  image.last_included_index = 10;
  image.last_included_term = 0;
  image.entries.emplace_back("new", "value");
  std::string error;

  EXPECT_FALSE(state_machine.restore(image, error));
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(store.get("live"), std::optional<std::string>("value"));
  EXPECT_EQ(store.get("new"), std::nullopt);
  EXPECT_EQ(state_machine.lastApplied(), 0U);
}

}  // namespace
}  // namespace distributed_kv::raft
