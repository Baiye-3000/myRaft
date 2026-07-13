#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "raft/types.h"

namespace distributed_kv::storage {
class KVStore;
}

namespace distributed_kv::raft {

enum class ApplyStatus {
  kOk,
  kNotFound,
  kStaleRequest,
};

struct ApplyResult {
  LogIndex log_index{0};
  std::uint64_t client_id{0};
  std::uint64_t request_id{0};
  ApplyStatus status{ApplyStatus::kOk};
  std::string payload;
  bool duplicate{false};
};

struct SnapshotSession {
  std::uint64_t client_id{0};
  std::uint64_t request_id{0};
  ApplyStatus status{ApplyStatus::kOk};
  std::string payload;
};

struct StateMachineSnapshot {
  LogIndex last_included_index{0};
  Term last_included_term{0};
  std::vector<std::pair<std::string, std::string>> entries;
  std::vector<SnapshotSession> sessions;
};

/**
 * Applies committed Raft entries to KVStore in strict index order.
 *
 * StateMachine owns the client-session deduplication table but not KVStore.
 * One application thread must serialize apply() and reads.
 */
class StateMachine final {
 public:
  /**
   * Creates an unapplied state machine over a store that outlives it.
   *
   * Input: non-owning KVStore reference.
   * Output: state machine with lastApplied zero.
   * Thread safety: construction requires exclusive access.
   */
  explicit StateMachine(storage::KVStore& store);

  /**
   * Applies exactly the next committed log entry.
   *
   * Input: entry at lastApplied()+1 and writable error.
   * Output: command result, nullopt for no-op, or nullopt plus error on fatal
   * ordering/codec failure.
   * Thread safety: owning application thread only.
   */
  [[nodiscard]] std::optional<ApplyResult> apply(
      const LogEntry& entry, std::string& error);

  /**
   * Reads an already-applied value directly from KVStore.
   *
   * Input: exact key.
   * Output: copied value or std::nullopt.
   * Thread safety: safe with KVStore operations; linearizability requires an
   * external Raft read barrier.
   */
  [[nodiscard]] std::optional<std::string> get(
      const std::string& key) const;

  /**
   * Returns the highest applied log index.
   *
   * Input: none.
   * Output: last applied index.
   * Thread safety: owning application thread only.
   */
  [[nodiscard]] LogIndex lastApplied() const noexcept;

  /**
   * Captures KV data and deduplication sessions at lastApplied.
   *
   * Input: term corresponding to lastApplied. Output: deterministic image.
   * Thread safety: owning application thread; KVStore handles readers.
   */
  [[nodiscard]] StateMachineSnapshot snapshot(
      Term last_included_term) const;

  /**
   * Atomically restores KV data, sessions, and applied index.
   *
   * Input: validated decoded image and writable error. Output: true on
   * complete replacement. Thread safety: owning application thread only.
   */
  [[nodiscard]] bool restore(const StateMachineSnapshot& snapshot,
                             std::string& error);

 private:
  struct CachedResponse {
    std::uint64_t request_id{0};
    ApplyStatus status{ApplyStatus::kOk};
    std::string payload;
  };

  storage::KVStore& store_;
  LogIndex last_applied_{0};
  std::unordered_map<std::uint64_t, CachedResponse> sessions_;
};

}  // namespace distributed_kv::raft
