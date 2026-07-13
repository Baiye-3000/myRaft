#pragma once

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace distributed_kv::storage {

/**
 * Thread-safe in-memory key-value store.
 *
 * KVStore stores only state-machine data that has already been committed. It
 * deliberately has no persistence behavior; WAL and snapshot responsibilities
 * belong to separate storage components.
 */
class KVStore final {
 public:
  /**
   * Creates an empty store.
   *
   * Output: an initialized store with size zero.
   * Thread safety: construction requires exclusive access to the new object.
   */
  KVStore() = default;

  /**
   * Releases all in-memory entries.
   *
   * Output: none.
   * Thread safety: the caller must ensure no operation is running concurrently
   * with destruction.
   */
  ~KVStore() = default;

  KVStore(const KVStore&) = delete;
  KVStore& operator=(const KVStore&) = delete;
  KVStore(KVStore&&) = delete;
  KVStore& operator=(KVStore&&) = delete;

  /**
   * Inserts a key/value pair or replaces the value of an existing key.
   *
   * Input: key and value are owned strings; empty strings are valid.
   * Output: true when a new key is inserted, false when an existing key is
   * replaced.
   * Thread safety: safe to call concurrently with every public operation.
   */
  [[nodiscard]] bool put(std::string key, std::string value);

  /**
   * Looks up the current value associated with a key.
   *
   * Input: key is the exact, case-sensitive key to search for.
   * Output: a copied value when present, or std::nullopt when absent.
   * Thread safety: safe to call concurrently with every public operation.
   */
  [[nodiscard]] std::optional<std::string> get(
      const std::string& key) const;

  /**
   * Removes a key and its value from the store.
   *
   * Input: key is the exact, case-sensitive key to remove.
   * Output: true when an entry was removed, false when the key was absent.
   * Thread safety: safe to call concurrently with every public operation.
   */
  [[nodiscard]] bool remove(const std::string& key);

  /**
   * Returns the number of entries currently held by the store.
   *
   * Input: none.
   * Output: a point-in-time entry count.
   * Thread safety: safe to call concurrently with every public operation.
   */
  [[nodiscard]] std::size_t size() const;

  /**
   * Copies all entries in deterministic key order for snapshot creation.
   *
   * Input: none. Output: stable key/value vector.
   * Thread safety: safe with concurrent public operations.
   */
  [[nodiscard]] std::vector<std::pair<std::string, std::string>>
  snapshotEntries() const;

  /**
   * Atomically replaces all entries from a validated snapshot.
   *
   * Input: key/value vector and writable error. Output: false on duplicates.
   * Thread safety: excludes concurrent mutations while swapping the map.
   */
  [[nodiscard]] bool replaceAll(
      const std::vector<std::pair<std::string, std::string>>& entries,
      std::string& error);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::string> entries_;
};

}  // namespace distributed_kv::storage
