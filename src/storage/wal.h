#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace distributed_kv::storage {

enum class WalOperation : std::uint8_t {
  kSet = 1,
  kRemove = 2,
};

enum class WalSyncPolicy {
  kAlways,
  kNone,
};

struct WalOptions {
  std::string path;
  WalSyncPolicy sync_policy{WalSyncPolicy::kAlways};
};

struct WalRecord {
  std::uint64_t sequence{0};
  WalOperation operation{WalOperation::kSet};
  std::string key;
  std::string value;
};

/**
 * Append-only, checksummed write-ahead log for state-machine commands.
 *
 * WAL owns and exclusively locks one file. Records are serialized in network
 * byte order and protected by CRC32C. It is independent from KVStore so
 * recovery can validate all bytes before mutating application state.
 */
class WAL final {
 public:
  /**
   * Creates a closed WAL with immutable options.
   *
   * Input: path and synchronization policy.
   * Output: closed WAL object.
   * Thread safety: construction requires exclusive access.
   */
  explicit WAL(WalOptions options);

  /**
   * Releases the file lock and closes the WAL descriptor.
   *
   * Input/output: none beyond owned resources.
   * Thread safety: no method may race with destruction.
   */
  ~WAL();

  WAL(const WAL&) = delete;
  WAL& operator=(const WAL&) = delete;
  WAL(WAL&&) = delete;
  WAL& operator=(WAL&&) = delete;

  /**
   * Opens, exclusively locks, and validates or initializes the WAL file.
   *
   * Input: writable error text.
   * Output: true when the file header is valid and the lock is held.
   * Thread safety: serialized internally; call before recover().
   */
  [[nodiscard]] bool open(std::string& error);

  /**
   * Validates all records and returns them in durable sequence order.
   *
   * Input: writable record vector and error text.
   * Output: true with records replaced; an incomplete final record is
   * truncated, while checksum/format corruption returns false.
   * Thread safety: serialized internally.
   */
  [[nodiscard]] bool recover(std::vector<WalRecord>& records,
                             std::string& error);

  /**
   * Durably appends one SET command according to the configured sync policy.
   *
   * Input: key, value and writable error text.
   * Output: true only after the complete record has been written and, for
   * kAlways, fdatasync has succeeded.
   * Thread safety: serialized internally.
   */
  [[nodiscard]] bool appendSet(const std::string& key,
                               const std::string& value,
                               std::string& error);

  /**
   * Durably appends one REMOVE command according to the sync policy.
   *
   * Input: key and writable error text.
   * Output: true after successful append/synchronization.
   * Thread safety: serialized internally.
   */
  [[nodiscard]] bool appendRemove(const std::string& key,
                                  std::string& error);

  /**
   * Returns the configured filesystem path.
   *
   * Input: none.
   * Output: immutable path reference valid for this object's lifetime.
   * Thread safety: options never change after construction.
   */
  [[nodiscard]] const std::string& path() const noexcept;

 private:
  /**
   * Appends one validated operation while mutex_ is held.
   *
   * Input: operation, key, value and writable error.
   * Output: successful durable append status.
   * Thread safety: caller must hold mutex_.
   */
  [[nodiscard]] bool appendLocked(WalOperation operation,
                                  const std::string& key,
                                  const std::string& value,
                                  std::string& error);

  /**
   * Marks the file unusable after a write/sync failure.
   *
   * Input: failure explanation.
   * Output: failed_ set and error populated.
   * Thread safety: caller must hold mutex_.
   */
  void markFailed(const std::string& message, std::string& error);

  WalOptions options_;
  mutable std::mutex mutex_;
  int file_fd_{-1};
  std::uint64_t next_sequence_{1};
  bool recovered_{false};
  bool failed_{false};
};

}  // namespace distributed_kv::storage
