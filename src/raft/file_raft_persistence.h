#pragma once

#include <cstddef>
#include <mutex>
#include <optional>
#include <string>

#include "raft/raft_persistence.h"

namespace distributed_kv::raft {

struct FileRaftPersistenceOptions {
  std::string path;
  std::size_t maximum_delta_records{1024};
  std::size_t maximum_file_bytes{64U * 1024U * 1024U};
};

/**
 * Append-only, checksummed persistent-state journal for one Raft node.
 *
 * The first durable record contains a complete state image. Later records
 * contain metadata plus a changed log suffix. Recovery replays valid records
 * and truncates only an incomplete final record.
 */
class FileRaftPersistence final : public RaftPersistence {
 public:
  /**
   * Creates a closed journal for path.
   *
   * Input: non-empty file path. Output: lazy-open persistence object.
   * Thread safety: construction requires exclusive access.
   */
  explicit FileRaftPersistence(std::string path);
  explicit FileRaftPersistence(FileRaftPersistenceOptions options);
  ~FileRaftPersistence() override;

  FileRaftPersistence(const FileRaftPersistence&) = delete;
  FileRaftPersistence& operator=(const FileRaftPersistence&) = delete;

  /**
   * Loads the last complete state and repairs an incomplete tail.
   *
   * Input: writable state and error. Output: true on valid recovery.
   * Thread safety: serialized internally.
   */
  [[nodiscard]] bool load(RaftPersistentState& state,
                          std::string& error) override;

  /**
   * Appends and synchronizes a full image or incremental changed suffix.
   *
   * Input: validated state and writable error. Output: true after fdatasync.
   * Thread safety: serialized internally.
   */
  [[nodiscard]] bool save(const RaftPersistentState& state,
                          std::string& error) override;

 private:
  [[nodiscard]] bool openLocked(std::string& error);
  [[nodiscard]] bool compactLocked(const RaftPersistentState& state,
                                   std::string& error);

  FileRaftPersistenceOptions options_;
  int file_fd_{-1};
  std::mutex mutex_;
  std::optional<RaftPersistentState> last_state_;
  std::size_t delta_record_count_{0};
  std::size_t file_size_{0};
};

}  // namespace distributed_kv::raft
