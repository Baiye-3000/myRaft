#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "raft/state_machine.h"

namespace distributed_kv::raft {

/**
 * Checksummed, atomically published StateMachine snapshot file.
 *
 * A save is durable only after temporary-file fdatasync, rename, and parent
 * directory fsync. The caller must serialize load and save operations.
 */
class FileSnapshotStore final {
 public:
  explicit FileSnapshotStore(std::string path);

  [[nodiscard]] bool save(const StateMachineSnapshot& snapshot,
                          std::string& error) const;

  /**
   * Loads the current snapshot, returning nullopt when no file exists.
   */
  [[nodiscard]] bool load(std::optional<StateMachineSnapshot>& snapshot,
                          std::string& error) const;

  /**
   * Returns the published snapshot file size, or false when absent.
   */
  [[nodiscard]] bool fileSize(std::uint64_t& size,
                              std::string& error) const;

  /**
   * Reads one byte range from the published snapshot file.
   *
   * Input: byte offset and maximum bytes. Output: chunk and eof when the
   * offset reaches the file end.
   */
  [[nodiscard]] bool readBytes(std::uint64_t offset, std::size_t max_count,
                               std::string& chunk, bool& eof,
                               std::string& error) const;

  [[nodiscard]] const std::string& path() const noexcept { return path_; }

 private:
  std::string path_;
};

/**
 * Assembles one incoming InstallSnapshot stream into a durable snapshot file.
 */
class SnapshotFileReceiver final {
 public:
  explicit SnapshotFileReceiver(std::string destination_path);

  void cancel() noexcept;

  [[nodiscard]] bool active() const noexcept;

  /**
   * Appends one chunk at the expected sequential offset.
   *
   * Input: snapshot boundary, byte offset, payload, and terminal flag.
   * Output: true after the chunk is accepted; false leaves prior bytes intact.
   */
  [[nodiscard]] bool appendChunk(LogIndex last_included_index,
                                 Term last_included_term,
                                 std::uint64_t offset,
                                 const std::string& data, bool done,
                                 std::string& error);

  /**
   * Validates, publishes, and decodes the completed snapshot image.
   */
  [[nodiscard]] bool finishAndLoad(
      std::optional<StateMachineSnapshot>& snapshot, std::string& error);

 private:
  [[nodiscard]] bool begin(LogIndex last_included_index,
                           Term last_included_term, std::string& error);

  std::string destination_path_;
  std::string temporary_path_;
  LogIndex last_included_index_{0};
  Term last_included_term_{0};
  std::uint64_t received_size_{0};
  bool active_{false};
};

}  // namespace distributed_kv::raft
