#pragma once

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

 private:
  std::string path_;
};

}  // namespace distributed_kv::raft
