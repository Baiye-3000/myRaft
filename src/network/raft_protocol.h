#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "raft/types.h"

namespace distributed_kv::network {

struct RaftMessage {
  raft::NodeId source{0};
  raft::RpcPayload payload;
};

enum class RaftDecodeStatus {
  kComplete,
  kNeedMoreData,
  kError,
};

/**
 * Versioned binary protocol for node-to-node Raft RPC traffic.
 */
class RaftProtocol final {
 public:
  static constexpr std::size_t kMaximumEntriesPerFrame = 256;
  static constexpr std::size_t kMaximumCommandSize = 1024U * 1024U;
  static constexpr std::size_t kMaximumSnapshotChunkSize = 1024U * 1024U;
  static constexpr std::size_t kMaximumFrameSize = 2U * 1024U * 1024U;

  /**
   * Encodes one source-authenticated RPC frame.
   *
   * Input: non-zero source and typed payload. Output: complete frame or error.
   * Thread safety: stateless.
   */
  [[nodiscard]] static bool encode(raft::NodeId source,
                                   const raft::RpcPayload& payload,
                                   std::vector<std::uint8_t>& frame,
                                   std::string& error);

  /**
   * Decodes and consumes at most one complete frame.
   *
   * Input: mutable stream buffer. Output: message, need-more-data, or error.
   * Thread safety: caller owns the buffer and result.
   */
  [[nodiscard]] static RaftDecodeStatus tryDecode(
      std::vector<std::uint8_t>& buffer, RaftMessage& message,
      std::string& error);
};

}  // namespace distributed_kv::network
