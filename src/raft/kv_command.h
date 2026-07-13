#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace distributed_kv::raft {

constexpr std::size_t kMaximumCommandKeySize = 4U * 1024U;
constexpr std::size_t kMaximumCommandValueSize = 1024U * 1024U;

enum class KVCommandType : std::uint8_t {
  kSet = 1,
  kDelete = 2,
};

struct KVCommand {
  KVCommandType type{KVCommandType::kSet};
  std::uint64_t client_id{0};
  std::uint64_t request_id{0};
  std::string key;
  std::string value;
};

/**
 * Versioned binary codec for opaque KV commands stored inside Raft entries.
 */
class KVCommandCodec final {
 public:
  KVCommandCodec() = delete;

  /**
   * Encodes one validated SET or DELETE command.
   *
   * Input: command and writable output/error.
   * Output: true with complete deterministic bytes, or false on validation.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool encode(const KVCommand& command,
                                   std::string& output,
                                   std::string& error);

  /**
   * Decodes and validates one complete command payload.
   *
   * Input: exact encoded bytes and writable command/error.
   * Output: true with command populated, or false for malformed input.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool decode(const std::string& encoded,
                                   KVCommand& command,
                                   std::string& error);
};

}  // namespace distributed_kv::raft
