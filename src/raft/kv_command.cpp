#include "raft/kv_command.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace distributed_kv::raft {
namespace {

constexpr std::array<char, 4> kCommandMagic{{'K', 'V', 'C', '1'}};
constexpr std::uint8_t kCommandVersion = 1;
constexpr std::size_t kCommandHeaderSize = 32;

// Input: output string and host-order integer. Output: appended network-order
// bytes. Thread safety: safe for distinct output strings.
void appendUint32(std::string& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(static_cast<char>(
        (value >> static_cast<unsigned>(shift)) & 0xffU));
  }
}

// Input: output string and host-order integer. Output: appended network-order
// bytes. Thread safety: safe for distinct output strings.
void appendUint64(std::string& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<char>(
        (value >> static_cast<unsigned>(shift)) & 0xffU));
  }
}

// Input: at least four readable bytes. Output: host-order integer.
// Thread safety: stateless.
std::uint32_t readUint32(const char* input) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = static_cast<std::uint32_t>(
        (value << 8U) |
        static_cast<std::uint8_t>(input[index]));
  }
  return value;
}

// Input: at least eight readable bytes. Output: host-order integer.
// Thread safety: stateless.
std::uint64_t readUint64(const char* input) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value =
        (value << 8U) | static_cast<std::uint8_t>(input[index]);
  }
  return value;
}

}  // namespace

bool KVCommandCodec::encode(const KVCommand& command, std::string& output,
                            std::string& error) {
  if ((command.type != KVCommandType::kSet &&
       command.type != KVCommandType::kDelete) ||
      command.client_id == 0 || command.request_id == 0) {
    error = "invalid KV command metadata";
    return false;
  }
  if (command.key.size() > kMaximumCommandKeySize ||
      command.value.size() > kMaximumCommandValueSize ||
      (command.type == KVCommandType::kDelete &&
       !command.value.empty()) ||
      command.key.size() > std::numeric_limits<std::uint32_t>::max() ||
      command.value.size() > std::numeric_limits<std::uint32_t>::max()) {
    error = "KV command fields exceed limits";
    return false;
  }

  output.clear();
  output.reserve(kCommandHeaderSize + command.key.size() +
                 command.value.size());
  output.append(kCommandMagic.data(), kCommandMagic.size());
  output.push_back(static_cast<char>(kCommandVersion));
  output.push_back(static_cast<char>(command.type));
  output.push_back('\0');
  output.push_back('\0');
  appendUint64(output, command.client_id);
  appendUint64(output, command.request_id);
  appendUint32(output, static_cast<std::uint32_t>(command.key.size()));
  appendUint32(output, static_cast<std::uint32_t>(command.value.size()));
  output.append(command.key);
  output.append(command.value);
  error.clear();
  return true;
}

bool KVCommandCodec::decode(const std::string& encoded, KVCommand& command,
                            std::string& error) {
  if (encoded.size() < kCommandHeaderSize ||
      !std::equal(kCommandMagic.begin(), kCommandMagic.end(),
                  encoded.begin()) ||
      static_cast<std::uint8_t>(encoded[4]) != kCommandVersion ||
      encoded[6] != '\0' || encoded[7] != '\0') {
    error = "invalid KV command header";
    return false;
  }

  const auto type =
      static_cast<KVCommandType>(static_cast<std::uint8_t>(encoded[5]));
  const std::uint64_t client_id = readUint64(encoded.data() + 8);
  const std::uint64_t request_id = readUint64(encoded.data() + 16);
  const std::size_t key_size =
      static_cast<std::size_t>(readUint32(encoded.data() + 24));
  const std::size_t value_size =
      static_cast<std::size_t>(readUint32(encoded.data() + 28));
  if ((type != KVCommandType::kSet &&
       type != KVCommandType::kDelete) ||
      client_id == 0 || request_id == 0 ||
      key_size > kMaximumCommandKeySize ||
      value_size > kMaximumCommandValueSize ||
      key_size + value_size != encoded.size() - kCommandHeaderSize ||
      (type == KVCommandType::kDelete && value_size != 0)) {
    error = "invalid KV command fields";
    return false;
  }

  command = KVCommand{
      type,
      client_id,
      request_id,
      encoded.substr(kCommandHeaderSize, key_size),
      encoded.substr(kCommandHeaderSize + key_size, value_size),
  };
  error.clear();
  return true;
}

}  // namespace distributed_kv::raft
