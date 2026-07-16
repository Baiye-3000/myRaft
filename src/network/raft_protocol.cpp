#include "network/raft_protocol.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>

namespace distributed_kv::network {
namespace {

constexpr std::uint32_t kMagic = 0x444b5652U;
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kHeaderSize = 12;

enum class WireType : std::uint8_t {
  kRequestVote = 1,
  kRequestVoteResponse = 2,
  kAppendEntries = 3,
  kAppendEntriesResponse = 4,
  kInstallSnapshot = 5,
  kInstallSnapshotResponse = 6,
};

void append16(std::vector<std::uint8_t>& output, std::uint16_t value) {
  output.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
  output.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void append32(std::vector<std::uint8_t>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::uint8_t>(
        (value >> static_cast<unsigned>(shift)) & 0xffU));
  }
}

void append64(std::vector<std::uint8_t>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::uint8_t>(
        (value >> static_cast<unsigned>(shift)) & 0xffU));
  }
}

std::uint16_t read16(const std::uint8_t* input) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(input[0]) << 8U) |
      static_cast<std::uint16_t>(input[1]));
}

std::uint32_t read32(const std::uint8_t* input) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = static_cast<std::uint32_t>(
        (value << 8U) | static_cast<std::uint32_t>(input[index]));
  }
  return value;
}

std::uint64_t read64(const std::uint8_t* input) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<std::uint64_t>(input[index]);
  }
  return value;
}

class Reader final {
 public:
  explicit Reader(const std::vector<std::uint8_t>& bytes) : bytes_(bytes) {}

  bool read8(std::uint8_t& value) {
    if (remaining() < 1) {
      return false;
    }
    value = bytes_[offset_++];
    return true;
  }

  bool read32Value(std::uint32_t& value) {
    if (remaining() < 4) {
      return false;
    }
    value = read32(bytes_.data() + offset_);
    offset_ += 4;
    return true;
  }

  bool read64Value(std::uint64_t& value) {
    if (remaining() < 8) {
      return false;
    }
    value = read64(bytes_.data() + offset_);
    offset_ += 8;
    return true;
  }

  bool readString(std::size_t length, std::string& value) {
    if (length > remaining()) {
      return false;
    }
    value.assign(reinterpret_cast<const char*>(bytes_.data() + offset_),
                 length);
    offset_ += length;
    return true;
  }

  [[nodiscard]] std::size_t remaining() const {
    return bytes_.size() - offset_;
  }

 private:
  const std::vector<std::uint8_t>& bytes_;
  std::size_t offset_{0};
};

template <typename T>
bool encodePayload(const T& rpc, std::vector<std::uint8_t>& payload,
                   std::string& error) {
  if constexpr (std::is_same_v<T, raft::RequestVoteRequest>) {
    append64(payload, rpc.term);
    append64(payload, rpc.candidate_id);
    append64(payload, rpc.last_log_index);
    append64(payload, rpc.last_log_term);
  } else if constexpr (std::is_same_v<T, raft::RequestVoteResponse>) {
    append64(payload, rpc.term);
    append64(payload, rpc.election_term);
    payload.push_back(rpc.vote_granted ? 1U : 0U);
  } else if constexpr (std::is_same_v<T, raft::AppendEntriesRequest>) {
    if (rpc.entries.size() > RaftProtocol::kMaximumEntriesPerFrame) {
      error = "AppendEntries batch exceeds frame limit";
      return false;
    }
    append64(payload, rpc.term);
    append64(payload, rpc.leader_id);
    append64(payload, rpc.previous_log_index);
    append64(payload, rpc.previous_log_term);
    append64(payload, rpc.leader_commit);
    append64(payload, rpc.read_context);
    append32(payload, static_cast<std::uint32_t>(rpc.entries.size()));
    for (const raft::LogEntry& entry : rpc.entries) {
      if (entry.command.size() > RaftProtocol::kMaximumCommandSize ||
          entry.command.size() >
              static_cast<std::size_t>(
                  std::numeric_limits<std::uint32_t>::max())) {
        error = "Raft command exceeds frame limit";
        return false;
      }
      append64(payload, entry.index);
      append64(payload, entry.term);
      payload.push_back(static_cast<std::uint8_t>(entry.type));
      append32(payload, static_cast<std::uint32_t>(entry.command.size()));
      payload.insert(payload.end(), entry.command.begin(),
                     entry.command.end());
    }
  } else if constexpr (std::is_same_v<T, raft::AppendEntriesResponse>) {
    append64(payload, rpc.term);
    payload.push_back(rpc.success ? 1U : 0U);
    append64(payload, rpc.request_previous_log_index);
    append64(payload, static_cast<std::uint64_t>(rpc.request_entry_count));
    append64(payload, rpc.match_index);
    append64(payload, rpc.conflict_index);
    payload.push_back(rpc.conflict_term.has_value() ? 1U : 0U);
    append64(payload, rpc.conflict_term.value_or(0));
    append64(payload, rpc.read_context);
  } else if constexpr (std::is_same_v<T, raft::InstallSnapshotRequest>) {
    if (rpc.data.size() > RaftProtocol::kMaximumSnapshotChunkSize ||
        rpc.data.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
      error = "InstallSnapshot chunk exceeds frame limit";
      return false;
    }
    append64(payload, rpc.term);
    append64(payload, rpc.leader_id);
    append64(payload, rpc.last_included_index);
    append64(payload, rpc.last_included_term);
    append64(payload, rpc.offset);
    payload.push_back(rpc.done ? 1U : 0U);
    append32(payload, static_cast<std::uint32_t>(rpc.data.size()));
    payload.insert(payload.end(), rpc.data.begin(), rpc.data.end());
  } else {
    append64(payload, rpc.term);
    payload.push_back(rpc.success ? 1U : 0U);
  }
  return true;
}

WireType wireType(const raft::RpcPayload& payload) {
  return std::visit(
      [](const auto& rpc) {
        using Type = std::decay_t<decltype(rpc)>;
        if constexpr (std::is_same_v<Type, raft::RequestVoteRequest>) {
          return WireType::kRequestVote;
        } else if constexpr (
            std::is_same_v<Type, raft::RequestVoteResponse>) {
          return WireType::kRequestVoteResponse;
        } else if constexpr (
            std::is_same_v<Type, raft::AppendEntriesRequest>) {
          return WireType::kAppendEntries;
        } else if constexpr (
            std::is_same_v<Type, raft::AppendEntriesResponse>) {
          return WireType::kAppendEntriesResponse;
        } else if constexpr (
            std::is_same_v<Type, raft::InstallSnapshotRequest>) {
          return WireType::kInstallSnapshot;
        } else {
          return WireType::kInstallSnapshotResponse;
        }
      },
      payload);
}

bool readBoolean(Reader& reader, bool& value) {
  std::uint8_t encoded = 0;
  if (!reader.read8(encoded) || encoded > 1U) {
    return false;
  }
  value = encoded == 1U;
  return true;
}

bool decodePayload(WireType type, Reader& reader, raft::RpcPayload& output,
                   std::string& error) {
  switch (type) {
    case WireType::kRequestVote: {
      raft::RequestVoteRequest rpc;
      if (!reader.read64Value(rpc.term) ||
          !reader.read64Value(rpc.candidate_id) ||
          !reader.read64Value(rpc.last_log_index) ||
          !reader.read64Value(rpc.last_log_term)) {
        error = "RequestVote payload is truncated";
        return false;
      }
      output = rpc;
      break;
    }
    case WireType::kRequestVoteResponse: {
      raft::RequestVoteResponse rpc;
      if (!reader.read64Value(rpc.term) ||
          !reader.read64Value(rpc.election_term) ||
          !readBoolean(reader, rpc.vote_granted)) {
        error = "RequestVote response is invalid";
        return false;
      }
      output = rpc;
      break;
    }
    case WireType::kAppendEntries: {
      raft::AppendEntriesRequest rpc;
      std::uint32_t count = 0;
      if (!reader.read64Value(rpc.term) ||
          !reader.read64Value(rpc.leader_id) ||
          !reader.read64Value(rpc.previous_log_index) ||
          !reader.read64Value(rpc.previous_log_term) ||
          !reader.read64Value(rpc.leader_commit) ||
          !reader.read64Value(rpc.read_context) ||
          !reader.read32Value(count) ||
          count > RaftProtocol::kMaximumEntriesPerFrame) {
        error = "AppendEntries metadata is invalid";
        return false;
      }
      rpc.entries.reserve(count);
      for (std::uint32_t index = 0; index < count; ++index) {
        raft::LogEntry entry;
        std::uint8_t type_value = 0;
        std::uint32_t command_size = 0;
        if (!reader.read64Value(entry.index) ||
            !reader.read64Value(entry.term) ||
            !reader.read8(type_value) ||
            !reader.read32Value(command_size) ||
            command_size > RaftProtocol::kMaximumCommandSize ||
            (type_value !=
                 static_cast<std::uint8_t>(raft::EntryType::kNoOp) &&
             type_value !=
                 static_cast<std::uint8_t>(raft::EntryType::kCommand) &&
             type_value !=
                 static_cast<std::uint8_t>(raft::EntryType::kConfChange)) ||
            !reader.readString(command_size, entry.command)) {
          error = "AppendEntries entry is invalid";
          return false;
        }
        entry.type = static_cast<raft::EntryType>(type_value);
        rpc.entries.push_back(std::move(entry));
      }
      output = std::move(rpc);
      break;
    }
    case WireType::kAppendEntriesResponse: {
      raft::AppendEntriesResponse rpc;
      std::uint64_t entry_count = 0;
      bool has_conflict_term = false;
      raft::Term conflict_term = 0;
      if (!reader.read64Value(rpc.term) ||
          !readBoolean(reader, rpc.success) ||
          !reader.read64Value(rpc.request_previous_log_index) ||
          !reader.read64Value(entry_count) ||
          entry_count > RaftProtocol::kMaximumEntriesPerFrame ||
          !reader.read64Value(rpc.match_index) ||
          !reader.read64Value(rpc.conflict_index) ||
          !readBoolean(reader, has_conflict_term) ||
          !reader.read64Value(conflict_term) ||
          !reader.read64Value(rpc.read_context) ||
          (!has_conflict_term && conflict_term != 0)) {
        error = "AppendEntries response is invalid";
        return false;
      }
      rpc.request_entry_count = static_cast<std::size_t>(entry_count);
      if (has_conflict_term) {
        rpc.conflict_term = conflict_term;
      }
      output = rpc;
      break;
    }
    case WireType::kInstallSnapshot: {
      raft::InstallSnapshotRequest rpc;
      std::uint32_t data_size = 0;
      if (!reader.read64Value(rpc.term) ||
          !reader.read64Value(rpc.leader_id) ||
          !reader.read64Value(rpc.last_included_index) ||
          !reader.read64Value(rpc.last_included_term) ||
          !reader.read64Value(rpc.offset) ||
          !readBoolean(reader, rpc.done) ||
          !reader.read32Value(data_size) ||
          data_size > RaftProtocol::kMaximumSnapshotChunkSize ||
          !reader.readString(data_size, rpc.data)) {
        error = "InstallSnapshot payload is invalid";
        return false;
      }
      output = std::move(rpc);
      break;
    }
    case WireType::kInstallSnapshotResponse: {
      raft::InstallSnapshotResponse rpc;
      if (!reader.read64Value(rpc.term) ||
          !readBoolean(reader, rpc.success)) {
        error = "InstallSnapshot response is invalid";
        return false;
      }
      output = rpc;
      break;
    }
  }
  if (reader.remaining() != 0) {
    error = "Raft RPC payload has trailing bytes";
    return false;
  }
  return true;
}

}  // namespace

bool RaftProtocol::encode(raft::NodeId source,
                          const raft::RpcPayload& payload,
                          std::vector<std::uint8_t>& frame,
                          std::string& error) {
  if (source == 0) {
    error = "Raft RPC source is zero";
    return false;
  }
  std::vector<std::uint8_t> body;
  append64(body, source);
  if (!std::visit(
          [&body, &error](const auto& rpc) {
            return encodePayload(rpc, body, error);
          },
          payload)) {
    return false;
  }
  if (body.size() > kMaximumFrameSize ||
      body.size() >
          static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    error = "Raft RPC frame exceeds size limit";
    return false;
  }
  frame.clear();
  append32(frame, kMagic);
  frame.push_back(kVersion);
  frame.push_back(static_cast<std::uint8_t>(wireType(payload)));
  append16(frame, 0);
  append32(frame, static_cast<std::uint32_t>(body.size()));
  frame.insert(frame.end(), body.begin(), body.end());
  error.clear();
  return true;
}

RaftDecodeStatus RaftProtocol::tryDecode(
    std::vector<std::uint8_t>& buffer, RaftMessage& message,
    std::string& error) {
  if (buffer.size() < kHeaderSize) {
    error.clear();
    return RaftDecodeStatus::kNeedMoreData;
  }
  if (read32(buffer.data()) != kMagic || buffer[4] != kVersion ||
      read16(buffer.data() + 6) != 0) {
    error = "invalid Raft RPC frame header";
    return RaftDecodeStatus::kError;
  }
  const std::uint8_t raw_type = buffer[5];
  if (raw_type < static_cast<std::uint8_t>(WireType::kRequestVote) ||
      raw_type >
          static_cast<std::uint8_t>(WireType::kInstallSnapshotResponse)) {
    error = "unknown Raft RPC message type";
    return RaftDecodeStatus::kError;
  }
  const std::size_t payload_size = read32(buffer.data() + 8);
  if (payload_size < 8 || payload_size > kMaximumFrameSize) {
    error = "invalid Raft RPC payload length";
    return RaftDecodeStatus::kError;
  }
  if (buffer.size() < kHeaderSize + payload_size) {
    error.clear();
    return RaftDecodeStatus::kNeedMoreData;
  }
  std::vector<std::uint8_t> body(
      buffer.begin() + static_cast<std::ptrdiff_t>(kHeaderSize),
      buffer.begin() +
          static_cast<std::ptrdiff_t>(kHeaderSize + payload_size));
  Reader reader(body);
  RaftMessage decoded;
  if (!reader.read64Value(decoded.source) || decoded.source == 0 ||
      !decodePayload(static_cast<WireType>(raw_type), reader,
                     decoded.payload, error)) {
    if (error.empty()) {
      error = "Raft RPC source is invalid";
    }
    return RaftDecodeStatus::kError;
  }
  buffer.erase(
      buffer.begin(),
      buffer.begin() +
          static_cast<std::ptrdiff_t>(kHeaderSize + payload_size));
  message = std::move(decoded);
  error.clear();
  return RaftDecodeStatus::kComplete;
}

}  // namespace distributed_kv::network
