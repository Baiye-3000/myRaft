#include "raft/cluster_config.h"

#include <limits>
#include <vector>

namespace distributed_kv::raft {
namespace {

constexpr std::size_t kMaximumMemberCount = 64U;
constexpr std::size_t kMaximumHostSize = 255U;
constexpr std::size_t kMaximumConfChangeSize = 64U * 1024U;
constexpr std::uint32_t kConfChangeMagic = 0x43434647U;

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

bool appendHost(std::vector<std::uint8_t>& output, const std::string& host,
                std::string& error) {
  if (host.empty() || host.size() > kMaximumHostSize ||
      host.size() >
          static_cast<std::size_t>(std::numeric_limits<std::uint8_t>::max())) {
    error = "cluster host is invalid";
    return false;
  }
  output.push_back(static_cast<std::uint8_t>(host.size()));
  output.insert(output.end(), host.begin(), host.end());
  return true;
}

bool readHost(const std::vector<std::uint8_t>& input, std::size_t& offset,
              std::string& host, std::string& error) {
  if (offset >= input.size()) {
    error = "cluster host length is truncated";
    return false;
  }
  const std::size_t length = input[offset++];
  if (length == 0 || length > kMaximumHostSize ||
      length > input.size() - offset) {
    error = "cluster host has invalid length";
    return false;
  }
  host.assign(reinterpret_cast<const char*>(input.data() + offset), length);
  offset += length;
  return true;
}

bool appendMember(std::vector<std::uint8_t>& output,
                  const ClusterMember& member, std::string& error) {
  append64(output, member.node_id);
  if (!appendHost(output, member.client_host, error) ||
      !appendHost(output, member.peer_host, error)) {
    return false;
  }
  append32(output, member.client_port);
  append32(output, member.peer_port);
  return true;
}

bool readMember(const std::vector<std::uint8_t>& input, std::size_t& offset,
                ClusterMember& member, std::string& error) {
  if (input.size() - offset < 8) {
    error = "cluster member is truncated";
    return false;
  }
  member.node_id = read64(input.data() + offset);
  offset += 8;
  if (!readHost(input, offset, member.client_host, error) ||
      !readHost(input, offset, member.peer_host, error) ||
      input.size() - offset < 8) {
    if (error.empty()) {
      error = "cluster member ports are truncated";
    }
    return false;
  }
  member.client_port =
      static_cast<std::uint16_t>(read32(input.data() + offset));
  offset += 4;
  member.peer_port =
      static_cast<std::uint16_t>(read32(input.data() + offset));
  offset += 4;
  if (member.node_id == 0 || member.client_port == 0 ||
      member.peer_port == 0) {
    error = "cluster member endpoint is invalid";
    return false;
  }
  return true;
}

}  // namespace

bool validateClusterConfiguration(const ClusterConfiguration& config,
                                  NodeId local_node_id,
                                  std::string& error) {
  if (config.config_id == 0 || config.members.empty() ||
      config.members.size() > kMaximumMemberCount) {
    error = "cluster configuration metadata is invalid";
    return false;
  }
  bool found_self = local_node_id == 0;
  NodeId previous_id = 0;
  for (const ClusterMember& member : config.members) {
    if (member.node_id == 0 || member.node_id <= previous_id ||
        member.client_host.empty() || member.peer_host.empty() ||
        member.client_port == 0 || member.peer_port == 0) {
      error = "cluster member is invalid or out of order";
      return false;
    }
    if (member.node_id == local_node_id) {
      found_self = true;
    }
    previous_id = member.node_id;
  }
  if (!found_self) {
    error = "cluster configuration does not include local node";
    return false;
  }
  error.clear();
  return true;
}

bool validateMembershipOperation(const MembershipOperation& operation,
                                 std::string& error) {
  if (operation.operation_id == 0 ||
      (operation.type != ConfChangeType::kAddNode &&
       operation.type != ConfChangeType::kRemoveNode) ||
      operation.member.node_id == 0) {
    error = "membership operation metadata is invalid";
    return false;
  }
  if (operation.type == ConfChangeType::kAddNode) {
    if (operation.member.client_host.empty() ||
        operation.member.client_host.size() > kMaximumHostSize ||
        operation.member.client_port == 0 ||
        operation.member.peer_host.empty() ||
        operation.member.peer_host.size() > kMaximumHostSize ||
        operation.member.peer_port == 0) {
      error = "add membership operation endpoint is invalid";
      return false;
    }
  } else if (!operation.member.client_host.empty() ||
             operation.member.client_port != 0 ||
             !operation.member.peer_host.empty() ||
             operation.member.peer_port != 0) {
    error = "remove membership operation contains an endpoint";
    return false;
  }
  error.clear();
  return true;
}

bool encodeMembershipOperation(const MembershipOperation& operation,
                               std::vector<std::uint8_t>& output,
                               std::string& error) {
  if (!validateMembershipOperation(operation, error)) return false;
  output.clear();
  append64(output, operation.operation_id);
  output.push_back(static_cast<std::uint8_t>(operation.type));
  if (operation.type == ConfChangeType::kAddNode) {
    if (!appendMember(output, operation.member, error)) return false;
  } else {
    append64(output, operation.member.node_id);
  }
  error.clear();
  return true;
}

bool decodeMembershipOperation(const std::vector<std::uint8_t>& input,
                               std::size_t& offset,
                               MembershipOperation& operation,
                               std::string& error) {
  if (input.size() - offset < 9U) {
    error = "membership operation is truncated";
    return false;
  }
  MembershipOperation decoded;
  decoded.operation_id = read64(input.data() + offset);
  offset += 8U;
  decoded.type = static_cast<ConfChangeType>(input[offset++]);
  if (decoded.type == ConfChangeType::kAddNode) {
    if (!readMember(input, offset, decoded.member, error)) return false;
  } else if (decoded.type == ConfChangeType::kRemoveNode) {
    if (input.size() - offset < 8U) {
      error = "remove membership operation is truncated";
      return false;
    }
    decoded.member.node_id = read64(input.data() + offset);
    offset += 8U;
  }
  if (!validateMembershipOperation(decoded, error)) return false;
  operation = std::move(decoded);
  error.clear();
  return true;
}

bool encodeClusterConfiguration(const ClusterConfiguration& config,
                                std::vector<std::uint8_t>& output,
                                std::string& error) {
  if (!validateClusterConfiguration(config, 0, error)) {
    return false;
  }
  output.clear();
  append64(output, config.config_id);
  append32(output, static_cast<std::uint32_t>(config.members.size()));
  for (const ClusterMember& member : config.members) {
    if (!appendMember(output, member, error)) {
      return false;
    }
  }
  error.clear();
  return true;
}

bool decodeClusterConfiguration(const std::vector<std::uint8_t>& input,
                                std::size_t& offset,
                                ClusterConfiguration& config,
                                std::string& error) {
  if (input.size() - offset < 12) {
    error = "cluster configuration is truncated";
    return false;
  }
  config.config_id = read64(input.data() + offset);
  offset += 8;
  const std::size_t count = read32(input.data() + offset);
  offset += 4;
  if (config.config_id == 0 || count == 0 || count > kMaximumMemberCount) {
    error = "cluster configuration count is invalid";
    return false;
  }
  config.members.clear();
  config.members.reserve(count);
  for (std::size_t index = 0; index < count; ++index) {
    ClusterMember member;
    if (!readMember(input, offset, member, error)) {
      return false;
    }
    config.members.push_back(std::move(member));
  }
  if (!validateClusterConfiguration(config, 0, error)) {
    return false;
  }
  error.clear();
  return true;
}

bool encodeConfChangeEntry(const ConfChangeEntry& entry, std::string& output,
                           std::string& error) {
  if ((entry.type != ConfChangeType::kAddNode &&
       entry.type != ConfChangeType::kRemoveNode) ||
      !validateClusterConfiguration(entry.target_config, 0, error)) {
    if (error.empty()) {
      error = "conf change entry is invalid";
    }
    return false;
  }
  if (entry.type == ConfChangeType::kRemoveNode &&
      entry.member.node_id == 0) {
    error = "remove conf change must carry node id";
    return false;
  }
  std::vector<std::uint8_t> bytes;
  append32(bytes, kConfChangeMagic);
  bytes.push_back(static_cast<std::uint8_t>(entry.type));
  bytes.push_back(entry.joint ? 1U : 0U);
  if (entry.type == ConfChangeType::kRemoveNode) {
    append64(bytes, entry.member.node_id);
  } else if (!appendMember(bytes, entry.member, error)) {
    return false;
  }
  std::vector<std::uint8_t> target;
  if (!encodeClusterConfiguration(entry.target_config, target, error)) {
    return false;
  }
  append32(bytes, static_cast<std::uint32_t>(target.size()));
  bytes.insert(bytes.end(), target.begin(), target.end());
  append64(bytes, entry.operation_id);
  if (bytes.size() > kMaximumConfChangeSize) {
    error = "conf change entry exceeds size limit";
    return false;
  }
  output.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  error.clear();
  return true;
}

bool decodeConfChangeEntry(const std::string& input, ConfChangeEntry& entry,
                           std::string& error) {
  if (input.size() < 16 || input.size() > kMaximumConfChangeSize) {
    error = "conf change entry has invalid size";
    return false;
  }
  std::vector<std::uint8_t> bytes(input.begin(), input.end());
  std::size_t offset = 0;
  if (read32(bytes.data() + offset) != kConfChangeMagic) {
    error = "conf change entry has invalid magic";
    return false;
  }
  offset += 4;
  const auto type_value = static_cast<ConfChangeType>(bytes[offset++]);
  const bool joint = bytes[offset++] != 0U;
  if (type_value != ConfChangeType::kAddNode &&
      type_value != ConfChangeType::kRemoveNode) {
    error = "conf change entry has invalid type";
    return false;
  }
  ClusterMember member;
  if (type_value == ConfChangeType::kRemoveNode) {
    if (offset + 8 > bytes.size()) {
      error = "remove conf change node id is truncated";
      return false;
    }
    member.node_id = read64(bytes.data() + offset);
    offset += 8;
    if (member.node_id == 0) {
      error = "remove conf change node id is invalid";
      return false;
    }
  } else if (!readMember(bytes, offset, member, error)) {
    return false;
  }
  if (offset + 4 > bytes.size()) {
    error = "conf change target length is truncated";
    return false;
  }
  const std::size_t target_size = read32(bytes.data() + offset);
  offset += 4;
  if (target_size == 0 || target_size > bytes.size() - offset) {
    error = "conf change target length is invalid";
    return false;
  }
  ClusterConfiguration target;
  if (!decodeClusterConfiguration(bytes, offset, target, error)) {
    return false;
  }
  std::uint64_t operation_id = 0;
  if (bytes.size() - offset == sizeof(std::uint64_t)) {
    operation_id = read64(bytes.data() + offset);
    offset += sizeof(std::uint64_t);
  } else if (offset != bytes.size()) {
    error = "conf change entry has trailing bytes";
    return false;
  }
  entry.type = type_value;
  entry.joint = joint;
  entry.member = std::move(member);
  entry.target_config = std::move(target);
  entry.operation_id = operation_id;
  error.clear();
  return true;
}

}  // namespace distributed_kv::raft
