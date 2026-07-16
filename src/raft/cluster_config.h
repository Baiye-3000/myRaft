#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "raft/types.h"

namespace distributed_kv::raft {

struct ClusterMember {
  NodeId node_id{0};
  std::string client_host;
  std::uint16_t client_port{0};
  std::string peer_host;
  std::uint16_t peer_port{0};

  [[nodiscard]] bool operator==(const ClusterMember& other) const {
    return node_id == other.node_id && client_host == other.client_host &&
           client_port == other.client_port &&
           peer_host == other.peer_host && peer_port == other.peer_port;
  }
};

struct ClusterConfiguration {
  std::uint64_t config_id{0};
  std::vector<ClusterMember> members;

  [[nodiscard]] bool operator==(const ClusterConfiguration& other) const {
    return config_id == other.config_id && members == other.members;
  }
};

enum class ConfChangeType : std::uint8_t {
  kAddNode = 1,
  kRemoveNode = 2,
};

struct MembershipOperation {
  std::uint64_t operation_id{0};
  ConfChangeType type{ConfChangeType::kAddNode};
  ClusterMember member;

  [[nodiscard]] bool operator==(const MembershipOperation& other) const {
    return operation_id == other.operation_id && type == other.type &&
           member == other.member;
  }
};

struct ConfChangeEntry {
  ConfChangeType type{ConfChangeType::kAddNode};
  bool joint{false};
  ClusterMember member;
  ClusterConfiguration target_config;
  std::uint64_t operation_id{0};
};

[[nodiscard]] bool validateMembershipOperation(
    const MembershipOperation& operation, std::string& error);

[[nodiscard]] bool encodeMembershipOperation(
    const MembershipOperation& operation, std::vector<std::uint8_t>& output,
    std::string& error);

[[nodiscard]] bool decodeMembershipOperation(
    const std::vector<std::uint8_t>& input, std::size_t& offset,
    MembershipOperation& operation, std::string& error);

/**
 * Validates one cluster configuration snapshot.
 *
 * Input: configuration and local node id (0 skips self-membership check).
 * Output: true when ids, endpoints and ordering are valid.
 */
[[nodiscard]] bool validateClusterConfiguration(
    const ClusterConfiguration& config, NodeId local_node_id,
    std::string& error);

/**
 * Encodes one cluster configuration for durable storage.
 */
[[nodiscard]] bool encodeClusterConfiguration(
    const ClusterConfiguration& config, std::vector<std::uint8_t>& output,
    std::string& error);

/**
 * Decodes one cluster configuration from durable storage bytes.
 */
[[nodiscard]] bool decodeClusterConfiguration(
    const std::vector<std::uint8_t>& input, std::size_t& offset,
    ClusterConfiguration& config, std::string& error);

/**
 * Encodes one ConfChange entry into an opaque log command string.
 */
[[nodiscard]] bool encodeConfChangeEntry(const ConfChangeEntry& entry,
                                         std::string& output,
                                         std::string& error);

/**
 * Decodes one ConfChange entry from an opaque log command string.
 */
[[nodiscard]] bool decodeConfChangeEntry(const std::string& input,
                                         ConfChangeEntry& entry,
                                         std::string& error);

}  // namespace distributed_kv::raft
