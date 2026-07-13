#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace distributed_kv::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

enum class Role {
  kFollower,
  kCandidate,
  kLeader,
};

enum class EntryType : std::uint8_t {
  kNoOp = 0,
  kCommand = 1,
};

struct LogEntry {
  LogIndex index{0};
  Term term{0};
  EntryType type{EntryType::kNoOp};
  std::string command;

  /**
   * Compares all replicated fields.
   *
   * Input: another immutable entry.
   * Output: true when entries are identical.
   * Thread safety: safe for immutable entries.
   */
  [[nodiscard]] bool operator==(const LogEntry& other) const {
    return index == other.index && term == other.term &&
           type == other.type && command == other.command;
  }
};

struct RequestVoteRequest {
  Term term{0};
  NodeId candidate_id{0};
  LogIndex last_log_index{0};
  Term last_log_term{0};
};

struct RequestVoteResponse {
  Term term{0};
  Term election_term{0};
  bool vote_granted{false};
};

struct AppendEntriesRequest {
  Term term{0};
  NodeId leader_id{0};
  LogIndex previous_log_index{0};
  Term previous_log_term{0};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{0};
  std::uint64_t read_context{0};
};

struct AppendEntriesResponse {
  Term term{0};
  bool success{false};
  LogIndex request_previous_log_index{0};
  std::size_t request_entry_count{0};
  LogIndex match_index{0};
  LogIndex conflict_index{1};
  std::optional<Term> conflict_term;
  std::uint64_t read_context{0};
};

using RpcPayload =
    std::variant<RequestVoteRequest, RequestVoteResponse,
                 AppendEntriesRequest, AppendEntriesResponse>;

struct OutboundRpc {
  NodeId destination{0};
  RpcPayload payload;
};

struct ProposeResult {
  bool accepted{false};
  LogIndex index{0};
  std::vector<OutboundRpc> outbound;
};

}  // namespace distributed_kv::raft
