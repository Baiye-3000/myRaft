#include "raft/state_machine.h"

#include <algorithm>
#include <utility>

#include "raft/kv_command.h"
#include "storage/kv_store.h"

namespace distributed_kv::raft {

StateMachine::StateMachine(storage::KVStore& store) : store_(store) {}

std::optional<ApplyResult> StateMachine::apply(
    const LogEntry& entry, std::string& error) {
  if (entry.index != last_applied_ + 1U) {
    error = "state machine entry is not the next committed index";
    return std::nullopt;
  }
  if (entry.type == EntryType::kNoOp) {
    if (!entry.command.empty()) {
      error = "Raft no-op entry contains command bytes";
      return std::nullopt;
    }
    last_applied_ = entry.index;
    error.clear();
    return std::nullopt;
  }
  if (entry.type != EntryType::kCommand) {
    error = "unknown Raft entry type";
    return std::nullopt;
  }

  KVCommand command;
  if (!KVCommandCodec::decode(entry.command, command, error)) {
    return std::nullopt;
  }

  const auto existing = sessions_.find(command.client_id);
  if (existing != sessions_.end() &&
      command.request_id <= existing->second.request_id) {
    last_applied_ = entry.index;
    error.clear();
    if (command.request_id == existing->second.request_id) {
      return ApplyResult{
          entry.index,
          command.client_id,
          command.request_id,
          existing->second.status,
          existing->second.payload,
          true,
      };
    }
    return ApplyResult{
        entry.index,
        command.client_id,
        command.request_id,
        ApplyStatus::kStaleRequest,
        "STALE_REQUEST",
        true,
    };
  }

  ApplyStatus status = ApplyStatus::kOk;
  std::string payload = "OK";
  if (command.type == KVCommandType::kSet) {
    const bool inserted = store_.put(command.key, command.value);
    static_cast<void>(inserted);
  } else {
    const bool removed = store_.remove(command.key);
    if (!removed) {
      status = ApplyStatus::kNotFound;
      payload = "NOT_FOUND";
    }
  }

  sessions_.insert_or_assign(
      command.client_id,
      CachedResponse{command.request_id, status, payload});
  last_applied_ = entry.index;
  error.clear();
  return ApplyResult{
      entry.index,
      command.client_id,
      command.request_id,
      status,
      std::move(payload),
      false,
  };
}

std::optional<std::string> StateMachine::get(
    const std::string& key) const {
  return store_.get(key);
}

LogIndex StateMachine::lastApplied() const noexcept {
  return last_applied_;
}

StateMachineSnapshot StateMachine::snapshot(
    Term last_included_term) const {
  StateMachineSnapshot image;
  image.last_included_index = last_applied_;
  image.last_included_term = last_included_term;
  image.entries = store_.snapshotEntries();
  image.sessions.reserve(sessions_.size());
  for (const auto& session : sessions_) {
    image.sessions.push_back(SnapshotSession{
        session.first, session.second.request_id, session.second.status,
        session.second.payload});
  }
  std::sort(image.sessions.begin(), image.sessions.end(),
            [](const SnapshotSession& left,
               const SnapshotSession& right) {
              return left.client_id < right.client_id;
            });
  return image;
}

bool StateMachine::restore(const StateMachineSnapshot& snapshot,
                           std::string& error) {
  if ((snapshot.last_included_index == 0) !=
      (snapshot.last_included_term == 0)) {
    error = "snapshot index and term must both be zero or nonzero";
    return false;
  }
  std::unordered_map<std::uint64_t, CachedResponse> restored_sessions;
  restored_sessions.reserve(snapshot.sessions.size());
  for (const SnapshotSession& session : snapshot.sessions) {
    if (session.client_id == 0 || session.request_id == 0 ||
        (session.status != ApplyStatus::kOk &&
         session.status != ApplyStatus::kNotFound &&
         session.status != ApplyStatus::kStaleRequest) ||
        !restored_sessions
             .emplace(session.client_id,
                      CachedResponse{session.request_id, session.status,
                                     session.payload})
             .second) {
      error = "snapshot contains an invalid or duplicate client session";
      return false;
    }
  }
  if (!store_.replaceAll(snapshot.entries, error)) {
    return false;
  }
  sessions_ = std::move(restored_sessions);
  last_applied_ = snapshot.last_included_index;
  error.clear();
  return true;
}

}  // namespace distributed_kv::raft
