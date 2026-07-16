#include "raft/raft_log.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace distributed_kv::raft {

RaftLog::RaftLog() {
  entries_.push_back(LogEntry{0, 0, EntryType::kNoOp, ""});
}

LogIndex RaftLog::firstIndex() const noexcept {
  return entries_.front().index;
}

LogIndex RaftLog::lastIndex() const noexcept {
  return entries_.back().index;
}

Term RaftLog::lastTerm() const noexcept { return entries_.back().term; }

std::size_t RaftLog::size() const noexcept { return entries_.size() - 1U; }

std::optional<Term> RaftLog::termAt(LogIndex index) const noexcept {
  const auto offset = offsetOf(index);
  if (!offset.has_value()) {
    return std::nullopt;
  }
  return entries_[*offset].term;
}

std::optional<LogEntry> RaftLog::entryAt(LogIndex index) const {
  const auto offset = offsetOf(index);
  if (!offset.has_value()) {
    return std::nullopt;
  }
  return entries_[*offset];
}

LogIndex RaftLog::append(Term term, EntryType type, std::string command) {
  const LogIndex index = lastIndex() + 1U;
  entries_.push_back(LogEntry{index, term, type, std::move(command)});
  return index;
}

std::vector<LogEntry> RaftLog::entriesFrom(
    LogIndex first_index, std::size_t maximum_count) const {
  const auto offset = offsetOf(first_index);
  if (!offset.has_value() || maximum_count == 0) {
    return {};
  }
  const std::size_t begin = *offset;
  const std::size_t available = entries_.size() - begin;
  const std::size_t count = std::min(available, maximum_count);
  return std::vector<LogEntry>(
      entries_.begin() +
          static_cast<std::vector<LogEntry>::difference_type>(begin),
      entries_.begin() +
          static_cast<std::vector<LogEntry>::difference_type>(begin + count));
}

bool RaftLog::appendFrom(LogIndex previous_index,
                         const std::vector<LogEntry>& incoming,
                         std::string& error) {
  if (!offsetOf(previous_index).has_value()) {
    error = "previous log index is absent";
    return false;
  }
  if (incoming.empty()) {
    error.clear();
    return true;
  }
  if (previous_index == std::numeric_limits<LogIndex>::max()) {
    error = "log index space exhausted";
    return false;
  }

  LogIndex expected = previous_index + 1U;
  for (const auto& entry : incoming) {
    if (entry.index != expected || entry.index == 0 || entry.term == 0) {
      error = "incoming Raft entries are not contiguous and valid";
      return false;
    }
    ++expected;
  }

  std::size_t incoming_offset = 0;
  while (incoming_offset < incoming.size()) {
    const LogEntry& entry = incoming[incoming_offset];
    if (entry.index > lastIndex()) {
      break;
    }
    const LogEntry& local = entries_[*offsetOf(entry.index)];
    if (local.term != entry.term) {
      truncateFrom(entry.index);
      break;
    }
    if (!(local == entry)) {
      error = "same Raft index and term contain different payloads";
      return false;
    }
    ++incoming_offset;
  }

  entries_.insert(entries_.end(), incoming.begin() +
                                      static_cast<std::vector<
                                          LogEntry>::difference_type>(
                                          incoming_offset),
                  incoming.end());
  error.clear();
  return true;
}

bool RaftLog::matches(LogIndex index, Term term) const noexcept {
  const auto local_term = termAt(index);
  return local_term.has_value() && *local_term == term;
}

std::optional<LogIndex> RaftLog::firstIndexOfTerm(Term term) const noexcept {
  for (const LogEntry& entry : entries_) {
    if (entry.term == term) {
      return entry.index;
    }
  }
  return std::nullopt;
}

std::optional<LogIndex> RaftLog::lastIndexOfTerm(Term term) const noexcept {
  for (std::size_t index = entries_.size(); index > 0; --index) {
    if (entries_[index - 1U].term == term) {
      return entries_[index - 1U].index;
    }
  }
  return std::nullopt;
}

std::vector<LogEntry> RaftLog::persistentEntries() const {
  return entries_;
}

bool RaftLog::compactTo(LogIndex boundary_index, Term boundary_term,
                        std::string& error) {
  const auto boundary_offset = offsetOf(boundary_index);
  if (!boundary_offset.has_value() ||
      entries_[*boundary_offset].term != boundary_term ||
      (boundary_index == 0 && boundary_term != 0) ||
      (boundary_index != 0 && boundary_term == 0)) {
    error = "compaction boundary is absent or has a different term";
    return false;
  }
  std::vector<LogEntry> compacted;
  compacted.reserve(entries_.size() - *boundary_offset);
  compacted.push_back(
      LogEntry{boundary_index, boundary_term, EntryType::kNoOp, ""});
  compacted.insert(
      compacted.end(),
      entries_.begin() +
          static_cast<std::vector<LogEntry>::difference_type>(
              *boundary_offset + 1U),
      entries_.end());
  entries_.swap(compacted);
  error.clear();
  return true;
}

bool RaftLog::restore(const std::vector<LogEntry>& entries,
                      std::string& error) {
  if (entries.empty() || entries.front().type != EntryType::kNoOp ||
      !entries.front().command.empty() ||
      (entries.front().index == 0 && entries.front().term != 0) ||
      (entries.front().index != 0 && entries.front().term == 0)) {
    error = "persistent Raft log has invalid boundary";
    return false;
  }
  for (std::size_t index = 1; index < entries.size(); ++index) {
    if (entries.front().index >
            std::numeric_limits<LogIndex>::max() -
                static_cast<LogIndex>(index) ||
        entries[index].index !=
            entries.front().index + static_cast<LogIndex>(index) ||
        entries[index].term == 0 ||
        (entries[index].type != EntryType::kNoOp &&
         entries[index].type != EntryType::kCommand &&
         entries[index].type != EntryType::kConfChange) ||
        (entries[index].type == EntryType::kNoOp &&
         !entries[index].command.empty()) ||
        ((entries[index].type == EntryType::kCommand ||
          entries[index].type == EntryType::kConfChange) &&
         entries[index].command.empty())) {
      error = "persistent Raft log is not contiguous and valid";
      return false;
    }
  }
  entries_ = entries;
  error.clear();
  return true;
}

void RaftLog::truncateFrom(LogIndex index) {
  const std::size_t offset = *offsetOf(index);
  entries_.erase(
      entries_.begin() +
          static_cast<std::vector<LogEntry>::difference_type>(offset),
      entries_.end());
}

std::optional<std::size_t> RaftLog::offsetOf(
    LogIndex index) const noexcept {
  if (index < firstIndex() || index > lastIndex()) {
    return std::nullopt;
  }
  const LogIndex offset = index - firstIndex();
  if (offset >= static_cast<LogIndex>(entries_.size())) {
    return std::nullopt;
  }
  return static_cast<std::size_t>(offset);
}

}  // namespace distributed_kv::raft
