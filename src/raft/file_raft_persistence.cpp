#include "raft/file_raft_persistence.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace distributed_kv::raft {
namespace {

constexpr std::array<std::uint8_t, 8> kFileMagic{
    {'D', 'K', 'V', 'R', 'A', 'F', 'T', '\0'}};
constexpr std::array<std::uint8_t, 4> kRecordMagic{{0xd6, 0x52, 0x46, 0x54}};
constexpr std::array<std::uint8_t, 4> kDeltaRecordMagic{
    {0xd6, 0x52, 0x46, 0x44}};
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kFileHeaderSize = 16;
constexpr std::size_t kRecordHeaderSize = 12;
constexpr std::size_t kMaximumPayloadSize = 64U * 1024U * 1024U;
constexpr std::size_t kMaximumCommandSize = 1024U * 1024U;
constexpr std::size_t kMaximumEntryCount = 1000000U;

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

std::uint32_t crc32c(const std::vector<std::uint8_t>& bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::uint8_t byte : bytes) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask =
          0U - static_cast<std::uint32_t>(crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

bool writeAll(int fd, const std::vector<std::uint8_t>& bytes, off_t offset,
              std::string& error) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t result =
        ::pwrite(fd, bytes.data() + written, bytes.size() - written,
                 offset + static_cast<off_t>(written));
    if (result > 0) {
      written += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      error = std::string("Raft journal write failed: ") +
              (result == 0 ? "no progress" : std::strerror(errno));
      return false;
    }
  }
  return true;
}

bool readAll(int fd, std::vector<std::uint8_t>& bytes, off_t offset,
             std::string& error) {
  std::size_t count = 0;
  while (count < bytes.size()) {
    const ssize_t result =
        ::pread(fd, bytes.data() + count, bytes.size() - count,
                offset + static_cast<off_t>(count));
    if (result > 0) {
      count += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      error = result == 0
                  ? "unexpected end of Raft journal"
                  : std::string("Raft journal read failed: ") +
                        std::strerror(errno);
      return false;
    }
  }
  return true;
}

bool encodeState(const RaftPersistentState& state,
                 std::vector<std::uint8_t>& output, std::string& error) {
  if (state.entries.empty() || state.entries.size() > kMaximumEntryCount ||
      state.commit_index >= state.entries.size() ||
      state.entries.size() >
          static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    error = "invalid Raft state image";
    return false;
  }
  output.clear();
  append64(output, state.current_term);
  output.push_back(state.voted_for.has_value() ? 1U : 0U);
  append64(output, state.voted_for.value_or(0));
  append64(output, state.commit_index);
  append32(output, static_cast<std::uint32_t>(state.entries.size()));
  for (const LogEntry& entry : state.entries) {
    if (entry.command.size() > kMaximumCommandSize ||
        entry.command.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
      error = "Raft command exceeds persistent limit";
      return false;
    }
    append64(output, entry.index);
    append64(output, entry.term);
    output.push_back(static_cast<std::uint8_t>(entry.type));
    append32(output, static_cast<std::uint32_t>(entry.command.size()));
    output.insert(output.end(), entry.command.begin(), entry.command.end());
    if (output.size() > kMaximumPayloadSize) {
      error = "Raft state image exceeds persistent limit";
      return false;
    }
  }
  return true;
}

bool decodeState(const std::vector<std::uint8_t>& input,
                 RaftPersistentState& state, std::string& error) {
  constexpr std::size_t kMetadataSize = 29;
  if (input.size() < kMetadataSize) {
    error = "Raft state image is truncated";
    return false;
  }
  std::size_t offset = 0;
  RaftPersistentState decoded;
  decoded.current_term = read64(input.data() + offset);
  offset += 8;
  const std::uint8_t has_vote = input[offset++];
  const NodeId voted_for = read64(input.data() + offset);
  offset += 8;
  if (has_vote > 1U || (has_vote == 0U && voted_for != 0U)) {
    error = "Raft state has invalid vote encoding";
    return false;
  }
  if (has_vote == 1U) {
    decoded.voted_for = voted_for;
  }
  decoded.commit_index = read64(input.data() + offset);
  offset += 8;
  const std::size_t count = read32(input.data() + offset);
  offset += 4;
  if (count == 0 || count > kMaximumEntryCount) {
    error = "Raft state has invalid entry count";
    return false;
  }
  decoded.entries.reserve(count);
  for (std::size_t entry_index = 0; entry_index < count; ++entry_index) {
    if (input.size() - offset < 21) {
      error = "Raft entry metadata is truncated";
      return false;
    }
    LogEntry entry;
    entry.index = read64(input.data() + offset);
    offset += 8;
    entry.term = read64(input.data() + offset);
    offset += 8;
    entry.type = static_cast<EntryType>(input[offset++]);
    const std::size_t command_size = read32(input.data() + offset);
    offset += 4;
    if (command_size > kMaximumCommandSize ||
        command_size > input.size() - offset) {
      error = "Raft entry command is invalid";
      return false;
    }
    entry.command.assign(
        reinterpret_cast<const char*>(input.data() + offset), command_size);
    offset += command_size;
    decoded.entries.push_back(std::move(entry));
  }
  if (offset != input.size()) {
    error = "Raft state image has trailing bytes";
    return false;
  }
  state = std::move(decoded);
  return true;
}

/**
 * Encodes metadata and the shortest suffix replacement from previous.
 *
 * Input: previously durable state and next complete state. Output: delta
 * payload whose truncate index retains their common prefix.
 * Thread safety: stateless.
 */
bool encodeDelta(const RaftPersistentState& previous,
                 const RaftPersistentState& state,
                 std::vector<std::uint8_t>& output, std::string& error) {
  if (state.current_term < previous.current_term ||
      state.commit_index < previous.commit_index ||
      state.entries.empty() || state.entries.size() > kMaximumEntryCount ||
      state.commit_index >= state.entries.size()) {
    error = "invalid incremental Raft state";
    return false;
  }
  const std::size_t shared_limit =
      std::min(previous.entries.size(), state.entries.size());
  std::size_t common_prefix = 0;
  while (common_prefix < shared_limit &&
         previous.entries[common_prefix] == state.entries[common_prefix]) {
    ++common_prefix;
  }
  if (common_prefix == 0 ||
      common_prefix >
          static_cast<std::size_t>(
              std::numeric_limits<std::uint64_t>::max())) {
    error = "incremental Raft state changed the sentinel";
    return false;
  }
  const std::size_t append_count = state.entries.size() - common_prefix;
  if (append_count >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    error = "incremental Raft suffix is too large";
    return false;
  }

  output.clear();
  append64(output, state.current_term);
  output.push_back(state.voted_for.has_value() ? 1U : 0U);
  append64(output, state.voted_for.value_or(0));
  append64(output, state.commit_index);
  append64(output, static_cast<std::uint64_t>(common_prefix));
  append32(output, static_cast<std::uint32_t>(append_count));
  for (std::size_t index = common_prefix; index < state.entries.size();
       ++index) {
    const LogEntry& entry = state.entries[index];
    if (entry.index != static_cast<LogIndex>(index) || entry.term == 0 ||
        (entry.type != EntryType::kNoOp &&
         entry.type != EntryType::kCommand) ||
        (entry.type == EntryType::kNoOp && !entry.command.empty()) ||
        entry.command.size() > kMaximumCommandSize ||
        entry.command.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
      error = "incremental Raft suffix contains an invalid entry";
      return false;
    }
    append64(output, entry.index);
    append64(output, entry.term);
    output.push_back(static_cast<std::uint8_t>(entry.type));
    append32(output, static_cast<std::uint32_t>(entry.command.size()));
    output.insert(output.end(), entry.command.begin(), entry.command.end());
    if (output.size() > kMaximumPayloadSize) {
      error = "incremental Raft state exceeds persistent limit";
      return false;
    }
  }
  return true;
}

/**
 * Applies one checksummed suffix delta to the latest complete state.
 *
 * Input: delta payload and existing state. Output: atomically replaced state
 * after all bounds and continuity checks pass.
 * Thread safety: caller owns latest.
 */
bool decodeDelta(const std::vector<std::uint8_t>& input,
                 RaftPersistentState& latest, std::string& error) {
  constexpr std::size_t kDeltaMetadataSize = 37;
  if (latest.entries.empty()) {
    error = "Raft journal delta has no base image";
    return false;
  }
  if (input.size() < kDeltaMetadataSize) {
    error = "Raft journal delta is truncated";
    return false;
  }
  std::size_t offset = 0;
  RaftPersistentState decoded;
  decoded.current_term = read64(input.data() + offset);
  offset += 8;
  const std::uint8_t has_vote = input[offset++];
  const NodeId voted_for = read64(input.data() + offset);
  offset += 8;
  decoded.commit_index = read64(input.data() + offset);
  offset += 8;
  const std::uint64_t truncate_index = read64(input.data() + offset);
  offset += 8;
  const std::size_t append_count = read32(input.data() + offset);
  offset += 4;
  if (has_vote > 1U || (has_vote == 0U && voted_for != 0U) ||
      decoded.current_term < latest.current_term ||
      decoded.commit_index < latest.commit_index ||
      truncate_index == 0 ||
      truncate_index > latest.entries.size() ||
      truncate_index > kMaximumEntryCount ||
      append_count > kMaximumEntryCount -
                         static_cast<std::size_t>(truncate_index)) {
    error = "Raft journal delta metadata is invalid";
    return false;
  }
  if (has_vote == 1U) {
    decoded.voted_for = voted_for;
  }
  decoded.entries.assign(
      latest.entries.begin(),
      latest.entries.begin() +
          static_cast<std::vector<LogEntry>::difference_type>(
              truncate_index));
  decoded.entries.reserve(decoded.entries.size() + append_count);
  for (std::size_t count = 0; count < append_count; ++count) {
    if (input.size() - offset < 21) {
      error = "Raft journal delta entry is truncated";
      return false;
    }
    LogEntry entry;
    entry.index = read64(input.data() + offset);
    offset += 8;
    entry.term = read64(input.data() + offset);
    offset += 8;
    entry.type = static_cast<EntryType>(input[offset++]);
    const std::size_t command_size = read32(input.data() + offset);
    offset += 4;
    const LogIndex expected_index =
        truncate_index + static_cast<LogIndex>(count);
    if (entry.index != expected_index || entry.term == 0 ||
        (entry.type != EntryType::kNoOp &&
         entry.type != EntryType::kCommand) ||
        command_size > kMaximumCommandSize ||
        command_size > input.size() - offset) {
      error = "Raft journal delta entry is invalid";
      return false;
    }
    entry.command.assign(
        reinterpret_cast<const char*>(input.data() + offset), command_size);
    offset += command_size;
    if (entry.type == EntryType::kNoOp && !entry.command.empty()) {
      error = "Raft journal delta no-op contains command bytes";
      return false;
    }
    decoded.entries.push_back(std::move(entry));
  }
  if (offset != input.size() ||
      decoded.commit_index >= decoded.entries.size()) {
    error = "Raft journal delta has invalid trailing data or commit index";
    return false;
  }
  latest = std::move(decoded);
  return true;
}

/**
 * Synchronizes the directory entry after atomic journal replacement.
 *
 * Input: journal path and writable error. Output: true after directory fsync.
 * Thread safety: caller serializes compaction for the path.
 */
bool syncParentDirectory(const std::string& path, std::string& error) {
  const std::size_t separator = path.find_last_of('/');
  const std::string directory =
      separator == std::string::npos ? "." : path.substr(0, separator);
  const int directory_fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    error = std::string("failed to open Raft journal directory: ") +
            std::strerror(errno);
    return false;
  }
  const int result = ::fsync(directory_fd);
  const int failure = errno;
  static_cast<void>(::close(directory_fd));
  if (result != 0) {
    error = std::string("failed to sync Raft journal directory: ") +
            std::strerror(failure);
    return false;
  }
  return true;
}

}  // namespace

FileRaftPersistence::FileRaftPersistence(std::string path)
    : FileRaftPersistence(
          FileRaftPersistenceOptions{std::move(path), 1024,
                                     64U * 1024U * 1024U}) {}

FileRaftPersistence::FileRaftPersistence(
    FileRaftPersistenceOptions options)
    : options_(std::move(options)) {}

FileRaftPersistence::~FileRaftPersistence() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_fd_ >= 0) {
    static_cast<void>(::flock(file_fd_, LOCK_UN));
    static_cast<void>(::close(file_fd_));
  }
}

bool FileRaftPersistence::openLocked(std::string& error) {
  if (file_fd_ >= 0) {
    return true;
  }
  if (options_.path.empty() || options_.maximum_delta_records == 0 ||
      options_.maximum_file_bytes < kFileHeaderSize + kRecordHeaderSize) {
    error = "invalid Raft journal path or compaction limits";
    return false;
  }
  const int fd =
      ::open(options_.path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0640);
  if (fd < 0) {
    error = std::string("failed to open Raft journal: ") +
            std::strerror(errno);
    return false;
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    error = std::string("failed to lock Raft journal: ") +
            std::strerror(errno);
    static_cast<void>(::close(fd));
    return false;
  }
  struct stat status {};
  if (::fstat(fd, &status) != 0) {
    error = std::string("failed to stat Raft journal: ") +
            std::strerror(errno);
    static_cast<void>(::close(fd));
    return false;
  }
  if (status.st_size == 0) {
    std::vector<std::uint8_t> header(kFileMagic.begin(), kFileMagic.end());
    append32(header, kVersion);
    append32(header, 0);
    if (!writeAll(fd, header, 0, error) || ::fdatasync(fd) != 0) {
      if (error.empty()) {
        error = std::string("failed to sync Raft journal header: ") +
                std::strerror(errno);
      }
      static_cast<void>(::close(fd));
      return false;
    }
  } else if (status.st_size < static_cast<off_t>(kFileHeaderSize)) {
    error = "Raft journal header is truncated";
    static_cast<void>(::close(fd));
    return false;
  }
  std::vector<std::uint8_t> header(kFileHeaderSize);
  if (!readAll(fd, header, 0, error) ||
      !std::equal(kFileMagic.begin(), kFileMagic.end(), header.begin()) ||
      read32(header.data() + 8) != kVersion ||
      read32(header.data() + 12) != 0) {
    if (error.empty()) {
      error = "Raft journal header is invalid";
    }
    static_cast<void>(::close(fd));
    return false;
  }
  file_fd_ = fd;
  file_size_ = static_cast<std::size_t>(status.st_size == 0
                                            ? kFileHeaderSize
                                            : status.st_size);
  return true;
}

bool FileRaftPersistence::load(RaftPersistentState& state,
                               std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!openLocked(error)) {
    return false;
  }
  struct stat status {};
  if (::fstat(file_fd_, &status) != 0) {
    error = std::string("failed to stat Raft journal: ") +
            std::strerror(errno);
    return false;
  }
  off_t offset = static_cast<off_t>(kFileHeaderSize);
  RaftPersistentState latest;
  delta_record_count_ = 0;
  while (offset < status.st_size) {
    const off_t remaining = status.st_size - offset;
    if (remaining < static_cast<off_t>(kRecordHeaderSize)) {
      if (::ftruncate(file_fd_, offset) != 0 ||
          ::fdatasync(file_fd_) != 0) {
        error = std::string("failed to repair Raft journal tail: ") +
                std::strerror(errno);
        return false;
      }
      break;
    }
    std::vector<std::uint8_t> header(kRecordHeaderSize);
    if (!readAll(file_fd_, header, offset, error)) {
      return false;
    }
    const bool is_full =
        std::equal(kRecordMagic.begin(), kRecordMagic.end(), header.begin());
    const bool is_delta = std::equal(kDeltaRecordMagic.begin(),
                                     kDeltaRecordMagic.end(),
                                     header.begin());
    if (!is_full && !is_delta) {
      error = "Raft journal record magic is invalid";
      return false;
    }
    const std::size_t payload_size = read32(header.data() + 4);
    if (payload_size == 0 || payload_size > kMaximumPayloadSize) {
      error = "Raft journal record length is invalid";
      return false;
    }
    const off_t record_size =
        static_cast<off_t>(kRecordHeaderSize + payload_size);
    if (record_size > remaining) {
      if (::ftruncate(file_fd_, offset) != 0 ||
          ::fdatasync(file_fd_) != 0) {
        error = std::string("failed to repair Raft journal tail: ") +
                std::strerror(errno);
        return false;
      }
      break;
    }
    std::vector<std::uint8_t> payload(payload_size);
    if (!readAll(file_fd_, payload,
                 offset + static_cast<off_t>(kRecordHeaderSize), error)) {
      return false;
    }
    if (crc32c(payload) != read32(header.data() + 8)) {
      error = "Raft journal checksum mismatch";
      return false;
    }
    if ((is_full && !decodeState(payload, latest, error)) ||
        (is_delta && !decodeDelta(payload, latest, error))) {
      return false;
    }
    if (is_full) {
      delta_record_count_ = 0;
    } else {
      ++delta_record_count_;
    }
    offset += record_size;
  }
  file_size_ = static_cast<std::size_t>(offset);
  state = latest;
  if (latest.entries.empty()) {
    last_state_.reset();
  } else {
    last_state_ = std::move(latest);
  }
  error.clear();
  return true;
}

bool FileRaftPersistence::save(const RaftPersistentState& state,
                               std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!openLocked(error)) {
    return false;
  }
  std::vector<std::uint8_t> payload;
  const bool write_delta = last_state_.has_value();
  if ((!write_delta && !encodeState(state, payload, error)) ||
      (write_delta &&
       !encodeDelta(*last_state_, state, payload, error))) {
    return false;
  }
  const auto& magic = write_delta ? kDeltaRecordMagic : kRecordMagic;
  std::vector<std::uint8_t> record(magic.begin(), magic.end());
  append32(record, static_cast<std::uint32_t>(payload.size()));
  append32(record, crc32c(payload));
  record.insert(record.end(), payload.begin(), payload.end());
  const off_t offset = ::lseek(file_fd_, 0, SEEK_END);
  if (offset < 0 || !writeAll(file_fd_, record, offset, error)) {
    if (error.empty()) {
      error = std::string("failed to seek Raft journal: ") +
              std::strerror(errno);
    }
    return false;
  }
  if (::fdatasync(file_fd_) != 0) {
    error = std::string("failed to sync Raft journal: ") +
            std::strerror(errno);
    return false;
  }
  last_state_ = state;
  file_size_ = static_cast<std::size_t>(offset) + record.size();
  if (write_delta) {
    ++delta_record_count_;
  } else {
    delta_record_count_ = 0;
  }
  if (delta_record_count_ >= options_.maximum_delta_records ||
      file_size_ >= options_.maximum_file_bytes) {
    if (!compactLocked(state, error)) {
      return false;
    }
  }
  error.clear();
  return true;
}

bool FileRaftPersistence::compactLocked(
    const RaftPersistentState& state, std::string& error) {
  std::vector<std::uint8_t> payload;
  if (!encodeState(state, payload, error)) {
    return false;
  }
  std::vector<std::uint8_t> image(kFileMagic.begin(), kFileMagic.end());
  append32(image, kVersion);
  append32(image, 0);
  image.insert(image.end(), kRecordMagic.begin(), kRecordMagic.end());
  append32(image, static_cast<std::uint32_t>(payload.size()));
  append32(image, crc32c(payload));
  image.insert(image.end(), payload.begin(), payload.end());

  const std::string temporary_path = options_.path + ".compact.tmp";
  const int compact_fd =
      ::open(temporary_path.c_str(),
             O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
  if (compact_fd < 0) {
    error = std::string("failed to open compact Raft journal: ") +
            std::strerror(errno);
    return false;
  }
  if (::flock(compact_fd, LOCK_EX | LOCK_NB) != 0) {
    error = std::string("failed to lock compact Raft journal: ") +
            std::strerror(errno);
    static_cast<void>(::close(compact_fd));
    static_cast<void>(::unlink(temporary_path.c_str()));
    return false;
  }
  if (!writeAll(compact_fd, image, 0, error) ||
      ::fdatasync(compact_fd) != 0) {
    if (error.empty()) {
      error = std::string("failed to sync compact Raft journal: ") +
              std::strerror(errno);
    }
    static_cast<void>(::flock(compact_fd, LOCK_UN));
    static_cast<void>(::close(compact_fd));
    static_cast<void>(::unlink(temporary_path.c_str()));
    return false;
  }
  if (::rename(temporary_path.c_str(), options_.path.c_str()) != 0) {
    error = std::string("failed to publish compact Raft journal: ") +
            std::strerror(errno);
    static_cast<void>(::flock(compact_fd, LOCK_UN));
    static_cast<void>(::close(compact_fd));
    static_cast<void>(::unlink(temporary_path.c_str()));
    return false;
  }
  if (!syncParentDirectory(options_.path, error)) {
    static_cast<void>(::flock(compact_fd, LOCK_UN));
    static_cast<void>(::close(compact_fd));
    return false;
  }

  static_cast<void>(::flock(file_fd_, LOCK_UN));
  static_cast<void>(::close(file_fd_));
  file_fd_ = compact_fd;
  delta_record_count_ = 0;
  file_size_ = image.size();
  return true;
}

}  // namespace distributed_kv::raft
