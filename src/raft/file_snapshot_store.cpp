#include "raft/file_snapshot_store.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace distributed_kv::raft {
namespace {

constexpr std::array<std::uint8_t, 8> kMagic{
    {'D', 'K', 'V', 'S', 'N', 'A', 'P', '\0'}};
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kHeaderSize = 20;
constexpr std::size_t kMaximumPayloadSize = 256U * 1024U * 1024U;
constexpr std::size_t kMaximumItemCount = 1000000U;
constexpr std::size_t kMaximumKeySize = 4096U;
constexpr std::size_t kMaximumValueSize = 1024U * 1024U;
constexpr std::size_t kMaximumResponseSize = 1024U * 1024U;

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

bool appendString(std::vector<std::uint8_t>& output,
                  const std::string& value, std::size_t maximum,
                  std::string& error) {
  if (value.size() > maximum ||
      value.size() >
          static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    error = "snapshot string exceeds persistent limit";
    return false;
  }
  append32(output, static_cast<std::uint32_t>(value.size()));
  output.insert(output.end(), value.begin(), value.end());
  if (output.size() > kMaximumPayloadSize) {
    error = "snapshot exceeds persistent size limit";
    return false;
  }
  return true;
}

bool encode(const StateMachineSnapshot& snapshot,
            std::vector<std::uint8_t>& output, std::string& error) {
  if ((snapshot.last_included_index == 0) !=
          (snapshot.last_included_term == 0) ||
      snapshot.entries.size() > kMaximumItemCount ||
      snapshot.sessions.size() > kMaximumItemCount) {
    error = "invalid state-machine snapshot metadata";
    return false;
  }
  output.clear();
  append64(output, snapshot.last_included_index);
  append64(output, snapshot.last_included_term);
  append32(output, static_cast<std::uint32_t>(snapshot.entries.size()));
  std::unordered_set<std::string> keys;
  for (const auto& entry : snapshot.entries) {
    if (!keys.insert(entry.first).second) {
      error = "snapshot contains duplicate KV key";
      return false;
    }
    if (!appendString(output, entry.first, kMaximumKeySize, error) ||
        !appendString(output, entry.second, kMaximumValueSize, error)) {
      return false;
    }
  }
  append32(output, static_cast<std::uint32_t>(snapshot.sessions.size()));
  std::unordered_set<std::uint64_t> client_ids;
  for (const SnapshotSession& session : snapshot.sessions) {
    if (session.client_id == 0 || session.request_id == 0 ||
        (session.status != ApplyStatus::kOk &&
         session.status != ApplyStatus::kNotFound &&
         session.status != ApplyStatus::kStaleRequest) ||
        !client_ids.insert(session.client_id).second) {
      error = "invalid snapshot client session";
      return false;
    }
    append64(output, session.client_id);
    append64(output, session.request_id);
    output.push_back(static_cast<std::uint8_t>(session.status));
    if (!appendString(output, session.payload, kMaximumResponseSize, error)) {
      return false;
    }
  }
  return true;
}

bool takeString(const std::vector<std::uint8_t>& input, std::size_t& offset,
                std::size_t maximum, std::string& value,
                std::string& error) {
  if (input.size() - offset < 4) {
    error = "snapshot string length is truncated";
    return false;
  }
  const std::size_t length = read32(input.data() + offset);
  offset += 4;
  if (length > maximum || length > input.size() - offset) {
    error = "snapshot string has invalid length";
    return false;
  }
  value.assign(reinterpret_cast<const char*>(input.data() + offset), length);
  offset += length;
  return true;
}

bool decode(const std::vector<std::uint8_t>& input,
            StateMachineSnapshot& snapshot, std::string& error) {
  if (input.size() < 20) {
    error = "snapshot payload is truncated";
    return false;
  }
  StateMachineSnapshot decoded;
  std::size_t offset = 0;
  decoded.last_included_index = read64(input.data() + offset);
  offset += 8;
  decoded.last_included_term = read64(input.data() + offset);
  offset += 8;
  if ((decoded.last_included_index == 0) !=
      (decoded.last_included_term == 0)) {
    error = "snapshot index and term are inconsistent";
    return false;
  }
  const std::size_t entry_count = read32(input.data() + offset);
  offset += 4;
  if (entry_count > kMaximumItemCount) {
    error = "snapshot has too many KV entries";
    return false;
  }
  std::unordered_set<std::string> keys;
  decoded.entries.reserve(entry_count);
  for (std::size_t index = 0; index < entry_count; ++index) {
    std::string key;
    std::string value;
    if (!takeString(input, offset, kMaximumKeySize, key, error) ||
        !takeString(input, offset, kMaximumValueSize, value, error) ||
        !keys.insert(key).second) {
      if (error.empty()) {
        error = "snapshot contains duplicate KV key";
      }
      return false;
    }
    decoded.entries.emplace_back(std::move(key), std::move(value));
  }
  if (input.size() - offset < 4) {
    error = "snapshot session count is truncated";
    return false;
  }
  const std::size_t session_count = read32(input.data() + offset);
  offset += 4;
  if (session_count > kMaximumItemCount) {
    error = "snapshot has too many client sessions";
    return false;
  }
  std::unordered_set<std::uint64_t> client_ids;
  decoded.sessions.reserve(session_count);
  for (std::size_t index = 0; index < session_count; ++index) {
    if (input.size() - offset < 17) {
      error = "snapshot client session is truncated";
      return false;
    }
    SnapshotSession session;
    session.client_id = read64(input.data() + offset);
    offset += 8;
    session.request_id = read64(input.data() + offset);
    offset += 8;
    session.status = static_cast<ApplyStatus>(input[offset++]);
    if (session.client_id == 0 || session.request_id == 0 ||
        (session.status != ApplyStatus::kOk &&
         session.status != ApplyStatus::kNotFound &&
         session.status != ApplyStatus::kStaleRequest) ||
        !client_ids.insert(session.client_id).second ||
        !takeString(input, offset, kMaximumResponseSize, session.payload,
                    error)) {
      if (error.empty()) {
        error = "snapshot contains invalid client session";
      }
      return false;
    }
    decoded.sessions.push_back(std::move(session));
  }
  if (offset != input.size()) {
    error = "snapshot payload has trailing bytes";
    return false;
  }
  snapshot = std::move(decoded);
  return true;
}

bool writeAll(int fd, const std::vector<std::uint8_t>& bytes,
              std::string& error) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t result =
        ::write(fd, bytes.data() + written, bytes.size() - written);
    if (result > 0) {
      written += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      error = std::string("snapshot write failed: ") +
              (result == 0 ? "no progress" : std::strerror(errno));
      return false;
    }
  }
  return true;
}

bool readAll(int fd, std::vector<std::uint8_t>& bytes,
             std::string& error) {
  std::size_t count = 0;
  while (count < bytes.size()) {
    const ssize_t result =
        ::read(fd, bytes.data() + count, bytes.size() - count);
    if (result > 0) {
      count += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      error = result == 0 ? "snapshot file is truncated"
                          : std::string("snapshot read failed: ") +
                                std::strerror(errno);
      return false;
    }
  }
  return true;
}

std::string parentDirectory(const std::string& path) {
  const std::size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) {
    return ".";
  }
  return slash == 0 ? "/" : path.substr(0, slash);
}

}  // namespace

FileSnapshotStore::FileSnapshotStore(std::string path)
    : path_(std::move(path)) {}

bool FileSnapshotStore::save(const StateMachineSnapshot& snapshot,
                             std::string& error) const {
  std::vector<std::uint8_t> payload;
  if (path_.empty() || !encode(snapshot, payload, error)) {
    if (path_.empty()) {
      error = "snapshot path is empty";
    }
    return false;
  }
  std::vector<std::uint8_t> file(kMagic.begin(), kMagic.end());
  append32(file, kVersion);
  append32(file, static_cast<std::uint32_t>(payload.size()));
  append32(file, crc32c(payload));
  file.insert(file.end(), payload.begin(), payload.end());

  const std::string temporary = path_ + ".tmp";
  int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                  S_IRUSR | S_IWUSR);
  if (fd < 0) {
    error = std::string("snapshot temporary open failed: ") +
            std::strerror(errno);
    return false;
  }
  bool success = writeAll(fd, file, error);
  if (success && ::fdatasync(fd) != 0) {
    error = std::string("snapshot fdatasync failed: ") + std::strerror(errno);
    success = false;
  }
  if (::close(fd) != 0 && success) {
    error = std::string("snapshot close failed: ") + std::strerror(errno);
    success = false;
  }
  if (success && ::rename(temporary.c_str(), path_.c_str()) != 0) {
    error = std::string("snapshot rename failed: ") + std::strerror(errno);
    success = false;
  }
  if (success) {
    const std::string parent = parentDirectory(path_);
    const int directory_fd =
        ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd < 0 || ::fsync(directory_fd) != 0) {
      error = std::string("snapshot directory sync failed: ") +
              std::strerror(errno);
      success = false;
    }
    if (directory_fd >= 0) {
      static_cast<void>(::close(directory_fd));
    }
  }
  if (!success) {
    static_cast<void>(::unlink(temporary.c_str()));
    return false;
  }
  error.clear();
  return true;
}

bool FileSnapshotStore::load(
    std::optional<StateMachineSnapshot>& snapshot,
    std::string& error) const {
  const int fd = ::open(path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      snapshot.reset();
      error.clear();
      return true;
    }
    error = std::string("snapshot open failed: ") + std::strerror(errno);
    return false;
  }
  struct stat status {};
  bool success = ::fstat(fd, &status) == 0;
  if (!success) {
    error = std::string("snapshot stat failed: ") + std::strerror(errno);
  } else if (status.st_size < static_cast<off_t>(kHeaderSize) ||
             status.st_size >
                 static_cast<off_t>(kHeaderSize + kMaximumPayloadSize)) {
    error = "snapshot file has invalid size";
    success = false;
  }
  std::vector<std::uint8_t> file;
  if (success) {
    file.resize(static_cast<std::size_t>(status.st_size));
    success = readAll(fd, file, error);
  }
  static_cast<void>(::close(fd));
  if (!success) {
    return false;
  }
  if (!std::equal(kMagic.begin(), kMagic.end(), file.begin()) ||
      read32(file.data() + 8) != kVersion) {
    error = "snapshot has invalid magic or version";
    return false;
  }
  const std::size_t payload_size = read32(file.data() + 12);
  if (payload_size != file.size() - kHeaderSize) {
    error = "snapshot payload length does not match file";
    return false;
  }
  std::vector<std::uint8_t> payload(file.begin() +
                                        static_cast<std::ptrdiff_t>(kHeaderSize),
                                    file.end());
  if (read32(file.data() + 16) != crc32c(payload)) {
    error = "snapshot checksum mismatch";
    return false;
  }
  StateMachineSnapshot decoded;
  if (!decode(payload, decoded, error)) {
    return false;
  }
  snapshot = std::move(decoded);
  error.clear();
  return true;
}

}  // namespace distributed_kv::raft
