#include "storage/wal.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace distributed_kv::storage {
namespace {

constexpr std::array<std::uint8_t, 8> kFileMagic{
    {'D', 'K', 'V', 'W', 'A', 'L', '1', '\0'}};
constexpr std::array<std::uint8_t, 4> kRecordMagic{{0xd1, 0x4b, 0x56, 0x52}};
constexpr std::uint32_t kFileVersion = 1;
constexpr std::size_t kFileHeaderSize = 16;
constexpr std::size_t kRecordHeaderSize = 12;
constexpr std::size_t kRecordMetadataSize = 20;
constexpr std::size_t kMaximumKeySize = 4U * 1024U;
constexpr std::size_t kMaximumValueSize = 1024U * 1024U;
constexpr std::size_t kMaximumPayloadSize =
    kRecordMetadataSize + kMaximumKeySize + kMaximumValueSize;

// Input: output buffer and host-order integer. Output: appended network-order
// bytes. Thread safety: safe for distinct buffers.
void appendUint32(std::vector<std::uint8_t>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(
        static_cast<std::uint8_t>((value >> static_cast<unsigned>(shift)) &
                                  0xffU));
  }
}

// Input: output buffer and host-order integer. Output: appended network-order
// bytes. Thread safety: safe for distinct buffers.
void appendUint64(std::vector<std::uint8_t>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(
        static_cast<std::uint8_t>((value >> static_cast<unsigned>(shift)) &
                                  0xffU));
  }
}

// Input: at least four readable bytes. Output: host-order integer.
// Thread safety: stateless.
std::uint32_t readUint32(const std::uint8_t* input) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = static_cast<std::uint32_t>(
        (value << 8U) | static_cast<std::uint32_t>(input[index]));
  }
  return value;
}

// Input: at least eight readable bytes. Output: host-order integer.
// Thread safety: stateless.
std::uint64_t readUint64(const std::uint8_t* input) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<std::uint64_t>(input[index]);
  }
  return value;
}

// Input: arbitrary bytes. Output: Castagnoli CRC32C.
// Thread safety: stateless.
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

// Input: fd, complete byte vector, starting offset and error output. Output:
// true only after all bytes are written. Thread safety: caller serializes fd.
bool writeAllAt(int file_fd, const std::vector<std::uint8_t>& bytes,
                off_t offset, std::string& error) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t result =
        ::pwrite(file_fd, bytes.data() + written, bytes.size() - written,
                 offset + static_cast<off_t>(written));
    if (result > 0) {
      written += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    error = result == 0 ? "WAL write made no progress"
                        : std::string("WAL write failed: ") +
                              std::strerror(errno);
    return false;
  }
  return true;
}

// Input: fd, writable vector, exact offset and error output. Output: true only
// after the whole vector is filled. Thread safety: concurrent pread is safe.
bool readAllAt(int file_fd, std::vector<std::uint8_t>& bytes, off_t offset,
               std::string& error) {
  std::size_t read_count = 0;
  while (read_count < bytes.size()) {
    const ssize_t result =
        ::pread(file_fd, bytes.data() + read_count, bytes.size() - read_count,
                offset + static_cast<off_t>(read_count));
    if (result > 0) {
      read_count += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    error = result == 0 ? "unexpected end of WAL"
                        : std::string("WAL read failed: ") +
                              std::strerror(errno);
    return false;
  }
  return true;
}

// Input: fd and last known-good offset. Output: truncated and synchronized
// file or error. Thread safety: caller exclusively owns WAL mutation.
bool truncateTail(int file_fd, off_t offset, std::string& error) {
  if (::ftruncate(file_fd, offset) != 0 || ::fdatasync(file_fd) != 0) {
    error = std::string("failed to truncate WAL tail: ") +
            std::strerror(errno);
    return false;
  }
  return true;
}

// Input: string. Output: raw bytes appended without a terminator.
// Thread safety: safe for distinct output vectors.
void appendStringBytes(std::vector<std::uint8_t>& output,
                       const std::string& value) {
  for (const char character : value) {
    output.push_back(static_cast<std::uint8_t>(character));
  }
}

// Input: newly initialized file path and writable error. Output: true after
// parent-directory metadata is durable. Thread safety: stateless.
bool syncParentDirectory(const std::string& path, std::string& error) {
  const std::size_t separator = path.find_last_of('/');
  const std::string directory =
      separator == std::string::npos
          ? "."
          : (separator == 0 ? "/" : path.substr(0, separator));
  const int directory_fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    error = std::string("failed to open WAL parent directory: ") +
            std::strerror(errno);
    return false;
  }
  if (::fsync(directory_fd) != 0) {
    const int failure = errno;
    ::close(directory_fd);
    error = std::string("failed to sync WAL parent directory: ") +
            std::strerror(failure);
    return false;
  }
  ::close(directory_fd);
  return true;
}

}  // namespace

WAL::WAL(WalOptions options) : options_(std::move(options)) {}

WAL::~WAL() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_fd_ >= 0) {
    ::flock(file_fd_, LOCK_UN);
    ::close(file_fd_);
  }
}

bool WAL::open(std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_fd_ >= 0) {
    error = "WAL is already open";
    return false;
  }
  if (options_.path.empty()) {
    error = "WAL path must not be empty";
    return false;
  }

  int descriptor =
      ::open(options_.path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0640);
  if (descriptor < 0) {
    error = std::string("failed to open WAL: ") + std::strerror(errno);
    return false;
  }
  if (::flock(descriptor, LOCK_EX | LOCK_NB) != 0) {
    error = std::string("failed to lock WAL: ") + std::strerror(errno);
    ::close(descriptor);
    return false;
  }

  struct stat file_status {};
  if (::fstat(descriptor, &file_status) != 0 ||
      !S_ISREG(file_status.st_mode)) {
    error = "WAL path is not a readable regular file";
    ::flock(descriptor, LOCK_UN);
    ::close(descriptor);
    return false;
  }

  if (file_status.st_size == 0) {
    std::vector<std::uint8_t> header(kFileMagic.begin(), kFileMagic.end());
    appendUint32(header, kFileVersion);
    appendUint32(header, static_cast<std::uint32_t>(kFileHeaderSize));
    if (!writeAllAt(descriptor, header, 0, error) ||
        ::fdatasync(descriptor) != 0) {
      if (error.empty()) {
        error = std::string("failed to sync WAL header: ") +
                std::strerror(errno);
      }
      ::flock(descriptor, LOCK_UN);
      ::close(descriptor);
      return false;
    }
    if (!syncParentDirectory(options_.path, error)) {
      ::flock(descriptor, LOCK_UN);
      ::close(descriptor);
      return false;
    }
  } else {
    if (file_status.st_size < static_cast<off_t>(kFileHeaderSize)) {
      error = "WAL file header is incomplete";
      ::flock(descriptor, LOCK_UN);
      ::close(descriptor);
      return false;
    }
    std::vector<std::uint8_t> header(kFileHeaderSize);
    if (!readAllAt(descriptor, header, 0, error) ||
        !std::equal(kFileMagic.begin(), kFileMagic.end(), header.begin()) ||
        readUint32(header.data() + 8) != kFileVersion ||
        readUint32(header.data() + 12) != kFileHeaderSize) {
      if (error.empty()) {
        error = "invalid WAL file header";
      }
      ::flock(descriptor, LOCK_UN);
      ::close(descriptor);
      return false;
    }
  }

  file_fd_ = descriptor;
  next_sequence_ = 1;
  recovered_ = false;
  failed_ = false;
  error.clear();
  return true;
}

bool WAL::recover(std::vector<WalRecord>& records, std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_fd_ < 0 || failed_) {
    error = "WAL is not open and healthy";
    return false;
  }

  struct stat file_status {};
  if (::fstat(file_fd_, &file_status) != 0) {
    error = std::string("failed to stat WAL: ") + std::strerror(errno);
    return false;
  }

  std::vector<WalRecord> recovered_records;
  off_t offset = static_cast<off_t>(kFileHeaderSize);
  std::uint64_t previous_sequence = 0;
  while (offset < file_status.st_size) {
    const off_t remaining = file_status.st_size - offset;
    if (remaining < static_cast<off_t>(kRecordHeaderSize)) {
      if (!truncateTail(file_fd_, offset, error)) {
        failed_ = true;
        return false;
      }
      break;
    }

    std::vector<std::uint8_t> header(kRecordHeaderSize);
    if (!readAllAt(file_fd_, header, offset, error)) {
      return false;
    }
    if (!std::equal(kRecordMagic.begin(), kRecordMagic.end(),
                    header.begin())) {
      error = "invalid WAL record magic";
      return false;
    }
    const std::size_t payload_size =
        static_cast<std::size_t>(readUint32(header.data() + 4));
    if (payload_size < kRecordMetadataSize ||
        payload_size > kMaximumPayloadSize) {
      error = "invalid WAL record length";
      return false;
    }
    const off_t record_size =
        static_cast<off_t>(kRecordHeaderSize + payload_size);
    if (remaining < record_size) {
      if (!truncateTail(file_fd_, offset, error)) {
        failed_ = true;
        return false;
      }
      break;
    }

    std::vector<std::uint8_t> payload(payload_size);
    if (!readAllAt(file_fd_, payload,
                   offset + static_cast<off_t>(kRecordHeaderSize), error)) {
      return false;
    }
    if (crc32c(payload) != readUint32(header.data() + 8)) {
      error = "WAL record checksum mismatch";
      return false;
    }

    const auto operation = static_cast<WalOperation>(payload[0]);
    if ((operation != WalOperation::kSet &&
         operation != WalOperation::kRemove) ||
        payload[1] != 0 || payload[2] != 0 || payload[3] != 0) {
      error = "invalid WAL operation metadata";
      return false;
    }
    const std::uint64_t sequence = readUint64(payload.data() + 4);
    const std::size_t key_size =
        static_cast<std::size_t>(readUint32(payload.data() + 12));
    const std::size_t value_size =
        static_cast<std::size_t>(readUint32(payload.data() + 16));
    if (sequence != previous_sequence + 1U || key_size > kMaximumKeySize ||
        value_size > kMaximumValueSize ||
        kRecordMetadataSize + key_size + value_size != payload.size() ||
        (operation == WalOperation::kRemove && value_size != 0)) {
      error = "invalid WAL record fields";
      return false;
    }

    const char* key_data = reinterpret_cast<const char*>(
        payload.data() + kRecordMetadataSize);
    const char* value_data = key_data + key_size;
    recovered_records.push_back(WalRecord{
        sequence, operation, std::string(key_data, key_size),
        std::string(value_data, value_size),
    });
    previous_sequence = sequence;
    offset += record_size;
  }

  if (previous_sequence == std::numeric_limits<std::uint64_t>::max()) {
    error = "WAL sequence space exhausted";
    return false;
  }
  next_sequence_ = previous_sequence + 1U;
  recovered_ = true;
  records = std::move(recovered_records);
  error.clear();
  return true;
}

bool WAL::appendSet(const std::string& key, const std::string& value,
                    std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  return appendLocked(WalOperation::kSet, key, value, error);
}

bool WAL::appendRemove(const std::string& key, std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  return appendLocked(WalOperation::kRemove, key, "", error);
}

const std::string& WAL::path() const noexcept { return options_.path; }

bool WAL::appendLocked(WalOperation operation, const std::string& key,
                       const std::string& value, std::string& error) {
  if (file_fd_ < 0 || !recovered_ || failed_) {
    error = "WAL must be open, recovered, and healthy before append";
    return false;
  }
  if (key.size() > kMaximumKeySize || value.size() > kMaximumValueSize ||
      (operation == WalOperation::kRemove && !value.empty())) {
    error = "WAL command exceeds storage limits";
    return false;
  }
  if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    error = "WAL sequence space exhausted";
    return false;
  }

  std::vector<std::uint8_t> payload;
  payload.reserve(kRecordMetadataSize + key.size() + value.size());
  payload.push_back(static_cast<std::uint8_t>(operation));
  payload.insert(payload.end(), 3, 0);
  appendUint64(payload, next_sequence_);
  appendUint32(payload, static_cast<std::uint32_t>(key.size()));
  appendUint32(payload, static_cast<std::uint32_t>(value.size()));
  appendStringBytes(payload, key);
  appendStringBytes(payload, value);

  std::vector<std::uint8_t> record(kRecordMagic.begin(), kRecordMagic.end());
  appendUint32(record, static_cast<std::uint32_t>(payload.size()));
  appendUint32(record, crc32c(payload));
  record.insert(record.end(), payload.begin(), payload.end());

  const off_t offset = ::lseek(file_fd_, 0, SEEK_END);
  if (offset < 0 || !writeAllAt(file_fd_, record, offset, error)) {
    if (error.empty()) {
      error = std::string("failed to seek WAL end: ") + std::strerror(errno);
    }
    markFailed(error, error);
    return false;
  }
  if (options_.sync_policy == WalSyncPolicy::kAlways &&
      ::fdatasync(file_fd_) != 0) {
    markFailed(std::string("failed to sync WAL record: ") +
                   std::strerror(errno),
               error);
    return false;
  }

  ++next_sequence_;
  error.clear();
  return true;
}

void WAL::markFailed(const std::string& message, std::string& error) {
  failed_ = true;
  error = message;
}

}  // namespace distributed_kv::storage
