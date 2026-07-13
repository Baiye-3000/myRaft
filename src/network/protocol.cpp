#include "network/protocol.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace distributed_kv::network {
namespace {

constexpr std::array<std::uint8_t, 4> kMagic{{'D', 'K', 'V', '1'}};
constexpr std::uint8_t kVersion = 1;
constexpr std::uint16_t kResponseFlag = 1;

// Input: output buffer and host-order value. Output: appended network-order
// bytes. Thread safety: safe for distinct buffers.
void appendUint16(std::vector<std::uint8_t>& output, std::uint16_t value) {
  output.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
  output.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

// Input: output buffer and host-order value. Output: appended network-order
// bytes. Thread safety: safe for distinct buffers.
void appendUint32(std::vector<std::uint8_t>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(
        static_cast<std::uint8_t>((value >> static_cast<unsigned>(shift)) &
                                  0xffU));
  }
}

// Input: output buffer and host-order value. Output: appended network-order
// bytes. Thread safety: safe for distinct buffers.
void appendUint64(std::vector<std::uint8_t>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(
        static_cast<std::uint8_t>((value >> static_cast<unsigned>(shift)) &
                                  0xffU));
  }
}

// Input: at least two readable bytes. Output: host-order integer.
// Thread safety: stateless.
std::uint16_t readUint16(const std::uint8_t* input) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(input[0]) << 8U) |
      static_cast<std::uint16_t>(input[1]));
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

// Input: output buffer and string within protocol limits. Output: length-prefixed
// bytes. Thread safety: safe for distinct buffers.
void appendString(std::vector<std::uint8_t>& output,
                  const std::string& value) {
  appendUint32(output, static_cast<std::uint32_t>(value.size()));
  for (const char character : value) {
    output.push_back(static_cast<std::uint8_t>(character));
  }
}

// Input: body, cursor and size limit. Output: decoded string and advanced
// cursor, or error. Thread safety: safe for distinct output objects.
bool readString(const std::vector<std::uint8_t>& body, std::size_t& offset,
                std::size_t maximum_size, std::string& value,
                std::string& error) {
  if (offset > body.size() ||
      body.size() - offset < sizeof(std::uint32_t)) {
    error = "missing string length";
    return false;
  }

  const auto length = static_cast<std::size_t>(readUint32(body.data() + offset));
  offset += sizeof(std::uint32_t);
  if (length > maximum_size || length > body.size() - offset) {
    error = "invalid string length";
    return false;
  }

  value.assign(reinterpret_cast<const char*>(body.data() + offset), length);
  offset += length;
  return true;
}

// Input: message type. Output: whether it is a supported request.
// Thread safety: stateless.
bool isRequestType(MessageType type) {
  return type == MessageType::kSetRequest ||
         type == MessageType::kGetRequest ||
         type == MessageType::kDeleteRequest;
}

// Input: validated frame fields and body. Output: complete encoded frame or
// error. Thread safety: safe for distinct output objects.
bool encodeFrame(MessageType type, std::uint16_t flags,
                 std::uint64_t request_id,
                 const std::vector<std::uint8_t>& body,
                 std::vector<std::uint8_t>& output, std::string& error) {
  if (body.size() > kMaxBodySize ||
      body.size() > std::numeric_limits<std::uint32_t>::max()) {
    error = "message body exceeds protocol limit";
    return false;
  }

  output.clear();
  output.reserve(kProtocolHeaderSize + body.size());
  output.insert(output.end(), kMagic.begin(), kMagic.end());
  output.push_back(kVersion);
  output.push_back(static_cast<std::uint8_t>(type));
  appendUint16(output, flags);
  appendUint32(output, static_cast<std::uint32_t>(body.size()));
  appendUint64(output, request_id);
  output.insert(output.end(), body.begin(), body.end());
  error.clear();
  return true;
}

}  // namespace

bool Protocol::encodeRequest(const Request& request,
                             std::vector<std::uint8_t>& output,
                             std::string& error) {
  if (!isRequestType(request.type)) {
    error = "unsupported request type";
    return false;
  }
  if (request.client_id == 0 || request.request_id == 0) {
    error = "client and request ids must be nonzero";
    return false;
  }
  if (request.key.size() > kMaxKeySize) {
    error = "key exceeds protocol limit";
    return false;
  }
  if (request.type != MessageType::kSetRequest && !request.value.empty()) {
    error = "GET/DELETE request must not contain a value";
    return false;
  }
  if (request.value.size() > kMaxValueSize) {
    error = "value exceeds protocol limit";
    return false;
  }

  std::vector<std::uint8_t> body;
  body.reserve(sizeof(std::uint64_t) + sizeof(std::uint32_t) +
               request.key.size() +
               sizeof(std::uint32_t) + request.value.size());
  appendUint64(body, request.client_id);
  appendString(body, request.key);
  if (request.type == MessageType::kSetRequest) {
    appendString(body, request.value);
  }
  return encodeFrame(request.type, 0, request.request_id, body, output, error);
}

bool Protocol::encodeResponse(const Response& response,
                              std::vector<std::uint8_t>& output,
                              std::string& error) {
  if (response.payload.size() > kMaxValueSize) {
    error = "response payload exceeds protocol limit";
    return false;
  }
  if (response.leader_host.size() > kMaxHostSize ||
      ((response.leader_host.empty() && response.leader_port != 0) ||
       (!response.leader_host.empty() && response.leader_port == 0))) {
    error = "response leader endpoint is invalid";
    return false;
  }

  std::vector<std::uint8_t> body;
  body.reserve(1U + sizeof(std::uint32_t) + response.payload.size() +
               sizeof(std::uint32_t) + response.leader_host.size() +
               sizeof(std::uint16_t));
  body.push_back(static_cast<std::uint8_t>(response.status));
  appendString(body, response.payload);
  appendString(body, response.leader_host);
  appendUint16(body, response.leader_port);
  return encodeFrame(MessageType::kResponse, kResponseFlag,
                     response.request_id, body, output, error);
}

DecodeStatus Protocol::tryDecode(std::vector<std::uint8_t>& buffer,
                                 Frame& frame, std::string& error) {
  if (buffer.size() < kProtocolHeaderSize) {
    return DecodeStatus::kNeedMoreData;
  }
  if (!std::equal(kMagic.begin(), kMagic.end(), buffer.begin())) {
    error = "invalid protocol magic";
    return DecodeStatus::kError;
  }
  if (buffer[4] != kVersion) {
    error = "unsupported protocol version";
    return DecodeStatus::kError;
  }

  const auto type = static_cast<MessageType>(buffer[5]);
  const std::uint16_t flags = readUint16(buffer.data() + 6);
  if ((!isRequestType(type) || flags != 0) &&
      (type != MessageType::kResponse || flags != kResponseFlag)) {
    error = "invalid message type or flags";
    return DecodeStatus::kError;
  }

  const auto body_size =
      static_cast<std::size_t>(readUint32(buffer.data() + 8));
  if (body_size > kMaxBodySize) {
    error = "message body exceeds protocol limit";
    return DecodeStatus::kError;
  }
  const std::size_t frame_size = kProtocolHeaderSize + body_size;
  if (buffer.size() < frame_size) {
    return DecodeStatus::kNeedMoreData;
  }

  frame.type = type;
  frame.flags = flags;
  frame.request_id = readUint64(buffer.data() + 12);
  frame.body.assign(buffer.begin() +
                        static_cast<std::vector<std::uint8_t>::difference_type>(
                            kProtocolHeaderSize),
                    buffer.begin() +
                        static_cast<std::vector<std::uint8_t>::difference_type>(
                            frame_size));
  buffer.erase(buffer.begin(),
               buffer.begin() +
                   static_cast<std::vector<std::uint8_t>::difference_type>(
                       frame_size));
  error.clear();
  return DecodeStatus::kComplete;
}

bool Protocol::decodeRequest(const Frame& frame, Request& request,
                             std::string& error) {
  if (!isRequestType(frame.type) || frame.flags != 0) {
    error = "frame is not a request";
    return false;
  }

  if (frame.body.size() < sizeof(std::uint64_t)) {
    error = "request is missing client id";
    return false;
  }
  const std::uint64_t client_id = readUint64(frame.body.data());
  if (client_id == 0 || frame.request_id == 0) {
    error = "client and request ids must be nonzero";
    return false;
  }
  std::size_t offset = sizeof(std::uint64_t);
  std::string key;
  std::string value;
  if (!readString(frame.body, offset, kMaxKeySize, key, error)) {
    return false;
  }
  if (frame.type == MessageType::kSetRequest &&
      !readString(frame.body, offset, kMaxValueSize, value, error)) {
    return false;
  }
  if (offset != frame.body.size()) {
    error = "request contains trailing bytes";
    return false;
  }

  request = Request{frame.type, frame.request_id, std::move(key),
                    std::move(value), client_id};
  error.clear();
  return true;
}

bool Protocol::decodeResponse(const Frame& frame, Response& response,
                              std::string& error) {
  if (frame.type != MessageType::kResponse ||
      frame.flags != kResponseFlag || frame.body.empty()) {
    error = "frame is not a valid response";
    return false;
  }

  const auto status = static_cast<StatusCode>(frame.body[0]);
  if (status > StatusCode::kUnavailable) {
    error = "unknown response status";
    return false;
  }
  std::size_t offset = 1;
  std::string payload;
  std::string leader_host;
  if (!readString(frame.body, offset, kMaxValueSize, payload, error) ||
      !readString(frame.body, offset, kMaxHostSize, leader_host, error) ||
      frame.body.size() - offset != sizeof(std::uint16_t)) {
    if (error.empty()) {
      error = "response contains trailing bytes";
    }
    return false;
  }
  const std::uint16_t leader_port =
      readUint16(frame.body.data() + offset);
  if ((leader_host.empty() && leader_port != 0) ||
      (!leader_host.empty() && leader_port == 0)) {
    error = "response leader endpoint is invalid";
    return false;
  }

  response = Response{frame.request_id, status, std::move(payload),
                      std::move(leader_host), leader_port};
  error.clear();
  return true;
}

}  // namespace distributed_kv::network
