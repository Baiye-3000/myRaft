#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace distributed_kv::network {

constexpr std::size_t kProtocolHeaderSize = 20;
constexpr std::size_t kMaxKeySize = 4U * 1024U;
constexpr std::size_t kMaxValueSize = 1024U * 1024U;
constexpr std::size_t kMaxHostSize = 255;
constexpr std::size_t kMaxBodySize =
    kMaxKeySize + kMaxValueSize + kMaxHostSize + 16U;

enum class MessageType : std::uint8_t {
  kSetRequest = 1,
  kGetRequest = 2,
  kDeleteRequest = 3,
  kAddNodeRequest = 4,
  kRemoveNodeRequest = 5,
  kListMembersRequest = 6,
  kResponse = 128,
};

enum class StatusCode : std::uint8_t {
  kOk = 0,
  kNotFound = 1,
  kInvalidRequest = 2,
  kInternalError = 3,
  kNotLeader = 4,
  kServerBusy = 5,
  kUnavailable = 6,
};

enum class DecodeStatus {
  kNeedMoreData,
  kComplete,
  kError,
};

struct Frame {
  MessageType type{MessageType::kGetRequest};
  std::uint16_t flags{0};
  std::uint64_t request_id{0};
  std::vector<std::uint8_t> body;
};

struct Request {
  MessageType type{MessageType::kGetRequest};
  std::uint64_t request_id{0};
  std::string key;
  std::string value;
  std::uint64_t client_id{1};
  std::uint64_t operation_id{0};
  std::uint64_t node_id{0};
  std::string client_host;
  std::uint16_t client_port{0};
  std::string peer_host;
  std::uint16_t peer_port{0};

  Request() = default;
  Request(MessageType request_type, std::uint64_t id, std::string request_key,
          std::string request_value, std::uint64_t requester_id = 1)
      : type(request_type),
        request_id(id),
        key(std::move(request_key)),
        value(std::move(request_value)),
        client_id(requester_id) {}
};

struct MemberEndpoint {
  std::uint64_t node_id{0};
  std::string client_host;
  std::uint16_t client_port{0};
  std::string peer_host;
  std::uint16_t peer_port{0};

  [[nodiscard]] bool operator==(const MemberEndpoint& other) const {
    return node_id == other.node_id && client_host == other.client_host &&
           client_port == other.client_port &&
           peer_host == other.peer_host && peer_port == other.peer_port;
  }
};

struct Response {
  std::uint64_t request_id{0};
  StatusCode status{StatusCode::kOk};
  std::string payload;
  std::string leader_host;
  std::uint16_t leader_port{0};
  std::vector<MemberEndpoint> members;

  Response() = default;
  Response(std::uint64_t id, StatusCode response_status,
           std::string response_payload, std::string host = {},
           std::uint16_t port = 0,
           std::vector<MemberEndpoint> response_members = {})
      : request_id(id),
        status(response_status),
        payload(std::move(response_payload)),
        leader_host(std::move(host)),
        leader_port(port),
        members(std::move(response_members)) {}
};

/**
 * Stateless encoder and incremental frame decoder for the wire protocol.
 *
 * Every integer is encoded in network byte order. Methods do not retain
 * references to caller-owned data and are safe to invoke concurrently.
 */
class Protocol final {
 public:
  Protocol() = delete;

  /**
   * Encodes one SET or GET request into a complete wire frame.
   *
   * Input: request and writable output/error objects.
   * Output: true with output replaced by the frame; false with an explanation.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool encodeRequest(const Request& request,
                                          std::vector<std::uint8_t>& output,
                                          std::string& error);

  /**
   * Encodes one server response into a complete wire frame.
   *
   * Input: response and writable output/error objects.
   * Output: true with output replaced by the frame; false with an explanation.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool encodeResponse(const Response& response,
                                           std::vector<std::uint8_t>& output,
                                           std::string& error);

  /**
   * Attempts to remove one complete frame from an incremental byte buffer.
   *
   * Input: buffer containing zero or more received bytes.
   * Output: complete frame and consumed bytes, need-more-data, or protocol
   * error. On incomplete/error input the buffer is unchanged.
   * Thread safety: safe across distinct buffers; callers must synchronize a
   * shared buffer.
   */
  [[nodiscard]] static DecodeStatus tryDecode(
      std::vector<std::uint8_t>& buffer, Frame& frame, std::string& error);

  /**
   * Converts a validated request frame into application fields.
   *
   * Input: a complete frame produced by tryDecode.
   * Output: true with request populated, or false with a validation error.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool decodeRequest(const Frame& frame, Request& request,
                                          std::string& error);

  /**
   * Converts a validated response frame into application fields.
   *
   * Input: a complete response frame produced by tryDecode.
   * Output: true with response populated, or false with a validation error.
   * Thread safety: stateless and safe for concurrent calls.
   */
  [[nodiscard]] static bool decodeResponse(const Frame& frame,
                                           Response& response,
                                           std::string& error);
};

}  // namespace distributed_kv::network
