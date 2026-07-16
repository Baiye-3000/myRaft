#include "network/protocol.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::network {
namespace {

// Verifies a SET request survives a complete encode/decode round trip.
TEST(ProtocolTest, RoundTripsSetRequest) {
  const Request original{
      MessageType::kSetRequest, 42, "name", "tom",
  };
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(original, bytes, error)) << error;

  Frame frame;
  EXPECT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Request decoded;
  ASSERT_TRUE(Protocol::decodeRequest(frame, decoded, error)) << error;

  EXPECT_EQ(decoded.type, original.type);
  EXPECT_EQ(decoded.request_id, original.request_id);
  EXPECT_EQ(decoded.client_id, original.client_id);
  EXPECT_EQ(decoded.key, original.key);
  EXPECT_EQ(decoded.value, original.value);
  EXPECT_TRUE(bytes.empty());
}

// Verifies partial TCP data is retained until an entire frame is available.
TEST(ProtocolTest, WaitsForCompleteFrame) {
  const Request request{
      MessageType::kGetRequest, 7, "name", "",
  };
  std::vector<std::uint8_t> encoded;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(request, encoded, error)) << error;

  std::vector<std::uint8_t> partial(encoded.begin(),
                                    encoded.begin() + 5);
  Frame frame;
  EXPECT_EQ(Protocol::tryDecode(partial, frame, error),
            DecodeStatus::kNeedMoreData);
  EXPECT_EQ(partial.size(), 5U);

  partial.insert(partial.end(), encoded.begin() + 5, encoded.end());
  EXPECT_EQ(Protocol::tryDecode(partial, frame, error),
            DecodeStatus::kComplete);
}

// Verifies the decoder consumes only one frame from a pipelined byte stream.
TEST(ProtocolTest, DecodesPipelinedFramesIndividually) {
  std::vector<std::uint8_t> first;
  std::vector<std::uint8_t> second;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(
      Request{MessageType::kGetRequest, 1, "first", ""}, first, error));
  ASSERT_TRUE(Protocol::encodeRequest(
      Request{MessageType::kGetRequest, 2, "second", ""}, second, error));
  first.insert(first.end(), second.begin(), second.end());

  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(first, frame, error),
            DecodeStatus::kComplete);
  EXPECT_EQ(frame.request_id, 1U);
  ASSERT_EQ(Protocol::tryDecode(first, frame, error),
            DecodeStatus::kComplete);
  EXPECT_EQ(frame.request_id, 2U);
  EXPECT_TRUE(first.empty());
}

// Verifies response status and payload are preserved by the protocol.
TEST(ProtocolTest, RoundTripsResponse) {
  const Response original{99, StatusCode::kNotFound, "missing key"};
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeResponse(original, bytes, error)) << error;

  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Response decoded;
  ASSERT_TRUE(Protocol::decodeResponse(frame, decoded, error)) << error;

  EXPECT_EQ(decoded.request_id, original.request_id);
  EXPECT_EQ(decoded.status, original.status);
  EXPECT_EQ(decoded.payload, original.payload);
}

// Verifies DELETE carries identity/key without an illegal value.
TEST(ProtocolTest, RoundTripsDeleteRequest) {
  const Request expected{MessageType::kDeleteRequest, 77, "name", "", 9};
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(expected, bytes, error)) << error;
  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Request actual;
  ASSERT_TRUE(Protocol::decodeRequest(frame, actual, error)) << error;
  EXPECT_EQ(actual.type, MessageType::kDeleteRequest);
  EXPECT_EQ(actual.client_id, expected.client_id);
  EXPECT_EQ(actual.key, expected.key);
  EXPECT_TRUE(actual.value.empty());
}

// Verifies NOT_LEADER uses a structured endpoint instead of payload parsing.
TEST(ProtocolTest, RoundTripsLeaderHint) {
  const Response expected{88, StatusCode::kNotLeader, "NOT_LEADER",
                          "127.0.0.1", 7200};
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeResponse(expected, bytes, error)) << error;
  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Response actual;
  ASSERT_TRUE(Protocol::decodeResponse(frame, actual, error)) << error;
  EXPECT_EQ(actual.status, StatusCode::kNotLeader);
  EXPECT_EQ(actual.leader_host, "127.0.0.1");
  EXPECT_EQ(actual.leader_port, 7200);
}

// Verifies ADD_NODE transports a complete client and peer endpoint.
TEST(ProtocolTest, RoundTripsAddNodeRequest) {
  Request expected;
  expected.type = MessageType::kAddNodeRequest;
  expected.request_id = 101;
  expected.client_id = 9;
  expected.operation_id = 9001;
  expected.node_id = 4;
  expected.client_host = "127.0.0.1";
  expected.client_port = 7004;
  expected.peer_host = "127.0.0.1";
  expected.peer_port = 8004;
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(expected, bytes, error)) << error;
  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Request actual;
  ASSERT_TRUE(Protocol::decodeRequest(frame, actual, error)) << error;
  EXPECT_EQ(actual.node_id, expected.node_id);
  EXPECT_EQ(actual.operation_id, expected.operation_id);
  EXPECT_EQ(actual.client_host, expected.client_host);
  EXPECT_EQ(actual.client_port, expected.client_port);
  EXPECT_EQ(actual.peer_host, expected.peer_host);
  EXPECT_EQ(actual.peer_port, expected.peer_port);
}

TEST(ProtocolTest, RoundTripsRemoveNodeOperationId) {
  Request expected;
  expected.type = MessageType::kRemoveNodeRequest;
  expected.request_id = 102;
  expected.client_id = 9;
  expected.operation_id = 9002;
  expected.node_id = 4;
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(expected, bytes, error)) << error;
  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Request actual;
  ASSERT_TRUE(Protocol::decodeRequest(frame, actual, error)) << error;
  EXPECT_EQ(actual.operation_id, expected.operation_id);
  EXPECT_EQ(actual.node_id, expected.node_id);
}

TEST(ProtocolTest, RejectsInvalidMembershipOperationIds) {
  Request add;
  add.type = MessageType::kAddNodeRequest;
  add.request_id = 103;
  add.client_id = 9;
  add.node_id = 4;
  add.client_host = "127.0.0.1";
  add.client_port = 7004;
  add.peer_host = "127.0.0.1";
  add.peer_port = 8004;
  std::vector<std::uint8_t> bytes;
  std::string error;
  EXPECT_FALSE(Protocol::encodeRequest(add, bytes, error));

  Request get{MessageType::kGetRequest, 104, "key", "", 9};
  get.operation_id = 9003;
  EXPECT_FALSE(Protocol::encodeRequest(get, bytes, error));
}

// Verifies LIST_MEMBERS responses carry machine-readable discovery endpoints.
TEST(ProtocolTest, RoundTripsMemberDiscovery) {
  const std::vector<MemberEndpoint> members{
      {1, "127.0.0.1", 7001, "127.0.0.1", 8001},
      {2, "127.0.0.1", 7002, "127.0.0.1", 8002}};
  const Response expected{102, StatusCode::kOk, "OK", {}, 0, members};
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeResponse(expected, bytes, error)) << error;
  Frame frame;
  ASSERT_EQ(Protocol::tryDecode(bytes, frame, error),
            DecodeStatus::kComplete);
  Response actual;
  ASSERT_TRUE(Protocol::decodeResponse(frame, actual, error)) << error;
  EXPECT_EQ(actual.members, members);
}

// Verifies malformed traffic is rejected before body allocation or parsing.
TEST(ProtocolTest, RejectsInvalidMagic) {
  std::vector<std::uint8_t> bytes;
  std::string error;
  ASSERT_TRUE(Protocol::encodeRequest(
      Request{MessageType::kGetRequest, 1, "key", ""}, bytes, error));
  bytes[0] = 0;

  Frame frame;
  EXPECT_EQ(Protocol::tryDecode(bytes, frame, error), DecodeStatus::kError);
  EXPECT_EQ(error, "invalid protocol magic");
}

// Verifies encoder limits prevent oversized user-controlled allocations.
TEST(ProtocolTest, RejectsOversizedKey) {
  const Request request{
      MessageType::kGetRequest, 1, std::string(kMaxKeySize + 1U, 'x'), "",
  };
  std::vector<std::uint8_t> bytes;
  std::string error;

  EXPECT_FALSE(Protocol::encodeRequest(request, bytes, error));
  EXPECT_EQ(error, "key exceeds protocol limit");
}

}  // namespace
}  // namespace distributed_kv::network
