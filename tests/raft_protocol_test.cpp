#include "network/raft_protocol.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace distributed_kv::network {
namespace {

// Verifies AppendEntries preserves source, metadata, and binary commands.
TEST(RaftProtocolTest, RoundTripsAppendEntries) {
  raft::AppendEntriesRequest request;
  request.term = 5;
  request.leader_id = 2;
  request.previous_log_index = 8;
  request.previous_log_term = 4;
  request.leader_commit = 7;
  request.entries = {
      raft::LogEntry{9, 5, raft::EntryType::kCommand,
                     std::string("a\0b", 3)},
  };
  std::vector<std::uint8_t> frame;
  std::string error;
  ASSERT_TRUE(
      RaftProtocol::encode(2, raft::RpcPayload{request}, frame, error))
      << error;

  RaftMessage message;
  EXPECT_EQ(RaftProtocol::tryDecode(frame, message, error),
            RaftDecodeStatus::kComplete)
      << error;
  EXPECT_EQ(message.source, 2U);
  ASSERT_TRUE(
      std::holds_alternative<raft::AppendEntriesRequest>(message.payload));
  const auto& decoded =
      std::get<raft::AppendEntriesRequest>(message.payload);
  EXPECT_EQ(decoded.term, request.term);
  EXPECT_EQ(decoded.leader_id, request.leader_id);
  EXPECT_EQ(decoded.previous_log_index, request.previous_log_index);
  EXPECT_EQ(decoded.previous_log_term, request.previous_log_term);
  EXPECT_EQ(decoded.leader_commit, request.leader_commit);
  ASSERT_EQ(decoded.entries.size(), 1U);
  EXPECT_EQ(decoded.entries.front(), request.entries.front());
  EXPECT_TRUE(frame.empty());
}

// Verifies stream decoding waits for the entire length-prefixed frame.
TEST(RaftProtocolTest, WaitsForPartialFrame) {
  raft::RequestVoteRequest request{3, 1, 4, 2};
  std::vector<std::uint8_t> frame;
  std::string error;
  ASSERT_TRUE(
      RaftProtocol::encode(1, raft::RpcPayload{request}, frame, error));
  std::vector<std::uint8_t> partial(frame.begin(), frame.end() - 1);
  RaftMessage message;
  EXPECT_EQ(RaftProtocol::tryDecode(partial, message, error),
            RaftDecodeStatus::kNeedMoreData);
  partial.push_back(frame.back());
  EXPECT_EQ(RaftProtocol::tryDecode(partial, message, error),
            RaftDecodeStatus::kComplete)
      << error;
}

// Verifies oversized entry batches are rejected before allocation or send.
TEST(RaftProtocolTest, RejectsOversizedEntryBatch) {
  raft::AppendEntriesRequest request;
  request.entries.resize(RaftProtocol::kMaximumEntriesPerFrame + 1U);
  std::vector<std::uint8_t> frame;
  std::string error;
  EXPECT_FALSE(
      RaftProtocol::encode(1, raft::RpcPayload{request}, frame, error));
  EXPECT_FALSE(error.empty());
}

// Verifies all response correlation and conflict fields survive encoding.
TEST(RaftProtocolTest, RoundTripsAppendResponse) {
  raft::AppendEntriesResponse response{
      8, false, 12, 3, 11, 6, raft::Term{4}};
  std::vector<std::uint8_t> frame;
  std::string error;
  ASSERT_TRUE(
      RaftProtocol::encode(3, raft::RpcPayload{response}, frame, error));
  RaftMessage message;
  ASSERT_EQ(RaftProtocol::tryDecode(frame, message, error),
            RaftDecodeStatus::kComplete)
      << error;
  const auto& decoded =
      std::get<raft::AppendEntriesResponse>(message.payload);
  EXPECT_EQ(decoded.term, response.term);
  EXPECT_EQ(decoded.request_previous_log_index,
            response.request_previous_log_index);
  EXPECT_EQ(decoded.request_entry_count, response.request_entry_count);
  EXPECT_EQ(decoded.conflict_term, response.conflict_term);
}

// Verifies InstallSnapshot chunk metadata and binary payload round-trip.
TEST(RaftProtocolTest, RoundTripsInstallSnapshot) {
  raft::InstallSnapshotRequest request;
  request.term = 7;
  request.leader_id = 2;
  request.last_included_index = 120;
  request.last_included_term = 5;
  request.offset = 4096;
  request.done = true;
  request.data = std::string("snap\0chunk", 10);
  std::vector<std::uint8_t> frame;
  std::string error;
  ASSERT_TRUE(
      RaftProtocol::encode(2, raft::RpcPayload{request}, frame, error))
      << error;

  RaftMessage message;
  EXPECT_EQ(RaftProtocol::tryDecode(frame, message, error),
            RaftDecodeStatus::kComplete)
      << error;
  EXPECT_EQ(message.source, 2U);
  ASSERT_TRUE(
      std::holds_alternative<raft::InstallSnapshotRequest>(message.payload));
  const auto& decoded =
      std::get<raft::InstallSnapshotRequest>(message.payload);
  EXPECT_EQ(decoded.term, request.term);
  EXPECT_EQ(decoded.leader_id, request.leader_id);
  EXPECT_EQ(decoded.last_included_index, request.last_included_index);
  EXPECT_EQ(decoded.last_included_term, request.last_included_term);
  EXPECT_EQ(decoded.offset, request.offset);
  EXPECT_EQ(decoded.done, request.done);
  EXPECT_EQ(decoded.data, request.data);
  EXPECT_TRUE(frame.empty());
}

// Verifies oversized snapshot chunks are rejected before send.
TEST(RaftProtocolTest, RejectsOversizedSnapshotChunk) {
  raft::InstallSnapshotRequest request;
  request.data.assign(RaftProtocol::kMaximumSnapshotChunkSize + 1U, 'x');
  std::vector<std::uint8_t> frame;
  std::string error;
  EXPECT_FALSE(
      RaftProtocol::encode(1, raft::RpcPayload{request}, frame, error));
  EXPECT_FALSE(error.empty());
}

// Verifies InstallSnapshot response fields survive encoding.
TEST(RaftProtocolTest, RoundTripsInstallSnapshotResponse) {
  raft::InstallSnapshotResponse response{9, true};
  std::vector<std::uint8_t> frame;
  std::string error;
  ASSERT_TRUE(
      RaftProtocol::encode(3, raft::RpcPayload{response}, frame, error));
  RaftMessage message;
  ASSERT_EQ(RaftProtocol::tryDecode(frame, message, error),
            RaftDecodeStatus::kComplete)
      << error;
  const auto& decoded =
      std::get<raft::InstallSnapshotResponse>(message.payload);
  EXPECT_EQ(decoded.term, response.term);
  EXPECT_EQ(decoded.success, response.success);
}

}  // namespace
}  // namespace distributed_kv::network
