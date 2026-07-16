#include "network/server.h"
#include "network/tcp_client.h"
#include "raft/raft_kv_service.h"
#include "storage/kv_store.h"
#include "storage/wal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <gtest/gtest.h>

namespace distributed_kv::network {
namespace {

class NetworkIntegrationTest : public ::testing::Test {
 protected:
  // Starts an epoll server on an OS-assigned loopback port.
  void SetUp() override {
    ServerConfig server_config;
    server_config.bind_address = "127.0.0.1";
    server_config.port = 0;
    server_ = std::make_unique<Server>(server_config, store_);

    std::string error;
    ASSERT_TRUE(server_->start(error)) << error;
    client_config_.host = "127.0.0.1";
    client_config_.port = server_->boundPort();
    client_config_.timeout = std::chrono::milliseconds(2000);

    server_thread_ = std::thread([this] {
      run_result_ = server_->run(run_error_);
    });
  }

  // Stops and joins the event loop before fixture-owned objects are destroyed.
  void TearDown() override {
    if (server_) {
      server_->stop();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    EXPECT_TRUE(run_result_) << run_error_;
  }

  storage::KVStore store_;
  std::unique_ptr<Server> server_;
  ClientConfig client_config_;
  std::thread server_thread_;
  bool run_result_{false};
  std::string run_error_;
};

// Verifies SET reaches KVStore and a later GET returns the committed value.
TEST_F(NetworkIntegrationTest, SetThenGetRoundTrip) {
  TcpClient client(client_config_);
  Response response;
  std::string error;

  ASSERT_TRUE(client.set("name", "tom", response, error)) << error;
  EXPECT_EQ(response.status, StatusCode::kOk);
  EXPECT_EQ(response.payload, "OK");

  ASSERT_TRUE(client.get("name", response, error)) << error;
  EXPECT_EQ(response.status, StatusCode::kOk);
  EXPECT_EQ(response.payload, "tom");
}

// Verifies a missing key is a protocol-level result, not a transport failure.
TEST_F(NetworkIntegrationTest, GetMissingKeyReturnsNotFound) {
  TcpClient client(client_config_);
  Response response;
  std::string error;

  ASSERT_TRUE(client.get("missing", response, error)) << error;
  EXPECT_EQ(response.status, StatusCode::kNotFound);
  EXPECT_EQ(response.payload, "NOT_FOUND");
}

// Verifies one epoll loop serves concurrent clients without losing writes.
TEST_F(NetworkIntegrationTest, HandlesConcurrentClients) {
  constexpr std::size_t kClientCount = 8;
  TcpClient client(client_config_);
  std::atomic<bool> failed{false};
  std::vector<std::thread> workers;
  workers.reserve(kClientCount);

  for (std::size_t index = 0; index < kClientCount; ++index) {
    workers.emplace_back([index, &client, &failed] {
      Response response;
      std::string error;
      const std::string key = "key-" + std::to_string(index);
      const std::string value = "value-" + std::to_string(index);
      if (!client.set(key, value, response, error) ||
          response.status != StatusCode::kOk) {
        failed.store(true, std::memory_order_relaxed);
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_EQ(store_.size(), kClientCount);
}

class PersistenceIntegrationTest : public ::testing::Test {
 protected:
  // Creates a unique WAL path for the restart test.
  void SetUp() override {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-restart-XXXXXX";
    ASSERT_LT(prefix.size(), directory_template.size());
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    directory_ = created;
    wal_path_ = directory_ + "/storage.wal";
  }

  // Removes the WAL after all local server/WAL objects have been destroyed.
  void TearDown() override {
    ::unlink(wal_path_.c_str());
    ::rmdir(directory_.c_str());
  }

  /**
   * Opens WAL, validates records, and rebuilds an empty store.
   *
   * Input: WAL, target store and writable error.
   * Output: true after complete recovery.
   * Thread safety: fixture thread only.
   */
  bool recoverStore(storage::WAL& wal, storage::KVStore& store,
                    std::string& error) {
    std::vector<storage::WalRecord> records;
    if (!wal.open(error) || !wal.recover(records, error)) {
      return false;
    }
    for (const auto& record : records) {
      if (record.operation == storage::WalOperation::kSet) {
        const bool inserted = store.put(record.key, record.value);
        static_cast<void>(inserted);
      } else {
        const bool removed = store.remove(record.key);
        static_cast<void>(removed);
      }
    }
    return true;
  }

  std::string directory_;
  std::string wal_path_;
};

// Verifies an acknowledged SET remains visible after full server reconstruction.
TEST_F(PersistenceIntegrationTest, RestoresAcknowledgedSetAfterRestart) {
  ServerConfig config;
  config.bind_address = "127.0.0.1";
  config.port = 0;

  {
    storage::KVStore store;
    storage::WAL wal(storage::WalOptions{
        wal_path_, storage::WalSyncPolicy::kAlways,
    });
    std::string error;
    ASSERT_TRUE(recoverStore(wal, store, error)) << error;

    Server server(config, store, &wal);
    ASSERT_TRUE(server.start(error)) << error;
    bool run_result = false;
    std::string run_error;
    std::thread server_thread(
        [&] { run_result = server.run(run_error); });

    TcpClient client(ClientConfig{
        "127.0.0.1", server.boundPort(), std::chrono::milliseconds(2000),
    });
    Response response;
    const bool request_result =
        client.set("persistent-key", "persistent-value", response, error);
    server.stop();
    server_thread.join();

    ASSERT_TRUE(run_result) << run_error;
    ASSERT_TRUE(request_result) << error;
    ASSERT_EQ(response.status, StatusCode::kOk);
  }

  {
    storage::KVStore store;
    storage::WAL wal(storage::WalOptions{
        wal_path_, storage::WalSyncPolicy::kAlways,
    });
    std::string error;
    ASSERT_TRUE(recoverStore(wal, store, error)) << error;

    Server server(config, store, &wal);
    ASSERT_TRUE(server.start(error)) << error;
    bool run_result = false;
    std::string run_error;
    std::thread server_thread(
        [&] { run_result = server.run(run_error); });

    TcpClient client(ClientConfig{
        "127.0.0.1", server.boundPort(), std::chrono::milliseconds(2000),
    });
    Response response;
    const bool request_result =
        client.get("persistent-key", response, error);
    server.stop();
    server_thread.join();

    ASSERT_TRUE(run_result) << run_error;
    ASSERT_TRUE(request_result) << error;
    EXPECT_EQ(response.status, StatusCode::kOk);
    EXPECT_EQ(response.payload, "persistent-value");
  }
}

// Verifies a TCP SET traverses Leader log, commit and StateMachine before OK.
TEST(NetworkRaftIntegrationTest, RoutesSetThroughSingleNodeRaft) {
  storage::KVStore store;
  raft::RaftKVService service(
      raft::NodeConfig{1, {}, std::nullopt, 100, 100, 20, 64, 1}, store);
  std::string error;
  const auto election = service.tick(100, error);
  ASSERT_TRUE(election.empty()) << error;
  ASSERT_EQ(service.raftNode().role(), raft::Role::kLeader);

  ServerConfig config;
  config.bind_address = "127.0.0.1";
  config.port = 0;
  Server server(config, [&service](const Request& request) {
    if (request.type == MessageType::kSetRequest) {
      std::string submit_error;
      const raft::SubmitResult submitted = service.submit(
          raft::KVCommand{
              raft::KVCommandType::kSet,
              request.client_id,
              request.request_id,
              request.key,
              request.value,
          },
          submit_error);
      if (submitted.status == raft::SubmitStatus::kApplied &&
          submitted.result.has_value()) {
        return Response{request.request_id, StatusCode::kOk,
                        submitted.result->payload};
      }
      return Response{request.request_id, StatusCode::kInternalError,
                      submit_error.empty() ? "NOT_LEADER" : submit_error};
    }
    const auto value = service.getApplied(request.key);
    return value.has_value()
               ? Response{request.request_id, StatusCode::kOk, *value}
               : Response{request.request_id, StatusCode::kNotFound,
                          "NOT_FOUND"};
  });
  ASSERT_TRUE(server.start(error)) << error;

  bool run_result = false;
  std::string run_error;
  std::thread server_thread([&] { run_result = server.run(run_error); });
  TcpClient client(ClientConfig{
      "127.0.0.1",
      server.boundPort(),
      std::chrono::milliseconds(2000),
      77,
  });
  Response response;
  const bool set_result = client.set("name", "tom", response, error);
  const bool get_result = client.get("name", response, error);
  server.stop();
  server_thread.join();

  ASSERT_TRUE(run_result) << run_error;
  ASSERT_TRUE(set_result) << error;
  ASSERT_TRUE(get_result) << error;
  EXPECT_EQ(response.payload, "tom");
  EXPECT_EQ(service.raftNode().commitIndex(), 2U);
  EXPECT_EQ(service.lastApplied(), 2U);
}

}  // namespace
}  // namespace distributed_kv::network
