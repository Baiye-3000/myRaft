#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>

#include "network/server.h"
#include "storage/kv_store.h"
#include "storage/wal.h"

namespace {

/**
 * Parses a decimal TCP port without accepting trailing characters.
 *
 * Input: C string and writable port.
 * Output: true for a value in [1, 65535].
 * Thread safety: uses no shared state.
 */
bool parsePort(const char* text, std::uint16_t& port) {
  if (text == nullptr || *text == '\0') {
    return false;
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long value = std::strtoul(text, &end, 10);
  if (errno != 0 || *end != '\0' || value == 0 ||
      value > std::numeric_limits<std::uint16_t>::max()) {
    return false;
  }
  port = static_cast<std::uint16_t>(value);
  return true;
}

/**
 * Applies validated WAL records to an initially empty KVStore.
 *
 * Input: ordered records, target store and writable error text.
 * Output: true after all commands are applied.
 * Thread safety: startup thread only; no concurrent store access is allowed.
 */
bool replayRecords(
    const std::vector<distributed_kv::storage::WalRecord>& records,
    distributed_kv::storage::KVStore& store, std::string& error) {
  try {
    for (const auto& record : records) {
      if (record.operation == distributed_kv::storage::WalOperation::kSet) {
        const bool inserted = store.put(record.key, record.value);
        static_cast<void>(inserted);
      } else if (record.operation ==
                 distributed_kv::storage::WalOperation::kRemove) {
        const bool removed = store.remove(record.key);
        static_cast<void>(removed);
      } else {
        error = "recovered WAL contains an unsupported operation";
        return false;
      }
    }
  } catch (const std::exception&) {
    error = "failed to rebuild KVStore from WAL";
    return false;
  }
  error.clear();
  return true;
}

}  // namespace

/**
 * Starts one persistent standalone KV server and waits for SIGINT/SIGTERM.
 *
 * Input: optional bind IPv4 address, port, and WAL path arguments.
 * Output: zero after orderly shutdown, nonzero on configuration/runtime error.
 * Thread safety: main owns resources; a signal-wait thread only calls stop().
 */
int main(int argc, char* argv[]) {
  using distributed_kv::network::Server;
  using distributed_kv::network::ServerConfig;
  using distributed_kv::storage::KVStore;
  using distributed_kv::storage::WAL;
  using distributed_kv::storage::WalOptions;
  using distributed_kv::storage::WalRecord;
  using distributed_kv::storage::WalSyncPolicy;

  if (argc > 4) {
    std::cerr << "Usage: dkv_server [bind-address] [port] [wal-path]\n";
    return 2;
  }

  ServerConfig config;
  if (argc >= 2) {
    config.bind_address = argv[1];
  }
  if (argc >= 3 && !parsePort(argv[2], config.port)) {
    std::cerr << "Invalid TCP port\n";
    return 2;
  }
  const std::string wal_path =
      argc == 4 ? argv[3] : "distributed-kv.wal";

  sigset_t signals;
  ::sigemptyset(&signals);
  ::sigaddset(&signals, SIGINT);
  ::sigaddset(&signals, SIGTERM);
  if (::pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
    std::cerr << "Failed to block shutdown signals\n";
    return 1;
  }

  KVStore store;
  WAL wal(WalOptions{wal_path, WalSyncPolicy::kAlways});
  std::string error;
  std::vector<WalRecord> records;
  if (!wal.open(error) || !wal.recover(records, error) ||
      !replayRecords(records, store, error)) {
    std::cerr << "WAL recovery failed: " << error << '\n';
    return 1;
  }

  Server server(config, store, &wal);
  if (!server.start(error)) {
    std::cerr << "Server start failed: " << error << '\n';
    return 1;
  }

  std::cout << "DistributedKV listening on " << config.bind_address << ':'
            << server.boundPort() << ", recovered " << records.size()
            << " WAL record(s) from " << wal.path() << '\n';
  std::thread signal_waiter([&server, &signals] {
    int received_signal = 0;
    if (::sigwait(&signals, &received_signal) == 0) {
      server.stop();
    }
  });

  const bool stopped_orderly = server.run(error);
  if (!stopped_orderly) {
    ::pthread_kill(signal_waiter.native_handle(), SIGTERM);
  }
  signal_waiter.join();

  if (!stopped_orderly) {
    std::cerr << "Server stopped after error: " << error << '\n';
    return 1;
  }
  std::cout << "DistributedKV stopped\n";
  return 0;
}
