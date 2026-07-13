#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "network/tcp_client.h"
#include "server/node_service.h"

namespace {

using Clock = std::chrono::steady_clock;

/**
 * Reserves and releases one loopback TCP port for immediate benchmark use.
 *
 * Input: none. Output: non-zero host-order port or throws.
 * Thread safety: benchmark setup thread only.
 */
std::uint16_t allocatePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create port-reservation socket");
  }
  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
      0) {
    static_cast<void>(::close(fd));
    throw std::runtime_error("failed to reserve loopback port");
  }
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) !=
      0) {
    static_cast<void>(::close(fd));
    throw std::runtime_error("failed to inspect reserved port");
  }
  const std::uint16_t port = ntohs(address.sin_port);
  static_cast<void>(::close(fd));
  return port;
}

/**
 * Owns an isolated three-node in-process cluster using real TCP sockets.
 *
 * Each NodeService still runs Client IO, Peer IO, and Raft event threads.
 */
class BenchmarkCluster final {
 public:
  explicit BenchmarkCluster(std::uint64_t read_batch_window_ms)
      : read_batch_window_ms_(read_batch_window_ms) {
    std::array<char, 64> directory_template{};
    const std::string prefix = "/tmp/distributed-kv-bench-XXXXXX";
    std::copy(prefix.begin(), prefix.end(), directory_template.begin());
    char* created = ::mkdtemp(directory_template.data());
    if (created == nullptr) {
      throw std::runtime_error("failed to create benchmark directory");
    }
    root_ = created;
    for (distributed_kv::raft::NodeId id = 1; id <= 3; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      if (::mkdir(directory.c_str(), 0750) != 0) {
        throw std::runtime_error("failed to create node directory");
      }
      members_.push_back(distributed_kv::server::ClusterNodeEndpoint{
          id, "127.0.0.1", allocatePort(), "127.0.0.1", allocatePort()});
    }
  }

  ~BenchmarkCluster() {
    for (auto& node : nodes_) {
      node->stop();
    }
    nodes_.clear();
    for (distributed_kv::raft::NodeId id = 1; id <= 3; ++id) {
      const std::string directory =
          root_ + "/node" + std::to_string(id);
      static_cast<void>(::unlink((directory + "/raft.wal").c_str()));
      static_cast<void>(::rmdir(directory.c_str()));
    }
    static_cast<void>(::rmdir(root_.c_str()));
  }

  BenchmarkCluster(const BenchmarkCluster&) = delete;
  BenchmarkCluster& operator=(const BenchmarkCluster&) = delete;

  /**
   * Starts all nodes and waits for election/no-op commitment.
   *
   * Input/output: none; throws on listener failure.
   * Thread safety: benchmark setup thread only.
   */
  void start() {
    for (distributed_kv::raft::NodeId id = 1; id <= 3; ++id) {
      distributed_kv::server::NodeServiceConfig config;
      config.node_id = id;
      config.data_directory =
          root_ + "/node" + std::to_string(id);
      config.members = members_;
      config.election_timeout_min_ms = 150 + id * 40;
      config.election_timeout_max_ms = 250 + id * 40;
      config.heartbeat_interval_ms = 50;
      config.read_batch_window_ms = read_batch_window_ms_;
      auto node =
          std::make_unique<distributed_kv::server::NodeService>(
              std::move(config));
      std::string error;
      if (!node->start(error)) {
        throw std::runtime_error("node start failed: " + error);
      }
      nodes_.push_back(std::move(node));
    }
    ::usleep(900000);
  }

  /**
   * Creates a client configured with every cluster endpoint.
   *
   * Input: total per-operation retry deadline.
   * Output: independent client with stable deduplication identity.
   * Thread safety: returned client is used by benchmark thread only.
   */
  distributed_kv::network::TcpClient makeClient() const {
    distributed_kv::network::ClientConfig config;
    config.timeout = std::chrono::milliseconds(5000);
    config.maximum_attempts = 100;
    for (const auto& member : members_) {
      config.endpoints.push_back(
          distributed_kv::network::ClientEndpoint{
              member.client_host, member.client_port});
    }
    return distributed_kv::network::TcpClient(std::move(config));
  }

  /**
   * Aggregates read request and quorum-barrier counters across all nodes.
   *
   * Input: none. Output: point-in-time cluster totals.
   * Thread safety: NodeService metrics are atomic.
   */
  distributed_kv::server::NodeServiceMetrics metrics() const {
    distributed_kv::server::NodeServiceMetrics total;
    for (const auto& node : nodes_) {
      const auto current = node->metrics();
      total.read_requests += current.read_requests;
      total.read_barriers += current.read_barriers;
    }
    return total;
  }

 private:
  std::string root_;
  std::uint64_t read_batch_window_ms_{0};
  std::vector<distributed_kv::server::ClusterNodeEndpoint> members_;
  std::vector<std::unique_ptr<distributed_kv::server::NodeService>> nodes_;
};

struct Metrics {
  double throughput{0.0};
  std::int64_t p50_us{0};
  std::int64_t p99_us{0};
};

/**
 * Summarizes completed operation latencies.
 *
 * Input: microsecond samples and wall duration. Output: throughput/P50/P99.
 * Thread safety: local immutable samples after workload completion.
 */
Metrics summarize(std::vector<std::int64_t> samples,
                  std::chrono::duration<double> duration) {
  std::sort(samples.begin(), samples.end());
  const auto percentile = [&samples](std::size_t numerator) {
    const std::size_t index =
        ((samples.size() - 1U) * numerator) / 100U;
    return samples[index];
  };
  return Metrics{
      static_cast<double>(samples.size()) / duration.count(),
      percentile(50),
      percentile(99),
  };
}

/**
 * Prints one stable machine-readable benchmark row.
 *
 * Input: operation name, count, and metrics. Output: one stdout line.
 * Thread safety: benchmark main thread only.
 */
void printMetrics(const std::string& operation, std::size_t count,
                  const Metrics& metrics) {
  std::cout << operation << " count=" << count << " throughput="
            << std::fixed << std::setprecision(2) << metrics.throughput
            << "_ops_s p50=" << metrics.p50_us
            << "_us p99=" << metrics.p99_us << "_us\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::size_t operation_count = 200;
    std::size_t concurrency = 1;
    std::uint64_t read_batch_window_ms = 0;
    if (argc >= 2) {
      operation_count = static_cast<std::size_t>(std::stoull(argv[1]));
    }
    if (argc >= 3) {
      concurrency = static_cast<std::size_t>(std::stoull(argv[2]));
    }
    if (argc >= 4) {
      read_batch_window_ms = std::stoull(argv[3]);
    }
    if (argc > 4 || operation_count == 0 || concurrency == 0) {
      std::cerr << "usage: dkv_cluster_benchmark [operation-count] "
                   "[GET-concurrency] [read-batch-window-ms]\n";
      return 2;
    }

    BenchmarkCluster cluster(read_batch_window_ms);
    cluster.start();
    auto client = cluster.makeClient();
    distributed_kv::network::Response response;
    std::string error;
    std::vector<std::int64_t> set_samples;
    set_samples.reserve(operation_count);
    const std::string value(64, 'x');
    auto workload_start = Clock::now();
    for (std::size_t index = 0; index < operation_count; ++index) {
      const auto start = Clock::now();
      if (!client.set("key-" + std::to_string(index), value, response,
                      error) ||
          response.status != distributed_kv::network::StatusCode::kOk) {
        throw std::runtime_error("SET failed: " + error);
      }
      set_samples.push_back(
          std::chrono::duration_cast<std::chrono::microseconds>(
              Clock::now() - start)
              .count());
    }
    printMetrics(
        "SET", operation_count,
        summarize(std::move(set_samples), Clock::now() - workload_start));

    std::vector<std::int64_t> get_samples;
    get_samples.reserve(operation_count);
    std::mutex samples_mutex;
    std::atomic<std::size_t> next_index{0};
    std::atomic<std::size_t> read_failures{0};
    workload_start = Clock::now();
    std::vector<std::thread> readers;
    readers.reserve(concurrency);
    for (std::size_t worker = 0; worker < concurrency; ++worker) {
      readers.emplace_back([&client, &get_samples, &samples_mutex,
                            &next_index, &read_failures,
                            operation_count] {
        while (true) {
          const std::size_t index = next_index.fetch_add(1);
          if (index >= operation_count) {
            return;
          }
          distributed_kv::network::Response read_response;
          std::string read_error;
          const auto start = Clock::now();
          if (!client.get("key-" + std::to_string(index), read_response,
                          read_error) ||
              read_response.status !=
                  distributed_kv::network::StatusCode::kOk) {
            read_failures.fetch_add(1);
            continue;
          }
          const auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  Clock::now() - start)
                  .count();
          std::lock_guard<std::mutex> lock(samples_mutex);
          get_samples.push_back(latency);
        }
      });
    }
    for (auto& reader : readers) {
      reader.join();
    }
    if (read_failures.load() != 0 ||
        get_samples.size() != operation_count) {
      throw std::runtime_error("concurrent GET workload failed");
    }
    printMetrics(
        "GET", operation_count,
        summarize(std::move(get_samples), Clock::now() - workload_start));
    const auto metrics = cluster.metrics();
    std::cout << "READ_BATCH requests=" << metrics.read_requests
              << " barriers=" << metrics.read_barriers << '\n';
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "benchmark failed: " << exception.what() << '\n';
    return 1;
  }
}
