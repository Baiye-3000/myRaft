#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

#include <unistd.h>

#include "network/tcp_client.h"

namespace {

std::uint64_t parseUnsigned(const char* text, const char* name) {
  if (text == nullptr || *text == '\0') {
    throw std::invalid_argument(std::string("invalid ") + name);
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long long value = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value == 0) {
    throw std::invalid_argument(std::string("invalid ") + name);
  }
  return static_cast<std::uint64_t>(value);
}

std::uint16_t parsePort(const char* text, const char* name) {
  const std::uint64_t value = parseUnsigned(text, name);
  if (value > std::numeric_limits<std::uint16_t>::max()) {
    throw std::invalid_argument(std::string(name) + " exceeds 65535");
  }
  return static_cast<std::uint16_t>(value);
}

void usage() {
  std::cerr
      << "usage:\n"
         "  dkv_admin <host> <port> show-members\n"
         "  dkv_admin <host> <port> add-node <id> <client-host> "
         "<client-port> <peer-host> <peer-port> [operation-id]\n"
         "  dkv_admin <host> <port> remove-node <id> [operation-id]\n";
}

std::uint64_t generateOperationId() {
  std::random_device entropy;
  std::uint64_t value =
      (static_cast<std::uint64_t>(entropy()) << 32U) ^ entropy();
  value ^= static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  value ^= static_cast<std::uint64_t>(::getpid());
  return value == 0 ? 1 : value;
}

void printMembers(const distributed_kv::network::Response& response) {
  for (const auto& member : response.members) {
    std::cout << member.node_id << ' ' << member.client_host << ':'
              << member.client_port << ' ' << member.peer_host << ':'
              << member.peer_port << '\n';
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    usage();
    return 2;
  }
  try {
    distributed_kv::network::ClientConfig config;
    config.host = argv[1];
    config.port = parsePort(argv[2], "server port");
    config.timeout = std::chrono::seconds(60);
    config.maximum_attempts = 10;
    distributed_kv::network::TcpClient client(std::move(config));
    distributed_kv::network::Response response;
    std::string error;
    const std::string command = argv[3];
    bool completed = false;
    if (command == "show-members" && argc == 4) {
      completed = client.listMembers(response, error);
    } else if (command == "add-node" && (argc == 9 || argc == 10)) {
      const distributed_kv::network::MemberEndpoint member{
          parseUnsigned(argv[4], "node id"), argv[5],
          parsePort(argv[6], "client port"), argv[7],
          parsePort(argv[8], "peer port")};
      const std::uint64_t operation_id =
          argc == 10 ? parseUnsigned(argv[9], "operation id")
                     : generateOperationId();
      std::cerr << "operation-id: " << operation_id << '\n';
      completed = client.addNode(member, response, error, operation_id);
    } else if (command == "remove-node" && (argc == 5 || argc == 6)) {
      const std::uint64_t operation_id =
          argc == 6 ? parseUnsigned(argv[5], "operation id")
                    : generateOperationId();
      std::cerr << "operation-id: " << operation_id << '\n';
      completed = client.removeNode(parseUnsigned(argv[4], "node id"),
                                    response, error, operation_id);
    } else {
      usage();
      return 2;
    }
    if (!completed) {
      std::cerr << "admin request failed: " << error << '\n';
      return 1;
    }
    if (response.status != distributed_kv::network::StatusCode::kOk) {
      std::cerr << "admin request rejected: " << response.payload << '\n';
      return 1;
    }
    printMembers(response);
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "invalid arguments: " << exception.what() << '\n';
    return 2;
  }
}
