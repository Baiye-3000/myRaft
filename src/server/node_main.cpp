#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "server/node_service.h"

namespace {

std::atomic<bool> g_stop{false};

void handleSignal(int) { g_stop.store(true); }

std::uint64_t parseUnsigned(const std::string& text,
                            const std::string& name) {
  std::size_t consumed = 0;
  const unsigned long long value = std::stoull(text, &consumed);
  if (consumed != text.size() || value == 0) {
    throw std::invalid_argument("invalid " + name);
  }
  return static_cast<std::uint64_t>(value);
}

std::vector<std::string> split(const std::string& text, char separator) {
  std::vector<std::string> parts;
  std::size_t begin = 0;
  while (true) {
    const std::size_t end = text.find(separator, begin);
    parts.push_back(text.substr(begin, end - begin));
    if (end == std::string::npos) {
      return parts;
    }
    begin = end + 1U;
  }
}

distributed_kv::server::ClusterNodeEndpoint parseMember(
    const std::string& specification) {
  const std::vector<std::string> fields = split(specification, ',');
  if (fields.size() != 5) {
    throw std::invalid_argument("member must be id,client-host,client-port,"
                                "peer-host,peer-port");
  }
  const auto client_port = parseUnsigned(fields[2], "client port");
  const auto peer_port = parseUnsigned(fields[4], "peer port");
  if (client_port > 65535U || peer_port > 65535U) {
    throw std::invalid_argument("member port exceeds 65535");
  }
  return distributed_kv::server::ClusterNodeEndpoint{
      parseUnsigned(fields[0], "member id"),
      fields[1],
      static_cast<std::uint16_t>(client_port),
      fields[3],
      static_cast<std::uint16_t>(peer_port),
  };
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 5) {
    std::cerr << "usage: dkv_node <node-id> <data-dir> "
                 "<member> <member> [member...]\n"
                 "member: id,client-host,client-port,peer-host,peer-port\n";
    return 2;
  }
  try {
    distributed_kv::server::NodeServiceConfig config;
    config.node_id = parseUnsigned(argv[1], "node id");
    config.data_directory = argv[2];
    for (int index = 3; index < argc; ++index) {
      config.members.push_back(parseMember(argv[index]));
    }
    std::filesystem::create_directories(config.data_directory);

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);
    distributed_kv::server::NodeService service(std::move(config));
    std::string error;
    if (!service.start(error)) {
      std::cerr << "node start failed: " << error << '\n';
      return 1;
    }
    while (!g_stop.load() && !service.stopped()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    service.stop();
    if (!service.wait(error)) {
      std::cerr << "node stopped after fatal error: " << error << '\n';
      return 1;
    }
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "invalid node configuration: " << exception.what() << '\n';
    return 2;
  }
}
