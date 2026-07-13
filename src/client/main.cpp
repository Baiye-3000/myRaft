#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

#include "network/tcp_client.h"

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
 * Converts an ASCII command token to upper case.
 *
 * Input: command token.
 * Output: normalized copy.
 * Thread safety: uses no shared state.
 */
std::string normalizeCommand(std::string command) {
  std::transform(command.begin(), command.end(), command.begin(),
                 [](unsigned char character) {
                   return static_cast<char>(std::toupper(character));
                 });
  return command;
}

/**
 * Parses and executes one interactive command.
 *
 * Input: command line and connected endpoint configuration held by client.
 * Output: true to continue the REPL, false for QUIT/EXIT.
 * Thread safety: intended for the single CLI thread.
 */
bool executeCommand(const std::string& line,
                    distributed_kv::network::TcpClient& client) {
  std::istringstream input(line);
  std::string command;
  input >> command;
  command = normalizeCommand(std::move(command));
  if (command.empty()) {
    return true;
  }
  if (command == "QUIT" || command == "EXIT") {
    return false;
  }

  std::string key;
  std::string value;
  std::string trailing;
  distributed_kv::network::Response response;
  std::string error;

  if (command == "SET" && (input >> key >> value) &&
      !(input >> trailing)) {
    if (!client.set(key, value, response, error)) {
      std::cerr << "ERROR " << error << '\n';
    } else {
      std::cout << response.payload << '\n';
    }
    return true;
  }
  if (command == "GET" && (input >> key) && !(input >> trailing)) {
    if (!client.get(key, response, error)) {
      std::cerr << "ERROR " << error << '\n';
    } else {
      std::cout << response.payload << '\n';
    }
    return true;
  }
  if (command == "DELETE" && (input >> key) && !(input >> trailing)) {
    if (!client.remove(key, response, error)) {
      std::cerr << "ERROR " << error << '\n';
    } else {
      std::cout << response.payload << '\n';
    }
    return true;
  }

  std::cerr << "Usage: SET <key> <value> | GET <key> | DELETE <key> | QUIT\n";
  return true;
}

}  // namespace

/**
 * Runs the interactive DistributedKV command-line client.
 *
 * Input: optional server IPv4 address and TCP port.
 * Output: zero after normal exit, nonzero for invalid startup arguments.
 * Thread safety: all console interaction occurs on the main thread.
 */
int main(int argc, char* argv[]) {
  using distributed_kv::network::ClientConfig;
  using distributed_kv::network::TcpClient;

  if (argc > 3) {
    std::cerr << "Usage: dkv_client [server-address] [port]\n";
    return 2;
  }

  ClientConfig config;
  if (argc >= 2) {
    config.host = argv[1];
  }
  if (argc == 3 && !parsePort(argv[2], config.port)) {
    std::cerr << "Invalid TCP port\n";
    return 2;
  }

  TcpClient client(config);
  std::cout << "Server endpoint: " << config.host << ':' << config.port
            << "\nCommands: SET <key> <value>, GET <key>, DELETE <key>, QUIT\n";

  std::string line;
  while (std::cout << "> " && std::getline(std::cin, line)) {
    if (!executeCommand(line, client)) {
      break;
    }
  }
  return 0;
}
