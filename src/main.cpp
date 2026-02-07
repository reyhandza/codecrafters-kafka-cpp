#include <arpa/inet.h>
#include <cstdint>
#include <iostream>
#include <netdb.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using NULLABLE_STRING = std::optional<std::string>;
constexpr int BUFFER_SIZE = 1024;

struct HttpRequest {
  std::int16_t request_api_key;
  std::int16_t request_api_version;
  std::int32_t correlation_id;
  NULLABLE_STRING client_id;
};

class Server {
public:
  static int createSocket() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
      std::cerr << "Failed to create server socket: " << std::endl;
      return -1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
      close(server_fd);
      std::cerr << "setsockopt failed: " << std::endl;
      return -1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0) {
      close(server_fd);
      std::cerr << "Failed to bind to port 9092" << std::endl;
      return -1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
      close(server_fd);
      std::cerr << "listen failed" << std::endl;
      return -1;
    }

    std::cout << "Waiting for a client to connect...\n";

    return server_fd;
  }
};

int main(int argc, char *argv[]) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  struct sockaddr_in client_addr{};
  socklen_t client_addr_len = sizeof(client_addr);

  std::cerr << "Logs from your program will appear here!\n";

  int server_fd = Server::createSocket();
  int client_fd =
      accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
  std::cout << "Client connected\n";

  char buffer[BUFFER_SIZE] = {0};
  size_t bytes = read(client_fd, buffer, sizeof(buffer) - 1);

  std::string_view raw_buffer(buffer, bytes);
  std::cout << raw_buffer << std::endl;

  const std::int32_t message_size{0};
  const std::int32_t correlation_id{7};

  const std::int32_t message_size_be = htonl(message_size);
  const std::int32_t correlation_id_be = htonl(correlation_id);

  send(client_fd, &message_size_be, sizeof(message_size_be), 0);
  send(client_fd, &correlation_id_be, sizeof(message_size_be), 0);

  close(client_fd);

  close(server_fd);
  return 0;
}
