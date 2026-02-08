#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

constexpr int BUFFER_SIZE = 1024;

struct HttpRequest {
  std::uint32_t message_size;
  std::int16_t request_api_key;
  std::int16_t request_api_version;
  std::uint32_t correlation_id;
};

struct HttpResponse {
  std::uint32_t message_size;
  std::uint32_t correlation_id;
  std::int16_t error_code;
};

HttpResponse parse_buffer(const char* buffer) {
  HttpResponse resp {};
  HttpRequest req {};

  ssize_t pos_correlation_id = 8;
  std::memcpy(&resp.correlation_id, buffer + pos_correlation_id, sizeof(resp.correlation_id));

  ssize_t pos_api_version = 6;
  std::memcpy(&req.request_api_version, buffer + pos_api_version, sizeof(req.request_api_version));

  if (ntohs(req.request_api_version) > 4) {
    resp.error_code = htonl(35);
  }

  return resp;
}

class Server {
public:
  static int createSocket() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
      std::cerr << "Failed to create server socket: " << std::endl;
      return -1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
      close(server_fd);
      std::cerr << "setsockopt failed: " << std::endl;
      return -1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
             sizeof(server_addr)) != 0) {
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
  std::cerr << "Logs from your program will appear here!\n";

  int server_fd = Server::createSocket();

  while (true) {
    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    int client_fd =
        accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
               &client_addr_len);
    std::cout << "Client connected\n";

    char buffer[BUFFER_SIZE] = {0};
    size_t complete_header = 12;
    ssize_t rec_header = read(client_fd, buffer, sizeof(buffer));

    if (rec_header < complete_header) {
      close(client_fd);
      continue;
    };

    HttpResponse response = parse_buffer(buffer);
    write(client_fd, &response, sizeof(response));
  }

  close(server_fd);

  return 0;
}
