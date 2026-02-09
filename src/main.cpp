#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

constexpr int BUFFER_SIZE = 1024;

struct RequestHeader { // 24 bytes
  std::uint32_t message_size;
  std::int16_t request_api_key;
  std::int16_t request_api_version;
  std::uint32_t correlation_id;
  char client_id_length[2];
  char client_id_contents[9];
  std::int8_t TAG_BUFFER;
};

struct RequestBody { // 15 bytes
  char client_id_length[1];
  char client_id_contents[9];
  std::uint8_t software_version;
  char software_version_contents[3];
  std::int8_t TAG_BUFFER;
};

struct Request {
  RequestHeader header;
  RequestBody body;
}__attribute__((packed));

struct Response { // 23 bytes
  std::uint32_t message_size;
  std::uint32_t correlation_id;
  std::int16_t error_code;
  std::int8_t api_key_array_length;
  std::int16_t api_key;
  std::int16_t min_version;
  std::int16_t max_version;
  std::int8_t TAG_BUFFER1;
  std::int32_t throttle_time_ms;
  std::int8_t TAG_BUFFER2;
} __attribute__((packed));

Request parse_buffer(const char* buffer) {
    return *reinterpret_cast<const Request*>(buffer);
}

Response set_response(const Request& req) {
  Response resp {};
  
  resp.message_size = htonl(sizeof(Response)); 
  resp.correlation_id = req.header.correlation_id;

  if (ntohs(req.header.request_api_version) > 4 || ntohs(req.header.request_api_version) < 0) {
    resp.error_code = htons(35);
  }

  resp.api_key_array_length = 0x02;
  resp.api_key = htons(18);
  resp.max_version = htons(4);

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

    std::cout << "sizeof(RequestHeader): " << sizeof(RequestHeader) << std::endl;
    std::cout << "sizeof(RequestBody): " << sizeof(RequestBody) << std::endl;
    std::cout << "sizeof(Request): " << sizeof(Request) << std::endl;
    std::cout << "sizeof(Response): " << sizeof(Response) << std::endl;

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

    Request req = parse_buffer(buffer);
    Response resp = set_response(req);

    write(client_fd, &resp, sizeof(resp));
  }

  close(server_fd);

  return 0;
}
