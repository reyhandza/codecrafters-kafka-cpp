#include "server.hpp"
#include <iostream>
#include <thread>

constexpr int BUFFER_SIZE = 1024;
constexpr int REQUEST_SIZE = 39;

struct RequestHeader { // 24 bytes
  std::uint32_t message_size;
  std::int16_t request_api_key;
  std::int16_t request_api_version;
  std::uint32_t correlation_id;
  char client_id_length[2];
  char client_id_contents[9];
  std::int8_t TAG_BUFFER;
};

struct ApiVersionBodyRequest { // 15 bytes
  char client_id_length[1];
  char client_id_contents[9];
  std::uint8_t software_version;
  char software_version_contents[3];
  std::int8_t TAG_BUFFER;
};

struct DescribeTopicPartitionsBodyRequest { // 12 bytes
  char client_id_length[1];
  char client_id_contents[9];
  std::uint8_t software_version;
  char software_version_contents[3];
  std::int8_t TAG_BUFFER;
};

struct RequestApiVersion { // 39 bytes
  RequestHeader header;
  ApiVersionBodyRequest body;
}__attribute__((packed));

struct RequestDescribeTopicPartitions { // 36 bytes
  RequestHeader header;
  DescribeTopicPartitionsBodyRequest body;
}__attribute__((packed));

struct TopicName {
  std::int8_t length; // unsigned varint
  char contents[3];
}__attribute__((packed));

struct Topic { // 29 bytes
  std::int16_t error_code;
  TopicName topic_name;
  char topic_id[16];
  bool is_internal;
  std::uint8_t partition_array; // unsigned varint
  std::int32_t authorized_operations;
  bool TAG_BUFFER;
}__attribute__((packed));

struct TopicArray {
  std::int8_t length; // unsigned varint
  Topic topic; 
}__attribute__((packed));

struct DescribeTopicPartitionsBodyResponse {
  std::uint32_t throttle_time_ms;
  TopicArray topic_array;
  std::int8_t cursor;
  bool TAG_BUFFER;
}__attribute__((packed));

struct ResponseDescribeTopicPartitions {
  std::uint32_t message_size;
  std::uint32_t correlation_id;
  bool TAG_BUFFER;
  DescribeTopicPartitionsBodyResponse body;
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
  std::int16_t api_key2;
  std::int16_t min_version2;
  std::int16_t max_version2;
  std::int8_t TAG_BUFFER2;
  std::int32_t throttle_time_ms;
  std::int8_t TAG_BUFFER3;
} __attribute__((packed));

RequestApiVersion parse_buffer(const char* buffer) {
    return *reinterpret_cast<const RequestApiVersion*>(buffer);
}

Response set_response(const RequestApiVersion& req) {
  Response resp {};
  
  resp.message_size = htonl(sizeof(Response) - sizeof(Response::message_size)); 
  resp.correlation_id = req.header.correlation_id;

  if (ntohs(req.header.request_api_version) > 4 || ntohs(req.header.request_api_version) < 0) {
    resp.error_code = htons(35);
  }

  resp.api_key_array_length = 0x03;
  resp.api_key = htons(18);
  resp.api_key2 = htons(75);
  resp.max_version = htons(4);

  return resp;
}

void handle_client(int client_fd) {
  while (true) {
    char buffer[BUFFER_SIZE] = {0};
    ssize_t recv = read(client_fd, buffer, sizeof(buffer));
    
    if (recv < REQUEST_SIZE) {
      close(client_fd);
      break;
    }

    RequestApiVersion req = parse_buffer(buffer);
    Response resp = set_response(req);

    ssize_t sent = write(client_fd, &resp, sizeof(resp));
    if (sent <= 0) {
      break;
    }
  }
  close(client_fd);
}

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
  
    std::thread(handle_client, client_fd).detach();
  }

  close(server_fd);

  return 0;
}
