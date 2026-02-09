#include "server.hpp"
#include "request.hpp"
#include "response.hpp"
#include <iostream>
#include <thread>

constexpr int BUFFER_SIZE = 1024;
constexpr int REQUEST_SIZE = 39;

Request::ApiVersion parse_to_api_version(const char* buffer) {
    return *reinterpret_cast<const Request::ApiVersion*>(buffer);
}

Request::DescribeTopicPartitions parse_to_topic(const char* buffer) {
    return *reinterpret_cast<const Request::DescribeTopicPartitions*>(buffer);
}

Response::DescribeTopicPartitions set_response(const Request::DescribeTopicPartitions& req) {
  Response::DescribeTopicPartitions resp {};
  
  resp.message_size = htonl(sizeof(Response::DescribeTopicPartitions) - sizeof(Response::DescribeTopicPartitions::message_size)); 
  resp.correlation_id = req.header.correlation_id;
  resp.body.topic_array.topic.error_code = 0x03;

  resp.body.topic_array.length = req.body.topic_array.length;
  resp.body.cursor = 0xff;

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
    
    Request::DescribeTopicPartitions req = parse_to_topic(buffer);
    Response::DescribeTopicPartitions resp = set_response(req);

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
