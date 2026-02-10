#include <iostream>
#include <thread>
#include "server.hpp"
#include "buffer.hpp"

class Protocol {
public:
  void handle_client(int client_fd){
    while (true) {
      int32_t message_size_be;
      ssize_t h_bytes = recv(client_fd, &message_size_be, 4, MSG_WAITALL);
      
      if (h_bytes <= 0) break;

      int32_t message_size = ntohl(message_size_be);
      if (message_size <= 0 || message_size > 1000000) break;

      std::vector<char> raw_buffer(message_size); 
      ssize_t bytes = recv(client_fd, raw_buffer.data(), message_size, MSG_WAITALL);
      
      if (bytes != message_size) break;

      Buffer req_buf(reinterpret_cast<char*>(raw_buffer.data()), raw_buffer.size());
      int16_t api_key = req_buf.ReadInt16();
      int16_t api_version = req_buf.ReadInt16();
      int32_t correlation_id = req_buf.ReadInt32();
      std::string client_id = req_buf.ReadNullableString();
      req_buf.SkipTagBuffer();

      Buffer res_buf;
      res_buf.WriteInt32(0); // message_size
      res_buf.WriteInt32(correlation_id);

      if (api_key == 75) {
        build_decribe_body_partitions_body_response(req_buf, res_buf);
      } else if (api_key == 18) {
        build_api_version_body_response(req_buf, res_buf);
      } else { 
        std::cerr << "Unknown api_key: " << api_key << std::endl; 
      }
      
      int32_t response_size = htonl(res_buf.GetSize() - 4);
      std::memcpy(res_buf.GetData().data(), &response_size, 4);

      if (api_version > 4 | api_version < 0) {
        int16_t error_code = htons(35);
        std::memcpy(res_buf.GetData().data() + 8, &error_code, 2);
      }
      
      write(client_fd, res_buf.GetData().data(), res_buf.GetSize());
    }
  close(client_fd);
  }

private:
  const uint32_t num_apis = 2;
  const uint16_t min_version = 0;
  const uint16_t max_version = 4;
  const uint16_t api_version_key = 18;
  const uint16_t api_describe_topic_partitions = 75;

  void build_api_version_body_response(Buffer req, Buffer& res) {
    std::string client_id = req.ReadCompactString();
    std::string client_software_version = req.ReadCompactString();
    req.SkipTagBuffer();

    int16_t error_code = 0;
    res.WriteInt16(error_code);
   
    res.writeCompactArrayLength(num_apis);

    res.WriteInt16(api_version_key);
    res.WriteInt16(min_version);
    res.WriteInt16(max_version);
    res.writeTagBuffer();
    res.WriteInt16(api_describe_topic_partitions);
    res.WriteInt16(min_version);
    res.WriteInt16(min_version);
    res.writeTagBuffer();

    res.WriteInt32(0); // throttle_ms
    res.writeTagBuffer();
  }

  void build_decribe_body_partitions_body_response(Buffer buf, Buffer& res) {
    uint32_t topic_array_length = buf.ReadUnsignedVarint();
    uint32_t num_topics = topic_array_length - 1;

    std::vector<std::string> topics;
    for (uint32_t i = 0; i < num_topics; i++) {
      std::string topic_name = buf.ReadCompactString();
      buf.SkipTagBuffer();
      topics.push_back(topic_name);
    }

    int32_t response_partition_limit = buf.ReadInt32();
    int8_t cursor_present = buf.ReadInt8();
    buf.SkipTagBuffer();

    res.writeTagBuffer();
    res.WriteInt32(0); // throttle_ms

    res.writeCompactArrayLength(topics.size());
    for (auto topic: topics) {
      int16_t error_code = 3;
      res.WriteInt16(error_code);

      res.writeCompactString(topic);

      std::vector<uint8_t> topic_id(16,0);
      res.writeBytes(topic_id);

      bool is_internal = false;
      res.WriteInt8(is_internal ? 1 : 0);

      res.writeCompactArrayLength(0);

      int32_t authorized_op = 0;
      res.WriteInt32(authorized_op);

      res.writeTagBuffer();
    }

    res.WriteInt8(0xff);
    res.writeTagBuffer();

    }
};

int main(int argc, char *argv[]) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  std::cerr << "Logs from your program will appear here!\n";

  int server_fd = Server::createSocket();

  while (true) {
    struct sockaddr_in client_addr {};

    socklen_t client_addr_len = sizeof(client_addr);

    int client_fd =
        accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
               &client_addr_len);
    std::cout << "Client connected\n";

    std::thread([client_fd]() { Protocol conn; conn.handle_client(client_fd); }).detach();
  }

  close(server_fd);

  return 0;
}
