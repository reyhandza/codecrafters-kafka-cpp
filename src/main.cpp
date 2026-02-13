#include <cstdint>
#include <iostream>
#include <thread>
#include <filesystem>
#include "metadata.hpp"
#include "server.hpp"
#include "buffer.hpp"

struct HeaderV0 {
  int16_t api_key;
  int16_t api_version;
  int32_t correlation_id;
  std::string client_id;
};

class Protocol {
public:
  Protocol(Metadata storage) : storage_(storage) {};

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
      HeaderV0 req_header;
      read_request_header(req_buf, req_header);

      Buffer res_buf;
      build_response(req_header, req_buf, res_buf);

      write(client_fd, res_buf.GetData().data(), res_buf.GetSize());
    }
  close(client_fd);
  }

private:
  Metadata storage_;
  const uint32_t num_apis = 2;
  const uint16_t min_version = 0;
  const uint16_t max_version = 4;
  const uint16_t api_version_key = 18;
  const uint16_t api_describe_topic_partitions = 75;

  void read_request_header(Buffer& req, HeaderV0& dst) {
      dst.api_key = req.ReadInt16();
      dst.api_version = req.ReadInt16();
      dst.correlation_id = req.ReadInt32();
      dst.client_id = req.ReadNullableString();
      req.SkipTagBuffer();
  }
  
  void build_response(const HeaderV0& src, Buffer& req_buf, Buffer& res_buf) {
      res_buf.WriteInt32(0); // message_size
      res_buf.WriteInt32(src.correlation_id);

      if (src.api_key == api_describe_topic_partitions) {
        build_decribe_body_partitions_body_response(req_buf, res_buf);
      } else if (src.api_key == api_version_key) {
        build_api_version_body_response(req_buf, res_buf);
      } else { 
        std::cerr << "Unknown api_key: " << src.api_key << std::endl; 
      }
      
      int32_t response_size = htonl(res_buf.GetSize() - 4);
      std::memcpy(res_buf.GetData().data(), &response_size, 4);

      if (src.api_version > 4 || src.api_version < 0) {
        int16_t error_code = htons(35);
        std::memcpy(res_buf.GetData().data() + 8, &error_code, 2);
      }
  }

  void build_api_version_body_response(Buffer& req, Buffer& res) {
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

  void build_decribe_body_partitions_body_response(Buffer& buf, Buffer& res) {
    std::vector<std::string> topics;
    
    uint32_t topic_array_length = buf.ReadUnsignedVarint();
    uint32_t num_topics = topic_array_length - 1;
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
      int16_t error_code = storage_.IsTopicAvailable(topic) ? 0 : 3;
      res.WriteInt16(error_code);

      res.writeCompactString(topic);

      UUID uuid = storage_.GetUUID(topic);
      res.writeUUID(uuid);

      bool is_internal = false;
      res.WriteInt8(is_internal ? 1 : 0);

      res.writeCompactArrayLength(0); // TODO

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

  std::filesystem::path path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
  Metadata log_file;
  log_file.load(path);

  int server_fd = Server::createSocket();

  while (true) {
    struct sockaddr_in client_addr {};

    socklen_t client_addr_len = sizeof(client_addr);

    int client_fd =
        accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
               &client_addr_len);
    std::cout << "Client connected\n";

    std::thread([client_fd, log_file]() { Protocol conn(log_file); conn.handle_client(client_fd); }).detach();
  }

  close(server_fd);

  return 0;
}
