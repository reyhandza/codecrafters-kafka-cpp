#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include "server.hpp"

class RequestBuffer {
public:
  RequestBuffer() = default;
  RequestBuffer(const char* raw_buffer, size_t size) {
    buffer.assign(raw_buffer, raw_buffer + size);
  }

  std::vector<uint8_t> GetData() const { return buffer; }
  
  int8_t ReadInt8() { return static_cast<int8_t>(buffer[read_offset++]); }
  int16_t ReadInt16() {
    int16_t val;
    std::memcpy(&val, &buffer[read_offset], sizeof(int16_t));
    read_offset += sizeof(int16_t);
    return ntohs(val);
  } 

  int32_t ReadInt32() {
    int32_t val;
    std::memcpy(&val, &buffer[read_offset], sizeof(int32_t));
    read_offset += sizeof(int32_t);
    return ntohl(val);
  }

  uint32_t ReadUnsignedVarint() {
    uint32_t value = 0;
    int i = 0;
    uint8_t b;
    do {
        b = buffer[read_offset++];
        value |= (b & 0x7f) << (7 * i);
        i++;
    } while (b & 0x80);
    return value;
  }

  std::string ReadCompactString() {
    uint32_t len = ReadUnsignedVarint(); 
    if (len == 0) return "";
    len -= 1;
    
    std::string s(buffer.begin() + read_offset, buffer.begin() + read_offset + len);
    read_offset += len;
    return s;
  }

  std::string ReadNullableString() {
    int16_t len = ReadInt16();
    if (len == -1) return "";
    
    std::string s(buffer.begin() + read_offset, buffer.begin() + read_offset + len);
    read_offset += len;
    return s;
  }

  void SkipTagBuffer() {
    uint32_t num_tags = ReadUnsignedVarint();
  }

  void ResetOffset() { read_offset = 0; }

private:
  std::vector<uint8_t> buffer;
  size_t read_offset = 0;
};

class ResponseBuffer {
public:
  ResponseBuffer() = default;
  ResponseBuffer(const char* raw_buffer, size_t size) {
    buffer.assign(raw_buffer, raw_buffer + size);
  }

  std::vector<uint8_t>& GetData() { return buffer; }
  const std::vector<uint8_t>& GetData() const { return buffer; }
  size_t GetSize() const { return buffer.size(); }

  void WriteInt8(const int8_t& val) { buffer.push_back(val); }
  void WriteInt16(const int16_t& val) {
    int16_t val_n = htons(val);
    const int8_t* ptr = reinterpret_cast<const int8_t*>(&val_n);
    buffer.insert(buffer.end(), ptr, ptr + sizeof(int16_t));
  }

  void WriteInt32(const int32_t& val) {
    int32_t val_n = htonl(val);
    const int8_t* ptr = reinterpret_cast<const int8_t*>(&val_n);
    buffer.insert(buffer.end(), ptr, ptr + sizeof(int32_t));
  }

  void writeBytes(const std::vector<uint8_t>& bytes) {
    buffer.insert(buffer.end(), bytes.begin(), bytes.end());
  }

  void writeUnsignedVarint(uint32_t value) {
    while ((value & 0xffffff80) != 0L) {
      uint8_t b = (value & 0x7f) | 0x80;
      buffer.push_back(b);
      value >>= 7;
    }
    buffer.push_back(static_cast<uint8_t>(value));
  }

  void writeTagBuffer() {
    writeUnsignedVarint(0); 
  }

  void writeCompactArrayLength(int length) {
    writeUnsignedVarint(length + 1);
  }

  void writeCompactString(const std::string& str) {
    writeUnsignedVarint(str.length() + 1);
    writeBytes(std::vector<uint8_t>(str.begin(), str.end()));
  }

  void writeCompactNullableString(const char* str) { 
    if (str == nullptr) {
      writeUnsignedVarint(0); 
    } else {
      std::string s(str);
      writeUnsignedVarint(s.length() + 1);
      writeBytes(std::vector<uint8_t>(s.begin(), s.end()));
    }
  }

private:
  std::vector<uint8_t> buffer;
};

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

        RequestBuffer req_buf(reinterpret_cast<char*>(raw_buffer.data()), raw_buffer.size());
        int16_t api_key = req_buf.ReadInt16();
        int16_t api_version = req_buf.ReadInt16();
        int32_t correlation_id = req_buf.ReadInt32();
        std::string client_id = req_buf.ReadNullableString();
        req_buf.SkipTagBuffer();

        ResponseBuffer res_buf;
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
        
        write(client_fd, res_buf.GetData().data(), res_buf.GetSize());
      }
    close(client_fd);
  }

private:
  const uint16_t num_apis = 2;
  const uint16_t min_version = 0;
  const uint16_t max_version = 4;
  const uint16_t api_version_key = 18;
  const uint16_t api_describe_topic_partitions = 75;

  void build_api_version_body_response(RequestBuffer req, ResponseBuffer& res) {
    std::string client_id = req.ReadCompactString();
    std::string client_software_version = req.ReadCompactString();
    req.SkipTagBuffer();

    int16_t error_code = 0;
    res.WriteInt32(error_code);
   
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
    
    // wrong, these are response
    uint32_t api_version_array_length = req.ReadUnsignedVarint();
    uint32_t num_api = api_version_array_length - 1;
    
    std::vector<uint16_t> apis;
    for (uint32_t i = 0; i < num_api; i++) { 

    uint16_t api_key = req.ReadInt16();
    apis.push_back(api_key);

    uint16_t min_version = req.ReadInt16();
    apis.push_back(min_version);
    
    uint16_t max_version = req.ReadInt16();
    apis.push_back(max_version);

    req.SkipTagBuffer();
    }
  }

  void build_decribe_body_partitions_body_response(RequestBuffer buf, ResponseBuffer& res) {
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
