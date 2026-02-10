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
    if (len == 0) return ""; // Null
    len -= 1; // N+1

    
    std::string s(buffer.begin() + read_offset, buffer.begin() + read_offset + len);
    read_offset += len;
    return s;
  }

  std::string ReadNullableString() {
    int16_t len = ReadInt16();
    if (len == -1) return ""; // Null
    
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

  std::vector<uint8_t> GetData() const { return buffer; }
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
      uint8_t b = (value & 0x7f) | 0x80; // Set MSB bit to 1 (continuation)
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
    try {
      while (true) {
        int32_t message_size_be;
        ssize_t h_bytes = recv(client_fd, &message_size_be, 4, MSG_WAITALL);
        
        if (h_bytes <= 0) { 
          std::cerr << "Connection closed or error on header read\n";
          break; 
        }

        int32_t message_size = ntohl(message_size_be);
        std::cerr << "Message size: " << message_size << std::endl;
        
        if (message_size <= 0 || message_size > 1000000) { 
          std::cerr << "Invalid message size\n";
          break; 
        }

        std::vector<char> raw_buffer(message_size); 
        ssize_t bytes = recv(client_fd, raw_buffer.data(), message_size, MSG_WAITALL);
        
        if (bytes != message_size) {
          std::cerr << "Incomplete message received\n";
          break;
        }

        RequestBuffer req_buf(reinterpret_cast<char*>(raw_buffer.data()), raw_buffer.size());
        int16_t api_key = req_buf.ReadInt16();
        int16_t api_version = req_buf.ReadInt16();
        int32_t correlation_id = req_buf.ReadInt32();
        
        std::cerr << "API Key: " << api_key << ", Version: " << api_version 
                  << ", Correlation ID: " << correlation_id << std::endl;
        
        // For DescribeTopicPartitions v0, header uses v1 format (non-flexible)
        // client_id is NULLABLE_STRING (int16 length), not compact string
        std::string client_id = req_buf.ReadNullableString();
        std::cerr << "Client ID: " << client_id << std::endl;
        
        // Header v1 with flexible API body still has tag buffer after client_id
        req_buf.SkipTagBuffer();
        std::cerr << "Header parsed, starting body\n";

        ResponseBuffer res_buf;
        res_buf.WriteInt32(0);
        res_buf.WriteInt32(correlation_id);

        if (api_key == 75) {
          std::cerr << "Processing DescribeTopicPartitions request\n";
          build_decribe_body_partitions_body_response(req_buf, res_buf);
          std::cerr << "Response built successfully\n";
        } else { 
          std::cerr << "Unknown api_key: " << api_key << std::endl; 
        }
        
        int32_t response_size = htonl(res_buf.GetSize() - 4);
        std::memcpy(const_cast<uint8_t*>(res_buf.GetData().data()), &response_size, 4);
        
        std::cerr << "Sending response of size: " << res_buf.GetSize() << std::endl;
        write(client_fd, res_buf.GetData().data(), res_buf.GetSize());
        std::cerr << "Response sent\n";
      }
    } catch (const std::exception& e) {
      std::cerr << "Exception in handle_client: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Unknown exception in handle_client\n";
    }
    close(client_fd);
  }

private:
  void build_decribe_body_partitions_body_response(RequestBuffer buf, ResponseBuffer& res) {
    std::cerr << "Reading topics array length\n";
    uint32_t topic_array_length = buf.ReadUnsignedVarint();
    uint32_t num_topics = topic_array_length - 1;
    std::cerr << "Number of topics: " << num_topics << std::endl;

    std::vector<std::string> topics;
    for (uint32_t i = 0; i < num_topics; i++) {
      std::string topic_name = buf.ReadCompactString();
      std::cerr << "Topic[" << i << "]: " << topic_name << std::endl;
      buf.SkipTagBuffer();
      topics.push_back(topic_name);
    }

    // Read ResponsePartitionLimit
    std::cerr << "Reading ResponsePartitionLimit\n";
    int32_t response_partition_limit = buf.ReadInt32();
    std::cerr << "ResponsePartitionLimit: " << response_partition_limit << std::endl;
    
    // Read Cursor (nullable)
    std::cerr << "Reading Cursor\n";
    int8_t cursor_present = buf.ReadInt8();
    std::cerr << "Cursor present flag: " << (int)cursor_present << std::endl;
    if (cursor_present != -1) {
      std::cerr << "Cursor is present (not null)\n";
      // If cursor is present, read cursor fields
      // For now, we'll skip cursor implementation
    }
    
    // Read tag buffer after cursor
    std::cerr << "Reading tag buffer after cursor\n";
    buf.SkipTagBuffer();

    // Now build response
    std::cerr << "Building response\n";
    res.WriteInt32(0); // throttle_time_ms
    res.writeTagBuffer();

    res.writeCompactArrayLength(topics.size());
    for (auto topic: topics) {
      int16_t error_code = 0;
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
