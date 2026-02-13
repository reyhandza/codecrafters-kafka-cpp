#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <array>
#include <vector>
#include <cstring>
#include <arpa/inet.h>

using UUID = std::array<char, 16>;

class Buffer {
public:
  Buffer() = default;
  Buffer(const char* raw_buffer, size_t size) {
    buffer.assign(raw_buffer, raw_buffer + size);
  }

  std::vector<uint8_t>& GetData() { return buffer; }
  size_t GetSize() const { return buffer.size(); }
  size_t GetReadOffset() const { return read_offset; }
  
  
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

  int64_t ReadInt64() {
    int64_t val;
    std::memcpy(&val, &buffer[read_offset], sizeof(int64_t));
    read_offset += sizeof(int64_t);

    const uint8_t* b = &buffer[read_offset - sizeof(int64_t)];
    return static_cast<int64_t>(
        (uint64_t)b[0] << 56 | (uint64_t)b[1] << 48 |
        (uint64_t)b[2] << 40 | (uint64_t)b[3] << 32 |
        (uint64_t)b[4] << 24 | (uint64_t)b[5] << 16 |
        (uint64_t)b[6] <<  8 | (uint64_t)b[7]
    );
  }
  
  UUID ReadUUID() {
    UUID id;
    memcpy(&id, &buffer[read_offset], sizeof(UUID));
    read_offset += sizeof(UUID);
    return id;
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

  // zig-zag varint
  int32_t ReadSignedVarint() {
    uint32_t n = ReadUnsignedVarint();
    return (int32_t)((n >> 1) ^ -(int32_t)(n & 1));
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

  void writeBytes(const std::vector<uint8_t>& bytes) {
    buffer.insert(buffer.end(), bytes.begin(), bytes.end());
  }

  void writeUUID(const UUID& bytes) {
    buffer.insert(buffer.end(), bytes.begin(), bytes.end());
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

  void ResetOffset() { read_offset = 0; }
  void SetReadOffset(size_t offset) { read_offset = offset; }
  bool HasBytes(size_t n) const { return read_offset + n <= buffer.size(); }

private:
  std::vector<uint8_t> buffer;
  size_t read_offset = 0;
};
