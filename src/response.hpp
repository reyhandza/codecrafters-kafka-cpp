#pragma once
#include <cstdint>

namespace Response {
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

  struct DescribeTopicPartitionsBody {
    std::uint32_t throttle_time_ms;
    TopicArray topic_array;
    std::int8_t cursor;
    bool TAG_BUFFER;
  }__attribute__((packed));

  struct DescribeTopicPartitions {
    std::uint32_t message_size;
    std::uint32_t correlation_id;
    bool TAG_BUFFER;
    DescribeTopicPartitionsBody body;
  }__attribute__((packed));

  struct ApiVersion { // 23 bytes
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
}
