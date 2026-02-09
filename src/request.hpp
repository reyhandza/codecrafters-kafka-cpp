#pragma once
#include <cstdint>

namespace Request {
  struct Header { // 24 bytes
    std::uint32_t message_size;
    std::int16_t request_api_key;
    std::int16_t request_api_version;
    std::uint32_t correlation_id;
    char client_id_length[2];
    char client_id_contents[9];
    std::int8_t TAG_BUFFER;
  };

  struct ApiVersionBody { // 15 bytes
    char client_id_length[1];
    char client_id_contents[9];
    std::uint8_t software_version;
    char software_version_contents[3];
    std::int8_t TAG_BUFFER;
  };

  struct Topic {
    std::int8_t length;
    char name[3];
    bool TAG_BUFFER;
  };

  struct TopicArray {
    std::int8_t length;
    Topic topic;
  };

  struct DescribeTopicPartitionsBody { // 12 bytes
    TopicArray topic_array;
    std::uint32_t partition_limit;
    std::int8_t cursor;
    std::int8_t TAG_BUFFER;
  };

  struct ApiVersion { // 39 bytes
    Header header;
    ApiVersionBody body;
  }__attribute__((packed));

  struct DescribeTopicPartitions { // 36 bytes
    Header header;
    DescribeTopicPartitionsBody body;
  }__attribute__((packed));
}
