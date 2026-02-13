#pragma once
#include <iostream>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <filesystem>
#include "buffer.hpp"

struct TopicInfo {
  std::string topic_name;
  UUID uuid;
  bool found = false;
};

struct PartitionInfo {
  int32_t partition_id;
  int32_t leader_id;
  int32_t leader_epoch;
  std::vector<int32_t> replica_nodes;
};

class Metadata {
public:
  void load(std::filesystem::path path) {
    std::vector<uint8_t> raw = ReadFile(path);
    Buffer buf(reinterpret_cast<char*>(raw.data()), raw.size());
    parse(buf);
  }

  std::string GetTopic(const std::string& name) const {
    auto iter = topics_.find(name);
    if (iter == topics_.end()) {
      std::cerr << "Can't find topic key\n";
      return "";
    }
    return iter->second.topic_name;
  }

  UUID GetUUID(std::string topic_name) {
    UUID uuid = {};
    auto iter = topics_.find(topic_name);
    if (iter == topics_.end()) {
      std::cerr << "Can't UUID\n";
      return uuid;
    }
    return iter->second.uuid; 
  }

  bool IsTopicAvailable(std::string name) const {
    return topics_.find(name) != topics_.end();
  }

private:
  const uint8_t topic_record_type_ = 2;
  const uint8_t partitions_record_type_ = 3;

  std::map<std::string, TopicInfo> topics_;
  std::map<std::string, std::vector<PartitionInfo>> partitions_;

  std::vector<uint8_t> ReadFile(std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);

    size_t size = file.tellg();
    std::cerr << "File size: " << size << std::endl;
    file.seekg(0, std::ios::beg);
    
    std::vector<uint8_t> data(size);
    file.read(reinterpret_cast<char*>(data.data()), size);
    file.close();

    return data;
  }

  void parse(Buffer& buf) {
        int64_t base_offset = buf.ReadInt64();
        std::cout << "Base offset: " << base_offset << std::endl;
        int32_t batch_len = buf.ReadInt32();
        int32_t part_leader_epoch = buf.ReadInt32();
        int8_t magic_byte = buf.ReadInt8();
        int32_t crc = buf.ReadInt32();
        int16_t attributes = buf.ReadInt16();
        int32_t last_offset_delta = buf.ReadInt32();
        int64_t base_timestamp = buf.ReadInt64();
        int64_t max_timestamp = buf.ReadInt64();
        int64_t producer_id = buf.ReadInt64();
        int16_t producer_epoch = buf.ReadInt16();
        int32_t base_sequence = buf.ReadInt32();
        
        int32_t num_records = buf.ReadInt32();
        std::cout << "Num of records: " << num_records << std::endl;
        for (int i = 0; i < num_records; i++) {
          int32_t record_length = buf.ReadSignedVarint();
          std::cerr << "Record length: " << record_length << std::endl;
          buf.SkipTagBuffer(); // attributes
          int32_t record_timestamp_delta = buf.ReadSignedVarint();
          std::cerr << "Timestamp delta: " << record_timestamp_delta << std::endl;
          int32_t record_offset_delta = buf.ReadSignedVarint();
          int32_t record_key_length = buf.ReadSignedVarint(); // if -1, key is null
          int32_t value_len = buf.ReadSignedVarint();
          int8_t frame_version = buf.ReadInt8();

          int8_t type = buf.ReadInt8();
          std::cout << "type (2/3): " << type << std::endl; 
          if (type == topic_record_type_) {
            int8_t version = buf.ReadInt8();

            TopicInfo info;
            info.topic_name = buf.ReadCompactString();
            std::cout << "topic_name :" << info.topic_name << "is saved\n";
            info.uuid = buf.ReadUUID();
            info.found = true;
            topics_[info.topic_name] = info; 

            int8_t tagged_field = buf.ReadInt8();
            int8_t headers_array_count = buf.ReadInt8();
          } 
          else if (type == partitions_record_type_) {
            int8_t version = buf.ReadInt8();
            int32_t partition_id = buf.ReadInt32();
            UUID topic_uuid = buf.ReadUUID();

            std::vector<int32_t> replica_nodes;
            int32_t replica_array_len = buf.ReadUnsignedVarint();
            int num_replica = replica_array_len - 1;
            for (int i = 0; i < num_replica; i++) {
              int32_t replica_array = buf.ReadInt32(); 
              replica_nodes.push_back(replica_array);
            }

            int32_t sync_replica_array_len = buf.ReadUnsignedVarint();
            int num_sync_replica = sync_replica_array_len - 1;
            for (int i = 0; i < num_sync_replica; i++) {
              int32_t sync_array = buf.ReadInt32(); 
            }

            int8_t remove_array_len = buf.ReadUnsignedVarint();
            int8_t add_array_len = buf.ReadUnsignedVarint();

            int32_t leader_id = buf.ReadInt32();
            int32_t leader_epoch = buf.ReadInt32();
            int32_t partition_epoch = buf.ReadInt32();
            int32_t directories_array_len = buf.ReadUnsignedVarint();

            UUID directories_uuid = buf.ReadUUID();
            int8_t tagged_field = buf.ReadInt8();
            int8_t headers_array_count = buf.ReadInt8();

            PartitionInfo partition_info;
            partition_info.partition_id = partition_id;
            partition_info.leader_id = leader_id;
            partition_info.leader_epoch = leader_epoch;
            partition_info.replica_nodes = replica_nodes;
        }
      }
  }
};
