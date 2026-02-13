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

  int GetPartitionSize(const UUID& id) {
    std::vector<PartitionInfo> partitions = GetPartitionInfo(id);
    return partitions.size();
  }

  TopicInfo GetTopicInfo(const std::string& topic_name) {
    auto iter = topics_.find(topic_name);
    if (iter == topics_.end()) {
      std::cerr << "Topic didn't match any keys\n";
      return {};
    }
    return iter->second;
  }

  std::vector<PartitionInfo> GetPartitionInfo(const UUID id) {
    auto iter = partitions_.find(id);
    if (iter == partitions_.end()) {
      std::cerr << "UUID didn't match any partitions\n";
      return {};
    }
    return iter->second;
  }

  bool IsTopicAvailable(std::string name) const {
    return topics_.find(name) != topics_.end();
  }

private:
  const uint8_t topic_record_type_ = 2;
  const uint8_t partitions_record_type_ = 3;

  std::map<std::string, TopicInfo> topics_;
  std::map<UUID, std::vector<PartitionInfo>> partitions_;

  std::vector<uint8_t> ReadFile(std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);

    size_t size = file.tellg();
    std::cerr << "File size: " << size << std::endl;

    if (size > 1024) return {};
    file.seekg(0, std::ios::beg);
   
    std::vector<uint8_t> data(size);
    file.read(reinterpret_cast<char*>(data.data()), size);
    file.close();

    return data;
  }

  void parse(Buffer& buf) {
    // Loop over all record batches in the file
    while (buf.HasBytes(12)) { // at least base_offset(8) + batch_len(4)
      size_t batch_start = buf.GetReadOffset();
      int64_t base_offset = buf.ReadInt64();
      int32_t batch_len = buf.ReadInt32();

      // batch_len covers everything after the first 12 bytes (base_offset + batch_len)
      size_t next_batch_offset = batch_start + 12 + static_cast<size_t>(batch_len);
      if (next_batch_offset > buf.GetSize()) break; // truncated batch

      std::cerr << "Base offset: " << base_offset
                << "  batch_len: " << batch_len << std::endl;

      buf.ReadInt32();  // part_leader_epoch
      buf.ReadInt8();   // magic_byte;
      buf.ReadInt32();  // crc;
      buf.ReadInt16();  // attributes;
      buf.ReadInt32();  // last_offset_delta;
      buf.ReadInt64();  // base_timestamp;
      buf.ReadInt64();  // max_timestamp;
      buf.ReadInt64();  // producer_id;
      buf.ReadInt16();  // producer_epoch;
      buf.ReadInt32();  // base_sequence;

      int32_t num_records = buf.ReadInt32();
      std::cerr << "Num of records: " << num_records << std::endl;

      for (int i = 0; i < num_records; i++) {
        buf.ReadSignedVarint(); // record_length;
        buf.ReadInt8();         // attributes (INT8)
        buf.ReadSignedVarint(); // record_ts_delta;
        buf.ReadSignedVarint(); // record_offset_delta;
        int32_t key_length = buf.ReadSignedVarint(); // -1 = null key

        // Skip key bytes if key is present
        if (key_length > 0) {
          for (int32_t k = 0; k < key_length; k++) buf.ReadInt8();
        }

        buf.ReadSignedVarint(); // value_len;
        buf.ReadInt8();         // frame_version;
        int8_t  type          = buf.ReadInt8();

        if (type == topic_record_type_) {
          (void)buf.ReadInt8(); // version

          TopicInfo info;
          info.topic_name = buf.ReadCompactString();
          info.uuid       = buf.ReadUUID();
          info.found      = true;
          topics_[info.topic_name] = info;

          std::cerr << "topic saved: " << info.topic_name << std::endl;

          buf.SkipTagBuffer();          // record-level tag buffer
          buf.ReadUnsignedVarint();     // headers array (compact array length = 1 → 0 headers)
        }
        else if (type == partitions_record_type_) {
          (void)buf.ReadInt8(); // version
          int32_t partition_id = buf.ReadInt32();
          UUID    topic_uuid   = buf.ReadUUID();

          std::vector<int32_t> replica_nodes;
          uint32_t replica_array_len = buf.ReadUnsignedVarint();
          uint32_t num_replica = replica_array_len > 0 ? replica_array_len - 1 : 0;
          for (uint32_t r = 0; r < num_replica; r++) {
            replica_nodes.push_back(buf.ReadInt32());
          }

          uint32_t sync_replica_array_len = buf.ReadUnsignedVarint();
          uint32_t num_sync = sync_replica_array_len > 0 ? sync_replica_array_len - 1 : 0;
          for (uint32_t r = 0; r < num_sync; r++) buf.ReadInt32();

          buf.ReadUnsignedVarint(); // removing replicas array
          buf.ReadUnsignedVarint(); // adding replicas array

          int32_t leader_id       = buf.ReadInt32();
          int32_t leader_epoch    = buf.ReadInt32();
          buf.ReadInt32();  // partition_epoch;

          uint32_t dirs_len = buf.ReadUnsignedVarint();
          uint32_t num_dirs = dirs_len > 0 ? dirs_len - 1 : 0;
          for (uint32_t d = 0; d < num_dirs; d++) buf.ReadUUID();

          buf.SkipTagBuffer();      // record-level tag buffer
          buf.ReadUnsignedVarint(); // headers array

          PartitionInfo partition_info;
          partition_info.partition_id  = partition_id;
          partition_info.leader_id     = leader_id;
          partition_info.leader_epoch  = leader_epoch;
          partition_info.replica_nodes = replica_nodes;

          partitions_[topic_uuid].push_back(partition_info);
        }
        else {
          // Unknown record type: skip to next batch boundary to stay safe
          std::cerr << "Unknown record type " << static_cast<int>(type)
                    << ", jumping to next batch\n";
          buf.SetReadOffset(next_batch_offset);
          goto next_batch;
        }
      }

      // Advance to exact start of next batch (guards against off-by-one in record parsing)
      buf.SetReadOffset(next_batch_offset);
      next_batch:;
    }
  }
};
