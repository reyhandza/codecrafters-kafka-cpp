template <typename T>
void from_buffer(const char* buffer, ssize_t& pos, T& dest) {
  std::memcpy(&dest, buffer, sizeof(T));
  pos += sizeof(T);
}

Request parse_buffer(const char* buffer) {
  Request req {};
  ssize_t pos = 0;

  from_buffer(buffer, pos, req.header.message_size);
  from_buffer(buffer, pos, req.header.request_api_key);
  from_buffer(buffer, pos, req.header.request_api_version);
  from_buffer(buffer, pos, req.header.correlation_id);
  from_buffer(buffer, pos, req.header.client_id_length);
  from_buffer(buffer, pos, req.header.client_id_contents);
  from_buffer(buffer, pos, req.header.TAG_BUFFER);

  from_buffer(buffer, pos, req.body.client_id_length);
  from_buffer(buffer, pos, req.body.client_id_contents);
  from_buffer(buffer, pos, req.body.software_version);
  from_buffer(buffer, pos, req.body.software_version_contents);
  from_buffer(buffer, pos, req.body.TAG_BUFFER);

  return req;
}


