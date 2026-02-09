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

Response::ApiVersion set_response(const Request::ApiVersion& req) {
  Response::ApiVersion resp {};
  
  resp.message_size = htonl(sizeof(Response) - sizeof(Response::message_size)); 
  resp.correlation_id = req.header.correlation_id;

  if (ntohs(req.header.request_api_version) > 4 || ntohs(req.header.request_api_version) < 0) {
    resp.error_code = htons(35);
  }

  resp.api_key_array_length = 0x03;
  resp.api_key = htons(18);
  resp.api_key2 = htons(75);
  resp.max_version = htons(4);

  return resp;
}

