#pragma once

#include <hiredis/hiredis.h>
#include <spdlog/spdlog.h>
#include <string>


namespace gammatrade {

class RedisPublisher {
public:
  RedisPublisher(const std::string &host = "localhost", int port = 6379);
  ~RedisPublisher();

  bool connect();
  void disconnect();
  bool isConnected() const;

  int publish(const std::string &channel, const void *data, size_t length);
  int publish(const std::string &channel, const std::string &message);

private:
  std::string host_;
  int port_;
  redisContext *context_;
  bool connected_;
};

} // namespace gammatrade
