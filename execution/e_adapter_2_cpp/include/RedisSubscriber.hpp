#pragma once

#include <atomic>
#include <functional>
#include <hiredis/hiredis.h>
#include <spdlog/spdlog.h>
#include <string>


namespace gammatrade {

class RedisSubscriber {
public:
  using MessageCallback = std::function<void(const std::string &channel,
                                             const char *data, size_t length)>;

  RedisSubscriber(const std::string &host = "localhost", int port = 6379);
  ~RedisSubscriber();

  bool connect();
  void disconnect();

  void subscribe(const std::string &channel, MessageCallback callback);
  void listen(); // Blocking call
  void stop();

private:
  std::string host_;
  int port_;
  redisContext *context_;
  bool connected_;
  std::atomic<bool> listening_;
  MessageCallback callback_;
};

} // namespace gammatrade
