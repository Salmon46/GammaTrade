#include "RedisPublisher.hpp"

namespace gammatrade {

RedisPublisher::RedisPublisher(const std::string &host, int port)
    : host_(host), port_(port), context_(nullptr), connected_(false) {}

RedisPublisher::~RedisPublisher() { disconnect(); }

bool RedisPublisher::connect() {
  if (connected_ && context_) {
    return true;
  }

  struct timeval timeout = {1, 500000}; // 1.5 seconds
  context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);

  if (context_ == nullptr || context_->err) {
    if (context_) {
      spdlog::error("RedisPublisher: Connection error: {}", context_->errstr);
      redisFree(context_);
      context_ = nullptr;
    } else {
      spdlog::error("RedisPublisher: Cannot allocate redis context");
    }
    connected_ = false;
    return false;
  }

  connected_ = true;
  spdlog::info("RedisPublisher: Connected to Redis at {}:{}", host_, port_);
  return true;
}

void RedisPublisher::disconnect() {
  if (context_) {
    redisFree(context_);
    context_ = nullptr;
  }
  connected_ = false;
  spdlog::info("RedisPublisher: Disconnected from Redis");
}

bool RedisPublisher::isConnected() const {
  return connected_ && context_ != nullptr;
}

int RedisPublisher::publish(const std::string &channel, const void *data,
                            size_t length) {
  if (!isConnected()) {
    spdlog::warn("RedisPublisher: Not connected, attempting reconnect...");
    if (!connect()) {
      return -1;
    }
  }

  redisReply *reply = static_cast<redisReply *>(
      redisCommand(context_, "PUBLISH %s %b", channel.c_str(), data, length));

  if (reply == nullptr) {
    spdlog::error("RedisPublisher: PUBLISH failed: {}", context_->errstr);
    disconnect();
    return -1;
  }

  int subscribers = 0;
  if (reply->type == REDIS_REPLY_INTEGER) {
    subscribers = static_cast<int>(reply->integer);
  }

  freeReplyObject(reply);
  return subscribers;
}

int RedisPublisher::publish(const std::string &channel,
                            const std::string &message) {
  return publish(channel, message.data(), message.size());
}

} // namespace gammatrade
