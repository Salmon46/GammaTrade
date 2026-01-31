#include "RedisSubscriber.hpp"

namespace gammatrade {

RedisSubscriber::RedisSubscriber(const std::string &host, int port)
    : host_(host), port_(port), context_(nullptr), connected_(false),
      listening_(false) {}

RedisSubscriber::~RedisSubscriber() { disconnect(); }

bool RedisSubscriber::connect() {
  if (connected_ && context_) {
    return true;
  }

  struct timeval timeout = {2, 0}; // 2 seconds
  context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);

  if (context_ == nullptr || context_->err) {
    if (context_) {
      spdlog::error("RedisSubscriber: Connection error: {}", context_->errstr);
      redisFree(context_);
      context_ = nullptr;
    }
    connected_ = false;
    return false;
  }

  connected_ = true;
  spdlog::info("RedisSubscriber: Connected to Redis at {}:{}", host_, port_);
  return true;
}

void RedisSubscriber::disconnect() {
  stop();
  if (context_) {
    redisFree(context_);
    context_ = nullptr;
  }
  connected_ = false;
  spdlog::info("RedisSubscriber: Disconnected from Redis");
}

void RedisSubscriber::subscribe(const std::string &channel,
                                MessageCallback callback) {
  if (!connected_)
    return;

  callback_ = callback;
  redisReply *reply = static_cast<redisReply *>(
      redisCommand(context_, "SUBSCRIBE %s", channel.c_str()));

  if (reply) {
    spdlog::info("RedisSubscriber: Subscribed to channel: {}", channel);
    freeReplyObject(reply);
  }
}

void RedisSubscriber::listen() {
  if (!connected_ || !context_)
    return;

  listening_ = true;
  spdlog::info("RedisSubscriber: Starting listener loop");

  while (listening_) {
    redisReply *reply = nullptr;

    // Check connection state before blocking call
    if (context_->err) {
      spdlog::error("RedisSubscriber: Connection lost: {}", context_->errstr);
      // Minimal reconnection logic could go here, but for now we break
      break;
    }

    if (redisGetReply(context_, reinterpret_cast<void **>(&reply)) ==
        REDIS_OK) {
      if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
        std::string type(reply->element[0]->str, reply->element[0]->len);

        if (type == "message") {
          std::string channel(reply->element[1]->str, reply->element[1]->len);
          const char *data = reply->element[2]->str;
          size_t length = reply->element[2]->len;

          if (callback_) {
            callback_(channel, data, length);
          }
        }
      }

      if (reply) {
        freeReplyObject(reply);
      }
    } else {
      // Error or timeout
      if (context_->err) {
        spdlog::error("RedisSubscriber: Error reading reply: {}",
                      context_->errstr);
        break;
      }
    }
  }
  spdlog::info("RedisSubscriber: Listener loop stopped");
}

void RedisSubscriber::stop() {
  listening_ = false;
  // Note: redisGetReply is blocking, so strictly speaking we might need
  // to unsubscribe or close socket to break the loop immediately from another
  // thread For this simple implementation, we rely on the loop condition or
  // main loop termination
}

} // namespace gammatrade
