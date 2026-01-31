#include "RedisClient.hpp"

namespace gammatrade {

RedisClient::RedisClient(const std::string &host, int port)
    : host_(host), port_(port), pubContext_(nullptr), subContext_(nullptr),
      connected_(false), listening_(false) {}

RedisClient::~RedisClient() { disconnect(); }

bool RedisClient::connect() {
  if (connected_)
    return true;

  struct timeval timeout = {2, 0}; // 2 seconds

  // Create publishing connection
  pubContext_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
  if (pubContext_ == nullptr || pubContext_->err) {
    if (pubContext_) {
      spdlog::error("RedisClient: Pub connection error: {}",
                    pubContext_->errstr);
      redisFree(pubContext_);
      pubContext_ = nullptr;
    }
    return false;
  }

  // Create subscription connection
  subContext_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
  if (subContext_ == nullptr || subContext_->err) {
    if (subContext_) {
      spdlog::error("RedisClient: Sub connection error: {}",
                    subContext_->errstr);
      redisFree(subContext_);
      subContext_ = nullptr;
    }
    redisFree(pubContext_);
    pubContext_ = nullptr;
    return false;
  }

  connected_ = true;
  spdlog::info("RedisClient: Connected to Redis at {}:{}", host_, port_);
  return true;
}

void RedisClient::disconnect() {
  stopListening();

  if (pubContext_) {
    redisFree(pubContext_);
    pubContext_ = nullptr;
  }
  if (subContext_) {
    redisFree(subContext_);
    subContext_ = nullptr;
  }
  connected_ = false;
  spdlog::info("RedisClient: Disconnected");
}

void RedisClient::psubscribe(const std::string &pattern,
                             MessageCallback callback) {
  if (!connected_ || !subContext_)
    return;

  messageCallback_ = callback;

  redisReply *reply = static_cast<redisReply *>(
      redisCommand(subContext_, "PSUBSCRIBE %s", pattern.c_str()));

  if (reply) {
    spdlog::info("RedisClient: Subscribed to pattern: {}", pattern);
    freeReplyObject(reply);
  }
}

void RedisClient::subscribe(const std::string &channel,
                            MessageCallback callback) {
  if (!connected_ || !subContext_)
    return;

  messageCallback_ = callback;

  redisReply *reply = static_cast<redisReply *>(
      redisCommand(subContext_, "SUBSCRIBE %s", channel.c_str()));

  if (reply) {
    spdlog::info("RedisClient: Subscribed to channel: {}", channel);
    freeReplyObject(reply);
  }
}

int RedisClient::publish(const std::string &channel, const void *data,
                         size_t length) {
  if (!connected_ || !pubContext_)
    return -1;

  redisReply *reply = static_cast<redisReply *>(redisCommand(
      pubContext_, "PUBLISH %s %b", channel.c_str(), data, length));

  if (reply == nullptr) {
    spdlog::error("RedisClient: Publish failed: {}", pubContext_->errstr);
    return -1;
  }

  int subscribers = 0;
  if (reply->type == REDIS_REPLY_INTEGER) {
    subscribers = static_cast<int>(reply->integer);
  }

  freeReplyObject(reply);
  return subscribers;
}

int RedisClient::publish(const std::string &channel,
                         const std::string &message) {
  return publish(channel, message.data(), message.size());
}

void RedisClient::listen() {
  if (!connected_ || !subContext_)
    return;

  listening_ = true;
  spdlog::info("RedisClient: Starting listener loop");

  while (listening_) {
    redisReply *reply = nullptr;

    // Use blocking read (no timeout)
    int status = redisGetReply(subContext_, reinterpret_cast<void **>(&reply));
    if (status == REDIS_OK) {
      if (reply) {
        // spdlog::debug("RedisClient: Received reply type: {}", reply->type);
        if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
          // Pattern message: ["pmessage", pattern, channel, message]
          if (reply->element[0]->str == nullptr) {
            spdlog::warn("RedisClient: Received NULL type string in reply");
            freeReplyObject(reply);
            continue;
          }
          std::string type(reply->element[0]->str, reply->element[0]->len);

          if (type == "pmessage" && reply->elements >= 4) {
            std::string channel(reply->element[2]->str, reply->element[2]->len);
            const char *data = reply->element[3]->str;
            size_t length = reply->element[3]->len;

            spdlog::info(
                "RedisClient: Received pmessage on channel '{}', len: {}",
                channel, length);

            if (messageCallback_) {
              messageCallback_(channel, data, length);
            }
          } else if (type == "message" && reply->elements >= 3) {
            std::string channel(reply->element[1]->str, reply->element[1]->len);
            const char *data = reply->element[2]->str;
            size_t length = reply->element[2]->len;

            spdlog::info(
                "RedisClient: Received message on channel '{}', len: {}",
                channel, length);

            if (messageCallback_) {
              messageCallback_(channel, data, length);
            }
          } else {
            spdlog::warn(
                "RedisClient: Unhandled reply type: {} with {} elements", type,
                reply->elements);
          }
        } else {
          // Heartbeats or other messages
          // spdlog::debug("RedisClient: Non-array or short reply. Type: {}",
          // reply->type);
        }
        freeReplyObject(reply);
      } else {
        spdlog::warn(
            "RedisClient: redisGetReply returned OK but reply is NULL");
      }
    } else {
      if (subContext_->err) {
        spdlog::error("RedisClient: redisGetReply error: {}",
                      subContext_->errstr);
        listening_ = false; // Break loop on critical error
      }
    }
  }

  spdlog::info("RedisClient: Listener loop stopped");
}

void RedisClient::stopListening() { listening_ = false; }

} // namespace gammatrade
