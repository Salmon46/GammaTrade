#pragma once

/**
 * RedisClient - Redis Pub/Sub client for the Execution Router
 *
 * Handles both subscribing to channels and publishing messages.
 */

#include <atomic>
#include <functional>
#include <hiredis/hiredis.h>
#include <spdlog/spdlog.h>
#include <string>
#include <thread>
#include <vector>


namespace gammatrade {

using MessageCallback = std::function<void(const std::string &channel,
                                           const void *data, size_t length)>;

/**
 * RedisClient - Manages Redis connections for pub/sub operations
 */
class RedisClient {
public:
  RedisClient(const std::string &host = "localhost", int port = 6379);
  ~RedisClient();

  /**
   * Connect to Redis server
   * @return true if connection successful
   */
  bool connect();

  /**
   * Disconnect from Redis server
   */
  void disconnect();

  /**
   * Check if connected
   */
  bool isConnected() const { return connected_; }

  /**
   * Subscribe to channels matching a pattern
   * @param pattern Channel pattern (e.g., "market_data_*")
   * @param callback Function to call when message received
   */
  void psubscribe(const std::string &pattern, MessageCallback callback);

  /**
   * Subscribe to a specific channel
   * @param channel Channel name
   * @param callback Function to call when message received
   */
  void subscribe(const std::string &channel, MessageCallback callback);

  /**
   * Publish binary data to a channel
   * @param channel Channel name
   * @param data Pointer to binary data
   * @param length Length of data
   * @return Number of subscribers that received the message
   */
  int publish(const std::string &channel, const void *data, size_t length);

  /**
   * Publish string message to a channel
   */
  int publish(const std::string &channel, const std::string &message);

  /**
   * Start the subscription listener loop (blocking)
   * Call this in a separate thread
   */
  void listen();

  /**
   * Stop the listener loop
   */
  void stopListening();

private:
  std::string host_;
  int port_;
  redisContext *pubContext_; // For publishing
  redisContext *subContext_; // For subscribing
  bool connected_;
  std::atomic<bool> listening_;

  MessageCallback messageCallback_;
  std::vector<std::string> subscriptions_;
};

} // namespace gammatrade
