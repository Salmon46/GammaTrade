#pragma once

#include <string>
#include <hiredis/hiredis.h>
#include <spdlog/spdlog.h>

namespace gammatrade {

/**
 * RedisPublisher - A simple wrapper for publishing binary data to Redis channels.
 * 
 * This class handles connection management and provides a clean interface
 * for publishing Protobuf-serialized messages to Redis pub/sub channels.
 */
class RedisPublisher {
public:
    RedisPublisher(const std::string& host = "localhost", int port = 6379);
    ~RedisPublisher();

    /**
     * Connect to Redis server.
     * @return true if connection successful, false otherwise.
     */
    bool connect();

    /**
     * Disconnect from Redis server.
     */
    void disconnect();

    /**
     * Check if connected to Redis.
     * @return true if connected, false otherwise.
     */
    bool isConnected() const;

    /**
     * Publish binary data to a Redis channel.
     * @param channel The channel name to publish to.
     * @param data Pointer to the binary data.
     * @param length Length of the data in bytes.
     * @return Number of subscribers that received the message, or -1 on error.
     */
    int publish(const std::string& channel, const void* data, size_t length);

    /**
     * Publish a string message to a Redis channel.
     * @param channel The channel name to publish to.
     * @param message The string message to publish.
     * @return Number of subscribers that received the message, or -1 on error.
     */
    int publish(const std::string& channel, const std::string& message);

private:
    std::string host_;
    int port_;
    redisContext* context_;
    bool connected_;
};

} // namespace gammatrade
