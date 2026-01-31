#pragma once

/**
 * CoinbaseAdapter - Refactored adapter that connects to Coinbase WebSocket
 * and publishes market data to Redis using Protobuf serialization.
 *
 * This is a port of the deprecated CoinbaseClient.hpp, modified to:
 * 1. Replace EventBus with RedisPublisher
 * 2. Use Protobuf messages instead of internal event types
 * 3. Remove paper trading logic (handled elsewhere)
 */

#define CPPHTTPLIB_OPENSSL_SUPPORT

#include "RedisPublisher.hpp"
#include "market_data.pb.h"

#include <atomic>
#include <chrono>
#include <ctime>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_client.hpp>

// JWT generation for Coinbase API authentication
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

using json = nlohmann::json;
typedef websocketpp::client<websocketpp::config::asio_tls_client> WsClient;

namespace gammatrade {

class CoinbaseAdapter {
public:
  CoinbaseAdapter(const std::string &api_key, const std::string &api_secret,
                  std::shared_ptr<RedisPublisher> publisher,
                  const std::string &redis_channel = "market_data_2");

  ~CoinbaseAdapter();

  /**
   * Connect to Coinbase WebSocket and start streaming.
   * @param product_ids List of product IDs to subscribe to (e.g., "BTC-USD").
   */
  void connect(const std::vector<std::string> &product_ids);

  /**
   * Disconnect from Coinbase WebSocket.
   */
  void disconnect();

  /**
   * Check if connected to Coinbase WebSocket.
   */
  bool isConnected() const { return is_connected_.load(); }

  /**
   * Sync local time with Coinbase server time.
   */
  void syncTime();

private:
  // WebSocket Handlers
  void onOpen(websocketpp::connection_hdl hdl);
  void onClose(websocketpp::connection_hdl hdl);
  void onFail(websocketpp::connection_hdl hdl);
  void onMessage(websocketpp::connection_hdl hdl, WsClient::message_ptr msg);

  // Internal methods
  void initWebSocket();
  void connectWebSocket();
  void subscribe();
  void attemptReconnect();

  // JWT generation for Coinbase Advanced Trade API
  std::string generateJWT(const std::string &method, const std::string &path);

  // Time utilities
  std::chrono::system_clock::time_point getCurrentTime();
  std::chrono::system_clock::time_point getServerTime();

  // Publish market data to Redis
  void publishMarketData(const std::string &symbol, double price, double volume,
                         const std::string &type = "TRADE");

  // Configuration
  std::string api_key_;
  std::string api_secret_;
  std::string redis_channel_;
  std::vector<std::string> product_ids_;

  // Redis publisher
  std::shared_ptr<RedisPublisher> publisher_;

  // WebSocket client
  WsClient ws_client_;
  websocketpp::connection_hdl hdl_;
  std::thread ws_thread_;
  std::mutex connection_mutex_;

  // REST client for time sync
  std::unique_ptr<httplib::Client> rest_client_;

  // State
  std::atomic<bool> is_connected_;
  std::atomic<bool> should_reconnect_;
  int reconnect_attempts_;
  long time_offset_;

  // Price cache for latest prices
  std::map<std::string, double> last_prices_;
};

} // namespace gammatrade
