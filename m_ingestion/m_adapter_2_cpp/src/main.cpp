/**
 * M-Adapter 2: Coinbase Market Data Adapter
 *
 * This service connects to Coinbase Advanced Trade WebSocket API,
 * receives real-time market data, normalizes it to Protobuf format,
 * and publishes to Redis channel "market_data_2".
 */

#include "CoinbaseAdapter.hpp"
#include "RedisPublisher.hpp"
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>
#include <vector>

// Global flag for graceful shutdown
std::atomic<bool> g_running{true};

void signalHandler(int signum) {
  spdlog::info("Received signal {}, shutting down...", signum);
  g_running = false;
}

std::vector<std::string> splitString(const std::string &str, char delimiter) {
  std::vector<std::string> tokens;
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, delimiter)) {
    if (!token.empty()) {
      tokens.push_back(token);
    }
  }
  return tokens;
}

int main(int argc, char *argv[]) {
  // Setup signal handlers
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  // Configure logging
  std::string log_level =
      std::getenv("LOG_LEVEL") ? std::getenv("LOG_LEVEL") : "info";
  if (log_level == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (log_level == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (log_level == "error") {
    spdlog::set_level(spdlog::level::err);
  } else {
    spdlog::set_level(spdlog::level::info);
  }

  spdlog::info("===========================================");
  spdlog::info("  GammaTrade M-Adapter 2 (Coinbase)");
  spdlog::info("===========================================");

  // Load configuration from environment
  std::string redis_host =
      std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "localhost";
  int redis_port =
      std::getenv("REDIS_PORT") ? std::stoi(std::getenv("REDIS_PORT")) : 6379;
  std::string api_key =
      std::getenv("COINBASE_API_KEY") ? std::getenv("COINBASE_API_KEY") : "";
  std::string api_secret = std::getenv("COINBASE_API_SECRET")
                               ? std::getenv("COINBASE_API_SECRET")
                               : "";
  std::string symbols_str =
      std::getenv("SYMBOLS") ? std::getenv("SYMBOLS") : "BTC-USD,ETH-USD";
  std::string redis_channel = std::getenv("REDIS_CHANNEL")
                                  ? std::getenv("REDIS_CHANNEL")
                                  : "market_data_2";

  // Parse symbols
  std::vector<std::string> symbols = splitString(symbols_str, ',');

  spdlog::info("Configuration:");
  spdlog::info("  Redis: {}:{}", redis_host, redis_port);
  spdlog::info("  Channel: {}", redis_channel);
  spdlog::info("  Symbols: {}", symbols_str);

  // Create Redis publisher
  auto publisher =
      std::make_shared<gammatrade::RedisPublisher>(redis_host, redis_port);
  if (!publisher->connect()) {
    spdlog::error("Failed to connect to Redis. Exiting.");
    return 1;
  }

  // Create Coinbase adapter
  gammatrade::CoinbaseAdapter adapter(api_key, api_secret, publisher,
                                      redis_channel);

  // Connect and start streaming
  spdlog::info("Connecting to Coinbase WebSocket...");
  adapter.connect(symbols);

  // Main loop - wait for shutdown signal
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (!adapter.isConnected()) {
      spdlog::warn("Adapter disconnected, waiting for reconnection...");
    }
  }

  // Cleanup
  spdlog::info("Shutting down adapter...");
  adapter.disconnect();
  publisher->disconnect();

  spdlog::info("Goodbye!");
  return 0;
}
