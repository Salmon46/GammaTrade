#include "CoinbaseExecutionAdapter.hpp"
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <spdlog/spdlog.h>
#include <thread>

namespace {
std::atomic<bool> running{true};
}

void signalHandler(int signum) {
  spdlog::info("Received signal {}, shutting down...", signum);
  running = false;
  // In a real app we might need to properly stop the blocking listener
  // For now we rely on the container restart or forceful kill if it hangs
  std::exit(signum);
}

int main(int argc, char *argv[]) {
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  spdlog::info("===========================================");
  spdlog::info("  GammaTrade Execution Adapter 2 (Coinbase)");
  spdlog::info("===========================================");

  const char *apiKey = std::getenv("COINBASE_API_KEY");
  const char *apiSecret = std::getenv("COINBASE_API_SECRET");
  const char *redisHost = std::getenv("REDIS_HOST");

  if (!apiKey || !apiSecret) {
    spdlog::error(
        "Error: COINBASE_API_KEY and COINBASE_API_SECRET must be set");
    return 1;
  }

  const char *apiUrl = std::getenv("COINBASE_API_URL");
  std::string host = redisHost ? redisHost : "redis";
  int port = 6379;

  std::string cbUrl = apiUrl ? apiUrl : "https://api.coinbase.com";

  spdlog::info("Configuration:");
  spdlog::info("  Redis: {}:{}", host, port);
  spdlog::info("  Coinbase URL: {}", cbUrl);

  gammatrade::CoinbaseExecutionAdapter adapter(apiKey, apiSecret, cbUrl, host,
                                               port);

  try {
    adapter.start();
  } catch (const std::exception &e) {
    spdlog::error("Fatal Error: {}", e.what());
    return 1;
  }

  return 0;
}
