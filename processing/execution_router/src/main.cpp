/**
 * Execution Router - Main Entry Point
 *
 * Routes trade signals from the Strategy Engine to appropriate E-Adapters,
 * validates orders through the RiskManager, and manages execution feedback.
 */

#include "ExecutionRouter.hpp"
#include "Metrics.hpp"
#include "RedisClient.hpp"
#include "RiskManager.hpp"


#include <csignal>
#include <cstdlib>
#include <memory>
#include <spdlog/spdlog.h>
#include <thread>

// Global flag for graceful shutdown
std::atomic<bool> g_running{true};

void signalHandler(int signum) {
  spdlog::info("Received signal {}, shutting down...", signum);
  g_running = false;
}

int main(int argc, char *argv[]) {
  // Start Metrics Server
  try {
    Metrics::instance().startServer(8000);
    Metrics::instance().setServiceStatus("execution_router", 1.0);
    spdlog::info("Metrics server started on port 8000");
  } catch (const std::exception &e) {
    spdlog::error("Failed to start metrics server: {}", e.what());
  }

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
  spdlog::info("  GammaTrade Execution Router");
  spdlog::info("===========================================");

  // Load configuration from environment
  std::string redis_host =
      std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "localhost";
  int redis_port =
      std::getenv("REDIS_PORT") ? std::stoi(std::getenv("REDIS_PORT")) : 6379;

  spdlog::info("Configuration:");
  spdlog::info("  Redis: {}:{}", redis_host, redis_port);

  // Create Redis client
  auto redis =
      std::make_shared<gammatrade::RedisClient>(redis_host, redis_port);
  if (!redis->connect()) {
    spdlog::error("Failed to connect to Redis. Exiting.");
    return 1;
  }

  // Create Risk Manager with configuration
  gammatrade::RiskConfig riskConfig;
  riskConfig.max_order_size = 1000.0;
  riskConfig.max_order_value = 100000.0;
  riskConfig.max_daily_loss = 5000.0;
  riskConfig.max_positions = 10;
  riskConfig.risk_percent_per_trade = 0.02;

  // Override from environment if set
  if (std::getenv("MAX_ORDER_SIZE")) {
    riskConfig.max_order_size = std::stod(std::getenv("MAX_ORDER_SIZE"));
  }
  if (std::getenv("MAX_DAILY_LOSS")) {
    riskConfig.max_daily_loss = std::stod(std::getenv("MAX_DAILY_LOSS"));
  }
  if (std::getenv("MAX_POSITIONS")) {
    riskConfig.max_positions = std::stoi(std::getenv("MAX_POSITIONS"));
  }

  auto riskManager = std::make_shared<gammatrade::RiskManager>(riskConfig);

  spdlog::info("Risk Configuration:");
  spdlog::info("  Max Order Size: {}", riskConfig.max_order_size);
  spdlog::info("  Max Order Value: ${}", riskConfig.max_order_value);
  spdlog::info("  Max Daily Loss: ${}", riskConfig.max_daily_loss);
  spdlog::info("  Max Positions: {}", riskConfig.max_positions);

  // Create Execution Router
  gammatrade::ExecutionRouter router(redis, riskManager);

  // Register execution adapters
  router.registerAdapter("alpaca", "exec_queue_alpaca");
  router.registerAdapter("coinbase", "exec_queue_coinbase");

  // Start the router
  router.start();

  // Main loop - wait for shutdown signal
  while (g_running && router.isRunning()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Cleanup
  spdlog::info("Shutting down router...");
  Metrics::instance().setServiceStatus("execution_router", 0.0);
  router.stop();
  redis->disconnect();

  spdlog::info("Goodbye!");
  return 0;
}
