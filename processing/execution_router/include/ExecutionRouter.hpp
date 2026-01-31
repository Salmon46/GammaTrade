#pragma once

/**
 * ExecutionRouter - Routes trade signals to appropriate execution adapters
 *
 * Responsibilities:
 * 1. Subscribe to trade_signals channel
 * 2. Validate signals through RiskManager
 * 3. Route to appropriate E-Adapter execution queue
 * 4. Subscribe to execution_report for feedback
 * 5. Forward status to strategy_feedback
 * 6. Publish completed trades to trades_completed (for Data Logger)
 */

#include "RedisClient.hpp"
#include "RiskManager.hpp"
#include "execution_report.pb.h"
#include "trade_signal.pb.h"


#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>


namespace gammatrade {

/**
 * Routing configuration for execution adapters
 */
struct AdapterConfig {
  std::string name;         // Adapter identifier
  std::string exec_channel; // Redis channel to publish execution requests
  bool enabled;             // Whether this adapter is active
};

/**
 * ExecutionRouter - Central routing and risk management
 */
class ExecutionRouter {
public:
  ExecutionRouter(std::shared_ptr<RedisClient> redis,
                  std::shared_ptr<RiskManager> riskManager);

  ~ExecutionRouter();

  /**
   * Register an execution adapter
   */
  void registerAdapter(const std::string &name, const std::string &execChannel);

  /**
   * Start the router (begins listening for signals)
   */
  void start();

  /**
   * Stop the router
   */
  void stop();

  /**
   * Check if router is running
   */
  bool isRunning() const { return running_; }

private:
  // Message handlers
  void handleTradeSignal(const std::string &channel, const void *data,
                         size_t length);

  void handleExecutionReport(const std::string &channel, const void *data,
                             size_t length);

  // Routing logic
  void routeSignal(const TradeSignal &signal);

  // Forward execution status to strategy
  void forwardToStrategy(const ExecutionReport &report);

  // Record completed trade
  void recordCompletedTrade(const ExecutionReport &report);

  // Dependencies
  std::shared_ptr<RedisClient> redis_;
  std::shared_ptr<RiskManager> riskManager_;

  // Adapter registry
  std::map<std::string, AdapterConfig> adapters_;

  // Channel names
  std::string signalChannel_;
  std::string reportChannel_;
  std::string feedbackChannel_;
  std::string completedChannel_;

  // State
  std::atomic<bool> running_;
  std::thread listenerThread_;

  // Latency tracking
  std::unordered_map<std::string, int64_t> signalTimestamps_;
  std::mutex timestampsMutex_;

  // Statistics
  uint64_t signalsReceived_;
  uint64_t signalsRouted_;
  uint64_t signalsRejected_;
};

} // namespace gammatrade