#include "ExecutionRouter.hpp"
#include "Metrics.hpp"
#include <chrono>
#include <spdlog/spdlog.h>

namespace gammatrade {

ExecutionRouter::ExecutionRouter(std::shared_ptr<RedisClient> redis,
                                 std::shared_ptr<RiskManager> riskManager)
    : redis_(redis), riskManager_(riskManager), signalChannel_("trade_signals"),
      reportChannel_("execution_report"), feedbackChannel_("strategy_feedback"),
      completedChannel_("trades_completed"), running_(false),
      signalsReceived_(0), signalsRouted_(0), signalsRejected_(0) {}

ExecutionRouter::~ExecutionRouter() { stop(); }

void ExecutionRouter::registerAdapter(const std::string &name,
                                      const std::string &execChannel) {
  AdapterConfig config;
  config.name = name;
  config.exec_channel = execChannel;
  config.enabled = true;

  adapters_[name] = config;
  spdlog::info("ExecutionRouter: Registered adapter '{}' -> channel '{}'", name,
               execChannel);
}

void ExecutionRouter::start() {
  if (running_)
    return;

  running_ = true;

  // Define a single dispatch callback to handle all incoming messages
  auto dispatchCallback = [this](const std::string &channel, const void *data,
                                 size_t length) {
    // Check if it's a trade signal (starts with "trade_signals")
    if (channel.rfind("trade_signals", 0) == 0) {
      handleTradeSignal(channel, data, length);
    }
    // Check if it's an execution report
    else if (channel == reportChannel_) {
      handleExecutionReport(channel, data, length);
    } else {
      spdlog::debug(
          "ExecutionRouter: Received message on unhandled channel: {}",
          channel);
    }
  };

  // Subscribe using the shared callback
  redis_->psubscribe("trade_signals*", dispatchCallback);
  redis_->subscribe(reportChannel_, dispatchCallback);

  // Start listener in background thread
  listenerThread_ = std::thread([this]() { redis_->listen(); });

  spdlog::info("ExecutionRouter: Started");
  spdlog::info("  Listening: {}, {}", signalChannel_, reportChannel_);
  spdlog::info("  Publishing to: {} (feedback), {} (completed)",
               feedbackChannel_, completedChannel_);
}

void ExecutionRouter::stop() {
  if (!running_)
    return;

  running_ = false;
  redis_->stopListening();

  if (listenerThread_.joinable()) {
    listenerThread_.join();
  }

  spdlog::info("ExecutionRouter: Stopped");
  spdlog::info("  Stats: {} received, {} routed, {} rejected", signalsReceived_,
               signalsRouted_, signalsRejected_);
}

void ExecutionRouter::handleTradeSignal(const std::string &channel,
                                        const void *data, size_t length) {
  signalsReceived_++;

  TradeSignal signal;
  if (!signal.ParseFromArray(data, static_cast<int>(length))) {
    spdlog::error("ExecutionRouter: Failed to parse TradeSignal");
    Metrics::instance().recordError("execution_router", "parse_signal_error");
    return;
  }

  spdlog::info("ExecutionRouter: Received signal {} - {} {} {}",
               signal.signal_id(), TradeAction_Name(signal.action()),
               signal.quantity(), signal.symbol());

  // Store timestamp for latency tracking
  {
    std::lock_guard<std::mutex> lock(timestampsMutex_);
    signalTimestamps_[signal.signal_id()] = signal.timestamp();
  }

  Metrics::instance().incrementSignalsReceived(signal.strategy_id(),
                                               signal.symbol());

  routeSignal(signal);
}

void ExecutionRouter::handleExecutionReport(const std::string &channel,
                                            const void *data, size_t length) {
  ExecutionReport report;
  if (!report.ParseFromArray(data, static_cast<int>(length))) {
    spdlog::error("ExecutionRouter: Failed to parse ExecutionReport");
    Metrics::instance().recordError("execution_router", "parse_report_error");
    return;
  }

  spdlog::info("ExecutionRouter: Execution report {} - {} @ {:.2f}",
               report.signal_id(), ExecutionStatus_Name(report.status()),
               report.average_fill_price());

  Metrics::instance().incrementExecutionsProcessed(
      report.adapter(), ExecutionStatus_Name(report.status()));

  // Record latency if filled
  if (report.status() == ExecutionStatus::FILLED) {
    int64_t signal_ts = 0;
    {
      std::lock_guard<std::mutex> lock(timestampsMutex_);
      auto it = signalTimestamps_.find(report.signal_id());
      if (it != signalTimestamps_.end()) {
        signal_ts = it->second;
        signalTimestamps_.erase(it);
      }
    }

    if (signal_ts > 0) {
      double latency_seconds = (report.timestamp() - signal_ts) / 1000.0;
      Metrics::instance().recordOrderLatency(report.adapter(), latency_seconds);
      spdlog::debug("ExecutionRouter: Order latency for {}: {:.3f}s",
                    report.signal_id(), latency_seconds);
    }

    // Update risk manager position
    std::string side = (report.filled_quantity() > 0) ? "BUY" : "SELL";
    riskManager_->updatePosition(report.symbol(),
                                 std::abs(report.filled_quantity()), side);

    recordCompletedTrade(report);
  } else if (report.status() == ExecutionStatus::REJECTED ||
             report.status() == ExecutionStatus::FAILED ||
             report.status() == ExecutionStatus::CANCELLED) {
    // Cleanup timestamp if it's a terminal state
    std::lock_guard<std::mutex> lock(timestampsMutex_);
    signalTimestamps_.erase(report.signal_id());
  }

  // Forward to strategy engine
  forwardToStrategy(report);
}

void ExecutionRouter::routeSignal(const TradeSignal &signal) {
  // Skip HOLD signals
  if (signal.action() == TradeAction::HOLD) {
    spdlog::debug("ExecutionRouter: Skipping HOLD signal");
    return;
  }

  // Determine side string
  std::string side = (signal.action() == TradeAction::BUY) ? "BUY" : "SELL";

  // Risk check
  bool approved = riskManager_->checkOrder(signal.symbol(), signal.quantity(),
                                           signal.limit_price() > 0
                                               ? signal.limit_price()
                                               : 100.0, // Estimate if no price
                                           side);

  if (!approved) {
    signalsRejected_++;
    spdlog::warn("ExecutionRouter: Signal {} REJECTED by RiskManager",
                 signal.signal_id());
    Metrics::instance().incrementExecutionsProcessed("router_risk", "REJECTED");

    // Cleanup timestamp since it won't be filled
    {
      std::lock_guard<std::mutex> lock(timestampsMutex_);
      signalTimestamps_.erase(signal.signal_id());
    }
    return;
  }

  // Find target adapter
  std::string target = signal.target_adapter();
  if (target.empty()) {
    target = "alpaca"; // Default
  }

  auto it = adapters_.find(target);
  if (it == adapters_.end() || !it->second.enabled) {
    spdlog::error("ExecutionRouter: Unknown or disabled adapter: {}", target);
    signalsRejected_++;
    return;
  }

  // Serialize and route to execution channel
  std::string serialized;
  if (!signal.SerializeToString(&serialized)) {
    spdlog::error("ExecutionRouter: Failed to serialize signal");
    return;
  }

  int subscribers = redis_->publish(it->second.exec_channel, serialized);

  if (subscribers > 0) {
    signalsRouted_++;
    spdlog::info("ExecutionRouter: Routed signal {} to {} ({} subscribers)",
                 signal.signal_id(), it->second.exec_channel, subscribers);
  } else {
    spdlog::warn("ExecutionRouter: No subscribers on channel {}",
                 it->second.exec_channel);
    Metrics::instance().recordError("execution_router",
                                    "no_adapter_subscribers");
  }
}

void ExecutionRouter::forwardToStrategy(const ExecutionReport &report) {
  std::string serialized;
  if (!report.SerializeToString(&serialized)) {
    spdlog::error("ExecutionRouter: Failed to serialize report for feedback");
    return;
  }

  redis_->publish(feedbackChannel_, serialized);
  spdlog::debug("ExecutionRouter: Forwarded status to strategy");
}

void ExecutionRouter::recordCompletedTrade(const ExecutionReport &report) {
  TradeCompleted trade;
  trade.set_signal_id(report.signal_id());
  trade.set_order_id(report.order_id());
  trade.set_symbol(report.symbol());
  trade.set_side(report.filled_quantity() >= 0 ? "BUY" : "SELL");
  trade.set_quantity(std::abs(report.filled_quantity()));
  trade.set_price(report.average_fill_price());
  trade.set_total_value(std::abs(report.filled_quantity()) *
                        report.average_fill_price());
  trade.set_commission(report.commission());
  trade.set_adapter(report.adapter());
  trade.set_timestamp(report.timestamp());

  std::string serialized;
  if (!trade.SerializeToString(&serialized)) {
    spdlog::error("ExecutionRouter: Failed to serialize completed trade");
    return;
  }

  redis_->publish(completedChannel_, serialized);
  spdlog::info("ExecutionRouter: Recorded completed trade {} - {} {} @ {:.2f}",
               trade.order_id(), trade.side(), trade.quantity(), trade.price());
}

} // namespace gammatrade