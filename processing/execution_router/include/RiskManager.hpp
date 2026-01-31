#pragma once

/**
 * RiskManager - Pre-trade risk checks for the Execution Router
 *
 * Ported from deprecated_platform/include/core/RiskManager.hpp
 * Simplified for use without PositionManager/PortfolioManager dependencies.
 */

#include <atomic>
#include <cmath>
#include <map>
#include <spdlog/spdlog.h>
#include <string>


namespace gammatrade {

/**
 * Risk configuration parameters
 */
struct RiskConfig {
  double max_order_size = 1000.0;       // Maximum quantity per order
  double max_order_value = 100000.0;    // Maximum USD value per order
  double max_daily_loss = 5000.0;       // Maximum daily loss before kill switch
  int max_positions = 10;               // Maximum concurrent positions
  double risk_percent_per_trade = 0.02; // 2% risk per trade
};

/**
 * RiskManager - Validates trade signals before execution
 *
 * Implements:
 * - Kill switch functionality
 * - Fat finger checks (order size limits)
 * - Daily loss limits / max drawdown
 * - Position concentration limits
 */
class RiskManager {
public:
  explicit RiskManager(const RiskConfig &config = RiskConfig())
      : config_(config), current_drawdown_(0.0), peak_equity_(0.0),
        kill_switch_active_(false) {}

  /**
   * Update the risk configuration
   */
  void setConfig(const RiskConfig &config) { config_ = config; }

  /**
   * Check if an order passes all risk checks
   * @param symbol Trading symbol
   * @param quantity Order quantity
   * @param price Estimated price (for value calculations)
   * @param side "BUY" or "SELL"
   * @return true if order passes all checks, false otherwise
   */
  bool checkOrder(const std::string &symbol, double quantity, double price,
                  const std::string &side) {

    // 1. Kill Switch Check
    if (kill_switch_active_) {
      spdlog::error("RiskManager: Order REJECTED - KILL SWITCH ACTIVE");
      return false;
    }

    // 2. Fat Finger Check - Quantity
    if (quantity > config_.max_order_size) {
      spdlog::error(
          "RiskManager: Order REJECTED - Quantity {} exceeds limit {}",
          quantity, config_.max_order_size);
      return false;
    }

    // 3. Fat Finger Check - Value
    double order_value = quantity * price;
    if (order_value > config_.max_order_value) {
      spdlog::error(
          "RiskManager: Order REJECTED - Value ${:.2f} exceeds limit ${:.2f}",
          order_value, config_.max_order_value);
      return false;
    }

    // 4. Daily Loss / Max Drawdown Limit
    if (current_drawdown_ >= config_.max_daily_loss) {
      spdlog::error("RiskManager: Order REJECTED - Max Drawdown Limit Reached "
                    "(${:.2f} >= ${:.2f})",
                    current_drawdown_, config_.max_daily_loss);
      return false;
    }

    // 5. Position Limit (only for opening new positions)
    if (side == "BUY" && positions_.find(symbol) == positions_.end()) {
      if (positions_.size() >= static_cast<size_t>(config_.max_positions)) {
        spdlog::error(
            "RiskManager: Order REJECTED - Max Positions Limit Reached "
            "({}/{})",
            positions_.size(), config_.max_positions);
        return false;
      }
    }

    spdlog::debug("RiskManager: Order APPROVED - {} {} {} @ {:.2f}", side,
                  quantity, symbol, price);
    return true;
  }

  /**
   * Update position tracking after a fill
   */
  void updatePosition(const std::string &symbol, double quantity,
                      const std::string &side) {
    if (side == "BUY") {
      positions_[symbol] += quantity;
    } else {
      positions_[symbol] -= quantity;
      if (std::abs(positions_[symbol]) < 0.0001) {
        positions_.erase(symbol);
      }
    }
  }

  /**
   * Update drawdown calculation
   * @param current_equity Current portfolio equity
   */
  void updateEquity(double current_equity) {
    if (current_equity > peak_equity_) {
      peak_equity_ = current_equity;
    }

    double drawdown = peak_equity_ - current_equity;
    if (drawdown < 0)
      drawdown = 0;

    current_drawdown_ = drawdown;

    // Auto-trigger kill switch at max drawdown
    if (current_drawdown_ >= config_.max_daily_loss && !kill_switch_active_) {
      triggerKillSwitch();
    }
  }

  /**
   * Trigger the kill switch (stops all trading)
   */
  void triggerKillSwitch() {
    kill_switch_active_ = true;
    spdlog::critical("RiskManager: KILL SWITCH TRIGGERED - All trading halted");
  }

  /**
   * Reset the kill switch (resume trading)
   */
  void resetKillSwitch() {
    kill_switch_active_ = false;
    spdlog::warn("RiskManager: Kill Switch RESET - Trading resumed");
  }

  /**
   * Check if kill switch is active
   */
  bool isKillSwitchActive() const { return kill_switch_active_.load(); }

  /**
   * Get current drawdown
   */
  double getCurrentDrawdown() const { return current_drawdown_; }

  /**
   * Get position count
   */
  size_t getPositionCount() const { return positions_.size(); }

private:
  RiskConfig config_;
  std::map<std::string, double> positions_; // symbol -> quantity
  double current_drawdown_;
  double peak_equity_;
  std::atomic<bool> kill_switch_active_;
};

} // namespace gammatrade
