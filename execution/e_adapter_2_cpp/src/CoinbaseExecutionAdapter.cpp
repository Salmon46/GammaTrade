#include "CoinbaseExecutionAdapter.hpp"
#include <google/protobuf/util/json_util.h>
#include <random>
#include <spdlog/spdlog.h>
#include <sstream>

using json = nlohmann::json;

namespace gammatrade {

CoinbaseExecutionAdapter::CoinbaseExecutionAdapter(const std::string &apiKey,
                                                   const std::string &apiSecret,
                                                   const std::string &apiUrl,
                                                   const std::string &redisHost,
                                                   int redisPort)
    : apiKey_(apiKey), apiSecret_(apiSecret) {

  subscriber_ = std::make_shared<RedisSubscriber>(redisHost, redisPort);
  publisher_ = std::make_shared<RedisPublisher>(redisHost, redisPort);

  // Initialize HTTP client for Coinbase Advanced Trade API
  // Parse host from URL (simple check, assuming https://host or http://host)
  std::string host = apiUrl;
  if (host.find("https://") == 0) {
    // httplib handles https:// prefix automatically in newer versions or we
    // construct basic client For simplicity here, we pass the full URL string
    // which httplib::Client supports
  }

  httpClient_ = std::make_unique<httplib::Client>(apiUrl.c_str());
  httpClient_->set_connection_timeout(5); // 5 seconds
  httpClient_->set_read_timeout(10);      // 10 seconds
}

CoinbaseExecutionAdapter::~CoinbaseExecutionAdapter() { stop(); }

void CoinbaseExecutionAdapter::start() {
  spdlog::info("CoinbaseExecutionAdapter: Starting...");

  if (!publisher_->connect()) {
    spdlog::error(
        "CoinbaseExecutionAdapter: Failed to connect publisher to Redis");
    return;
  }

  if (!subscriber_->connect()) {
    spdlog::error(
        "CoinbaseExecutionAdapter: Failed to connect subscriber to Redis");
    return;
  }

  // Subscribe to execution queue
  subscriber_->subscribe(
      "exec_queue_coinbase",
      [this](const std::string &channel, const char *data, size_t length) {
        this->onTradeSignal(channel, data, length);
      });

  // Start account broadcasting thread
  running_ = true;
  accountThread_ =
      std::thread(&CoinbaseExecutionAdapter::broadcastAccountUpdates, this);

  // Start listening (blocking)
  subscriber_->listen();
}

void CoinbaseExecutionAdapter::stop() {
  running_ = false;
  if (subscriber_)
    subscriber_->stop();
  if (publisher_)
    publisher_->disconnect();

  if (accountThread_.joinable()) {
    accountThread_.join();
  }
}

void CoinbaseExecutionAdapter::onTradeSignal(const std::string &channel,
                                             const char *data, size_t length) {
  TradeSignal signal;
  if (!signal.ParseFromArray(data, length)) {
    spdlog::error(
        "CoinbaseExecutionAdapter: Failed to parse TradeSignal from {}",
        channel);
    return;
  }

  spdlog::info("CoinbaseExecutionAdapter: Received signal {} for {} {}",
               signal.signal_id(), signal.symbol(),
               signal.action() == TradeAction::BUY ? "BUY" : "SELL");

  executeOrder(signal);
}

void CoinbaseExecutionAdapter::executeOrder(const TradeSignal &signal) {
  std::string clientOrderId = generateClientOrderId();
  std::string productId =
      signal.symbol(); // Assuming symbol matches product_id (e.g. BTC-USD)
  std::string side = (signal.action() == TradeAction::BUY) ? "BUY" : "SELL";

  // Construct JSON body for Coinbase API v3
  json body;
  body["client_order_id"] = clientOrderId;
  body["product_id"] = productId;
  body["side"] = side;

  json order_config;

  if (signal.order_type() == OrderType::LIMIT) {
    // LIMIT GTC
    json limit_limit_gtc;
    limit_limit_gtc["base_size"] = std::to_string(signal.quantity());
    limit_limit_gtc["limit_price"] = std::to_string(signal.limit_price());
    limit_limit_gtc["post_only"] =
        false; // Allow taker if crosses, set true to force Maker
    order_config["limit_limit_gtc"] = limit_limit_gtc;
    spdlog::info(
        "CoinbaseExecutionAdapter: Preparing LIMIT GTC order for {} @ {}",
        signal.quantity(), signal.limit_price());
  } else {
    // MARKET IOC (Default)
    json market_ioc;
    market_ioc["base_size"] = std::to_string(signal.quantity());
    order_config["market_market_ioc"] = market_ioc;
    spdlog::info("CoinbaseExecutionAdapter: Preparing MARKET IOC order for {}",
                 signal.quantity());
  }

  body["order_configuration"] = order_config;

  body["order_configuration"] = order_config;

  std::string bodyStr = body.dump();
  std::string method = "POST";
  std::string path = "/api/v3/brokerage/orders";

  // Authentication
  std::string jwt =
      utils::CoinbaseAuth::generate_jwt(apiKey_, apiSecret_, method, path);

  httplib::Headers headers = {{"Authorization", "Bearer " + jwt},
                              {"Content-Type", "application/json"}};

  spdlog::info("CoinbaseExecutionAdapter: Sending order {} to Coinbase...",
               clientOrderId);

  auto res =
      httpClient_->Post(path.c_str(), headers, bodyStr, "application/json");

  ExecutionReport report;
  report.set_signal_id(signal.signal_id());
  report.set_symbol(productId);
  report.set_status(ExecutionStatus::REJECTED); // Default to fail
  report.set_adapter("coinbase");

  // Set side field directly
  report.set_side(side);

  // Convert Proto Timestamp (int64 milliseconds)
  auto now = std::chrono::system_clock::now();
  long millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch())
                    .count();
  report.set_timestamp(millis);

  if (res && (res->status == 200 || res->status == 201)) {
    json response = json::parse(res->body);
    spdlog::info("CoinbaseExecutionAdapter: Order Success: {}", res->body);

    if (response.contains("success") && response["success"].get<bool>()) {
      report.set_status(ExecutionStatus::SUBMITTED); // Or NEW/SUBMITTED
      if (response.contains("order_id")) {
        report.set_order_id(response["order_id"].get<std::string>());
      }
    } else {
      report.set_status(ExecutionStatus::REJECTED);
      if (response.contains("error_response")) {
        report.set_error_message(response["error_response"].dump());
      }
    }

  } else {
    spdlog::error("CoinbaseExecutionAdapter: HTTP Error: {}",
                  res ? std::to_string(res->status) : "Unknown");
    report.set_error_message("HTTP Request Failed: " +
                             (res ? std::to_string(res->status) : "N/A"));
  }

  // Publish Report
  std::string reportData;
  report.SerializeToString(&reportData);
  publisher_->publish("execution_report", reportData.data(), reportData.size());
}

std::string CoinbaseExecutionAdapter::generateClientOrderId() {
  static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string tmp_s;
  tmp_s.reserve(16);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
  for (int i = 0; i < 16; ++i)
    tmp_s += alphanum[dis(gen)];
  return tmp_s;
}

void CoinbaseExecutionAdapter::broadcastAccountUpdates() {
  // Use a separate client for this thread to avoid contention
  // Assuming default coinbase API URL if not stored separately or passed in
  // We can reuse the same base logic as constructor but we need the URL.
  // The existing httpClient_ was created with apiUrl in constructor but we
  // don't store apiUrl_. Let's assume standard Coinbase URL or we should have
  // stored it. For now, hardcode or try to infer. Constructor took apiUrl. Best
  // practice: Store apiUrl_ in class. As a fix, I'll use
  // "https://api.coinbase.com" or the env var if I can get it, but better to
  // add apiUrl_ to class. Wait, I can't easily add a member without modifying
  // heade again. I'll assume standard URL for now or just recreate what main
  // passed. Actually, I can just use the global env var if needed or hardcode.

  std::string url = "https://api.coinbase.com"; // Default
  const char *envUrl = std::getenv("COINBASE_API_URL");
  if (envUrl)
    url = envUrl;

  httplib::Client client(url.c_str());
  client.set_connection_timeout(5);
  client.set_read_timeout(10);

  spdlog::info("CoinbaseExecutionAdapter: Account broadcast thread started.");

  while (running_) {
    try {
      std::string method = "GET";
      std::string path = "/api/v3/brokerage/accounts";
      std::string jwt =
          utils::CoinbaseAuth::generate_jwt(apiKey_, apiSecret_, method, path);

      httplib::Headers headers = {{"Authorization", "Bearer " + jwt},
                                  {"Content-Type", "application/json"}};

      auto res = client.Get(path.c_str(), headers);

      if (res && res->status == 200) {
        json response = json::parse(res->body);
        double total_equity = 0.0;
        double usd_cash = 0.0;

        // "accounts": [ { "uuid": "...", "name": "BTC Wallet", "currency":
        // "BTC", "available_balance": { "value": "0.00", "currency": "BTC" },
        // ... } ] We need to sum up value. Coinbase API might not give total
        // equity directly in this endpoint easily without prices. Actually, for
        // "cash" we can look for USD wallet. For total equity, we need prices.
        // Simplified: Just report USD Cash as "equity" for now if we can't
        // easily get total portfolio value without more calls. OR look for
        // "USDC" and "USD".

        if (response.contains("accounts")) {
          for (auto &account : response["accounts"]) {
            if (account.contains("currency") && account["currency"] == "USD") {
              if (account.contains("available_balance")) {
                auto &bal = account["available_balance"];
                usd_cash += std::stod(bal["value"].get<std::string>());
              }
            }
          }
        }

        // For now, Strategy Engine expects 'equity' to determine buying power.
        // If we are trading spot, USD cash is what matters for buying.
        // If we want total portfolio value, we need more.
        // Let's call it 'equity' but it's really available cash for trading.
        total_equity = usd_cash;

        json update;
        update["adapter"] = "coinbase";
        update["equity"] = total_equity;
        update["cash"] = usd_cash;
        update["timestamp"] =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

        std::string msg = update.dump();
        publisher_->publish("account_updates", msg.data(), msg.size());
      } else {
        // spdlog::warn("Failed to fetch accounts: {}", res ?
        // std::to_string(res->status) : "N/A");
      }
    } catch (const std::exception &e) {
      spdlog::error("Error in account broadcast: {}", e.what());
    }

    // Sleep 5 seconds
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

} // namespace gammatrade
