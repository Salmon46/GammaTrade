#include "CoinbaseAdapter.hpp"
#include <algorithm>

namespace gammatrade {

CoinbaseAdapter::CoinbaseAdapter(const std::string &api_key,
                                 const std::string &api_secret,
                                 std::shared_ptr<RedisPublisher> publisher,
                                 const std::string &redis_channel)
    : api_key_(api_key), api_secret_(api_secret), publisher_(publisher),
      redis_channel_(redis_channel), is_connected_(false),
      should_reconnect_(true), reconnect_attempts_(0), time_offset_(0) {
  initWebSocket();
  rest_client_ = std::make_unique<httplib::Client>("https://api.coinbase.com");
}

CoinbaseAdapter::~CoinbaseAdapter() { disconnect(); }

void CoinbaseAdapter::initWebSocket() {
  ws_client_.init_asio();

  // Setup TLS
  ws_client_.set_tls_init_handler([](websocketpp::connection_hdl) {
    return websocketpp::lib::make_shared<websocketpp::lib::asio::ssl::context>(
        websocketpp::lib::asio::ssl::context::tlsv12);
  });

  // Disable verbose logging
  ws_client_.clear_access_channels(websocketpp::log::alevel::all);
  ws_client_.clear_error_channels(websocketpp::log::elevel::all);

  // Set handlers
  using websocketpp::lib::bind;
  using websocketpp::lib::placeholders::_1;
  using websocketpp::lib::placeholders::_2;

  ws_client_.set_open_handler(bind(&CoinbaseAdapter::onOpen, this, _1));
  ws_client_.set_close_handler(bind(&CoinbaseAdapter::onClose, this, _1));
  ws_client_.set_fail_handler(bind(&CoinbaseAdapter::onFail, this, _1));
  ws_client_.set_message_handler(
      bind(&CoinbaseAdapter::onMessage, this, _1, _2));
}

void CoinbaseAdapter::connect(const std::vector<std::string> &product_ids) {
  product_ids_ = product_ids;
  should_reconnect_ = true;

  // Sync time before connecting
  syncTime();

  connectWebSocket();
}

void CoinbaseAdapter::connectWebSocket() {
  websocketpp::lib::error_code ec;
  WsClient::connection_ptr con =
      ws_client_.get_connection("wss://advanced-trade-ws.coinbase.com", ec);

  if (ec) {
    spdlog::error("CoinbaseAdapter: Connection initialization error: {}",
                  ec.message());
    attemptReconnect();
    return;
  }

  ws_client_.connect(con);
  ws_thread_ = std::thread([this]() { ws_client_.run(); });
}

void CoinbaseAdapter::disconnect() {
  should_reconnect_ = false;

  try {
    if (is_connected_) {
      ws_client_.stop();
    }

    std::lock_guard<std::mutex> lock(connection_mutex_);
    if (ws_thread_.joinable()) {
      ws_thread_.join();
    }
    spdlog::info("CoinbaseAdapter: Disconnected");
  } catch (const std::exception &e) {
    spdlog::error("CoinbaseAdapter: Exception during disconnect: {}", e.what());
  }
}

void CoinbaseAdapter::onOpen(websocketpp::connection_hdl hdl) {
  spdlog::info("CoinbaseAdapter: WebSocket connected");
  hdl_ = hdl;
  is_connected_ = true;
  reconnect_attempts_ = 0;
  subscribe();
}

void CoinbaseAdapter::onClose(websocketpp::connection_hdl hdl) {
  spdlog::warn("CoinbaseAdapter: WebSocket closed");
  is_connected_ = false;
  attemptReconnect();
}

void CoinbaseAdapter::onFail(websocketpp::connection_hdl hdl) {
  spdlog::error("CoinbaseAdapter: WebSocket connection failed");
  is_connected_ = false;
  attemptReconnect();
}

void CoinbaseAdapter::onMessage(websocketpp::connection_hdl hdl,
                                WsClient::message_ptr msg) {
  try {
    json j = json::parse(msg->get_payload());
    std::string channel = j.value("channel", "");

    if (channel == "market_trades") {
      auto events = j["events"];
      for (const auto &event : events) {
        if (event["type"] == "update") {
          auto trades = event["trades"];
          for (const auto &trade : trades) {
            if (!trade.contains("price") || !trade.contains("size") ||
                !trade.contains("product_id")) {
              continue;
            }

            if (trade["price"].is_null() || trade["size"].is_null() ||
                trade["product_id"].is_null()) {
              continue;
            }

            try {
              std::string product_id = trade.value("product_id", "");
              double price = std::stod(trade.value("price", "0.0"));
              double size = std::stod(trade.value("size", "0.0"));

              // Update price cache
              last_prices_[product_id] = price;

              // Publish to Redis
              publishMarketData(product_id, price, size, "TRADE");

            } catch (const std::exception &e) {
              spdlog::warn("CoinbaseAdapter: Error parsing trade: {}",
                           e.what());
            }
          }
        }
      }
    } else if (channel == "ticker") {
      // Handle ticker data for quotes
      auto events = j["events"];
      for (const auto &event : events) {
        if (event.contains("tickers")) {
          for (const auto &ticker : event["tickers"]) {
            if (ticker.contains("product_id") && ticker.contains("price")) {
              std::string product_id = ticker.value("product_id", "");
              double price = std::stod(ticker.value("price", "0.0"));

              last_prices_[product_id] = price;
              publishMarketData(product_id, price, 0.0, "QUOTE");
            }
          }
        }
      }
    }

  } catch (const std::exception &e) {
    spdlog::error("CoinbaseAdapter: JSON parse error: {}", e.what());
  }
}

void CoinbaseAdapter::subscribe() {
  // Market trades subscription (public, no auth required)
  json sub_msg;
  sub_msg["type"] = "subscribe";
  sub_msg["product_ids"] = product_ids_;
  sub_msg["channel"] = "market_trades";

  websocketpp::lib::error_code ec;
  ws_client_.send(hdl_, sub_msg.dump(), websocketpp::frame::opcode::text, ec);

  if (ec) {
    spdlog::error("CoinbaseAdapter: Failed to subscribe: {}", ec.message());
  } else {
    spdlog::info("CoinbaseAdapter: Subscribed to market_trades for {} products",
                 product_ids_.size());
  }

  // Ticker subscription for quote data
  json ticker_msg;
  ticker_msg["type"] = "subscribe";
  ticker_msg["product_ids"] = product_ids_;
  ticker_msg["channel"] = "ticker";

  ws_client_.send(hdl_, ticker_msg.dump(), websocketpp::frame::opcode::text,
                  ec);
  if (!ec) {
    spdlog::info("CoinbaseAdapter: Subscribed to ticker channel");
  }
}

void CoinbaseAdapter::attemptReconnect() {
  if (!should_reconnect_)
    return;

  // Spawn reconnection thread
  std::thread([this]() {
    int wait_seconds = std::min(30, (1 << reconnect_attempts_));
    spdlog::info("CoinbaseAdapter: Reconnecting in {} seconds (attempt {})",
                 wait_seconds, reconnect_attempts_ + 1);

    std::this_thread::sleep_for(std::chrono::seconds(wait_seconds));

    if (!should_reconnect_)
      return;

    reconnect_attempts_++;

    {
      std::lock_guard<std::mutex> lock(connection_mutex_);
      if (!should_reconnect_)
        return;

      if (ws_thread_.joinable()) {
        ws_thread_.join();
      }

      ws_client_.reset();
      connectWebSocket();
    }
  }).detach();
}

void CoinbaseAdapter::publishMarketData(const std::string &symbol, double price,
                                        double volume,
                                        const std::string &type) {
  // Create Protobuf message
  MarketData msg;
  msg.set_type(type);
  msg.set_symbol(symbol);
  msg.set_price(price);
  msg.set_volume(volume);
  msg.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                        getCurrentTime().time_since_epoch())
                        .count());
  msg.set_source("coinbase");

  // Serialize and publish
  std::string serialized;
  if (msg.SerializeToString(&serialized)) {
    int subscribers = publisher_->publish(redis_channel_, serialized);
    if (subscribers < 0) {
      spdlog::warn("CoinbaseAdapter: Failed to publish to Redis");
    }
  } else {
    spdlog::error("CoinbaseAdapter: Failed to serialize MarketData");
  }
}

void CoinbaseAdapter::syncTime() {
  auto server_time = getServerTime();
  auto local_time = std::chrono::system_clock::now();
  time_offset_ =
      std::chrono::duration_cast<std::chrono::seconds>(server_time - local_time)
          .count();
  spdlog::info("CoinbaseAdapter: Time synced. Offset: {} seconds",
               time_offset_);
}

std::chrono::system_clock::time_point CoinbaseAdapter::getServerTime() {
  auto res = rest_client_->Get("/api/v3/brokerage/time");
  if (res && res->status == 200) {
    try {
      json j = json::parse(res->body);
      if (j.contains("iso")) {
        std::string iso = j["iso"];
        std::tm tm = {};
        std::istringstream ss(iso);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
#ifdef _WIN32
        return std::chrono::system_clock::from_time_t(_mkgmtime(&tm));
#else
        return std::chrono::system_clock::from_time_t(timegm(&tm));
#endif
      }
    } catch (...) {
    }
  }
  return std::chrono::system_clock::now();
}

std::chrono::system_clock::time_point CoinbaseAdapter::getCurrentTime() {
  return std::chrono::system_clock::now() + std::chrono::seconds(time_offset_);
}

std::string CoinbaseAdapter::generateJWT(const std::string &method,
                                         const std::string &path) {
  // JWT generation for authenticated endpoints
  // This is a simplified version - full implementation would use ES256 signing
  // For now, public endpoints don't require JWT
  return "";
}

} // namespace gammatrade
