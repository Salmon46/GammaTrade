#pragma once

#include "CoinbaseAuth.hpp"
#include "RedisPublisher.hpp"
#include "RedisSubscriber.hpp"
#include "execution_report.pb.h"
#include "trade_signal.pb.h"


#include <httplib.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>


namespace gammatrade {

class CoinbaseExecutionAdapter {
public:
  CoinbaseExecutionAdapter(const std::string &apiKey,
                           const std::string &apiSecret,
                           const std::string &apiUrl = "https://api.coinbase.com",
                           const std::string &redisHost = "redis",
                           int redisPort = 6379);
  ~CoinbaseExecutionAdapter();

  void start();
  void stop();

private:
  void onTradeSignal(const std::string &channel, const char *data,
                     size_t length);
  void executeOrder(const TradeSignal &signal);
  std::string generateClientOrderId();

  std::string apiKey_;
  std::string apiSecret_;

  std::shared_ptr<RedisSubscriber> subscriber_;
  std::shared_ptr<RedisPublisher> publisher_;
  std::unique_ptr<httplib::Client> httpClient_;
};

} // namespace gammatrade
