#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;

class Handler : public EventHandler {
 public:
  void processEvent(const Event& event, Session*) override { std::cout << event.toPrettyString(2, 2) << std::endl; }
};
} /* namespace ccapi */

int main() {
  if (ccapi::UtilSystem::getEnvAsString(CCAPI_BINANCE_SBE_API_KEY).empty()) {
    std::cerr << "Set BINANCE_SBE_API_KEY to the API key of an Ed25519 key pair.\n";
    return EXIT_FAILURE;
  }
  ccapi::Handler handler;
  ccapi::Session session(ccapi::SessionOptions{}, ccapi::SessionConfigs{}, &handler);
  std::vector<ccapi::Subscription> subscriptions{
      ccapi::Subscription("binance", "BTCUSDT", "MARKET_DEPTH", "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=depth&MARKET_DEPTH_MAX=20",
                          "sbe-depth"),
      ccapi::Subscription("binance", "BTCUSDT", "TRADE", "BINANCE_MARKET_DATA_FORMAT=SBE", "sbe-trades"),
      ccapi::Subscription("binance", "BTCUSDT", "MARKET_DEPTH", "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=bestBidAsk", "sbe-bbo")};
  std::cout << "SBE streams: btcusdt@depth (20 ms nominal), btcusdt@trade, btcusdt@bestBidAsk\n";
  session.subscribe(subscriptions);
  std::this_thread::sleep_for(std::chrono::seconds(30));
  session.stop();
}
