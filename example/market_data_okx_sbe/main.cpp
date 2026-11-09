#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;

class Handler : public EventHandler {
 public:
  void processEvent(const Event& event, Session* session) override {
    std::cout << event.toPrettyString(2, 2) << std::endl;
    for (const auto& message : event.getMessageList()) {
      if (message.getType() != Message::Type::GET_INSTRUMENT || subscribed) continue;
      for (const auto& element : message.getElementList()) {
        auto code = element.getValue(CCAPI_INST_ID_CODE);
        if (code.empty()) continue;
        std::string options = "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=" + code;
        std::vector<Subscription> subscriptions{
            Subscription("okx", "BTC-USDT", "MARKET_DEPTH", options + "&OKX_MARKET_DATA_STREAM=books-l2-tbt&MARKET_DEPTH_MAX=20", "sbe-depth"),
            Subscription("okx", "BTC-USDT", "TRADE", options, "sbe-trades"),
            Subscription("okx", "BTC-USDT", "MARKET_DEPTH", options + "&OKX_MARKET_DATA_STREAM=bbo-tbt", "sbe-bbo")};
        std::cout << "OKX SBE instIdCode=" << code << ": books-l2-tbt (10 ms nominal), trades, bbo-tbt (real time)\n";
        subscribed = true;
        session->subscribe(subscriptions);
        break;
      }
    }
  }

  bool subscribed{};
};
} /* namespace ccapi */

int main() {
  for (const auto& name : {CCAPI_OKX_API_KEY, CCAPI_OKX_API_SECRET, CCAPI_OKX_API_PASSPHRASE}) {
    if (ccapi::UtilSystem::getEnvAsString(name).empty()) {
      std::cerr << "Set OKX_API_KEY, OKX_API_SECRET and OKX_API_PASSPHRASE in the process environment.\n";
      return EXIT_FAILURE;
    }
  }
  ccapi::Handler handler;
  ccapi::Session session(ccapi::SessionOptions{}, ccapi::SessionConfigs{}, &handler);
  ccapi::Request instrument(ccapi::Request::Operation::GET_INSTRUMENT, "okx", "BTC-USDT");
  instrument.appendParam({{CCAPI_INSTRUMENT_TYPE, "SPOT"}});
  session.sendRequest(instrument);
  std::this_thread::sleep_for(std::chrono::seconds(30));
  session.stop();
}
