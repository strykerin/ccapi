#pragma once

#ifdef CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE
#ifndef CCAPI_BINANCE_CREATE_ORDER_PATH
#define CCAPI_BINANCE_CREATE_ORDER_PATH "/api/v3/order"
#endif
#include "ccapi_cpp/service/ccapi_execution_management_service_binance_base.h"
#include "ccapi_cpp/service/ccapi_binance_ws_sbe_generated.h"

namespace ccapi {

class ExecutionManagementServiceBinance : public ExecutionManagementServiceBinanceBase {
 public:
  ExecutionManagementServiceBinance(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                                    ServiceContextPtr serviceContextPtr)
      : ExecutionManagementServiceBinanceBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_BINANCE;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/ws";
    this->baseUrlWsOrderEntry = sessionConfigs.getUrlWebsocketOrderEntryBase().at(this->exchangeName) + CCAPI_BINANCE_WS_ORDER_ENTRY_PATH;
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    // this->setHostWsFromUrlWs(this->baseUrlWs);
    // this->setHostWsFromUrlWsOrderEntry(this->baseUrlWsOrderEntry);
    this->apiKeyName = CCAPI_BINANCE_API_KEY;
    this->apiSecretName = CCAPI_BINANCE_API_SECRET;
    this->websocketOrderEntryApiKeyName = CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_API_KEY;
    this->websocketOrderEntryApiPrivateKeyPathName = CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PATH;
    this->websocketOrderEntryApiPrivateKeyPasswordName = CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PASSWORD;
    this->setupCredential({this->apiKeyName, this->apiSecretName, this->websocketOrderEntryApiKeyName, this->websocketOrderEntryApiPrivateKeyPathName,
                           this->websocketOrderEntryApiPrivateKeyPasswordName});
    this->websocketOrderEntryHost = CCAPI_BINANCE_HOST_WS_ORDER_ENTRY;
    this->createOrderTarget = CCAPI_BINANCE_CREATE_ORDER_PATH;
    this->cancelOrderTarget = "/api/v3/order";
    this->getOrderTarget = "/api/v3/order";
    this->getOpenOrdersTarget = "/api/v3/openOrders";
    this->cancelOpenOrdersTarget = "/api/v3/openOrders";
    this->listenKeyTarget = CCAPI_BINANCE_LISTEN_KEY_PATH;
    this->getAccountBalancesTarget = "/api/v3/account";
    this->createOrderMarginTarget = "/sapi/v1/margin/order";
    this->cancelOrderMarginTarget = "/sapi/v1/margin/order";
    this->getOrderMarginTarget = "/sapi/v1/margin/order";
    this->getOpenOrdersMarginTarget = "/sapi/v1/margin/openOrders";
    this->cancelOpenOrdersMarginTarget = "/sapi/v1/margin/openOrders";
    this->getAccountBalancesCrossMarginTarget = "/sapi/v1/margin/account";
    this->getAccountBalancesIsolatedMarginTarget = "/sapi/v1/margin/isolated/account";
    this->listenKeyCrossMarginTarget = CCAPI_BINANCE_LISTEN_KEY_CROSS_MARGIN_PATH;
    this->listenKeyIsolatedMarginTarget = CCAPI_BINANCE_LISTEN_KEY_ISOLATED_MARGIN_PATH;
  }

  virtual ~ExecutionManagementServiceBinance() {}
#ifndef CCAPI_EXPOSE_INTERNAL
 protected:
#endif
  using ExecutionManagementServiceBinanceBase::onTextMessage;

  static bool usesSbeOrderEntry(const Subscription& subscription) {
    auto it = subscription.getOptionMap().find(CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING);
    return it != subscription.getOptionMap().end() && it->second == "SBE";
  }

  static std::string sbeOrderEntryUrl(const std::string& base) {
    if (base.find("responseFormat=") != std::string::npos || base.find("sbeSchemaId=") != std::string::npos ||
        base.find("sbeSchemaVersion=") != std::string::npos) throw std::runtime_error("Configure SBE through the subscription option, not the base URL");
    return base + (base.find('?') == std::string::npos ? "?" : "&") +
        "responseFormat=sbe&sbeSchemaId=3&sbeSchemaVersion=5";
  }

  void prepareConnect(std::shared_ptr<WsConnection> connection) override {
    const auto& subscription = connection->subscriptionList.at(0);
    auto it = subscription.getOptionMap().find(CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING);
    if (it != subscription.getOptionMap().end() && it->second != "JSON" && it->second != "SBE") {
      this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
          "Unsupported Binance websocket order entry encoding", {subscription.getCorrelationId()});
      return;
    }
    if (usesSbeOrderEntry(subscription)) {
      if (!subscription.getMarginType().empty()) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
            "Binance SBE order entry requires Spot", {subscription.getCorrelationId()});
        return;
      }
      try { connection->setUrl(sbeOrderEntryUrl(this->baseUrlWsOrderEntry)); }
      catch (const std::exception& error) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, error.what(), {subscription.getCorrelationId()});
        return;
      }
    }
    ExecutionManagementServiceBinanceBase::prepareConnect(connection);
  }

  static void convertSbeEventTimesToMilliseconds(rapidjson::Value& object, rapidjson::Document::AllocatorType& allocator) {
    for (const char* key : {"E", "T", "O", "W", "D", "u"}) {
      auto it = object.FindMember(key);
      if (it != object.MemberEnd() && it->value.IsString()) {
        auto value = BinanceProtocolSbe::integer<int64_t>(it->value.GetString());
        if (value >= 0) {
          auto text = std::to_string(value / 1000);
          it->value.SetString(text.c_str(), static_cast<rapidjson::SizeType>(text.size()), allocator);
        }
      }
    }
  }

  void failSbeConnection(std::shared_ptr<WsConnection> connection, const std::string& reason) {
    this->unregisterWebsocketConnectionForRequests(connection);
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
        "Binance SBE response failed; reconcile account state: " + reason, {connection->subscriptionList.at(0).getCorrelationId()});
    if (connection->status == WsConnection::Status::OPEN) {
      ErrorCode ec;
      this->close(connection, beast::websocket::close_code::normal,
          beast::websocket::close_reason(beast::websocket::close_code::normal, "SBE response failure"), ec);
    }
  }

  void onTextMessage(std::shared_ptr<WsConnection> connection, const Subscription& subscription,
                     boost::beast::string_view bytes, const TimePoint& received) override {
    if (usesSbeOrderEntry(subscription)) return failSbeConnection(connection, "Unexpected JSON frame on SBE order entry connection");
    ExecutionManagementServiceBinanceBase::onTextMessage(connection, subscription, bytes, received);
  }

  void onBinaryMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view bytes, const TimePoint& received) override {
    const auto& subscription = connection->subscriptionList.at(0);
    if (!usesSbeOrderEntry(subscription)) return ExecutionManagementServiceBinanceBase::onBinaryMessage(connection, bytes, received);
    try {
      rapidjson::Document document;
      auto& allocator = document.GetAllocator();
      auto decoded = BinanceProtocolSbe::Ws::decode(std::string_view(bytes.data(), bytes.size()), allocator);
      auto tid = BinanceProtocolSbe::integer<unsigned>(decoded["_templateId"].GetString());
      TimePoint exchangeTime{};
      if (tid == 50) {
        if (!decoded.HasMember("result")) throw std::runtime_error("Missing SBE websocket response body");
        const auto& result = decoded["result"];
        if (result.HasMember("transactTime")) exchangeTime = TimePoint(std::chrono::microseconds(BinanceProtocolSbe::timestampUs(result["transactTime"].GetString())));
        document.CopyFrom(decoded, allocator);
      } else {
        const char* name = nullptr;
        switch (tid) {
          case 601: name = "balanceUpdate"; break;
          case 602: throw std::runtime_error("Binance SBE user data stream terminated");
          case 603: name = "executionReport"; break;
          case 604: name = "externalLockUpdate"; break;
          case 606: name = "listStatus"; break;
          case 607: name = "outboundAccountPosition"; break;
          case 610: throw std::runtime_error("Binance SBE server shutdown");
          default: throw std::runtime_error("Unexpected SBE websocket event");
        }
        exchangeTime = TimePoint(std::chrono::microseconds(BinanceProtocolSbe::timestampUs(decoded["E"].GetString())));
        // Existing ccapi element names explicitly promise milliseconds; Message::time retains microseconds.
        if (tid == 603) {
          for (const char* key : {"E", "T", "O", "W", "D"}) {
            auto it = decoded.FindMember(key);
            if (it != decoded.MemberEnd() && it->value.IsString()) {
              auto n = BinanceProtocolSbe::integer<int64_t>(it->value.GetString());
              auto text = std::to_string(n < 0 ? n : n / 1000);
              it->value.SetString(text.c_str(), static_cast<rapidjson::SizeType>(text.size()), allocator);
            }
          }
        } else convertSbeEventTimesToMilliseconds(decoded, allocator);
        BinanceProtocolSbe::put(decoded, "e", name, allocator);
        document.SetObject();
        document.AddMember("event", decoded, allocator);
      }
      std::string diagnostic = "Binance SBE response";
      if (document.HasMember("result") && document["result"].HasMember("msg")) {
        diagnostic += ": " + std::string(document["result"]["msg"].GetString());
        if (document["result"].HasMember("code")) diagnostic += " (" + std::string(document["result"]["code"].GetString()) + ")";
      }
      Event event = this->createEvent(connection, subscription, diagnostic, document, received);
      if (exchangeTime != TimePoint{}) {
        auto messages = event.getMessageList();
        for (auto& message : messages) message.setTime(exchangeTime);
        event.setMessageList(std::move(messages));
      }
      if (!event.getMessageList().empty()) this->eventHandler(event, nullptr);
    } catch (const std::exception& error) {
      failSbeConnection(connection, error.what());
    }
  }

};

} /* namespace ccapi */
#endif
#endif
