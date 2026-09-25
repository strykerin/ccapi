#pragma once

#ifdef CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE_USDS_FUTURES
#include "ccapi_cpp/service/ccapi_execution_management_service_binance_derivatives_base.h"

namespace ccapi {

class ExecutionManagementServiceBinanceUsdsFutures : public ExecutionManagementServiceBinanceDerivativesBase {
 public:
  ExecutionManagementServiceBinanceUsdsFutures(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions,
                                               SessionConfigs sessionConfigs, ServiceContextPtr serviceContextPtr)
      : ExecutionManagementServiceBinanceDerivativesBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES;
    auto websocketRoot = UtilString::rtrim(sessionConfigs.getUrlWebsocketBase().at(this->exchangeName), '/');
    this->baseUrlWs = websocketRoot + "/private/ws";
    auto websocketOrderEntryRoot = UtilString::rtrim(sessionConfigs.getUrlWebsocketOrderEntryBase().at(this->exchangeName), '/');
    this->baseUrlWsOrderEntry = UtilString::endsWith(websocketOrderEntryRoot, CCAPI_BINANCE_USDS_FUTURES_WS_ORDER_ENTRY_PATH)
                                        ? websocketOrderEntryRoot
                                        : websocketOrderEntryRoot + CCAPI_BINANCE_USDS_FUTURES_WS_ORDER_ENTRY_PATH;
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    // this->setHostWsFromUrlWs(this->baseUrlWs);
    // this->setHostWsFromUrlWsOrderEntry(this->baseUrlWsOrderEntry);
    this->apiKeyName = CCAPI_BINANCE_USDS_FUTURES_API_KEY;
    this->apiSecretName = CCAPI_BINANCE_USDS_FUTURES_API_SECRET;
    this->websocketOrderEntryApiKeyName = CCAPI_BINANCE_USDS_FUTURES_WEBSOCKET_ORDER_ENTRY_API_KEY;
    this->websocketOrderEntryApiPrivateKeyPathName = CCAPI_BINANCE_USDS_FUTURES_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PATH;
    this->websocketOrderEntryApiPrivateKeyPasswordName = CCAPI_BINANCE_USDS_FUTURES_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PASSWORD;
    this->setupCredential({this->apiKeyName, this->apiSecretName, this->websocketOrderEntryApiKeyName, this->websocketOrderEntryApiPrivateKeyPathName,
                           this->websocketOrderEntryApiPrivateKeyPasswordName});
    this->websocketOrderEntryHost = CCAPI_BINANCE_USDS_FUTURES_HOST_WS_ORDER_ENTRY;
    this->createOrderTarget = CCAPI_BINANCE_USDS_FUTURES_CREATE_ORDER_PATH;
    this->cancelOrderTarget = "/fapi/v1/order";
    this->getOrderTarget = "/fapi/v1/order";
    this->getOpenOrdersTarget = "/fapi/v1/openOrders";
    this->cancelOpenOrdersTarget = "/fapi/v1/allOpenOrders";
    this->isDerivatives = true;
    this->listenKeyTarget = CCAPI_BINANCE_USDS_FUTURES_LISTEN_KEY_PATH;
    this->getAccountBalancesTarget = "/fapi/v3/account";
    this->getAccountPositionsTarget = "/fapi/v3/positionRisk";
  }

  virtual ~ExecutionManagementServiceBinanceUsdsFutures() {}
#ifndef CCAPI_EXPOSE_INTERNAL

 protected:
#endif
  bool useWebsocketOrderEntryConnection(const std::set<std::string>& fieldSet) override {
    return fieldSet.find(CCAPI_EM_WEBSOCKET_ORDER_ENTRY) != fieldSet.end();
  }

  bool shouldRetryRequest(const Request& request) const override {
    return request.getOperation() != Request::Operation::CREATE_ORDER && request.getOperation() != Request::Operation::CANCEL_ORDER &&
           request.getOperation() != Request::Operation::CANCEL_OPEN_ORDERS;
  }

  Message::Type requestFailureMessageType(const Request& request) const override {
    return this->shouldRetryRequest(request) ? Message::Type::REQUEST_FAILURE : Message::Type::REQUEST_OUTCOME_UNKNOWN;
  }

 public:
  void subscribe(std::vector<Subscription>& subscriptionList) override {
    std::vector<Subscription> validSubscriptionList;
    for (const auto& subscription : subscriptionList) {
      const auto& fieldSet = subscription.getFieldSet();
      bool hasOrderEntry = fieldSet.find(CCAPI_EM_WEBSOCKET_ORDER_ENTRY) != fieldSet.end();
      bool hasAccountEvents = fieldSet.find(CCAPI_EM_ORDER_UPDATE) != fieldSet.end() || fieldSet.find(CCAPI_EM_PRIVATE_TRADE) != fieldSet.end() ||
                              fieldSet.find(CCAPI_EM_PRIVATE_TRADE_LITE) != fieldSet.end() || fieldSet.find(CCAPI_EM_BALANCE_UPDATE) != fieldSet.end() ||
                              fieldSet.find(CCAPI_EM_POSITION_UPDATE) != fieldSet.end();
      if (hasOrderEntry && hasAccountEvents) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
                      "USD-M WEBSOCKET_ORDER_ENTRY and account-event fields require separate subscriptions", {subscription.getCorrelationId()});
      } else {
        validSubscriptionList.push_back(subscription);
      }
    }
    if (!validSubscriptionList.empty()) {
      ExecutionManagementService::subscribe(validSubscriptionList);
    }
  }
};

} /* namespace ccapi */
#endif
#endif
