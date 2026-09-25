#pragma once

#ifdef CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
#if defined(CCAPI_ENABLE_EXCHANGE_BINANCE_US) || defined(CCAPI_ENABLE_EXCHANGE_BINANCE) || defined(CCAPI_ENABLE_EXCHANGE_BINANCE_USDS_FUTURES) || \
    defined(CCAPI_ENABLE_EXCHANGE_BINANCE_COIN_FUTURES)
#include <cstdint>
#include <memory>

#include "ccapi_cpp/service/ccapi_execution_management_service.h"

namespace ccapi {

class ExecutionManagementServiceBinanceBase : public ExecutionManagementService {
 public:
  ExecutionManagementServiceBinanceBase(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                                        ServiceContextPtr serviceContextPtr)
      : ExecutionManagementService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->enableCheckPingPongWebsocketApplicationLevel = false;
    this->pingListenKeyIntervalSeconds = 600;
  }

  virtual ~ExecutionManagementServiceBinanceBase() {
    for (const auto& item : this->pingListenKeyTimerMapByConnectionIdMap) {
      item.second->cancel();
    }
  }
#ifndef CCAPI_EXPOSE_INTERNAL

 protected:
#endif

  enum class ConnectionRole { PRIVATE_LISTEN_KEY, ORDER_ENTRY };

  bool useWebsocketOrderEntryConnection(const std::set<std::string>& fieldSet) override {
    return fieldSet.find(CCAPI_EM_WEBSOCKET_ORDER_ENTRY) != fieldSet.end() || fieldSet.find(CCAPI_EM_ORDER_UPDATE) != fieldSet.end() ||
           fieldSet.find(CCAPI_EM_PRIVATE_TRADE) != fieldSet.end() || fieldSet.find(CCAPI_EM_BALANCE_UPDATE) != fieldSet.end();
  }

  bool doesHttpBodyContainError(boost::beast::string_view bodyView) override {
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(bodyView.data(), bodyView.size());
    if (document.HasParseError() || !document.IsObject()) {
      return false;
    }
    auto codeIt = document.FindMember("code");
    if (codeIt == document.MemberEnd() || codeIt->value.IsNull() || !codeIt->value.IsString()) {
      return false;
    }
    try {
      int code = std::stoi(codeIt->value.GetString());
      return code != 0 && code != 200;
    } catch (const std::exception&) {
      return true;
    }
  }

  bool isOrderEntryConnection(const std::shared_ptr<WsConnection>& wsConnectionPtr, const std::set<std::string>* fieldSetPtr = nullptr) const {
    auto it = this->connectionRoleByConnectionIdMap.find(wsConnectionPtr->id);
    if (it != this->connectionRoleByConnectionIdMap.end()) {
      return it->second == ConnectionRole::ORDER_ENTRY;
    }
    if (fieldSetPtr) {
      return const_cast<ExecutionManagementServiceBinanceBase*>(this)->useWebsocketOrderEntryConnection(*fieldSetPtr);
    }
    return false;
  }

  bool shouldRegisterWebsocketConnectionOnOpen(const std::shared_ptr<WsConnection>& wsConnectionPtr) override { return false; }

  uint64_t beginConnectionGeneration(const std::shared_ptr<WsConnection>& wsConnectionPtr) {
    auto generation = ++this->nextConnectionGeneration;
    this->connectionGenerationByConnectionIdMap[wsConnectionPtr->id] = generation;
    return generation;
  }

  bool isConnectionGenerationCurrent(const std::shared_ptr<WsConnection>& wsConnectionPtr, uint64_t generation) const {
    auto it = this->connectionGenerationByConnectionIdMap.find(wsConnectionPtr->id);
    return it != this->connectionGenerationByConnectionIdMap.end() && it->second == generation;
  }

  void cancelPingListenKeyTimer(const std::string& connectionId) {
    auto it = this->pingListenKeyTimerMapByConnectionIdMap.find(connectionId);
    if (it != this->pingListenKeyTimerMapByConnectionIdMap.end()) {
      it->second->cancel();
      this->pingListenKeyTimerMapByConnectionIdMap.erase(it);
    }
  }

  std::string createListenKeyWebsocketUrl(const std::string& listenKey) const { return this->baseUrlWs + "/" + listenKey; }

  void recoverListenKey(const std::shared_ptr<WsConnection>& wsConnectionPtr, const std::string& reason) {
    this->cancelPingListenKeyTimer(wsConnectionPtr->id);
    this->beginConnectionGeneration(wsConnectionPtr);
    this->unregisterWebsocketConnectionForRequests(wsConnectionPtr);
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
                  "Binance listen key unavailable; account state must be reconciled: " + reason, wsConnectionPtr->correlationIdList);
    if (wsConnectionPtr->status == WsConnection::Status::OPEN) {
      ErrorCode ec;
      this->close(wsConnectionPtr, beast::websocket::close_code::normal,
                  beast::websocket::close_reason(beast::websocket::close_code::normal, "listen key recovery"), ec);
      if (ec) {
        this->onFail(wsConnectionPtr);
      }
    } else {
      this->onFail(wsConnectionPtr);
    }
  }

  void prepareConnect(std::shared_ptr<WsConnection> wsConnectionPtr) override {
    const auto& fieldSet = wsConnectionPtr->subscriptionList.at(0).getFieldSet();
    bool orderEntry = this->useWebsocketOrderEntryConnection(fieldSet);
    this->connectionRoleByConnectionIdMap[wsConnectionPtr->id] =
        orderEntry ? ConnectionRole::ORDER_ENTRY : ConnectionRole::PRIVATE_LISTEN_KEY;
    auto generation = this->beginConnectionGeneration(wsConnectionPtr);
    if (orderEntry) {
      ExecutionManagementService::prepareConnect(wsConnectionPtr);
    } else {
      auto hostPort = this->extractHostFromUrl(this->baseUrlRest);
      std::string host = hostPort.first;
      std::string port = hostPort.second;
      http::request<http::string_body> req;
      req.set(http::field::host, host);
      req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
      req.method(http::verb::post);
      std::string target = this->listenKeyTarget;
      const auto& marginType = wsConnectionPtr->subscriptionList.at(0).getMarginType();
      if (marginType == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN) {
        target = this->listenKeyCrossMarginTarget;
      } else if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
        target = this->listenKeyIsolatedMarginTarget;
      }
      if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
        auto symbol = wsConnectionPtr->subscriptionList.at(0).getInstrument();
        target += "?" + symbol;
      }
      req.target(target);
      auto credential = wsConnectionPtr->subscriptionList.at(0).getCredential();
      if (credential.empty()) {
        credential = this->credentialDefault;
      }
      auto apiKey = mapGetWithDefault(credential, this->apiKeyName);
      req.set("X-MBX-APIKEY", apiKey);
      this->sendRequest(
          req,
          [wsConnectionPtr, generation, that = shared_from_base<ExecutionManagementServiceBinanceBase>()](const beast::error_code& ec) {
            if (that->isConnectionGenerationCurrent(wsConnectionPtr, generation)) {
              that->recoverListenKey(wsConnectionPtr, "listen-key creation transport failure: " + ec.message());
            }
          },
          [wsConnectionPtr, generation, that = shared_from_base<ExecutionManagementServiceBinanceBase>()](const http::response<http::string_body>& res) {
            if (!that->isConnectionGenerationCurrent(wsConnectionPtr, generation)) {
              return;
            }
            int statusCode = res.result_int();
            std::string body = res.body();
            if (statusCode / 100 == 2 && !that->doesHttpBodyContainError(body)) {
              that->jsonDocumentAllocator.Clear();
              rj::Document document(&that->jsonDocumentAllocator);
              document.Parse<rj::kParseNumbersAsStringsFlag>(body.c_str());
              if (!document.HasParseError() && document.IsObject() && document.HasMember("listenKey") && document["listenKey"].IsString()) {
                std::string listenKey = document["listenKey"].GetString();
                std::string url = that->createListenKeyWebsocketUrl(listenKey);
                wsConnectionPtr->setUrl(url);
                that->extraPropertyByConnectionIdMap[wsConnectionPtr->id]["listenKey"] = listenKey;
                that->connect(wsConnectionPtr);
                return;
              }
            }
            that->recoverListenKey(wsConnectionPtr,
                                   "listen-key creation failed with HTTP " + std::to_string(statusCode) + ": " + body);
          },
          this->sessionOptions.httpRequestTimeoutMilliseconds);
    }
  }

  void onOpen(std::shared_ptr<WsConnection> wsConnectionPtr) override {
    ExecutionManagementService::onOpen(wsConnectionPtr);
    if (!this->isOrderEntryConnection(wsConnectionPtr)) {
      auto now = UtilTime::now();
      Event event;
      event.setType(Event::Type::SUBSCRIPTION_STATUS);
      Message message;
      message.setTimeReceived(now);
      message.setType(Message::Type::SUBSCRIPTION_STARTED);
      message.setCorrelationIdList({wsConnectionPtr->subscriptionList.at(0).getCorrelationId()});
      event.setMessageList({message});
      this->eventHandler(event, nullptr);
      this->setPingListenKeyTimer(wsConnectionPtr);
    }
  }

  void setPingListenKeyTimer(const std::shared_ptr<WsConnection> wsConnectionPtr) {
    this->cancelPingListenKeyTimer(wsConnectionPtr->id);
    auto generationIt = this->connectionGenerationByConnectionIdMap.find(wsConnectionPtr->id);
    if (generationIt == this->connectionGenerationByConnectionIdMap.end()) {
      return;
    }
    auto generation = generationIt->second;
    TimerPtr timerPtr(
        new boost::asio::steady_timer(*this->serviceContextPtr->ioContextPtr, std::chrono::milliseconds(this->pingListenKeyIntervalSeconds * 1000)));
    timerPtr->async_wait([wsConnectionPtr, generation, that = shared_from_base<ExecutionManagementServiceBinanceBase>()](ErrorCode const& ec) {
      if (ec) {
        return;
      }
      if (!that->isConnectionGenerationCurrent(wsConnectionPtr, generation)) {
        return;
      }
      http::request<http::string_body> req;
      req.set(http::field::host, that->hostRest);
      req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
      req.method(http::verb::put);
      std::string target = that->listenKeyTarget;
      const auto& marginType = wsConnectionPtr->subscriptionList.at(0).getMarginType();
      if (marginType == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN) {
        target = that->listenKeyCrossMarginTarget;
      } else if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
        target = that->listenKeyIsolatedMarginTarget;
      }
      if (!that->isDerivatives) {
        std::map<std::string, std::string> params;
        auto connectionIt = that->extraPropertyByConnectionIdMap.find(wsConnectionPtr->id);
        if (connectionIt == that->extraPropertyByConnectionIdMap.end() || connectionIt->second.find("listenKey") == connectionIt->second.end()) {
          that->recoverListenKey(wsConnectionPtr, "listen key missing during keepalive");
          return;
        }
        auto listenKey = connectionIt->second.at("listenKey");
        params.insert({"listenKey", listenKey});
        if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
          auto symbol = wsConnectionPtr->subscriptionList.at(0).getInstrument();
          params.insert({"symbol", symbol});
        }
        target += "?";
        for (const auto& param : params) {
          target += param.first + "=" + Url::urlEncode(param.second);
          target += "&";
        }
      }
      req.target(target);
      auto credential = wsConnectionPtr->subscriptionList.at(0).getCredential();
      if (credential.empty()) {
        credential = that->credentialDefault;
      }
      auto apiKey = mapGetWithDefault(credential, that->apiKeyName);
      req.set("X-MBX-APIKEY", apiKey);
      that->sendRequest(
          req,
          [wsConnectionPtr, generation, that_2 = that->shared_from_base<ExecutionManagementServiceBinanceBase>()](const beast::error_code& ec) {
            if (that_2->isConnectionGenerationCurrent(wsConnectionPtr, generation)) {
              that_2->recoverListenKey(wsConnectionPtr, "listen-key keepalive transport failure: " + ec.message());
            }
          },
          [wsConnectionPtr, generation, that_2 = that->shared_from_base<ExecutionManagementServiceBinanceBase>()](const http::response<http::string_body>& res) {
            if (!that_2->isConnectionGenerationCurrent(wsConnectionPtr, generation)) {
              return;
            }
            if (res.result_int() / 100 != 2 || that_2->doesHttpBodyContainError(res.body())) {
              that_2->recoverListenKey(wsConnectionPtr,
                                       "listen-key keepalive failed with HTTP " + std::to_string(res.result_int()) + ": " + res.body());
              return;
            }
            CCAPI_LOGGER_DEBUG("ping listen key success");
            that_2->setPingListenKeyTimer(wsConnectionPtr);
          },
          that->sessionOptions.httpRequestTimeoutMilliseconds);
    });
    this->pingListenKeyTimerMapByConnectionIdMap[wsConnectionPtr->id] = timerPtr;
  }

  void onClose(std::shared_ptr<WsConnection> wsConnectionPtr, ErrorCode ec) override {
    this->cancelPingListenKeyTimer(wsConnectionPtr->id);
    ExecutionManagementService::onClose(wsConnectionPtr, ec);
  }

  void clearStates(std::shared_ptr<WsConnection> wsConnectionPtr) override {
    this->cancelPingListenKeyTimer(wsConnectionPtr->id);
    this->beginConnectionGeneration(wsConnectionPtr);
    ExecutionManagementService::clearStates(wsConnectionPtr);
  }

  void signReqeustForRestGenericPrivateRequest(http::request<http::string_body>& req, const Request& request, std::string& methodString,
                                               std::string& headerString, std::string& path, std::string& queryString, std::string& body, const TimePoint& now,
                                               const std::map<std::string, std::string>& credential) override {
    if (queryString.find("timestamp=") == std::string::npos) {
      if (!queryString.empty()) {
        queryString += "&";
      }
      queryString += "timestamp=";
      queryString += std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
    }
    auto apiSecret = mapGetWithDefault(credential, this->apiSecretName);
    auto signature = Hmac::hmac(Hmac::ShaVersion::SHA256, apiSecret, queryString, true);
    queryString += "&signature=";
    queryString += signature;
  }

  void signRequest(std::string& queryString, const std::map<std::string, std::string>& param, const TimePoint& now,
                   const std::map<std::string, std::string>& credential) {
    if (param.find("timestamp") == param.end()) {
      queryString += "timestamp=";
      queryString += std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
      queryString += "&";
    }
    if (queryString.back() == '&') {
      queryString.pop_back();
    }
    auto apiSecret = mapGetWithDefault(credential, this->apiSecretName);
    auto signature = Hmac::hmac(Hmac::ShaVersion::SHA256, apiSecret, queryString, true);
    queryString += "&signature=";
    queryString += signature;
  }

  void appendParam(std::string& queryString, const std::map<std::string, std::string>& param,
                   const std::map<std::string, std::string> standardizationMap = {}) {
    for (const auto& kv : param) {
      queryString += standardizationMap.find(kv.first) != standardizationMap.end() ? standardizationMap.at(kv.first) : kv.first;
      queryString += "=";
      queryString += Url::urlEncode(kv.second);
      queryString += "&";
    }
  }

  void appendSymbolId(std::string& queryString, const std::string& symbolId) {
    queryString += "symbol=";
    queryString += Url::urlEncode(symbolId);
    queryString += "&";
  }

  void prepareReq(http::request<http::string_body>& req, const std::map<std::string, std::string>& credential) {
    auto apiKey = mapGetWithDefault(credential, this->apiKeyName);
    req.set("X-MBX-APIKEY", apiKey);
  }

  void convertRequestForRest(http::request<http::string_body>& req, const Request& request, const TimePoint& now, const std::string& symbolId,
                             const std::map<std::string, std::string>& credential) override {
    this->prepareReq(req, credential);
    switch (request.getOperation()) {
      case Request::Operation::GENERIC_PRIVATE_REQUEST: {
        ExecutionManagementService::convertRequestForRestGenericPrivateRequest(req, request, now, symbolId, credential);
      } break;
      case Request::Operation::CREATE_ORDER: {
        req.method(http::verb::post);
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_EM_ORDER_SIDE, "side"},
                              {CCAPI_EM_ORDER_QUANTITY, "quantity"},
                              {CCAPI_EM_ORDER_LIMIT_PRICE, "price"},
                              {CCAPI_EM_CLIENT_ORDER_ID, "newClientOrderId"},
                          });
        this->appendSymbolId(queryString, symbolId);
        if (param.find("type") == param.end()) {
          queryString += "type=LIMIT&";
          if (param.find("timeInForce") == param.end()) {
            queryString += "timeInForce=GTC&";
          }
        }
        if (param.find("newClientOrderId") == param.end() && param.find(CCAPI_EM_CLIENT_ORDER_ID) == param.end()) {
          std::string nonce = std::to_string(this->generateNonce(now, request.getIndex()));
          queryString += std::string("newClientOrderId=x-") + (this->isDerivatives ? CCAPI_BINANCE_USDS_FUTURES_API_LINK_ID : CCAPI_BINANCE_API_LINK_ID) + "-" +
                         nonce + "&";
        }
        this->signRequest(queryString, param, now, credential);
        req.target((request.getMarginType() == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN || request.getMarginType() == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN
                        ? this->createOrderMarginTarget
                        : this->createOrderTarget) +
                   "?" + queryString);
      } break;
      case Request::Operation::CANCEL_ORDER: {
        req.method(http::verb::delete_);
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_EM_ORDER_ID, "orderId"},
                              {CCAPI_EM_CLIENT_ORDER_ID, "origClientOrderId"},
                          });
        this->appendSymbolId(queryString, symbolId);
        this->signRequest(queryString, param, now, credential);
        req.target((request.getMarginType() == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN || request.getMarginType() == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN
                        ? this->cancelOrderMarginTarget
                        : this->cancelOrderTarget) +
                   "?" + queryString);
      } break;
      case Request::Operation::GET_ORDER: {
        req.method(http::verb::get);
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_EM_ORDER_ID, "orderId"},
                              {CCAPI_EM_CLIENT_ORDER_ID, "origClientOrderId"},
                          });
        this->appendSymbolId(queryString, symbolId);
        this->signRequest(queryString, param, now, credential);
        req.target((request.getMarginType() == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN || request.getMarginType() == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN
                        ? this->getOrderMarginTarget
                        : this->getOrderTarget) +
                   "?" + queryString);
      } break;
      case Request::Operation::GET_OPEN_ORDERS: {
        req.method(http::verb::get);
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param);
        if (!symbolId.empty()) {
          this->appendSymbolId(queryString, symbolId);
        }
        this->signRequest(queryString, param, now, credential);
        req.target((request.getMarginType() == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN || request.getMarginType() == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN
                        ? this->getOpenOrdersMarginTarget
                        : this->getOpenOrdersTarget) +
                   "?" + queryString);
      } break;
      case Request::Operation::CANCEL_OPEN_ORDERS: {
        req.method(http::verb::delete_);
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param);
        this->appendSymbolId(queryString, symbolId);
        this->signRequest(queryString, param, now, credential);
        req.target((request.getMarginType() == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN || request.getMarginType() == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN
                        ? this->cancelOpenOrdersMarginTarget
                        : this->cancelOpenOrdersTarget) +
                   "?" + queryString);
      } break;
      case Request::Operation::GET_ACCOUNT_BALANCES: {
        req.method(http::verb::get);
        std::string queryString;
        this->appendParam(queryString, {});
        this->signRequest(queryString, {}, now, credential);
        const auto& marginType = request.getMarginType();
        std::string target = this->getAccountBalancesTarget;
        if (marginType == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN) {
          target = this->getAccountBalancesCrossMarginTarget;
        } else if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
          target = this->getAccountBalancesIsolatedMarginTarget;
        }
        req.target(target + "?" + queryString);
      } break;
      default:
        this->convertRequestForRestCustom(req, request, now, symbolId, credential);
    }
  }

  void extractOrderInfoFromRequest(std::vector<Element>& elementList, const Request& request, const Request::Operation operation,
                                   const rj::Document& document) override {
    std::map<std::string_view, std::pair<std::string_view, JsonDataType>> extractionFieldNameMap = {
        {CCAPI_EM_ORDER_ID, std::make_pair("orderId", JsonDataType::INTEGER)},
        {CCAPI_EM_ORDER_SIDE, std::make_pair("side", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_QUANTITY, std::make_pair("origQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("price", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("executedQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY, std::make_pair(this->isDerivatives ? "cumQuote" : "cummulativeQuoteQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_STATUS, std::make_pair("status", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("symbol", JsonDataType::STRING)},
        {CCAPI_LAST_UPDATED_TIME_SECONDS, std::make_pair("updateTime", JsonDataType::STRING)},
    };
    extractionFieldNameMap.emplace(CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("clientOrderId", JsonDataType::STRING));
    if (operation == Request::Operation::CANCEL_OPEN_ORDERS && document.IsObject() && document.HasMember("code") && document.HasMember("msg")) {
      Element element;
      if (document["code"].IsString()) {
        element.insert(CCAPI_HTTP_STATUS_CODE, document["code"].GetString());
      }
      if (document["msg"].IsString()) {
        element.insert(CCAPI_INFO_MESSAGE, document["msg"].GetString());
      }
      elementList.emplace_back(std::move(element));
      return;
    }
    if (document.IsObject()) {
      Element element;
      this->extractOrderInfo(
          element, document, extractionFieldNameMap,
          {
              {CCAPI_LAST_UPDATED_TIME_SECONDS, [](const std::string& input) { return UtilTime::convertMillisecondsStrToSecondsStr(input); }},
          });
      if (element.getValue(CCAPI_EM_CLIENT_ORDER_ID).empty()) {
        auto it = document.FindMember("origClientOrderId");
        if (it != document.MemberEnd() && it->value.IsString()) {
          element.insert_or_assign(CCAPI_EM_CLIENT_ORDER_ID, it->value.GetString());
        }
      }
      elementList.emplace_back(std::move(element));
    } else {
      for (const auto& x : document.GetArray()) {
        Element element;
        this->extractOrderInfo(
            element, x, extractionFieldNameMap,
            {
                {CCAPI_LAST_UPDATED_TIME_SECONDS, [](const std::string& input) { return UtilTime::convertMillisecondsStrToSecondsStr(input); }},
            });
        if (element.getValue(CCAPI_EM_CLIENT_ORDER_ID).empty()) {
          auto it = x.FindMember("origClientOrderId");
          if (it != x.MemberEnd() && it->value.IsString()) {
            element.insert_or_assign(CCAPI_EM_CLIENT_ORDER_ID, it->value.GetString());
          }
        }
        elementList.emplace_back(std::move(element));
      }
    }
  }

  void extractAccountInfoFromRequest(std::vector<Element>& elementList, const Request& request, const Request::Operation operation,
                                     const rj::Document& document) override {
    switch (request.getOperation()) {
      case Request::Operation::GET_ACCOUNT_BALANCES: {
        const auto& marginType = request.getMarginType();
        if (this->isDerivatives) {
          for (const auto& x : document["assets"].GetArray()) {
            const auto& quantityTotalDecimal = Decimal(x["walletBalance"].GetString());
            if (quantityTotalDecimal != Decimal::zero) {
              Element element;
              element.insert(CCAPI_EM_ASSET, x["asset"].GetString());
              element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(quantityTotalDecimal));
              element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, x["availableBalance"].GetString());
              if (this->isDerivatives) {
                element.insert(CCAPI_LAST_UPDATED_TIME_SECONDS, UtilTime::convertMillisecondsStrToSecondsStr(x["updateTime"].GetString()));
              }
              elementList.emplace_back(std::move(element));
            }
          }
        } else {
          if (marginType == CCAPI_EM_MARGIN_TYPE_CROSS_MARGIN) {
            for (const auto& x : document["userAssets"].GetArray()) {
              const auto& quantityTotalDecimal = Decimal(x["free"].GetString()) + Decimal(x["locked"].GetString());
              if (quantityTotalDecimal != Decimal::zero) {
                Element element;
                element.insert(CCAPI_EM_ASSET, x["asset"].GetString());
                element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(quantityTotalDecimal));
                element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, x["free"].GetString());
                element.insert(CCAPI_EM_QUANTITY_LIABILITY, ConvertDecimalToString(Decimal(x["borrowed"].GetString()) + (Decimal(x["interest"].GetString()))));
                elementList.emplace_back(std::move(element));
              }
            }
          } else if (marginType == CCAPI_EM_MARGIN_TYPE_ISOLATED_MARGIN) {
            for (const auto& x : document["assets"].GetArray()) {
              std::string symbol = x["symbol"].GetString();
              {
                const auto& y = x["baseAsset"];
                const auto& quantityTotalDecimal = Decimal(y["free"].GetString()) + Decimal(y["locked"].GetString());
                if (quantityTotalDecimal != Decimal::zero) {
                  Element element;
                  element.insert(CCAPI_EM_INSTRUMENT, symbol);
                  element.insert(CCAPI_EM_ASSET, y["asset"].GetString());
                  element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(quantityTotalDecimal));
                  element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, y["free"].GetString());
                  element.insert(CCAPI_EM_QUANTITY_LIABILITY,
                                 ConvertDecimalToString(Decimal(y["borrowed"].GetString()) + (Decimal(y["interest"].GetString()))));
                  elementList.emplace_back(std::move(element));
                }
              }
              {
                const auto& y = x["quoteAsset"];
                const auto& quantityTotalDecimal = Decimal(y["free"].GetString()) + Decimal(y["locked"].GetString());
                if (quantityTotalDecimal != Decimal::zero) {
                  Element element;
                  element.insert(CCAPI_EM_INSTRUMENT, symbol);
                  element.insert(CCAPI_EM_ASSET, y["asset"].GetString());
                  element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(quantityTotalDecimal));
                  element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, y["free"].GetString());
                  element.insert(CCAPI_EM_QUANTITY_LIABILITY,
                                 ConvertDecimalToString(Decimal(y["borrowed"].GetString()) + (Decimal(y["interest"].GetString()))));
                  elementList.emplace_back(std::move(element));
                }
              }
            }
          } else {
            for (const auto& x : document["balances"].GetArray()) {
              const auto& quantityTotalDecimal = Decimal(x["free"].GetString()) + Decimal(x["locked"].GetString());
              if (quantityTotalDecimal != Decimal::zero) {
                Element element;
                element.insert(CCAPI_EM_ASSET, x["asset"].GetString());
                element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(quantityTotalDecimal));
                element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, x["free"].GetString());
                elementList.emplace_back(std::move(element));
              }
            }
          }
        }
      } break;
      default:
        CCAPI_LOGGER_FATAL(CCAPI_UNSUPPORTED_VALUE);
    }
  }

  void onTextMessage(std::shared_ptr<WsConnection> wsConnectionPtr, const Subscription& subscription, boost::beast::string_view textMessageView,
                     const TimePoint& timeReceived) override {
    this->jsonDocumentAllocator.Clear();
    rj::Document document(&this->jsonDocumentAllocator);
    document.Parse<rj::kParseNumbersAsStringsFlag>(textMessageView.data(), textMessageView.size());
    Event event = this->createEvent(wsConnectionPtr, subscription, textMessageView, document, timeReceived);
    if (!event.getMessageList().empty()) {
      this->eventHandler(event, nullptr);
    }
  }

  Event createEvent(const std::shared_ptr<WsConnection> wsConnectionPtr, const Subscription& subscription, boost::beast::string_view textMessageView,
                    const rj::Document& document, const TimePoint& timeReceived) {
    Event event;
    std::vector<Message> messageList;
    const auto& fieldSet = subscription.getFieldSet();
    if (!document.IsObject()) {
      event.setType(Event::Type::SUBSCRIPTION_STATUS);
      Message message;
      message.setTimeReceived(timeReceived);
      message.setType(Message::Type::SUBSCRIPTION_FAILURE);
      message.setCorrelationIdList({subscription.getCorrelationId()});
      Element element;
      element.insert(CCAPI_ERROR_MESSAGE, "malformed Binance websocket payload");
      message.setElementList({element});
      messageList.emplace_back(std::move(message));
    } else if (this->isOrderEntryConnection(wsConnectionPtr, &fieldSet)) {
      if (document.HasMember("event") && document["event"].IsObject()) {
        // unsolicited user-data event delivered on the ws-api connection (wrapped as {"subscriptionId":<int>,"event":{...}})
        this->appendUserDataMessages(wsConnectionPtr, event, messageList, subscription, document["event"], timeReceived);
      } else {
        auto statusIt = document.FindMember("status");
        int statusCode{};
        bool validStatus = false;
        if (statusIt != document.MemberEnd() && statusIt->value.IsString()) {
          try {
            statusCode = std::stoi(statusIt->value.GetString());
            validStatus = true;
          } catch (const std::exception&) {
          }
        }
        if (!validStatus) {
          event.setType(Event::Type::SUBSCRIPTION_STATUS);
          Message message;
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          message.setType(Message::Type::SUBSCRIPTION_FAILURE);
          Element element;
          element.insert(CCAPI_ERROR_MESSAGE, "Binance websocket response is missing a valid status");
          message.setElementList({element});
          messageList.emplace_back(std::move(message));
          event.setMessageList(messageList);
          return event;
        }
        auto idIt = document.FindMember("id");
        bool idIsNull = idIt != document.MemberEnd() && idIt->value.IsNull();
        std::string id = idIt != document.MemberEnd() && idIt->value.IsString() ? idIt->value.GetString() : "";
        bool success = statusCode / 100 == 2;
        if (statusCode == 401 && (idIsNull || id.empty())) {
          this->unregisterWebsocketConnectionForRequests(wsConnectionPtr);
          event.setType(Event::Type::AUTHORIZATION_STATUS);
          Message message;
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          message.setType(Message::Type::AUTHORIZATION_FAILURE);
          Element element;
          element.insert(CCAPI_CONNECTION_ID, wsConnectionPtr->id);
          element.insert(CCAPI_CONNECTION_URL, wsConnectionPtr->url);
          element.insert(CCAPI_ERROR_MESSAGE, textMessageView);
          message.setElementList({element});
          messageList.emplace_back(std::move(message));
          event.setMessageList(messageList);
          return event;
        }
        if (id.empty()) {
          event.setMessageList(messageList);
          return event;
        }
        Message message;
        message.setTimeReceived(timeReceived);
        message.setCorrelationIdList({subscription.getCorrelationId()});
        if (id == this->websocketOrderEntrySessionLogonJsonId) {
          if (success) {
            this->registerWebsocketConnectionForRequests(wsConnectionPtr);
            event.setType(Event::Type::AUTHORIZATION_STATUS);
            message.setType(Message::Type::AUTHORIZATION_SUCCESS);
            Element element;
            element.insert(CCAPI_CONNECTION_ID, wsConnectionPtr->id);
            element.insert(CCAPI_CONNECTION_URL, wsConnectionPtr->url);
            element.insert(CCAPI_INFO_MESSAGE, textMessageView);
            message.setElementList({element});
            // the session is now authenticated; subscribe to the user data stream so account events flow on this same socket
            const auto& fieldSet = subscription.getFieldSet();
            if (this->supportsWebsocketUserDataStreamSubscription() &&
                (fieldSet.find(CCAPI_EM_ORDER_UPDATE) != fieldSet.end() || fieldSet.find(CCAPI_EM_PRIVATE_TRADE) != fieldSet.end() ||
                 fieldSet.find(CCAPI_EM_PRIVATE_TRADE_LITE) != fieldSet.end() || fieldSet.find(CCAPI_EM_BALANCE_UPDATE) != fieldSet.end() ||
                 fieldSet.find(CCAPI_EM_POSITION_UPDATE) != fieldSet.end())) {
              std::string sub = R"({"id":")" + this->websocketUserDataStreamSubscribeJsonId + R"(","method":"userDataStream.subscribe"})";
              ErrorCode ec;
              this->send(wsConnectionPtr, sub, ec);
              if (ec) {
                this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "userDataStream.subscribe");
              }
            }
          } else {
            this->unregisterWebsocketConnectionForRequests(wsConnectionPtr);
            event.setType(Event::Type::AUTHORIZATION_STATUS);
            message.setType(Message::Type::AUTHORIZATION_FAILURE);
            Element element;
            element.insert(CCAPI_CONNECTION_ID, wsConnectionPtr->id);
            element.insert(CCAPI_CONNECTION_URL, wsConnectionPtr->url);
            element.insert(CCAPI_ERROR_MESSAGE, textMessageView);
            message.setElementList({element});
          }
          messageList.emplace_back(std::move(message));
        } else if (id == this->websocketUserDataStreamSubscribeJsonId) {
          event.setType(Event::Type::SUBSCRIPTION_STATUS);
          message.setType(success ? Message::Type::SUBSCRIPTION_STARTED : Message::Type::SUBSCRIPTION_FAILURE);
          Element element;
          element.insert(success ? CCAPI_INFO_MESSAGE : CCAPI_ERROR_MESSAGE, textMessageView);
          message.setElementList({element});
          messageList.emplace_back(std::move(message));
        } else if (UtilString::startsWith(id, this->websocketOrderEntryCreateOrderJsonIdPrefix) ||
                   UtilString::startsWith(id, this->websocketOrderEntryCancelOrderJsonIdPrefix)) {
          bool isCreateOrder = UtilString::startsWith(id, this->websocketOrderEntryCreateOrderJsonIdPrefix);
          std::string_view wsRequestIdStr;
          if (isCreateOrder) {
            wsRequestIdStr = std::string_view(id).substr(this->websocketOrderEntryCreateOrderJsonIdPrefix.size());
          } else {
            wsRequestIdStr = std::string_view(id).substr(this->websocketOrderEntryCancelOrderJsonIdPrefix.size());
          }
          unsigned long wsRequestId{};
          try {
            wsRequestId = std::stoul(std::string(wsRequestIdStr));
          } catch (const std::exception&) {
            event.setMessageList(messageList);
            return event;
          }
          auto connectionIt = this->requestCorrelationIdByWsRequestIdByConnectionIdMap.find(wsConnectionPtr->id);
          if (connectionIt == this->requestCorrelationIdByWsRequestIdByConnectionIdMap.end()) {
            event.setMessageList(messageList);
            return event;
          }
          auto requestIt = connectionIt->second.find(wsRequestId);
          if (requestIt == connectionIt->second.end()) {
            event.setMessageList(messageList);
            return event;
          }
          auto requestCorrelationId = requestIt->second;
          connectionIt->second.erase(requestIt);
          event.setType(Event::Type::RESPONSE);
          if (!success || !document.HasMember("result") || !document["result"].IsObject()) {
            message.setType(Message::Type::RESPONSE_ERROR);
            Element element;
            element.insert(CCAPI_ERROR_MESSAGE, textMessageView);
            message.setElementList({element});
            message.setCorrelationIdList({requestCorrelationId});
          } else {
            std::vector<Element> elementList;
            if (isCreateOrder) {
              message.setType(Message::Type::CREATE_ORDER);
            } else {
              message.setType(Message::Type::CANCEL_ORDER);
            }
            this->extractOrderInfoFromResponse(elementList, document);
            message.setElementList(elementList);
            message.setCorrelationIdList({requestCorrelationId});
          }
          messageList.emplace_back(std::move(message));
        }
      }
    } else {
      // legacy listenKey path (still used by other Binance variants): the whole document is the event payload
      this->appendUserDataMessages(wsConnectionPtr, event, messageList, subscription, document, timeReceived);
    }
    event.setMessageList(messageList);
    return event;
  }

  bool supportsWebsocketUserDataStreamSubscription() const { return !this->isDerivatives; }

  void appendUserDataMessages(const std::shared_ptr<WsConnection>& wsConnectionPtr, Event& event, std::vector<Message>& messageList,
                              const Subscription& subscription, const rj::Value& eventData, const TimePoint& timeReceived) {
    const auto& fieldSet = subscription.getFieldSet();
    const auto& instrumentSet = subscription.getInstrumentSet();
    if (!eventData.IsObject() || !eventData.HasMember("e") || !eventData["e"].IsString()) {
      return;
    }
    auto insertStringIfPresent = [](Element& element, const rj::Value& value, const char* memberName, const char* elementName) {
      auto it = value.FindMember(memberName);
      if (it != value.MemberEnd() && !it->value.IsNull() && it->value.IsString()) {
        element.insert(elementName, it->value.GetString());
      }
    };
    auto insertBoolIfPresent = [](Element& element, const rj::Value& value, const char* memberName, const char* elementName) {
      auto it = value.FindMember(memberName);
      if (it != value.MemberEnd() && !it->value.IsNull() && it->value.IsBool()) {
        element.insert(elementName, it->value.GetBool() ? "1" : "0");
      }
    };
    auto insertEventTimes = [&insertStringIfPresent](Element& element, const rj::Value& outer, const rj::Value* order = nullptr) {
      insertStringIfPresent(element, outer, "E", CCAPI_EVENT_TIME_MILLISECONDS);
      if (order && order->IsObject() && order->HasMember("T")) {
        insertStringIfPresent(element, *order, "T", CCAPI_TRANSACTION_TIME_MILLISECONDS);
      } else {
        insertStringIfPresent(element, outer, "T", CCAPI_TRANSACTION_TIME_MILLISECONDS);
      }
    };
    std::string type = eventData["e"].GetString();
    if (type == "listenKeyExpired") {
      this->recoverListenKey(wsConnectionPtr, "listenKeyExpired event");
      return;
    } else if (type == "TRADE_LITE") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      const rj::Value& data = eventData;
      std::string instrument = data["s"].GetString();
      if (instrumentSet.empty() || instrumentSet.find(UtilString::toUpper(instrument)) != instrumentSet.end() ||
          instrumentSet.find(UtilString::toLower(instrument)) != instrumentSet.end()) {
        if (fieldSet.find(CCAPI_EM_PRIVATE_TRADE_LITE) != fieldSet.end()) {
          Message message;
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          message.setTime(TimePoint(std::chrono::milliseconds(std::stoll(data["E"].GetString()))));
          message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_PRIVATE_TRADE_LITE);
          std::vector<Element> elementList;
          Element element;
          element.insert(CCAPI_TRADE_ID, data["t"].GetString());
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE, data["L"].GetString());
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE, data["l"].GetString());
          element.insert(CCAPI_EM_ORDER_SIDE, std::string_view(data["S"].GetString()) == "BUY" ? CCAPI_EM_ORDER_SIDE_BUY : CCAPI_EM_ORDER_SIDE_SELL);
          element.insert(CCAPI_IS_MAKER, data["m"].GetBool() ? "1" : "0");
          element.insert(CCAPI_EM_ORDER_ID, data["i"].GetString());
          element.insert(CCAPI_EM_CLIENT_ORDER_ID, data["c"].GetString());
          element.insert(CCAPI_EM_ORDER_INSTRUMENT, instrument);
          insertStringIfPresent(element, data, "n", CCAPI_EM_ORDER_FEE_QUANTITY);
          insertStringIfPresent(element, data, "N", CCAPI_EM_ORDER_FEE_ASSET);
          insertStringIfPresent(element, data, "rp", CCAPI_EM_ORDER_REALIZED_PNL);
          insertBoolIfPresent(element, data, "R", CCAPI_EM_ORDER_REDUCE_ONLY);
          insertStringIfPresent(element, data, "ps", CCAPI_EM_POSITION_SIDE);
          insertStringIfPresent(element, data, "x", CCAPI_EM_ORDER_EXECUTION_TYPE);
          insertEventTimes(element, eventData, &data);
          elementList.emplace_back(std::move(element));
          message.setElementList(elementList);
          messageList.emplace_back(std::move(message));
        }
      }
    } else if (type == (this->isDerivatives ? "ORDER_TRADE_UPDATE" : "executionReport")) {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      const rj::Value& data = this->isDerivatives ? eventData["o"] : eventData;
      std::string executionType = data["x"].GetString();
      std::string instrument = data["s"].GetString();
      if (instrumentSet.empty() || instrumentSet.find(UtilString::toUpper(instrument)) != instrumentSet.end() ||
          instrumentSet.find(UtilString::toLower(instrument)) != instrumentSet.end()) {
        if (executionType == "TRADE" && fieldSet.find(CCAPI_EM_PRIVATE_TRADE) != fieldSet.end()) {
          Message message;
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          message.setTime(TimePoint(std::chrono::milliseconds(std::stoll((this->isDerivatives ? eventData : data)["E"].GetString()))));
          message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_PRIVATE_TRADE);
          std::vector<Element> elementList;
          Element element;
          element.insert(CCAPI_TRADE_ID, data["t"].GetString());
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE, data["L"].GetString());
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE, data["l"].GetString());
          element.insert(CCAPI_EM_ORDER_SIDE, std::string_view(data["S"].GetString()) == "BUY" ? CCAPI_EM_ORDER_SIDE_BUY : CCAPI_EM_ORDER_SIDE_SELL);
          element.insert(CCAPI_IS_MAKER, data["m"].GetBool() ? "1" : "0");
          element.insert(CCAPI_EM_ORDER_ID, data["i"].GetString());
          insertStringIfPresent(element, data, "c", CCAPI_EM_CLIENT_ORDER_ID);
          insertStringIfPresent(element, data, "C", CCAPI_EM_ORIGINAL_CLIENT_ORDER_ID);
          element.insert(CCAPI_EM_ORDER_INSTRUMENT, instrument);
          {
            auto it = data.FindMember("n");
            if (it != data.MemberEnd() && !it->value.IsNull()) {
              element.insert(CCAPI_EM_ORDER_FEE_QUANTITY, it->value.GetString());
            }
          }
          insertStringIfPresent(element, data, "rp", CCAPI_EM_ORDER_REALIZED_PNL);
          insertBoolIfPresent(element, data, "R", CCAPI_EM_ORDER_REDUCE_ONLY);
          insertStringIfPresent(element, data, "ps", CCAPI_EM_POSITION_SIDE);
          insertStringIfPresent(element, data, "x", CCAPI_EM_ORDER_EXECUTION_TYPE);
          insertEventTimes(element, eventData, &data);
          {
            auto it = data.FindMember("N");
            if (it != data.MemberEnd() && !it->value.IsNull()) {
              element.insert(CCAPI_EM_ORDER_FEE_ASSET, it->value.GetString());
            }
          }
          elementList.emplace_back(std::move(element));
          message.setElementList(elementList);
          messageList.emplace_back(std::move(message));
        }
        if (fieldSet.find(CCAPI_EM_ORDER_UPDATE) != fieldSet.end()) {
          Message message;
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          message.setTime(TimePoint(std::chrono::milliseconds(std::stoll((this->isDerivatives ? eventData : data)["E"].GetString()))));
          message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
          const std::map<std::string_view, std::pair<std::string_view, JsonDataType>>& extractionFieldNameMap = {
              {CCAPI_EM_ORDER_ID, std::make_pair("i", JsonDataType::INTEGER)},
              {CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("c", JsonDataType::STRING)},
              {CCAPI_EM_ORIGINAL_CLIENT_ORDER_ID, std::make_pair("C", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_SIDE, std::make_pair("S", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("p", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_QUANTITY, std::make_pair("q", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("z", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY, std::make_pair("Z", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_AVERAGE_FILLED_PRICE, std::make_pair("ap", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_REALIZED_PNL, std::make_pair("rp", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_REDUCE_ONLY, std::make_pair("R", JsonDataType::BOOLEAN)},
              {CCAPI_EM_POSITION_SIDE, std::make_pair("ps", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_EXECUTION_TYPE, std::make_pair("x", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_STATUS, std::make_pair("X", JsonDataType::STRING)},
              {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("s", JsonDataType::STRING)},
          };
          Element info;
          this->extractOrderInfo(info, data, extractionFieldNameMap);
          if (info.getValue(CCAPI_EM_CLIENT_ORDER_ID).empty()) {
            auto it = data.FindMember("c");
            if (it != data.MemberEnd() && !it->value.IsNull() && it->value.GetStringLength()) {
              info.insert_or_assign(CCAPI_EM_CLIENT_ORDER_ID, std::string(it->value.GetString()));
            }
          }
          insertEventTimes(info, eventData, &data);
          std::vector<Element> elementList;
          elementList.emplace_back(std::move(info));
          message.setElementList(elementList);
          messageList.emplace_back(std::move(message));
        }
      }
    } else if (this->isDerivatives && type == "ACCOUNT_UPDATE") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      auto accountIt = eventData.FindMember("a");
      if (accountIt == eventData.MemberEnd() || !accountIt->value.IsObject()) {
        return;
      }
      const rj::Value& data = accountIt->value;
      auto balanceIt = data.FindMember("B");
      if (fieldSet.find(CCAPI_EM_BALANCE_UPDATE) != fieldSet.end() && balanceIt != data.MemberEnd() && balanceIt->value.IsArray() &&
          !balanceIt->value.Empty()) {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setCorrelationIdList({subscription.getCorrelationId()});
        message.setTime(TimePoint(std::chrono::milliseconds(std::stoll(eventData["E"].GetString()))));
        message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_BALANCE_UPDATE);
        std::vector<Element> elementList;
        for (const auto& x : balanceIt->value.GetArray()) {
          Element element;
          insertStringIfPresent(element, data, "m", CCAPI_EM_ACCOUNT_UPDATE_REASON);
          insertStringIfPresent(element, x, "a", CCAPI_EM_ASSET);
          insertStringIfPresent(element, x, "wb", CCAPI_EM_QUANTITY_TOTAL);
          insertStringIfPresent(element, x, "cw", CCAPI_EM_CROSS_WALLET_BALANCE);
          insertStringIfPresent(element, x, "bc", CCAPI_EM_BALANCE_CHANGE);
          insertEventTimes(element, eventData);
          elementList.emplace_back(std::move(element));
        }
        message.setElementList(elementList);
        messageList.emplace_back(std::move(message));
      }
      auto positionIt = data.FindMember("P");
      if (fieldSet.find(CCAPI_EM_POSITION_UPDATE) != fieldSet.end() && positionIt != data.MemberEnd() && positionIt->value.IsArray() &&
          !positionIt->value.Empty()) {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setCorrelationIdList({subscription.getCorrelationId()});
        message.setTime(TimePoint(std::chrono::milliseconds(std::stoll((this->isDerivatives ? eventData : data)["E"].GetString()))));
        message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_POSITION_UPDATE);
        std::vector<Element> elementList;
        for (const auto& x : positionIt->value.GetArray()) {
          Element element;
          insertStringIfPresent(element, data, "m", CCAPI_EM_ACCOUNT_UPDATE_REASON);
          insertStringIfPresent(element, x, "s", CCAPI_INSTRUMENT);
          insertStringIfPresent(element, x, "ps", CCAPI_EM_POSITION_SIDE);
          insertStringIfPresent(element, x, "pa", CCAPI_EM_POSITION_QUANTITY);
          insertStringIfPresent(element, x, "ep", CCAPI_EM_POSITION_ENTRY_PRICE);
          insertStringIfPresent(element, x, "bep", CCAPI_EM_POSITION_BREAK_EVEN_PRICE);
          insertStringIfPresent(element, x, "cr", CCAPI_EM_POSITION_ACCUMULATED_REALIZED_PNL);
          insertStringIfPresent(element, x, "up", CCAPI_EM_UNREALIZED_PNL);
          insertStringIfPresent(element, x, "mt", CCAPI_EM_POSITION_MARGIN_TYPE);
          insertStringIfPresent(element, x, "iw", CCAPI_EM_POSITION_ISOLATED_WALLET);
          insertEventTimes(element, eventData);
          elementList.emplace_back(std::move(element));
        }
        message.setElementList(elementList);
        messageList.emplace_back(std::move(message));
      }
    } else if (this->isDerivatives && type == "ACCOUNT_CONFIG_UPDATE") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      Message message;
      message.setTimeReceived(timeReceived);
      message.setCorrelationIdList({subscription.getCorrelationId()});
      message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_ACCOUNT_CONFIG_UPDATE);
      std::vector<Element> elementList;
      auto accountConfigIt = eventData.FindMember("ac");
      if (accountConfigIt != eventData.MemberEnd() && accountConfigIt->value.IsObject()) {
        Element element;
        insertStringIfPresent(element, accountConfigIt->value, "s", CCAPI_INSTRUMENT);
        insertStringIfPresent(element, accountConfigIt->value, "l", CCAPI_EM_POSITION_LEVERAGE);
        insertEventTimes(element, eventData);
        elementList.emplace_back(std::move(element));
      }
      auto accountInfoIt = eventData.FindMember("ai");
      if (accountInfoIt != eventData.MemberEnd() && accountInfoIt->value.IsObject()) {
        Element element;
        insertBoolIfPresent(element, accountInfoIt->value, "j", CCAPI_EM_MULTI_ASSETS_MODE);
        insertEventTimes(element, eventData);
        elementList.emplace_back(std::move(element));
      }
      if (!elementList.empty()) {
        message.setElementList(elementList);
        messageList.emplace_back(std::move(message));
      }
    } else if (this->isDerivatives && type == "MARGIN_CALL") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      Message message;
      message.setTimeReceived(timeReceived);
      message.setCorrelationIdList({subscription.getCorrelationId()});
      message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_MARGIN_CALL);
      std::vector<Element> elementList;
      auto positionsIt = eventData.FindMember("p");
      if (positionsIt != eventData.MemberEnd() && positionsIt->value.IsArray()) {
        for (const auto& position : positionsIt->value.GetArray()) {
          Element element;
          insertStringIfPresent(element, eventData, "cw", CCAPI_EM_CROSS_WALLET_BALANCE);
          insertStringIfPresent(element, position, "s", CCAPI_INSTRUMENT);
          insertStringIfPresent(element, position, "ps", CCAPI_EM_POSITION_SIDE);
          insertStringIfPresent(element, position, "pa", CCAPI_EM_POSITION_QUANTITY);
          insertStringIfPresent(element, position, "mt", CCAPI_EM_POSITION_MARGIN_TYPE);
          insertStringIfPresent(element, position, "iw", CCAPI_EM_POSITION_ISOLATED_WALLET);
          insertStringIfPresent(element, position, "mp", CCAPI_MARK_PRICE_VALUE);
          insertStringIfPresent(element, position, "up", CCAPI_EM_UNREALIZED_PNL);
          insertEventTimes(element, eventData);
          elementList.emplace_back(std::move(element));
        }
      }
      if (elementList.empty()) {
        Element element;
        insertStringIfPresent(element, eventData, "cw", CCAPI_EM_CROSS_WALLET_BALANCE);
        insertEventTimes(element, eventData);
        elementList.emplace_back(std::move(element));
      }
      message.setElementList(elementList);
      messageList.emplace_back(std::move(message));
    } else if (!this->isDerivatives && type == "outboundAccountPosition") {
      // spot balance snapshot after a change; mirrors the spot REST GET_ACCOUNT_BALANCES mapping (total = free + locked, available = free)
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      if (fieldSet.find(CCAPI_EM_BALANCE_UPDATE) != fieldSet.end()) {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setCorrelationIdList({subscription.getCorrelationId()});
        message.setTime(TimePoint(std::chrono::milliseconds(std::stoll(eventData["E"].GetString()))));
        message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_BALANCE_UPDATE);
        std::vector<Element> elementList;
        for (const auto& x : eventData["B"].GetArray()) {
          Element element;
          element.insert(CCAPI_EM_ASSET, x["a"].GetString());
          element.insert(CCAPI_EM_QUANTITY_TOTAL, ConvertDecimalToString(Decimal(x["f"].GetString()) + Decimal(x["l"].GetString())));
          element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, x["f"].GetString());
          elementList.emplace_back(std::move(element));
        }
        message.setElementList(elementList);
        messageList.emplace_back(std::move(message));
      }
    }
  }

  void convertRequestForWebsocket(rj::Document& document, rj::Document::AllocatorType& allocator, std::shared_ptr<WsConnection> wsConnectionPtr,
                                  const Request& request, unsigned long wsRequestId, const TimePoint& now, const std::string& symbolId,
                                  const std::map<std::string, std::string>& credential) override {
    document.SetObject();
    this->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id][wsRequestId] = request.getCorrelationId();
    Request::Operation operation = request.getOperation();
    switch (operation) {
      case Request::Operation::CREATE_ORDER: {
        document.AddMember("id", rj::Value((this->websocketOrderEntryCreateOrderJsonIdPrefix + std::to_string(wsRequestId)).c_str(), allocator).Move(),
                           allocator);
        document.AddMember("method", rj::Value("order.place").Move(), allocator);
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        rj::Value params(rj::kObjectType);
        this->appendParam(params, allocator, param);
        if (!symbolId.empty()) {
          ExecutionManagementService::appendSymbolId(params, allocator, symbolId, "symbol");
        }
        if (param.find("type") == param.end()) {
          params.AddMember("type", "LIMIT", allocator);
          if (param.find("timeInForce") == param.end()) {
            params.AddMember("timeInForce", "GTC", allocator);
          }
        }
        if (param.find("newClientOrderId") == param.end() && param.find(CCAPI_EM_CLIENT_ORDER_ID) == param.end()) {
          std::string nonce = std::to_string(this->generateNonce(now, request.getIndex()));
          params.AddMember(
              "newClientOrderId",
              rj::Value((std::string("x-") + (this->isDerivatives ? CCAPI_BINANCE_USDS_FUTURES_API_LINK_ID : CCAPI_BINANCE_API_LINK_ID) + "-" + nonce).c_str(),
                        allocator)
                  .Move(),
              allocator);
        }
        if (param.find("timestamp") == param.end()) {
          params.AddMember("timestamp", rj::Value(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count()).Move(), allocator);
        }
        document.AddMember("params", params, allocator);
      } break;
      case Request::Operation::CANCEL_ORDER: {
        document.AddMember("id", rj::Value((this->websocketOrderEntryCancelOrderJsonIdPrefix + std::to_string(wsRequestId)).c_str(), allocator).Move(),
                           allocator);
        document.AddMember("method", rj::Value("order.cancel").Move(), allocator);
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        rj::Value params(rj::kObjectType);
        this->appendParam(params, allocator, param,
                          {
                              {CCAPI_EM_ORDER_ID, "orderId"},
                              {CCAPI_EM_CLIENT_ORDER_ID, "origClientOrderId"},
                          });
        if (!symbolId.empty()) {
          ExecutionManagementService::appendSymbolId(params, allocator, symbolId, "symbol");
        }
        if (param.find("timestamp") == param.end()) {
          params.AddMember("timestamp", rj::Value(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count()).Move(), allocator);
        }
        document.AddMember("params", params, allocator);
      } break;
      default:
        this->convertRequestForWebsocketCustom(document, allocator, wsConnectionPtr, request, wsRequestId, now, symbolId, credential);
    }
  }

  void appendParam(rj::Value& rjValue, rj::Document::AllocatorType& allocator, const std::map<std::string, std::string>& param,
                   const std::map<std::string, std::string> standardizationMap = {
                       {CCAPI_EM_ORDER_SIDE, "side"},
                       {CCAPI_EM_ORDER_QUANTITY, "quantity"},
                       {CCAPI_EM_ORDER_LIMIT_PRICE, "price"},
                       {CCAPI_EM_CLIENT_ORDER_ID, "newClientOrderId"},
                       {CCAPI_EM_ORDER_ID, "orderId"},
                   }) {
    for (const auto& kv : param) {
      auto key = standardizationMap.find(kv.first) != standardizationMap.end() ? standardizationMap.at(kv.first) : kv.first;
      auto value = kv.second;
      if (key == "side") {
        value = value == CCAPI_EM_ORDER_SIDE_BUY ? "BUY" : "SELL";
      }
      if (value != "null") {
        rjValue.AddMember(rj::Value(key.c_str(), allocator).Move(), rj::Value(value.c_str(), allocator).Move(), allocator);
      }
    }
  }

  virtual void extractOrderInfoFromResponse(std::vector<Element>& elementList, const rj::Document& document) {
    std::map<std::string_view, std::pair<std::string_view, JsonDataType>> extractionFieldNameMap = {
        {CCAPI_EM_ORDER_ID, std::make_pair("orderId", JsonDataType::INTEGER)},
        {CCAPI_EM_ORDER_SIDE, std::make_pair("side", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_QUANTITY, std::make_pair("origQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("price", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("executedQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY, std::make_pair(this->isDerivatives ? "cumQuote" : "cummulativeQuoteQty", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_STATUS, std::make_pair("status", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("symbol", JsonDataType::STRING)},
        {CCAPI_LAST_UPDATED_TIME_SECONDS, std::make_pair(this->isDerivatives ? "updateTime" : "transactTime", JsonDataType::STRING)},
        {CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("clientOrderId", JsonDataType::STRING)},
    };

    Element element;
    this->extractOrderInfo(element, document["result"], extractionFieldNameMap,
                           {
                               {CCAPI_LAST_UPDATED_TIME_SECONDS, [](const std::string& input) { return UtilTime::convertMillisecondsStrToSecondsStr(input); }},
                           });
    if (element.getValue(CCAPI_EM_CLIENT_ORDER_ID).empty()) {
      auto it = document["result"].FindMember("origClientOrderId");
      if (it != document["result"].MemberEnd() && it->value.IsString()) {
        element.insert_or_assign(CCAPI_EM_CLIENT_ORDER_ID, it->value.GetString());
      }
    }
    elementList.emplace_back(std::move(element));
  }

  std::vector<std::string> createSendStringListFromSubscription(std::shared_ptr<WsConnection> wsConnectionPtr, const Subscription& subscription,
                                                                const TimePoint& now, const std::map<std::string, std::string>& credential) override {
    if (this->isOrderEntryConnection(wsConnectionPtr, &subscription.getFieldSet())) {
      auto it = credential.find(this->websocketOrderEntryApiPrivateKeyPathName);
      if (it == credential.end() || it->second.empty()) {
        throw std::runtime_error("Missing credential: " + this->websocketOrderEntryApiPrivateKeyPathName);
      }
      auto apiKeyIt = credential.find(this->websocketOrderEntryApiKeyName);
      if (apiKeyIt == credential.end() || apiKeyIt->second.empty()) {
        throw std::runtime_error("Missing credential: " + this->websocketOrderEntryApiKeyName);
      }
      rj::Document document;
      document.SetObject();
      rj::Document::AllocatorType& allocator = document.GetAllocator();

      document.AddMember("id", rj::Value(this->websocketOrderEntrySessionLogonJsonId.c_str(), allocator).Move(), allocator);
      document.AddMember("method", "session.logon", allocator);

      const auto& apiKey = apiKeyIt->second;
      const auto& timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

      std::map<std::string, std::string> paramsMap{
          {"apiKey", apiKey},
          {"timestamp", std::to_string(timestamp)},
      };

      rj::Value params(rj::kObjectType);
      std::string payload;
      int i = 0;
      for (const auto& [key, value] : paramsMap) {
        if (key == "timestamp") {
          params.AddMember("timestamp", rj::Value().SetInt64(std::stoll(value)), allocator);
        } else {
          params.AddMember("apiKey", rj::Value(value.c_str(), allocator).Move(), allocator);
        }
        payload += key;
        payload += "=";
        payload += value;

        if (i < paramsMap.size() - 1) {
          payload += "&";
        }
        ++i;
      }

      std::string password;
      if (auto it = credential.find(this->websocketOrderEntryApiPrivateKeyPasswordName); it != credential.end()) {
        password = it->second;
      }
      std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(UtilAlgorithm::loadPrivateKey(UtilAlgorithm::readFile(it->second), password), EVP_PKEY_free);
      if (!pkey) {
        throw std::runtime_error("Invalid private key: " + this->websocketOrderEntryApiPrivateKeyPathName);
      }
      if (EVP_PKEY_base_id(pkey.get()) != EVP_PKEY_ED25519) {
        throw std::runtime_error("Private key must be Ed25519: " + this->websocketOrderEntryApiPrivateKeyPathName);
      }
      std::string signature = UtilAlgorithm::signPayload(pkey.get(), payload);
      params.AddMember("signature", rj::Value(signature.c_str(), allocator).Move(), allocator);

      document.AddMember("params", params, allocator);

      rj::StringBuffer buffer;
      rj::Writer<rj::StringBuffer> writer(buffer);
      document.Accept(writer);

      return {buffer.GetString()};

    } else {
      return {};
    }
  }

  bool isDerivatives{};
  std::string listenKeyTarget;
  int pingListenKeyIntervalSeconds;
  std::map<std::string, TimerPtr> pingListenKeyTimerMapByConnectionIdMap;
  std::string createOrderMarginTarget;
  std::string cancelOrderMarginTarget;
  std::string getOrderMarginTarget;
  std::string getOpenOrdersMarginTarget;
  std::string cancelOpenOrdersMarginTarget;
  std::string getAccountBalancesCrossMarginTarget;
  std::string getAccountBalancesIsolatedMarginTarget;
  std::string listenKeyCrossMarginTarget;
  std::string listenKeyIsolatedMarginTarget;

  std::string websocketOrderEntryApiKeyName;
  std::string websocketOrderEntryApiPrivateKeyPathName;
  std::string websocketOrderEntryApiPrivateKeyPasswordName;
  std::string websocketOrderEntrySessionLogonJsonId{"session_logon"};
  std::string websocketOrderEntryCreateOrderJsonIdPrefix{"order_place"};
  std::string websocketOrderEntryCancelOrderJsonIdPrefix{"order_cancel"};
  std::string websocketUserDataStreamSubscribeJsonId{"userdata_subscribe"};
  std::string websocketOrderEntryHost;
  std::map<std::string, ConnectionRole> connectionRoleByConnectionIdMap;
  std::map<std::string, uint64_t> connectionGenerationByConnectionIdMap;
  uint64_t nextConnectionGeneration{};
};

} /* namespace ccapi */
#endif
#endif
