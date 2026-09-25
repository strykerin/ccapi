#pragma once

#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE_USDS_FUTURES
#include "ccapi_cpp/service/ccapi_market_data_service_binance_derivatives_base.h"

namespace ccapi {

class MarketDataServiceBinanceUsdsFutures : public MarketDataServiceBinanceDerivativesBase {
 public:
  MarketDataServiceBinanceUsdsFutures(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                                      ServiceContext* serviceContextPtr)
      : MarketDataServiceBinanceDerivativesBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES;
    this->websocketRoot = UtilString::rtrim(sessionConfigs.getUrlWebsocketBase().at(this->exchangeName), '/');
    this->baseUrlWs = this->websocketRoot + "/market/stream";
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    // this->setHostWsFromUrlWs(this->baseUrlWs);
    this->apiKeyName = CCAPI_BINANCE_USDS_FUTURES_API_KEY;
    this->setupCredential({this->apiKeyName});
    this->getRecentTradesTarget = "/fapi/v1/trades";
    this->getHistoricalTradesTarget = "/fapi/v1/historicalTrades";
    this->getRecentAggTradesTarget = "/fapi/v1/aggTrades";
    this->getHistoricalAggTradesTarget = "/fapi/v1/aggTrades";
    this->getRecentCandlesticksTarget = "/fapi/v1/klines";
    this->getHistoricalCandlesticksTarget = "/fapi/v1/klines";
    this->getMarketDepthTarget = "/fapi/v1/depth";
    this->getServerTimeTarget = "/fapi/v1/time";
    this->getInstrumentTarget = "/fapi/v1/exchangeInfo";
    this->getInstrumentsTarget = "/fapi/v1/exchangeInfo";
    this->getBbosTarget = "/fapi/v1/ticker/bookTicker";
    this->enableOrderBookUpdateRangeCheck = true;
  }

  virtual ~MarketDataServiceBinanceUsdsFutures() {}
#ifndef CCAPI_EXPOSE_INTERNAL

 protected:
#endif
  std::string getInstrumentGroup(const Subscription& subscription) override {
    const auto& field = subscription.getField();
    if (field == CCAPI_GENERIC_PUBLIC_SUBSCRIPTION) {
      return MarketDataService::getInstrumentGroup(subscription);
    }
    const auto& fieldMap = this->sessionConfigs.getExchangeFieldWebsocketChannelMap().at(this->exchangeName);
    if (fieldMap.find(field) == fieldMap.end()) {
      throw std::invalid_argument("unsupported Binance USD-M market-data field: " + field);
    }
    const auto& route = field == CCAPI_MARKET_DEPTH ? "/public/stream" : "/market/stream";
    return this->websocketRoot + route + "|" + field + "|" + subscription.getSerializedOptions() + "|" + subscription.getSerializedCredential() + "|" +
           subscription.getProxyUrl();
  }

  std::string websocketRoot;
};

} /* namespace ccapi */
#endif
#endif
