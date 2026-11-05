#pragma once

#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE
#include "ccapi_cpp/ccapi_binance_sbe.h"
#include "ccapi_cpp/service/ccapi_market_data_service_binance_base.h"

namespace ccapi {

class MarketDataServiceBinance : public MarketDataServiceBinanceBase {
 public:
  MarketDataServiceBinance(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                           ServiceContext* serviceContextPtr)
      : MarketDataServiceBinanceBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_BINANCE;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/stream";
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    // this->setHostWsFromUrlWs(this->baseUrlWs);
    this->apiKeyName = CCAPI_BINANCE_API_KEY;
    this->setupCredential({this->apiKeyName});
    this->getRecentTradesTarget = "/api/v3/trades";
    this->getHistoricalTradesTarget = "/api/v3/historicalTrades";
    this->getRecentAggTradesTarget = "/api/v3/aggTrades";
    this->getHistoricalAggTradesTarget = "/api/v3/aggTrades";
    this->getRecentCandlesticksTarget = "/api/v3/klines";
    this->getHistoricalCandlesticksTarget = "/api/v3/klines";
    this->getMarketDepthTarget = "/api/v3/depth";
    this->getServerTimeTarget = "/api/v3/time";
    this->getInstrumentTarget = "/api/v3/exchangeInfo";
    this->getInstrumentsTarget = "/api/v3/exchangeInfo";
    this->getBbosTarget = "/api/v3/ticker/bookTicker";
  }

  virtual ~MarketDataServiceBinance() {}
#ifndef CCAPI_EXPOSE_INTERNAL
 protected:
#endif

  bool usesSbe(const Subscription& subscription) const {
    return mapGetWithDefault(subscription.getOptionMap(), std::string(CCAPI_BINANCE_MARKET_DATA_ENCODING), std::string("JSON")) == "SBE";
  }

  bool usesSbe(const std::shared_ptr<WsConnection>& connection) const {
    return !connection->subscriptionList.empty() && usesSbe(connection->subscriptionList.front());
  }

  std::string getInstrumentGroup(const Subscription& subscription) override {
    auto group = MarketDataServiceBinanceBase::getInstrumentGroup(subscription);
    if (!usesSbe(subscription)) return group;
    const auto& field = subscription.getField();
    if (field != CCAPI_TRADE && field != CCAPI_MARKET_DEPTH) {
      throw std::invalid_argument("Binance SBE supports only TRADE and MARKET_DEPTH");
    }
    const auto options = subscription.getOptionMap();
    const auto mode = mapGetWithDefault(options, std::string(CCAPI_BINANCE_SBE_DEPTH_MODE), std::string("diff"));
    if (mode != "diff" && mode != "partial") throw std::invalid_argument("invalid BINANCE_SBE_DEPTH_MODE");
    if (field == CCAPI_MARKET_DEPTH) {
      const auto depth = std::stoi(options.at(CCAPI_MARKET_DEPTH_MAX));
      if (depth < 1 || depth > 5000 || (mode == "partial" && depth > 20)) {
        throw std::invalid_argument("invalid Binance SBE market depth");
      }
    }
    const auto credential = subscription.getCredential().empty() ? this->credentialDefault : subscription.getCredential();
    if (mapGetWithDefault(credential, this->apiKeyName).empty()) throw std::invalid_argument("Binance SBE requires an Ed25519 API key");
    const auto fingerprint = UtilAlgorithm::computeHash(UtilAlgorithm::ShaVersion::SHA256, ccapi::toString(credential), true);
    return std::string(CCAPI_BINANCE_SBE_URL_WS_BASE) + "/stream|" + field + "|" + subscription.getSerializedOptions() + "|" + fingerprint + "|" +
           subscription.getProxyUrl();
  }

  void prepareConnect(std::shared_ptr<WsConnection> connection) override {
    if (usesSbe(connection)) {
      const auto key = mapGetWithDefault(connection->credential, this->apiKeyName);
      if (key.empty()) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, "Binance SBE requires an Ed25519 API key",
                      connection->correlationIdList);
        return;
      }
      connection->headers["X-MBX-APIKEY"] = key;
    }
    MarketDataServiceBinanceBase::prepareConnect(connection);
  }

  void prepareSubscriptionDetail(std::string& channel, std::string& symbol, const std::string& field, std::shared_ptr<WsConnection> connection,
                                 const Subscription& subscription, const std::map<std::string, std::string> options) override {
    if (!usesSbe(subscription)) {
      MarketDataServiceBinanceBase::prepareSubscriptionDetail(channel, symbol, field, connection, subscription, options);
      return;
    }
    if (field == CCAPI_MARKET_DEPTH) {
      const auto depth = std::stoi(options.at(CCAPI_MARKET_DEPTH_MAX));
      if (depth == 1) {
        channel = "bestBidAsk";
        this->l2UpdateIsReplaceByConnectionIdChannelIdSymbolIdMap[connection->id][channel][symbol] = true;
      } else if (mapGetWithDefault(options, std::string(CCAPI_BINANCE_SBE_DEPTH_MODE), std::string("diff")) == "partial") {
        channel = "depth?MARKET_DEPTH_SUBSCRIBED_TO_EXCHANGE=20";
        this->marketDepthSubscribedToExchangeByConnectionIdChannelIdSymbolIdMap[connection->id][channel][symbol] = 20;
      } else {
        channel = "depth";
        this->marketDepthSubscribedToExchangeByConnectionIdChannelIdSymbolIdMap[connection->id][channel][symbol] = 5000;
      }
    }
  }

  bool usesSpotOrderBookUpdateRangeCheck(const std::shared_ptr<WsConnection>& connection) const override { return usesSbe(connection); }

  virtual void failSbe(std::shared_ptr<WsConnection> connection, const std::string& reason) {
    this->clearStates(connection);
    this->shouldProcessRemainingMessageOnClosingByConnectionIdMap[connection->id] = false;
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, reason, connection->correlationIdList);
    ErrorCode ec;
    this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("Binance SBE resync"), ec);
  }

  void onTextMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view text, const TimePoint& received) override {
    if (usesSbe(connection)) {
      rj::Document document;
      document.Parse(text.data(), text.size());
      if (document.HasParseError() || !document.IsObject()) {
        failSbe(connection, "malformed Binance SBE control message");
        return;
      }
      const auto& payload = document.HasMember("data") && document["data"].IsObject() ? document["data"] : document;
      if (payload.HasMember("e") && payload["e"].IsString() && std::string_view(payload["e"].GetString()) == "serverShutdown") {
        failSbe(connection, "Binance SBE server shutdown");
        return;
      }
    }
    MarketDataServiceBinanceBase::onTextMessage(connection, text, received);
  }

  void onBinaryMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view bytes, const TimePoint& received) override {
    if (!usesSbe(connection)) {
      MarketDataServiceBinanceBase::onBinaryMessage(connection, bytes, received);
      return;
    }
    try {
      // The owned strings must outlive both normalization and snapshot buffering.
      auto decoded = BinanceSbe::decode(std::string_view(bytes.data(), bytes.size()));
      const auto stream = UtilString::toLower(decoded.symbol) + "@" +
                          (decoded.templateId == 10000   ? "trade"
                           : decoded.templateId == 10001 ? "bestBidAsk"
                           : decoded.templateId == 10002 ? "depth20"
                                                         : "depth");
      const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
      const auto& channel = route.at(CCAPI_CHANNEL_ID);
      const auto& symbol = route.at(CCAPI_SYMBOL_ID);
      const auto& options = this->optionMapByConnectionIdChannelIdSymbolIdMap.at(connection->id).at(channel).at(symbol);
      MarketDataMessage message;
      message.type = decoded.templateId == 10000 ? MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE : MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
      message.tp = TimePoint(std::chrono::microseconds(decoded.templateId == 10000 ? decoded.transactTime : decoded.eventTime));
      message.exchangeSubscriptionId = stream;
      message.recapType = MarketDataMessage::RecapType::NONE;
      if (decoded.templateId == 10002) {
        const auto depth = static_cast<size_t>(std::stoi(options.at(CCAPI_MARKET_DEPTH_MAX)));
        for (auto& side : decoded.data)
          if (side.second.size() > depth) side.second.resize(depth);
      }
      message.data = MarketDataMessage::ConvertOwningDataToData(decoded.data);
      std::vector<MarketDataMessage> messages;
      if (decoded.templateId == 10003) {
        if (!this->processOrderBookWithVersionId(decoded.lastUpdateId, connection, channel, symbol, stream, options, messages, message,
                                                 decoded.firstUpdateId)) {
          failSbe(connection, "Binance SBE order book update sequence gap");
          return;
        }
      } else {
        if (decoded.templateId != 10000 && !this->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[connection->id][channel][symbol]) {
          message.recapType = MarketDataMessage::RecapType::SOLICITED;
        }
        messages.push_back(std::move(message));
      }
      Event event;
      this->processMarketDataMessageList(connection, "Binance SBE", received, event, messages);
      if (!event.getMessageList().empty()) this->eventHandler(event, nullptr);
    } catch (const std::exception& e) {
      failSbe(connection, std::string("invalid Binance SBE event: ") + e.what());
    }
  }
};

} /* namespace ccapi */
#endif
#endif
