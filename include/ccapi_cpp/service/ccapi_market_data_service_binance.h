#pragma once

#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE
#include <deque>

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
    this->sbeApiKeyDefault =
        mapGetWithDefault(sessionConfigs.getCredential(), std::string(CCAPI_BINANCE_SBE_API_KEY), UtilSystem::getEnvAsString(CCAPI_BINANCE_SBE_API_KEY));
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

  void subscribe(std::vector<Subscription>& subscriptionList) override {
    std::vector<Subscription> valid;
    for (const auto& subscription : subscriptionList) {
      try {
        this->validateSubscription(subscription);
        valid.push_back(subscription);
      } catch (const std::exception& e) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, e.what(), {subscription.getCorrelationId()});
      }
    }
    MarketDataService::subscribe(valid);
  }

#ifndef CCAPI_EXPOSE_INTERNAL
 protected:
#endif
  struct SbeBook {
    std::deque<BinanceSbe::Event> buffer;
    std::optional<BinanceSbe::Event> snapshot;
    int64_t version{-1};
    bool fetching{};
    int retryMilliseconds{1000};
  };

  void validateSubscription(const Subscription& subscription) const {
    const auto& options = subscription.getOptionMap();
    auto format = mapGetWithDefault(options, std::string(CCAPI_BINANCE_MARKET_DATA_FORMAT), std::string("JSON"));
    auto stream = mapGetWithDefault(options, std::string(CCAPI_BINANCE_MARKET_DATA_STREAM));
    if (format != "JSON" && format != "SBE") throw std::invalid_argument("Binance market data format must be JSON or SBE");
    if (format == "JSON") {
      if (!stream.empty()) throw std::invalid_argument("BINANCE_MARKET_DATA_STREAM requires SBE");
      return;
    }
    auto field = subscription.getField();
    if (field != CCAPI_MARKET_DEPTH && field != CCAPI_TRADE) throw std::invalid_argument("Binance SBE supports MARKET_DEPTH and TRADE only");
    if (field == CCAPI_TRADE && !stream.empty() && stream != "trade") throw std::invalid_argument("Binance SBE TRADE requires trade stream");
    if (field == CCAPI_MARKET_DEPTH) {
      if (!stream.empty() && stream != "depth" && stream != "bestBidAsk")
        throw std::invalid_argument("Binance SBE MARKET_DEPTH requires depth or bestBidAsk (depth20 is not supported)");
      int levels = std::stoi(options.at(CCAPI_MARKET_DEPTH_MAX));
      if (levels < 1 || levels > 5000 || (stream == "bestBidAsk" && levels != 1))
        throw std::invalid_argument("Binance SBE depth supports 1..5000 local levels; bestBidAsk requires 1");
    }
    if (mapGetWithDefault(subscription.getCredential(), std::string(CCAPI_BINANCE_SBE_API_KEY), this->sbeApiKeyDefault).empty())
      throw std::invalid_argument("Binance SBE requires BINANCE_SBE_API_KEY from an Ed25519 key");
  }

  std::string getInstrumentGroup(const Subscription& subscription) override {
    this->validateSubscription(subscription);
    if (!subscription.isBinanceSpotSbe()) return MarketDataService::getInstrumentGroup(subscription);
    return "wss://stream-sbe.binance.com:9443/ws|" + subscription.getField() + "|" + subscription.getSerializedOptions() + "|" +
           UtilAlgorithm::computeHash(UtilAlgorithm::ShaVersion::SHA256, subscription.getSerializedCredential(), true) + "|" + subscription.getProxyUrl();
  }

  void prepareConnect(std::shared_ptr<WsConnection> connection) override {
    if (connection->isBinanceSpotSbe()) {
      if (!this->shouldContinue.load()) return;
      auto key = mapGetWithDefault(connection->subscriptionList.front().getCredential(), std::string(CCAPI_BINANCE_SBE_API_KEY), this->sbeApiKeyDefault);
      if (key.empty()) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, "Binance SBE: missing Ed25519 API key",
                      connection->correlationIdList);
        return;
      }
      connection->headers["X-MBX-APIKEY"] = key;
      if (connection->isSecure) {
        ErrorCode ec;
        this->serviceContextPtr->sslContextPtr->set_default_verify_paths(ec);
        if (ec) {
          this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "Binance SBE TLS trust store",
                        connection->correlationIdList);
          return;
        }
        std::visit(
            [&](auto& stream) {
              using Stream = std::decay_t<decltype(*stream)>;
              if constexpr (std::is_same_v<Stream, beast::websocket::stream<beast::ssl_stream<beast::tcp_stream>>>) {
                if (stream) {
                  stream->next_layer().set_verify_mode(net::ssl::verify_peer);
                  stream->next_layer().set_verify_callback(net::ssl::host_name_verification(connection->host));
                }
              }
            },
            connection->streamPtr);
      }
    }
    MarketDataService::prepareConnect(connection);
  }

  void onOpen(std::shared_ptr<WsConnection> connection) override {
    if (connection->isBinanceSpotSbe() && !this->shouldContinue.load()) {
      ErrorCode ec;
      this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("stop"), ec);
      return;
    }
    MarketDataServiceBinanceBase::onOpen(connection);
  }

  void onWsHandshakeError(std::shared_ptr<WsConnection> connection, const ErrorCode& ec) override {
    if (connection->isBinanceSpotSbe()) {
      auto status = connection->handshakeResponse.result_int();
      std::string reason = status == 401 || status == 403 ? "authentication/access rejected (check Ed25519 API key and IP whitelist)" : "handshake failed";
      this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
                    "Binance SBE: " + reason + ", HTTP " + std::to_string(status) + ", " + ec.message(), connection->correlationIdList);
    }
    MarketDataService::onWsHandshakeError(connection, ec);
  }

  void prepareSubscriptionDetail(std::string& channelId, std::string& symbolId, const std::string& field, std::shared_ptr<WsConnection> connection,
                                 const Subscription& subscription, const std::map<std::string, std::string> options) override {
    if (!subscription.isBinanceSpotSbe()) {
      MarketDataServiceBinanceBase::prepareSubscriptionDetail(channelId, symbolId, field, connection, subscription, options);
      return;
    }
    channelId = field == CCAPI_TRADE ? "trade" : mapGetWithDefault(options, std::string(CCAPI_BINANCE_MARKET_DATA_STREAM), std::string("depth"));
    this->l2UpdateIsReplaceByConnectionIdChannelIdSymbolIdMap[connection->id][channelId][symbolId] = channelId == "bestBidAsk";
  }

  void clearStates(std::shared_ptr<WsConnection> connection) override {
    this->sbeBooks.erase(connection->id);
    if (connection->isBinanceSpotSbe()) {
      this->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap.erase(connection->id);
      this->exchangeJsonPayloadIdByConnectionIdMap.erase(connection->id);
    }
    MarketDataServiceBinanceBase::clearStates(connection);
  }

  virtual void restartSbe(std::shared_ptr<WsConnection> connection, const std::string& reason) {
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, reason, connection->correlationIdList);
    this->sbeBooks.erase(connection->id);
    this->shouldProcessRemainingMessageOnClosingByConnectionIdMap[connection->id] = false;
    ErrorCode ec;
    this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("Binance SBE resynchronization"), ec);
  }

  void onTextMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view text, const TimePoint& received) override {
    if (!connection->isBinanceSpotSbe()) {
      MarketDataService::onTextMessage(connection, text, received);
      return;
    }
    rj::Document document;
    try {
      document.Parse(text.data(), text.size());
    } catch (const std::exception&) {
      this->restartSbe(connection, "Binance SBE: malformed JSON control response");
      return;
    }
    if (!document.HasParseError() && document.IsObject()) {
      if (document.HasMember("e") && document["e"].IsString() && std::string(document["e"].GetString()) == "serverShutdown") {
        this->restartSbe(connection, "Binance SBE: serverShutdown; reconnecting");
        return;
      }
      if (document.HasMember("id") && document["id"].IsInt() && document.HasMember("result") && document["result"].IsNull()) {
        auto id = document["id"].GetInt();
        if (this->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap[connection->id].count(id)) {
          MarketDataService::onTextMessage(connection, text, received);
          return;
        }
      }
      if (document.HasMember("code")) {
        this->restartSbe(connection,
                         "Binance SBE: subscription rejected" + (document["code"].IsInt() ? ", code=" + std::to_string(document["code"].GetInt()) : ""));
        return;
      }
    }
    this->restartSbe(connection, "Binance SBE: invalid JSON control response or unexpected text market data");
  }

  bool onBinaryMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view frame, const TimePoint& received) override {
    if (!connection->isBinanceSpotSbe()) return false;
    try {
      auto data = BinanceSbe::decode(std::string_view(frame.data(), frame.size()));
      data.timeReceived = received;
      auto stream = UtilString::toLower(data.symbol) + "@" + (data.templateId == 10000 ? "trade" : data.templateId == 10001 ? "bestBidAsk" : "depth");
      if (!this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).count(stream))
        throw std::runtime_error("Binance SBE: event for an unsubscribed stream");
      if (data.templateId != 10003) {
        this->emitSbe(connection, stream, data, false);
      } else {
        auto& slot = this->sbeBooks[connection->id][stream];
        if (!slot) slot = std::make_shared<SbeBook>();
        auto book = slot;
        if (book->version >= 0) {
          if (data.lastUpdateId <= book->version) return true;
          if (data.firstUpdateId - 1 > book->version) throw std::runtime_error("Binance SBE: order book sequence gap; resynchronizing");
          book->version = data.lastUpdateId;
          this->emitSbe(connection, stream, data, false);
        } else {
          if (book->buffer.size() >= 10000) throw std::runtime_error("Binance SBE: snapshot diff buffer exceeded 10000 events");
          book->buffer.push_back(std::move(data));
          this->synchronizeSbe(connection, stream, book);
          if (!book->snapshot && !book->fetching && book->version < 0) this->fetchSbeSnapshot(connection, stream, book);
        }
      }
    } catch (const std::exception& e) {
      this->restartSbe(connection, e.what());
    }
    return true;
  }

  void emitSbe(std::shared_ptr<WsConnection> connection, const std::string& stream, const BinanceSbe::Event& data, bool snapshot) {
    const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
    bool initial =
        snapshot || (data.templateId == 10001 &&
                     !this->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[connection->id][route.at(CCAPI_CHANNEL_ID)][route.at(CCAPI_SYMBOL_ID)]);
    MarketDataMessage message;
    message.type = data.templateId == 10000 ? MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE : MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
    message.exchangeSubscriptionId = stream;
    message.recapType = initial ? MarketDataMessage::RecapType::SOLICITED : MarketDataMessage::RecapType::NONE;
    message.tp = snapshot ? data.timeReceived : TimePoint(std::chrono::microseconds(data.templateId == 10000 ? data.transactTime : data.eventTime));
    message.data = MarketDataMessage::ConvertOwningDataToData(data.data);
    // Keep empty sides visible to the normalizer, including an empty initial book.
    if (data.templateId != 10000) {
      message.data[MarketDataMessage::DataType::BID];
      message.data[MarketDataMessage::DataType::ASK];
    }
    std::vector<MarketDataMessage> list{message};
    Event event;
    this->processMarketDataMessageList(connection, {}, data.timeReceived, event, list);
    auto messages = event.getMessageList();
    for (auto& normalized : messages) {
      if (normalized.getTime() != message.tp) continue;
      auto elements = normalized.getElementList();
      for (auto& element : elements) {
        if (!snapshot) element.insert(CCAPI_EVENT_TIME_MICROSECONDS, std::to_string(data.eventTime));
        if (data.templateId != 10000) element.insert(CCAPI_SEQUENCE_NUMBER, std::to_string(data.lastUpdateId));
        if (data.templateId == 10003 && !snapshot) element.insert(CCAPI_FIRST_SEQUENCE_NUMBER, std::to_string(data.firstUpdateId));
      }
      normalized.setElementList(elements);
    }
    event.setMessageList(messages);
    if (!event.getMessageList().empty()) this->eventHandler(event, nullptr);
  }

  bool isCurrentSbeBook(std::shared_ptr<WsConnection> connection, const std::string& stream, const std::shared_ptr<SbeBook>& book) const {
    auto it = this->sbeBooks.find(connection->id);
    return this->shouldContinue.load() && connection->status == WsConnection::Status::OPEN && it != this->sbeBooks.end() && it->second.count(stream) &&
           it->second.at(stream) == book;
  }

  void synchronizeSbe(std::shared_ptr<WsConnection> connection, const std::string& stream, const std::shared_ptr<SbeBook>& book) {
    if (!book->snapshot) return;
    auto version = book->snapshot->lastUpdateId;
    while (!book->buffer.empty() && book->buffer.front().lastUpdateId <= version) book->buffer.pop_front();
    if (book->buffer.empty()) return;
    if (book->buffer.front().firstUpdateId - 1 > version) {
      book->snapshot.reset();
      this->retrySbeSnapshot(connection, stream, book, "Binance SBE: REST snapshot is older than buffered diffs");
      return;
    }
    for (const auto& diff : book->buffer) {
      if (diff.lastUpdateId <= version) continue;
      if (diff.firstUpdateId - 1 > version) throw std::runtime_error("Binance SBE: buffered order book sequence gap; resynchronizing");
      version = diff.lastUpdateId;
    }
    book->version = book->snapshot->lastUpdateId;
    this->emitSbe(connection, stream, *book->snapshot, true);
    for (const auto& diff : book->buffer) {
      if (!this->isCurrentSbeBook(connection, stream, book)) return;
      if (diff.lastUpdateId <= book->version) continue;
      book->version = diff.lastUpdateId;
      this->emitSbe(connection, stream, diff, false);
    }
    book->snapshot.reset();
    book->buffer.clear();
  }

  virtual void fetchSbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book) {
    if (!this->isCurrentSbeBook(connection, stream, book) || book->version >= 0 || book->snapshot) return;
    book->fetching = true;
    const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
    http::request<http::string_body> req;
    this->createFetchOrderBookInitialReq(req, UtilString::toUpper(route.at(CCAPI_SYMBOL_ID)), UtilTime::now(), {});
    this->sendRequest(
        req,
        [that = shared_from_base<MarketDataServiceBinance>(), connection, stream, book](const ErrorCode& ec) {
          that->retrySbeSnapshot(connection, stream, book, "Binance SBE: REST snapshot transport failure: " + ec.message());
        },
        [that = shared_from_base<MarketDataServiceBinance>(), connection, stream, book](const http::response<http::string_body>& response) {
          that->onSbeSnapshot(connection, stream, book, response);
        },
        this->sessionOptions.httpRequestTimeoutMilliseconds);
  }

  void onSbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book,
                     const http::response<http::string_body>& response) {
    if (!this->isCurrentSbeBook(connection, stream, book)) return;
    book->fetching = false;
    try {
      if (response.result_int() / 100 != 2) throw std::runtime_error("Binance SBE: REST snapshot HTTP " + std::to_string(response.result_int()));
      rj::Document document;
      document.Parse<rj::kParseNumbersAsStringsFlag>(response.body().data(), response.body().size());
      if (document.HasParseError() || !document.IsObject() || !document.HasMember("lastUpdateId") || !document["lastUpdateId"].IsString())
        throw std::runtime_error("Binance SBE: invalid REST snapshot");
      BinanceSbe::Event snapshot;
      snapshot.templateId = 10003;
      std::string id = document["lastUpdateId"].GetString();
      if (id.empty() || id.find_first_not_of("0123456789") != std::string::npos) throw std::runtime_error("Binance SBE: invalid snapshot update ID");
      snapshot.lastUpdateId = std::stoll(id);
      snapshot.timeReceived = UtilTime::now();
      for (const auto& side : {"bids", "asks"}) {
        if (!document.HasMember(side) || !document[side].IsArray()) throw std::runtime_error("Binance SBE: invalid snapshot levels");
        auto& levels = snapshot.data[std::string(side) == "bids" ? MarketDataMessage::DataType::BID : MarketDataMessage::DataType::ASK];
        for (const auto& level : document[side].GetArray()) {
          if (!level.IsArray() || level.Size() != 2 || !level[0].IsString() || !level[1].IsString())
            throw std::runtime_error("Binance SBE: invalid snapshot level");
          for (int i = 0; i < 2; ++i) {
            std::string value = level[i].GetString();
            if (value.empty() || value == "." || value.find_first_not_of("0123456789.") != std::string::npos || std::count(value.begin(), value.end(), '.') > 1)
              throw std::runtime_error("Binance SBE: invalid snapshot decimal");
            Decimal checked(value);
          }
          levels.push_back({{MarketDataMessage::DataFieldType::PRICE, std::string(UtilString::normalizeDecimalStringView(level[0].GetString()))},
                            {MarketDataMessage::DataFieldType::SIZE, std::string(UtilString::normalizeDecimalStringView(level[1].GetString()))}});
        }
      }
      book->snapshot = std::move(snapshot);
    } catch (const std::exception& e) {
      this->retrySbeSnapshot(connection, stream, book, e.what());
      return;
    }
    try {
      this->synchronizeSbe(connection, stream, book);
    } catch (const std::exception& e) {
      this->restartSbe(connection, e.what());
    }
  }

  void retrySbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book, const std::string& reason) {
    if (!this->isCurrentSbeBook(connection, stream, book)) return;
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::GENERIC_ERROR, reason, connection->correlationIdList);
    book->fetching = true;
    TimerPtr timer(new boost::asio::steady_timer(*this->serviceContextPtr->ioContextPtr, std::chrono::milliseconds(book->retryMilliseconds)));
    book->retryMilliseconds = std::min(book->retryMilliseconds * 2, 30000);
    timer->async_wait([that = shared_from_base<MarketDataServiceBinance>(), connection, stream, book](const ErrorCode& ec) {
      if (!ec) that->fetchSbeSnapshot(connection, stream, book);
    });
    this->fetchMarketDepthInitialSnapshotTimerByConnectionIdExchangeSubscriptionIdMap[connection->id][stream] = timer;
  }

  std::string sbeApiKeyDefault;
  std::map<std::string, std::map<std::string, std::shared_ptr<SbeBook>>> sbeBooks;
};

} /* namespace ccapi */
#endif
#endif
