#pragma once

#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_OKX

#include <deque>

#include "ccapi_cpp/ccapi_okx_sbe.h"
#include "ccapi_cpp/service/ccapi_market_data_service.h"

namespace ccapi {

class MarketDataServiceOkx : public MarketDataService {
 public:
  MarketDataServiceOkx(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                       ServiceContext* serviceContextPtr)
      : MarketDataService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_OKX;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + CCAPI_OKX_PUBLIC_WS_PATH;
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    // this->setHostWsFromUrlWs(this->baseUrlWs);
    this->apiKeyName = CCAPI_OKX_API_KEY;
    this->apiSecretName = CCAPI_OKX_API_SECRET;
    this->apiPassphraseName = CCAPI_OKX_API_PASSPHRASE;
    this->setupCredential({this->apiKeyName, this->apiSecretName, this->apiPassphraseName});
    this->getRecentTradesTarget = "/api/v5/market/trades";
    this->getHistoricalTradesTarget = "/api/v5/market/history-trades";
    this->getRecentCandlesticksTarget = "/api/v5/market/candles";
    this->getHistoricalCandlesticksTarget = "/api/v5/market/history-candles";
    this->getMarketDepthTarget = "/api/v5/market/books";
    this->getRecentTradesTarget = "/api/v5/market/trades";
    this->getServerTimeTarget = "/api/v5/public/time";
    this->getInstrumentTarget = "/api/v5/public/instruments";
    this->getInstrumentsTarget = "/api/v5/public/instruments";
    this->getBbosTarget = "/api/v5/market/tickers";
    this->getTickersTarget = "/api/v5/market/tickers";
  }

  virtual ~MarketDataServiceOkx() {}

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

 private:
#endif
  struct SbeBook {
    std::deque<OkxSbe::Event> buffer;
    std::optional<OkxSbe::Event> snapshot;
    int64_t sequence{-1};
    int priceExponent{};
    int sizeExponent{};
    bool fetching{};
    int retryMilliseconds{1000};
  };

  void validateSubscription(const Subscription& subscription) const {
    const auto& options = subscription.getOptionMap();
    auto format = mapGetWithDefault(options, std::string(CCAPI_OKX_MARKET_DATA_FORMAT), std::string("JSON"));
    auto stream = mapGetWithDefault(options, std::string(CCAPI_OKX_MARKET_DATA_STREAM));
    if (format != "JSON" && format != "SBE") throw std::invalid_argument("OKX market data format must be JSON or SBE");
    if (format == "JSON") {
      if (!stream.empty()) throw std::invalid_argument("OKX_MARKET_DATA_STREAM requires SBE");
      return;
    }
    auto field = subscription.getField();
    if (field != CCAPI_MARKET_DEPTH && field != CCAPI_TRADE) throw std::invalid_argument("OKX SBE supports MARKET_DEPTH and TRADE only");
    if (field == CCAPI_TRADE && !stream.empty() && stream != "trades") throw std::invalid_argument("OKX SBE TRADE requires trades stream");
    if (field == CCAPI_MARKET_DEPTH) {
      if (!stream.empty() && stream != "books-l2-tbt" && stream != "bbo-tbt")
        throw std::invalid_argument("OKX SBE MARKET_DEPTH requires books-l2-tbt or bbo-tbt");
      const auto& text = options.at(CCAPI_MARKET_DEPTH_MAX);
      if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos) throw std::invalid_argument("OKX SBE: invalid depth");
      auto levels = std::stoll(text);
      if (levels < 1 || levels > 400 || (stream == "bbo-tbt" && levels != 1))
        throw std::invalid_argument("OKX SBE depth supports 1..400 local levels; bbo-tbt requires 1");
    }
    auto code = mapGetWithDefault(options, std::string(CCAPI_INST_ID_CODE));
    if (subscription.getInstrumentSet().size() != 1 || code.empty() || code.find_first_not_of("0123456789") != std::string::npos || std::stoll(code) <= 0)
      throw std::invalid_argument("OKX SBE requires one instrument and its current INST_ID_CODE from GET_INSTRUMENT");
    const auto& credential = subscription.getCredential().empty() ? this->credentialDefault : subscription.getCredential();
    for (const auto& name : {CCAPI_OKX_API_KEY, CCAPI_OKX_API_SECRET, CCAPI_OKX_API_PASSPHRASE}) {
      if (mapGetWithDefault(credential, std::string(name)).empty()) throw std::invalid_argument("OKX SBE requires API key, secret and passphrase");
    }
  }

  void setSbeHeaders(std::shared_ptr<WsConnection> connection, const TimePoint& now) {
    auto timestamp = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
    const auto& credential = connection->credential;
    connection->headers["OK-ACCESS-KEY"] = credential.at(CCAPI_OKX_API_KEY);
    connection->headers["OK-ACCESS-PASSPHRASE"] = credential.at(CCAPI_OKX_API_PASSPHRASE);
    connection->headers["OK-ACCESS-TIMESTAMP"] = timestamp;
    connection->headers["OK-ACCESS-SIGN"] =
        UtilAlgorithm::base64Encode(Hmac::hmac(Hmac::ShaVersion::SHA256, credential.at(CCAPI_OKX_API_SECRET), timestamp + "GET/users/self/verify"));
  }

  void prepareConnect(std::shared_ptr<WsConnection> connection) override {
    if (connection->isOkxSbe()) {
      if (!this->shouldContinue.load()) return;
      this->setSbeHeaders(connection, UtilTime::now());
      if (connection->isSecure) {
        ErrorCode ec;
        this->serviceContextPtr->sslContextPtr->set_default_verify_paths(ec);
        if (ec) {
          this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "OKX SBE TLS trust store", connection->correlationIdList);
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
    if (!connection->isOkxSbe()) {
      MarketDataService::onOpen(connection);
      return;
    }
    if (!this->shouldContinue.load()) {
      ErrorCode ec;
      this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("stop"), ec);
      return;
    }
    Service::onOpen(connection);
    auto& timers = this->pingTimerByMethodByConnectionIdMap[connection->id];
    auto it = timers.find(PingPongMethod::WEBSOCKET_APPLICATION_LEVEL);
    if (it != timers.end()) {
      it->second->cancel();
      timers.erase(it);
    }
    if (this->shouldContinue.load()) {
      this->startSubscribe(connection);
    } else {
      ErrorCode ec;
      this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("stop"), ec);
    }
  }

  static std::string sbeErrorCode(const rj::Value& value) {
    if (!value.IsObject() || !value.HasMember("code")) return "";
    const auto& code = value["code"];
    auto text = code.IsString() ? std::string(code.GetString()) : code.IsInt64() ? std::to_string(code.GetInt64()) : "";
    return !text.empty() && text.find_first_not_of("0123456789") == std::string::npos ? ", code=" + text : "";
  }

  void onWsHandshakeError(std::shared_ptr<WsConnection> connection, const ErrorCode& ec) override {
    if (connection->isOkxSbe()) {
      auto status = connection->handshakeResponse.result_int();
      auto reason =
          status == 401 || status == 403 ? "authentication/access rejected (check key, signature, clock, IP whitelist and VIP tier)" : "handshake failed";
      this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE,
                    "OKX SBE: " + std::string(reason) + ", HTTP " + std::to_string(status) + ", " + ec.message(), connection->correlationIdList);
    }
    MarketDataService::onWsHandshakeError(connection, ec);
  }

  void clearStates(std::shared_ptr<WsConnection> connection) override {
    this->sbeBooks.erase(connection->id);
    MarketDataService::clearStates(connection);
  }

  virtual void restartSbe(std::shared_ptr<WsConnection> connection, const std::string& reason) {
    this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, reason, connection->correlationIdList);
    this->sbeBooks.erase(connection->id);
    this->shouldProcessRemainingMessageOnClosingByConnectionIdMap[connection->id] = false;
    ErrorCode ec;
    this->close(connection, beast::websocket::close_code::normal, beast::websocket::close_reason("OKX SBE resynchronization"), ec);
  }

  void onTextMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view text, const TimePoint& received) override {
    if (!connection->isOkxSbe()) {
      MarketDataService::onTextMessage(connection, text, received);
      return;
    }
    if (!this->shouldContinue.load() || connection->status != WsConnection::Status::OPEN) return;
    try {
      rj::Document document;
      try {
        document.Parse(text.data(), text.size());
      } catch (const std::exception&) {
        throw std::runtime_error("OKX SBE: malformed JSON control response");
      }
      if (!document.IsObject() || !document.HasMember("event") || !document["event"].IsString())
        throw std::runtime_error("OKX SBE: invalid control response or unexpected text market data");
      std::string kind = document["event"].GetString();
      if (kind == "error") throw std::runtime_error("OKX SBE: subscription rejected" + sbeErrorCode(document));
      if (kind == "notice") {
        if (sbeErrorCode(document) == ", code=64008") throw std::runtime_error("OKX SBE: service upgrade; reconnecting");
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::GENERIC_ERROR, "OKX SBE: service notice" + sbeErrorCode(document),
                      connection->correlationIdList);
        return;
      }
      if (kind != "subscribe" || !document.HasMember("arg") || !document["arg"].IsObject()) throw std::runtime_error("OKX SBE: unexpected control response");
      const auto& arg = document["arg"];
      if (!arg.HasMember("channel") || !arg["channel"].IsString() || !arg.HasMember("instIdCode") || !arg["instIdCode"].IsInt64())
        throw std::runtime_error("OKX SBE: invalid subscription acknowledgement");
      auto stream = std::string(arg["channel"].GetString()) + ":" + std::to_string(arg["instIdCode"].GetInt64());
      const auto& routes = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id);
      if (!routes.count(stream)) throw std::runtime_error("OKX SBE: acknowledgement for an unsubscribed stream");
      const auto& route = routes.at(stream);
      Message message;
      message.setTimeReceived(received);
      message.setType(Message::Type::SUBSCRIPTION_STARTED);
      message.setCorrelationIdList(
          this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(connection->id).at(route.at(CCAPI_CHANNEL_ID)).at(route.at(CCAPI_SYMBOL_ID)));
      Element element;
      element.insert(CCAPI_INFO_MESSAGE, "OKX SBE subscribed " + stream);
      message.setElementList({element});
      Event event;
      event.setType(Event::Type::SUBSCRIPTION_STATUS);
      event.setMessageList({message});
      this->eventHandler(event, nullptr);
    } catch (const std::exception& e) {
      this->restartSbe(connection, e.what());
    }
  }

  bool onBinaryMessage(std::shared_ptr<WsConnection> connection, boost::beast::string_view frame, const TimePoint& received) override {
    if (!connection->isOkxSbe()) return false;
    if (!this->shouldContinue.load() || connection->status != WsConnection::Status::OPEN) return true;
    try {
      auto data = OkxSbe::decode(std::string_view(frame.data(), frame.size()));
      data.timeReceived = received;
      if (data.templateId == 1006) throw std::runtime_error("OKX SBE: REST snapshot received on WebSocket");
      auto channel = data.templateId == 1000 ? "bbo-tbt" : data.templateId == 1005 ? "trades" : "books-l2-tbt";
      auto stream = std::string(channel) + ":" + std::to_string(data.instIdCode);
      if (!this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).count(stream))
        throw std::runtime_error("OKX SBE: event for an unsubscribed stream");
      if (data.templateId == 1000 || data.templateId == 1005) {
        this->emitSbe(connection, stream, data, false);
      } else {
        auto& slot = this->sbeBooks[connection->id][stream];
        if (!slot) slot = std::make_shared<SbeBook>();
        auto book = slot;
        if (book->sequence >= 0) {
          if (data.previousSequence != book->sequence) throw std::runtime_error("OKX SBE: order book sequence gap; resynchronizing");
          this->applySbeDiff(connection, stream, book, data);
        } else {
          if (data.sequence < data.previousSequence) throw std::runtime_error("OKX SBE: sequence reset during snapshot; resynchronizing");
          if (!book->buffer.empty() && data.previousSequence != book->buffer.back().sequence)
            throw std::runtime_error("OKX SBE: buffered order book sequence gap; resynchronizing");
          if (book->buffer.size() >= 10000) throw std::runtime_error("OKX SBE: snapshot diff buffer exceeded 10000 events");
          book->buffer.push_back(std::move(data));
          this->synchronizeSbe(connection, stream, book);
          if (!book->snapshot && !book->fetching && book->sequence < 0) this->fetchSbeSnapshot(connection, stream, book);
        }
      }
    } catch (const std::exception& e) {
      this->restartSbe(connection, e.what());
    }
    return true;
  }

  void emitSbe(std::shared_ptr<WsConnection> connection, const std::string& stream, const OkxSbe::Event& data, bool snapshot) {
    const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
    bool initial =
        snapshot || (data.templateId == 1000 &&
                     !this->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[connection->id][route.at(CCAPI_CHANNEL_ID)][route.at(CCAPI_SYMBOL_ID)]);
    MarketDataMessage message;
    message.type = data.templateId == 1005 ? MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE : MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
    message.exchangeSubscriptionId = stream;
    message.recapType = initial ? MarketDataMessage::RecapType::SOLICITED : MarketDataMessage::RecapType::NONE;
    message.tp = TimePoint(std::chrono::microseconds(data.eventTime));
    message.data = MarketDataMessage::ConvertOwningDataToData(data.data);
    if (data.templateId != 1005) {
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
        element.insert(CCAPI_INST_ID_CODE, std::to_string(data.instIdCode));
        element.insert(CCAPI_EVENT_TIME_MICROSECONDS, std::to_string(data.eventTime));
        element.insert(CCAPI_SEQUENCE_NUMBER, std::to_string(data.sequence));
        if (!snapshot) element.insert(CCAPI_OUT_TIME_MICROSECONDS, std::to_string(data.outTime));
        if (data.templateId == 1001) element.insert(CCAPI_PREVIOUS_SEQUENCE_NUMBER, std::to_string(data.previousSequence));
        if (data.templateId == 1005) {
          element.insert(CCAPI_TRADE_COUNT, std::to_string(data.tradeCount));
          element.insert(CCAPI_TRADE_SOURCE, std::to_string(data.tradeSource));
        }
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

  void applySbeDiff(std::shared_ptr<WsConnection> connection, const std::string& stream, const std::shared_ptr<SbeBook>& book, const OkxSbe::Event& data) {
    book->sequence = data.sequence;
    book->priceExponent = data.priceExponent;
    book->sizeExponent = data.sizeExponent;
    if (data.templateId != 1002) this->emitSbe(connection, stream, data, false);
  }

  void synchronizeSbe(std::shared_ptr<WsConnection> connection, const std::string& stream, const std::shared_ptr<SbeBook>& book) {
    if (!book->snapshot) return;
    auto sequence = book->snapshot->sequence;
    bool caughtUp = !book->buffer.empty() && book->buffer.back().sequence == sequence;
    while (!book->buffer.empty() && book->buffer.front().sequence <= sequence) book->buffer.pop_front();
    if (book->buffer.empty() && !caughtUp) return;
    if (!book->buffer.empty() && book->buffer.front().previousSequence > sequence) {
      book->snapshot.reset();
      this->retrySbeSnapshot(connection, stream, book, "OKX SBE: REST snapshot is older than buffered diffs");
      return;
    }
    book->sequence = sequence;
    book->priceExponent = book->snapshot->priceExponent;
    book->sizeExponent = book->snapshot->sizeExponent;
    this->emitSbe(connection, stream, *book->snapshot, true);
    for (const auto& diff : book->buffer) {
      if (!this->isCurrentSbeBook(connection, stream, book)) return;
      this->applySbeDiff(connection, stream, book, diff);
    }
    book->snapshot.reset();
    book->buffer.clear();
  }

  virtual void fetchSbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book) {
    if (!this->isCurrentSbeBook(connection, stream, book) || book->sequence >= 0 || book->snapshot) return;
    book->fetching = true;
    const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
    auto req = this->createSbeSnapshotRequest(route.at(CCAPI_SYMBOL_ID));
    this->sendRequest(
        req,
        [that = shared_from_base<MarketDataServiceOkx>(), connection, stream, book](const ErrorCode& ec) {
          that->retrySbeSnapshot(connection, stream, book, "OKX SBE: REST snapshot transport failure: " + ec.message());
        },
        [that = shared_from_base<MarketDataServiceOkx>(), connection, stream, book](const http::response<http::string_body>& response) {
          that->onSbeSnapshot(connection, stream, book, response);
        },
        this->sessionOptions.httpRequestTimeoutMilliseconds);
  }

  http::request<http::string_body> createSbeSnapshotRequest(const std::string& code) const {
    http::request<http::string_body> req(http::verb::get, "/api/v5/market/books-sbe?instIdCode=" + code + "&source=0", 11);
    req.set(http::field::host, this->hostRest);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    return req;
  }

  void onSbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book,
                     const http::response<http::string_body>& response) {
    if (!this->isCurrentSbeBook(connection, stream, book)) return;
    book->fetching = false;
    if (response.result_int() / 100 != 2 || response[http::field::content_type] != "application/sbe") {
      std::string code;
      try {
        rj::Document document;
        document.Parse(response.body().data(), response.body().size());
        code = sbeErrorCode(document);
      } catch (const std::exception&) {
      }
      this->retrySbeSnapshot(connection, stream, book, "OKX SBE: REST snapshot failure, HTTP " + std::to_string(response.result_int()) + code);
      return;
    }
    try {
      auto snapshot = OkxSbe::decode(response.body());
      const auto& route = this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(connection->id).at(stream);
      if (snapshot.templateId != 1006 || std::to_string(snapshot.instIdCode) != route.at(CCAPI_SYMBOL_ID))
        throw std::runtime_error("OKX SBE: unexpected REST snapshot template/instrument");
      snapshot.timeReceived = UtilTime::now();
      book->snapshot = std::move(snapshot);
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
    timer->async_wait([that = shared_from_base<MarketDataServiceOkx>(), connection, stream, book](const ErrorCode& ec) {
      if (!ec) that->fetchSbeSnapshot(connection, stream, book);
    });
    this->fetchMarketDepthInitialSnapshotTimerByConnectionIdExchangeSubscriptionIdMap[connection->id][stream] = timer;
  }

  std::map<std::string, std::map<std::string, std::shared_ptr<SbeBook>>> sbeBooks;

  std::string getInstrumentGroup(const Subscription& subscription) override {
    this->validateSubscription(subscription);
    if (subscription.isOkxSbe()) {
      return this->sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/ws/v5/public-sbe|" + subscription.getField() + "|" +
             subscription.getSerializedOptions() + "|" +
             UtilAlgorithm::computeHash(UtilAlgorithm::ShaVersion::SHA256, subscription.getSerializedCredential(), true) + "|" + subscription.getProxyUrl();
    }
    std::string baseUrlWsGivenSubscription(this->baseUrlWs);
    if (subscription.getField() == CCAPI_CANDLESTICK) {
      baseUrlWsGivenSubscription = this->sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + CCAPI_OKX_BUSINESS_WS_PATH;
    }
    return baseUrlWsGivenSubscription + "|" + subscription.getField() + "|" + subscription.getSerializedOptions() + "|" +
           subscription.getSerializedCredential() + "|" + subscription.getProxyUrl();
  }

  bool doesHttpBodyContainError(boost::beast::string_view bodyView) override {
    return !std::regex_search(bodyView.begin(), bodyView.end(), std::regex("\"code\":\\s*\"0\""));
  }

  void prepareSubscriptionDetail(std::string& channelId, std::string& symbolId, const std::string& field, std::shared_ptr<WsConnection> wsConnectionPtr,
                                 const Subscription& subscription, const std::map<std::string, std::string> optionMap) override {
    if (subscription.isOkxSbe()) {
      channelId = field == CCAPI_TRADE ? "trades" : mapGetWithDefault(optionMap, std::string(CCAPI_OKX_MARKET_DATA_STREAM), std::string("books-l2-tbt"));
      symbolId = std::to_string(std::stoll(optionMap.at(CCAPI_INST_ID_CODE)));
      this->l2UpdateIsReplaceByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channelId][symbolId] = channelId == "bbo-tbt";
      return;
    }
    auto marketDepthRequested = std::stoi(optionMap.at(CCAPI_MARKET_DEPTH_MAX));
    auto conflateIntervalMilliseconds = std::stoi(optionMap.at(CCAPI_CONFLATE_INTERVAL_MILLISECONDS));
    if (field == CCAPI_MARKET_DEPTH) {
      if (conflateIntervalMilliseconds < 100) {
        if (marketDepthRequested == 1) {
          channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH1_L2_TBT;
        } else if (marketDepthRequested <= 50) {
          channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH50_L2_TBT;
        } else {
          channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH400_L2_TBT;
        }
      } else {
        if (marketDepthRequested <= 5) {
          channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH5;
        } else {
          channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH400;
        }
      }
    } else if (field == CCAPI_CANDLESTICK) {
      std::string interval =
          this->convertCandlestickIntervalSecondsToInterval(std::stoi(optionMap.at(CCAPI_CANDLESTICK_INTERVAL_SECONDS)), "s", "m", "H", "D", "W");
      channelId = CCAPI_WEBSOCKET_OKX_CHANNEL_CANDLESTICK + interval;
    }
  }

  void pingOnApplicationLevel(std::shared_ptr<WsConnection> wsConnectionPtr, ErrorCode& ec) override {
    if (!wsConnectionPtr->isOkxSbe()) this->send(wsConnectionPtr, "ping", ec);
  }

  std::vector<std::string> createSendStringList(std::shared_ptr<WsConnection> wsConnectionPtr) override {
    std::vector<std::string> sendStringList;
    rj::Document document;
    document.SetObject();
    rj::Document::AllocatorType& allocator = document.GetAllocator();
    document.AddMember("op", rj::Value("subscribe").Move(), allocator);
    rj::Value args(rj::kArrayType);
    for (const auto& subscriptionListByChannelIdSymbolId : this->subscriptionListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id)) {
      auto channelId = subscriptionListByChannelIdSymbolId.first;
      for (const auto& subscriptionListBySymbolId : subscriptionListByChannelIdSymbolId.second) {
        std::string symbolId = subscriptionListBySymbolId.first;
        if (channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH1_L2_TBT || channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH5) {
          this->l2UpdateIsReplaceByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channelId][symbolId] = true;
        }
        std::string exchangeSubscriptionId = UtilString::split(channelId, "?").at(0) + ":" + symbolId;
        rj::Value arg(rj::kObjectType);
        arg.AddMember("channel", rj::Value(channelId.c_str(), allocator).Move(), allocator);
        if (wsConnectionPtr->isOkxSbe()) {
          arg.AddMember("instIdCode", rj::Value(static_cast<int64_t>(std::stoll(symbolId))).Move(), allocator);
        } else {
          arg.AddMember("instId", rj::Value(symbolId.c_str(), allocator).Move(), allocator);
        }
        args.PushBack(arg, allocator);
        this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id][exchangeSubscriptionId][CCAPI_CHANNEL_ID] = channelId;
        this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id][exchangeSubscriptionId][CCAPI_SYMBOL_ID] = symbolId;
      }
    }
    document.AddMember("args", args, allocator);
    rj::StringBuffer stringBuffer;
    rj::Writer<rj::StringBuffer> writer(stringBuffer);
    document.Accept(writer);
    std::string sendString = stringBuffer.GetString();
    sendStringList.push_back(sendString);
    return sendStringList;
  }

  std::string calculateOrderBookChecksum(const std::map<Decimal, std::string>& snapshotBid, const std::map<Decimal, std::string>& snapshotAsk) override {
    auto i = 0;
    auto i1 = snapshotBid.rbegin();
    auto i2 = snapshotAsk.begin();
    std::vector<std::string> csData;
    while (i < 25 && (i1 != snapshotBid.rend() || i2 != snapshotAsk.end())) {
      if (i1 != snapshotBid.rend()) {
        csData.push_back(ConvertDecimalToString(i1->first));
        csData.push_back(i1->second);
        ++i1;
      }
      if (i2 != snapshotAsk.end()) {
        csData.push_back(ConvertDecimalToString(i2->first));
        csData.push_back(i2->second);
        ++i2;
      }
      ++i;
    }
    std::string csStr = UtilString::join(csData, ":");
    uint_fast32_t csCalc = UtilAlgorithm::crc(csStr.begin(), csStr.end());
    return intToHex(csCalc);
  }

  void processTextMessage(std::shared_ptr<WsConnection> wsConnectionPtr, boost::beast::string_view textMessageView, const TimePoint& timeReceived, Event& event,
                          std::vector<MarketDataMessage>& marketDataMessageList) override {
    if (textMessageView != "pong") {
      this->jsonDocumentAllocator.Clear();
      rj::Document document(&this->jsonDocumentAllocator);
      document.Parse<rj::kParseNumbersAsStringsFlag>(textMessageView.data(), textMessageView.size());
      auto it = document.FindMember("event");
      std::string eventStr = it != document.MemberEnd() ? it->value.GetString() : "";
      if (eventStr == "login") {
        this->startSubscribe(wsConnectionPtr);

      } else {
        if (document.IsObject() && document.HasMember("arg")) {
          const rj::Value& arg = document["arg"];
          std::string channelId = arg["channel"].GetString();
          std::string symbolId = arg["instId"].GetString();
          std::string exchangeSubscriptionId = channelId + ":" + symbolId;
          if (!eventStr.empty()) {
            if (eventStr == "subscribe") {
              event.setType(Event::Type::SUBSCRIPTION_STATUS);
              std::vector<Message> messageList;
              Message message;
              message.setTimeReceived(timeReceived);
              std::vector<std::string> correlationIdList;
              if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.find(wsConnectionPtr->id) !=
                  this->correlationIdListByConnectionIdChannelIdSymbolIdMap.end()) {
                const rj::Value& arg = document["arg"];
                std::string channelId = arg["channel"].GetString();
                std::string symbolId = arg["instId"].GetString();
                if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id).find(channelId) !=
                    this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id).end()) {
                  if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id).at(channelId).find(symbolId) !=
                      this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id).at(channelId).end()) {
                    std::vector<std::string> correlationIdList_2 =
                        this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnectionPtr->id).at(channelId).at(symbolId);
                    correlationIdList.insert(correlationIdList.end(), correlationIdList_2.begin(), correlationIdList_2.end());
                  }
                }
              }
              message.setCorrelationIdList(correlationIdList);
              message.setType(Message::Type::SUBSCRIPTION_STARTED);
              Element element;
              element.insert(CCAPI_INFO_MESSAGE, textMessageView);
              message.setElementList({element});
              messageList.emplace_back(std::move(message));
              event.setMessageList(messageList);
            } else if (eventStr == "error") {
              event.setType(Event::Type::SUBSCRIPTION_STATUS);
              std::vector<Message> messageList;
              Message message;
              message.setTimeReceived(timeReceived);
              message.setType(Message::Type::SUBSCRIPTION_FAILURE);
              Element element;
              element.insert(CCAPI_ERROR_MESSAGE, textMessageView);
              message.setElementList({element});
              messageList.emplace_back(std::move(message));
              event.setMessageList(messageList);
            }
          } else {
            if (channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH1_L2_TBT || channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH5 ||
                channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH400 || channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH50_L2_TBT ||
                channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH400_L2_TBT) {
              std::string action = channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH1_L2_TBT || channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH5
                                       ? ""
                                       : document["action"].GetString();
              for (const auto& datum : document["data"].GetArray()) {
                if (this->sessionOptions.enableCheckOrderBookChecksum) {
                  auto it = datum.FindMember("checksum");
                  if (it != datum.MemberEnd()) {
                    this->orderBookChecksumByConnectionIdSymbolIdMap[wsConnectionPtr->id][symbolId] =
                        intToHex(static_cast<uint_fast32_t>(static_cast<uint32_t>(std::stoi(it->value.GetString()))));
                  }
                }
                MarketDataMessage marketDataMessage;
                marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(datum["ts"].GetString())));
                marketDataMessage.exchangeSubscriptionId = exchangeSubscriptionId;
                marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
                if (channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH1_L2_TBT || channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_PUBLIC_DEPTH5) {
                  if (this->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channelId][symbolId]) {
                    marketDataMessage.recapType = MarketDataMessage::RecapType::NONE;
                  } else {
                    marketDataMessage.recapType = MarketDataMessage::RecapType::SOLICITED;
                  }
                } else {
                  marketDataMessage.recapType = action == "update" ? MarketDataMessage::RecapType::NONE : MarketDataMessage::RecapType::SOLICITED;
                }
                for (const auto& x : datum["bids"].GetArray()) {
                  MarketDataMessage::TypeForDataPoint dataPoint;
                  dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalStringView(x[0].GetString()));
                  dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalStringView(x[1].GetString()));
                  marketDataMessage.data[MarketDataMessage::DataType::BID].emplace_back(std::move(dataPoint));
                }
                for (const auto& x : datum["asks"].GetArray()) {
                  MarketDataMessage::TypeForDataPoint dataPoint;
                  dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalStringView(x[0].GetString()));
                  dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalStringView(x[1].GetString()));
                  marketDataMessage.data[MarketDataMessage::DataType::ASK].emplace_back(std::move(dataPoint));
                }
                marketDataMessageList.emplace_back(std::move(marketDataMessage));
              }
            } else if (channelId == CCAPI_WEBSOCKET_OKX_CHANNEL_TRADE) {
              for (const auto& datum : document["data"].GetArray()) {
                MarketDataMessage marketDataMessage;
                marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE;
                marketDataMessage.recapType = MarketDataMessage::RecapType::NONE;
                marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(datum["ts"].GetString())));
                marketDataMessage.exchangeSubscriptionId = exchangeSubscriptionId;
                MarketDataMessage::TypeForDataPoint dataPoint;
                dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalStringView(datum["px"].GetString()));
                dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalStringView(datum["sz"].GetString()));
                dataPoint.emplace(MarketDataMessage::DataFieldType::TRADE_ID, datum["tradeId"].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::IS_BUYER_MAKER, std::string_view(datum["side"].GetString()) == "sell" ? "1" : "0");
                marketDataMessage.data[MarketDataMessage::DataType::TRADE].emplace_back(std::move(dataPoint));
                marketDataMessageList.emplace_back(std::move(marketDataMessage));
              }
            } else if (channelId.rfind(CCAPI_WEBSOCKET_OKX_CHANNEL_CANDLESTICK, 0) == 0) {
              for (const auto& datum : document["data"].GetArray()) {
                MarketDataMessage marketDataMessage;
                marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_CANDLESTICK;
                marketDataMessage.recapType = MarketDataMessage::RecapType::NONE;
                marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(datum[0].GetString())));
                marketDataMessage.exchangeSubscriptionId = exchangeSubscriptionId;
                MarketDataMessage::TypeForDataPoint dataPoint;
                dataPoint.emplace(MarketDataMessage::DataFieldType::OPEN_PRICE, datum[1].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::HIGH_PRICE, datum[2].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::LOW_PRICE, datum[3].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::CLOSE_PRICE, datum[4].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::VOLUME, datum[5].GetString());
                dataPoint.emplace(MarketDataMessage::DataFieldType::QUOTE_VOLUME, datum[7].GetString());
                marketDataMessage.data[MarketDataMessage::DataType::CANDLESTICK].emplace_back(std::move(dataPoint));
                marketDataMessageList.emplace_back(std::move(marketDataMessage));
              }
            }
          }
        }
      }
    }
  }

  void convertRequestForRest(http::request<http::string_body>& req, const Request& request, const TimePoint& now, const std::string& symbolId,
                             const std::map<std::string, std::string>& credential) override {
    switch (request.getOperation()) {
      case Request::Operation::GENERIC_PUBLIC_REQUEST: {
        MarketDataService::convertRequestForRestGenericPublicRequest(req, request, now, symbolId, credential);
      } break;
      case Request::Operation::GET_RECENT_TRADES: {
        req.method(http::verb::get);
        auto target = this->getRecentTradesTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_LIMIT, "limit"},
                          });
        this->appendSymbolId(queryString, symbolId, "instId");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_HISTORICAL_TRADES: {
        req.method(http::verb::get);
        auto target = this->getHistoricalTradesTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_LIMIT, "limit"},
                              {CCAPI_START_TRADE_ID, "before"},
                              {CCAPI_END_TRADE_ID, "after"},
                              {CCAPI_START_TIME_SECONDS, "before"},
                              {CCAPI_END_TIME_SECONDS, "after"},
                          },
                          {
                              {CCAPI_START_TIME_SECONDS, [that = shared_from_base<MarketDataServiceOkx>()](
                                                             const std::string& input) { return that->convertParamTimeSecondsToTimeMilliseconds(input); }},
                              {CCAPI_END_TIME_SECONDS, [that = shared_from_base<MarketDataServiceOkx>()](
                                                           const std::string& input) { return that->convertParamTimeSecondsToTimeMilliseconds(input); }},
                          });
        this->appendSymbolId(queryString, symbolId, "instId");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_RECENT_CANDLESTICKS:
      case Request::Operation::GET_HISTORICAL_CANDLESTICKS: {
        req.method(http::verb::get);
        auto target = this->getRecentCandlesticksTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_CANDLESTICK_INTERVAL_SECONDS, "bar"},
                              {CCAPI_LIMIT, "limit"},
                              {CCAPI_START_TIME_SECONDS, "before"},
                              {CCAPI_END_TIME_SECONDS, "after"},
                          },
                          {
                              {CCAPI_CANDLESTICK_INTERVAL_SECONDS,
                               [that = shared_from_base<MarketDataServiceOkx>()](const std::string& input) {
                                 return that->convertCandlestickIntervalSecondsToInterval(std::stoi(input), "s", "m", "H", "D", "W");
                               }},
                              {CCAPI_START_TIME_SECONDS, [that = shared_from_base<MarketDataServiceOkx>()](
                                                             const std::string& input) { return that->convertParamTimeSecondsToTimeMilliseconds(input); }},
                              {CCAPI_END_TIME_SECONDS, [that = shared_from_base<MarketDataServiceOkx>()](
                                                           const std::string& input) { return that->convertParamTimeSecondsToTimeMilliseconds(input); }},
                          });
        this->appendSymbolId(queryString, symbolId, "instId");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_MARKET_DEPTH: {
        req.method(http::verb::get);
        auto target = this->getMarketDepthTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_MARKET_DEPTH_MAX, "sz"},
                          });
        this->appendSymbolId(queryString, symbolId, "instId");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_SERVER_TIME: {
        req.method(http::verb::get);
        auto target = this->getServerTimeTarget;
        req.target(target);
      } break;
      case Request::Operation::GET_INSTRUMENT: {
        req.method(http::verb::get);
        auto target = this->getInstrumentTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_INSTRUMENT_TYPE, "instType"},
                          });
        this->appendSymbolId(queryString, symbolId, "instId");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_INSTRUMENTS: {
        req.method(http::verb::get);
        auto target = this->getInstrumentsTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_INSTRUMENT_TYPE, "instType"},
                          });
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_BBOS: {
        req.method(http::verb::get);
        auto target = this->getBbosTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_INSTRUMENT_TYPE, "instType"},
                          });
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_TICKERS: {
        req.method(http::verb::get);
        auto target = this->getBbosTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendParam(queryString, param,
                          {
                              {CCAPI_INSTRUMENT_TYPE, "instType"},
                          });
        req.target(target + "?" + queryString);
      } break;
      default:
        this->convertRequestForRestCustom(req, request, now, symbolId, credential);
    }
  }

  void extractInstrumentInfo(Element& element, const rj::Value& x) {
    element.insert(CCAPI_INSTRUMENT, x["instId"].GetString());
    std::string instFamily = x["instFamily"].GetString();
    bool instFamilyHasDash = instFamily.find('-') != std::string::npos;
    std::string baseCcy = x["baseCcy"].GetString();
    element.insert(CCAPI_BASE_ASSET, baseCcy.empty() ? (instFamilyHasDash ? UtilString::split(instFamily, '-').at(0) : "") : baseCcy);
    std::string quoteCcy = x["quoteCcy"].GetString();
    element.insert(CCAPI_QUOTE_ASSET, quoteCcy.empty() ? (instFamilyHasDash ? UtilString::split(instFamily, '-').at(1) : "") : quoteCcy);
    element.insert(CCAPI_ORDER_PRICE_INCREMENT, x["tickSz"].GetString());
    element.insert(CCAPI_ORDER_QUANTITY_INCREMENT, x["lotSz"].GetString());
    element.insert(CCAPI_ORDER_QUANTITY_MIN, x["minSz"].GetString());
    element.insert(CCAPI_SETTLE_ASSET, x["settleCcy"].GetString());
    element.insert(CCAPI_UNDERLYING_SYMBOL, x["uly"].GetString());
    element.insert(CCAPI_CONTRACT_SIZE, x["ctVal"].GetString());
    element.insert(CCAPI_CONTRACT_MULTIPLIER, x["ctMult"].GetString());
    element.insert(CCAPI_INSTRUMENT_STATUS, x["state"].GetString());
    auto itInstIdCode = x.FindMember("instIdCode");
    if (itInstIdCode != x.MemberEnd()) {
      const rj::Value& v = itInstIdCode->value;
      if (v.IsString()) {
        element.insert(CCAPI_INST_ID_CODE, v.GetString());
      } else if (v.IsInt64()) {
        element.insert(CCAPI_INST_ID_CODE, std::to_string(v.GetInt64()));
      } else if (v.IsUint64()) {
        element.insert(CCAPI_INST_ID_CODE, std::to_string(v.GetUint64()));
      }
    }
  }

  void convertTextMessageToMarketDataMessage(const Request& request, boost::beast::string_view textMessageView, const TimePoint& timeReceived, Event& event,
                                             std::vector<MarketDataMessage>& marketDataMessageList) override {
    this->jsonDocumentAllocator.Clear();
    rj::Document document(&this->jsonDocumentAllocator);
    document.Parse<rj::kParseNumbersAsStringsFlag>(textMessageView.data(), textMessageView.size());
    switch (request.getOperation()) {
      case Request::Operation::GET_RECENT_TRADES:
      case Request::Operation::GET_HISTORICAL_TRADES: {
        for (const auto& datum : document["data"].GetArray()) {
          MarketDataMessage marketDataMessage;
          marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE;
          marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(datum["ts"].GetString())));
          MarketDataMessage::TypeForDataPoint dataPoint;
          dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalStringView(datum["px"].GetString()));
          dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalStringView(datum["sz"].GetString()));
          dataPoint.emplace(MarketDataMessage::DataFieldType::TRADE_ID, datum["tradeId"].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::IS_BUYER_MAKER, std::string_view(datum["side"].GetString()) == "sell" ? "1" : "0");
          marketDataMessage.data[MarketDataMessage::DataType::TRADE].emplace_back(std::move(dataPoint));
          marketDataMessageList.emplace_back(std::move(marketDataMessage));
        }
      } break;
      case Request::Operation::GET_RECENT_CANDLESTICKS:
      case Request::Operation::GET_HISTORICAL_CANDLESTICKS: {
        for (const auto& x : document["data"].GetArray()) {
          MarketDataMessage marketDataMessage;
          marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_CANDLESTICK;
          marketDataMessage.tp = UtilTime::makeTimePointFromMilliseconds(std::stoll(x[0].GetString()));
          MarketDataMessage::TypeForDataPoint dataPoint;
          dataPoint.emplace(MarketDataMessage::DataFieldType::OPEN_PRICE, x[1].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::HIGH_PRICE, x[2].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::LOW_PRICE, x[3].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::CLOSE_PRICE, x[4].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::VOLUME, x[5].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::QUOTE_VOLUME, x[7].GetString());
          marketDataMessage.data[MarketDataMessage::DataType::CANDLESTICK].emplace_back(std::move(dataPoint));
          marketDataMessageList.emplace_back(std::move(marketDataMessage));
        }
      } break;
      case Request::Operation::GET_MARKET_DEPTH: {
        MarketDataMessage marketDataMessage;
        marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
        const rj::Value& data = document["data"][0];
        marketDataMessage.tp = UtilTime::makeTimePointFromMilliseconds(std::stoll(data["ts"].GetString()));
        for (const auto& x : data["bids"].GetArray()) {
          MarketDataMessage::TypeForDataPoint dataPoint;
          dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, x[0].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, x[1].GetString());
          marketDataMessage.data[MarketDataMessage::DataType::BID].emplace_back(std::move(dataPoint));
        }
        for (const auto& x : data["asks"].GetArray()) {
          MarketDataMessage::TypeForDataPoint dataPoint;
          dataPoint.emplace(MarketDataMessage::DataFieldType::PRICE, x[0].GetString());
          dataPoint.emplace(MarketDataMessage::DataFieldType::SIZE, x[1].GetString());
          marketDataMessage.data[MarketDataMessage::DataType::ASK].emplace_back(std::move(dataPoint));
        }
        marketDataMessageList.emplace_back(std::move(marketDataMessage));
      } break;
      case Request::Operation::GET_SERVER_TIME: {
        Message message;
        message.setTime(UtilTime::makeTimePointMilli(UtilTime::divideMilli(document["data"][0]["ts"].GetString())));
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      case Request::Operation::GET_INSTRUMENT: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        for (const auto& x : document["data"].GetArray()) {
          if (std::string_view(x["instId"].GetString()) == request.getInstrument()) {
            Element element;
            this->extractInstrumentInfo(element, x);
            message.setElementList({element});
            break;
          }
        }
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      case Request::Operation::GET_INSTRUMENTS: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        std::vector<Element> elementList;
        for (const auto& x : document["data"].GetArray()) {
          Element element;
          this->extractInstrumentInfo(element, x);
          elementList.push_back(element);
        }
        message.setElementList(elementList);
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      case Request::Operation::GET_BBOS: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        std::vector<Element> elementList;
        for (const auto& x : document["data"].GetArray()) {
          Element element;
          element.insert(CCAPI_INSTRUMENT, x["instId"].GetString());
          element.insert(CCAPI_BEST_BID_N_PRICE, x["bidPx"].GetString());
          element.insert(CCAPI_BEST_BID_N_SIZE, x["bidSz"].GetString());
          element.insert(CCAPI_BEST_ASK_N_PRICE, x["askPx"].GetString());
          element.insert(CCAPI_BEST_ASK_N_SIZE, x["askSz"].GetString());
          elementList.push_back(element);
        }
        message.setElementList(elementList);
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      case Request::Operation::GET_TICKERS: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        std::vector<Element> elementList;
        for (const auto& x : document["data"].GetArray()) {
          Element element;
          element.insert(CCAPI_INSTRUMENT, x["instId"].GetString());
          element.insert(CCAPI_BEST_BID_N_PRICE, x["bidPx"].GetString());
          element.insert(CCAPI_BEST_BID_N_SIZE, x["bidSz"].GetString());
          element.insert(CCAPI_BEST_ASK_N_PRICE, x["askPx"].GetString());
          element.insert(CCAPI_BEST_ASK_N_SIZE, x["askSz"].GetString());
          element.insert(CCAPI_LAST_PRICE, x["last"].GetString());
          element.insert(CCAPI_LAST_SIZE, x["lastSz"].GetString());
          element.insert(CCAPI_OPEN_24H_PRICE, x["open24h"].GetString());
          element.insert(CCAPI_HIGH_24H_PRICE, x["high24h"].GetString());
          element.insert(CCAPI_LOW_24H_PRICE, x["low24h"].GetString());
          element.insert(CCAPI_VOLUME_24H, x["vol24h"].GetString());
          elementList.push_back(element);
        }
        message.setElementList(elementList);
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      default:
        CCAPI_LOGGER_FATAL(CCAPI_UNSUPPORTED_VALUE);
    }
  }

  std::vector<std::string> createSendStringListFromSubscriptionList(std::shared_ptr<WsConnection> wsConnectionPtr,
                                                                    const std::vector<Subscription>& subscriptionList, const TimePoint& now,
                                                                    const std::map<std::string, std::string>& credential) override {
    std::vector<std::string> sendStringList;
    rj::Document document;
    document.SetObject();
    auto& allocator = document.GetAllocator();
    document.AddMember("op", rj::Value("login").Move(), allocator);
    rj::Value arg(rj::kObjectType);
    auto apiKey = mapGetWithDefault(credential, this->apiKeyName);
    auto apiSecret = mapGetWithDefault(credential, this->apiSecretName);
    auto apiPassphrase = mapGetWithDefault(credential, this->apiPassphraseName);
    std::string ts = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
    arg.AddMember("apiKey", rj::Value(apiKey.c_str(), allocator).Move(), allocator);
    arg.AddMember("passphrase", rj::Value(apiPassphrase.c_str(), allocator).Move(), allocator);
    arg.AddMember("timestamp", rj::Value(ts.c_str(), allocator).Move(), allocator);
    std::string signData = ts + "GET" + "/users/self/verify";
    std::string sign = UtilAlgorithm::base64Encode(Hmac::hmac(Hmac::ShaVersion::SHA256, apiSecret, signData));
    arg.AddMember("sign", rj::Value(sign.c_str(), allocator).Move(), allocator);
    rj::Value args(rj::kArrayType);
    args.PushBack(arg, allocator);
    document.AddMember("args", args, allocator);
    rj::StringBuffer stringBuffer;
    rj::Writer<rj::StringBuffer> writer(stringBuffer);
    document.Accept(writer);
    std::string sendString = stringBuffer.GetString();
    sendStringList.push_back(sendString);
    return sendStringList;
  }
};

} /* namespace ccapi */
#endif
#endif
