#include "ccapi_cpp/service/ccapi_market_data_service_binance.h"
#include "gtest/gtest.h"

namespace ccapi {
namespace {
using D = MarketDataMessage::DataType;
using F = MarketDataMessage::DataFieldType;
constexpr int64_t eventUs = 1720000000123456;
const std::string tradeFrame = UtilAlgorithm::hexToString(
    "1200102701000000406205b5541c0600c56105b5541c0600f8f81900020000000100000000002000010000000000200015cd5b070000000001"
    "020000000000200015cd5b07000000000200000000000000000742544355534454");
const std::string bboFrame = UtilAlgorithm::hexToString(
    "3200112701000000406205b5541c06006400000000000000f8f8010000000000200015cd5b0700000000020000000000200002000000000000000742544355534454");
const std::string depthFrame = UtilAlgorithm::hexToString(
    "1a00132701000000406205b5541c060065000000000000006600000000000000f8f810000100010000000000200000000000000000001000010002000000000020000200000000000000074254"
    "4355534454");

void put(std::string& frame, size_t offset, uint64_t value, size_t size = 8) {
  for (size_t i = 0; i < size; ++i) frame.at(offset + i) = static_cast<char>(value >> (i * 8));
}

std::string diff(int64_t first, int64_t last, int64_t quantity = 0) {
  auto frame = depthFrame;
  put(frame, 16, first);
  put(frame, 24, last);
  put(frame, 46, quantity);
  return frame;
}

TEST(BinanceSbeDecoder, tradesUseUint32GroupCountAndPreserveAllTrades) {
  auto event = BinanceSbe::decode(tradeFrame);
  EXPECT_EQ(event.templateId, 10000);
  EXPECT_EQ(event.symbol, "BTCUSDT");
  EXPECT_EQ(event.eventTime, eventUs);
  EXPECT_EQ(event.transactTime, eventUs - 123);
  const auto& trades = event.data.at(D::TRADE);
  ASSERT_EQ(trades.size(), 2);
  EXPECT_EQ(trades[0].at(F::PRICE), "90071992.54740993");
  EXPECT_EQ(trades[0].at(F::SIZE), "1.23456789");
  EXPECT_EQ(trades[0].at(F::TRADE_ID), "9007199254740993");
  EXPECT_EQ(trades[0].at(F::IS_BUYER_MAKER), "1");
  EXPECT_EQ(trades[1].at(F::TRADE_ID), "9007199254740994");
  EXPECT_EQ(trades[1].at(F::IS_BUYER_MAKER), "0");
}

TEST(BinanceSbeDecoder, bestBidAskAndDiffDepthHaveDifferentLayouts) {
  auto ticker = BinanceSbe::decode(bboFrame);
  EXPECT_EQ(ticker.lastUpdateId, 100);
  EXPECT_EQ(ticker.data.at(D::BID)[0].at(F::PRICE), "90071992.54740993");
  EXPECT_EQ(ticker.data.at(D::ASK)[0].at(F::PRICE), "90071992.54740994");
  auto depth = BinanceSbe::decode(depthFrame);
  EXPECT_EQ(depth.firstUpdateId, 101);
  EXPECT_EQ(depth.lastUpdateId, 102);
  EXPECT_EQ(depth.eventTime, eventUs);
  EXPECT_EQ(depth.data.at(D::BID)[0].at(F::SIZE), "0");
  EXPECT_EQ(depth.data.at(D::ASK)[0].at(F::SIZE), "0.00000002");
}

TEST(BinanceSbeDecoder, decimalConversionIsExactForEveryExponent) {
  EXPECT_EQ(BinanceSbe::decimal(std::numeric_limits<int64_t>::min(), 0), "-9223372036854775808");
  EXPECT_EQ(BinanceSbe::decimal(std::numeric_limits<int64_t>::max(), -8), "92233720368.54775807");
  EXPECT_EQ(BinanceSbe::decimal(12300, -4), "1.23");
  EXPECT_EQ(BinanceSbe::decimal(123, 2), "12300");
  EXPECT_EQ(BinanceSbe::decimal(-1, -8), "-0.00000001");
  for (int exponent = -127; exponent <= 127; ++exponent) {
    EXPECT_EQ(BinanceSbe::decimal(0, exponent), "0");
    EXPECT_EQ(BinanceSbe::decimal(1, exponent), exponent < 0 ? "0." + std::string(-exponent - 1, '0') + "1" : "1" + std::string(exponent, '0'));
  }
}

TEST(BinanceSbeDecoder, rejectsEveryTruncatedPrefixAndTrailingBytes) {
  for (const auto& frame : {tradeFrame, bboFrame, depthFrame}) {
    for (size_t size = 0; size < frame.size(); ++size) {
      EXPECT_THROW(BinanceSbe::decode(std::string_view(frame.data(), size)), std::runtime_error) << size;
    }
    EXPECT_THROW(BinanceSbe::decode(frame + '\0'), std::runtime_error);
  }
}

TEST(BinanceSbeDecoder, rejectsUnsupportedSchemaTemplateLengthsAndEnum) {
  for (auto offset : {0, 2, 4, 6, 26}) {
    auto frame = tradeFrame;
    put(frame, offset, 999, 2);
    EXPECT_THROW(BinanceSbe::decode(frame), std::runtime_error);
  }
  auto snapshot = depthFrame;
  put(snapshot, 2, 10002, 2);
  EXPECT_THROW(BinanceSbe::decode(snapshot), std::runtime_error);
  auto frame = tradeFrame;
  put(frame, 28, 0xffffffff, 4);
  EXPECT_THROW(BinanceSbe::decode(frame), std::runtime_error);
  frame = tradeFrame;
  frame[56] = 2;
  EXPECT_THROW(BinanceSbe::decode(frame), std::runtime_error);
  frame = tradeFrame;
  frame[24] = '\x80';
  EXPECT_THROW(BinanceSbe::decode(frame), std::runtime_error);
  frame = tradeFrame;
  put(frame, 8, std::numeric_limits<uint64_t>::max());
  EXPECT_THROW(BinanceSbe::decode(frame), std::runtime_error);
  EXPECT_THROW(BinanceSbe::decode(diff(103, 102)), std::runtime_error);
}

TEST(BinanceSbeDecoder, acceptsUtf8SymbolsAndEmptyDepthGroups) {
  auto frame = depthFrame.substr(0, 34);
  frame += UtilAlgorithm::hexToString("1000000010000000");
  std::string symbol = u8"币USDT";
  frame += static_cast<char>(symbol.size());
  frame += symbol;
  auto event = BinanceSbe::decode(frame);
  EXPECT_EQ(event.symbol, symbol);
  EXPECT_TRUE(event.data.at(D::BID).empty());
  EXPECT_TRUE(event.data.at(D::ASK).empty());
}

class TestService : public MarketDataServiceBinance {
 public:
  using MarketDataServiceBinance::MarketDataServiceBinance;

  void connect(std::shared_ptr<WsConnection>) override { ++connections; }

  void fetchSbeSnapshot(std::shared_ptr<WsConnection> connection, const std::string& stream, std::shared_ptr<SbeBook> book) override {
    if (isCurrentSbeBook(connection, stream, book)) {
      book->fetching = true;
      ++fetches;
    }
  }

  void restartSbe(std::shared_ptr<WsConnection> connection, const std::string& reason) override {
    restartReason = reason;
    onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, reason, connection->correlationIdList);
    connection->status = WsConnection::Status::CLOSING;
    clearStates(connection);
  }

  int fetches{};
  int connections{};
  std::string restartReason;
};

class BinanceSbeServiceTest : public ::testing::Test {
 public:
  void SetUp() override {
    service = std::make_shared<TestService>([this](Event& event, Queue<Event>*) { events.push_back(event); }, SessionOptions{}, SessionConfigs{}, &context);
    service->sbeApiKeyDefault = "test-ed25519-api-key";
  }

  std::shared_ptr<WsConnection> connection(const std::string& field = CCAPI_MARKET_DEPTH,
                                           const std::string& options = "BINANCE_MARKET_DATA_FORMAT=SBE&MARKET_DEPTH_MAX=20") {
    Subscription subscription("binance", "BTCUSDT", field, options, "correlation");
    auto group = service->getInstrumentGroup(subscription);
    auto ws =
        std::make_shared<WsConnection>(UtilString::split(group, "|")[0], group, std::vector<Subscription>{subscription}, std::map<std::string, std::string>{});
    ws->status = WsConnection::Status::OPEN;
    service->prepareSubscription(ws, subscription);
    service->createSendStringList(ws);
    return ws;
  }

  void snapshot(std::shared_ptr<WsConnection> ws, int64_t version,
                const std::string& levels = R"("bids":[["90071992.54740993","2"],["90071992.54740992","3"]],"asks":[["90071992.54740994","4"]])") {
    http::response<http::string_body> response(http::status::ok, 11);
    response.body() = "{\"lastUpdateId\":" + std::to_string(version) + "," + levels + "}";
    service->onSbeSnapshot(ws, "btcusdt@depth", service->sbeBooks.at(ws->id).at("btcusdt@depth"), response);
  }

  void receive(std::shared_ptr<WsConnection> ws, const std::string& frame) { EXPECT_TRUE(service->onBinaryMessage(ws, frame, now)); }

  ServiceContext context;
  std::shared_ptr<TestService> service;
  std::vector<Event> events;
  TimePoint now{std::chrono::microseconds(eventUs + 100)};
};

TEST_F(BinanceSbeServiceTest, selectsIncrementalDepthRegardlessOfLocalDepthAndConflation) {
  for (auto levels : {1, 5, 20, 21, 5000}) {
    auto ws = connection(CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE&MARKET_DEPTH_MAX=" + std::to_string(levels));
    auto request = service->createSendStringList(ws).front();
    rj::Document document;
    document.Parse(request.c_str());
    EXPECT_STREQ(document["method"].GetString(), "SUBSCRIBE");
    ASSERT_EQ(document["params"].Size(), 1);
    EXPECT_STREQ(document["params"][0].GetString(), "btcusdt@depth");
    EXPECT_EQ(ws->url, "wss://stream-sbe.binance.com:9443/ws");
  }
  auto ws = connection(CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE&MARKET_DEPTH_MAX=20&CONFLATE_INTERVAL_MILLISECONDS=100");
  EXPECT_NE(service->createSendStringList(ws).front().find("btcusdt@depth\""), std::string::npos);
}

TEST_F(BinanceSbeServiceTest, bestBidAskAndTradeUseOfficialChannelNames) {
  auto bbo = connection(CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=bestBidAsk");
  EXPECT_NE(service->createSendStringList(bbo).front().find("btcusdt@bestBidAsk"), std::string::npos);
  auto trade = connection(CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE");
  EXPECT_NE(service->createSendStringList(trade).front().find("btcusdt@trade"), std::string::npos);
  EXPECT_NE(bbo->group, trade->group);
}

TEST_F(BinanceSbeServiceTest, jsonRemainsDefaultWithSeparateConnectionsAndUnchangedChannels) {
  auto json = connection(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=20");
  auto sbe = connection();
  EXPECT_FALSE(json->isBinanceSpotSbe());
  EXPECT_NE(json->group, sbe->group);
  EXPECT_NE(service->createSendStringList(json).front().find("btcusdt@depth20@100ms"), std::string::npos);
  auto ticker = connection(CCAPI_MARKET_DEPTH, "");
  EXPECT_NE(service->createSendStringList(ticker).front().find("btcusdt@bookTicker"), std::string::npos);
  service->prepareConnect(json);
  EXPECT_TRUE(json->headers.empty());
  EXPECT_FALSE(service->onBinaryMessage(json, depthFrame, now));
  auto trade = connection(CCAPI_TRADE, "");
  service->onTextMessage(trade, R"({"stream":"btcusdt@trade","data":{"T":1720000000123,"p":"1.23000000","q":"2.00","t":91,"m":true}})", now);
  ASSERT_EQ(events.size(), 1);
  auto message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::milliseconds(1720000000123)));
  EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"correlation"});
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_LAST_PRICE), "1.23");
}

TEST_F(BinanceSbeServiceTest, rejectsMissingKeyUnknownFormatAndSnapshotStream) {
  for (const auto& options : {"BINANCE_MARKET_DATA_FORMAT=garbage", "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=depth20",
                              "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=bestBidAsk&MARKET_DEPTH_MAX=20", "BINANCE_MARKET_DATA_STREAM=depth",
                              "BINANCE_MARKET_DATA_FORMAT=SBE&MARKET_DEPTH_MAX=0"}) {
    EXPECT_THROW(service->getInstrumentGroup(Subscription("binance", "BTCUSDT", CCAPI_MARKET_DEPTH, options)), std::invalid_argument);
  }
  EXPECT_THROW(service->getInstrumentGroup(Subscription("binance", "BTCUSDT", CCAPI_AGG_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE")), std::invalid_argument);
  service->sbeApiKeyDefault.clear();
  std::vector<Subscription> subscriptions{Subscription("binance", "BTCUSDT", CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE", "missing-key")};
  service->subscribe(subscriptions);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
  EXPECT_EQ(events[0].getMessageList()[0].getCorrelationIdList()[0], "missing-key");
}

TEST_F(BinanceSbeServiceTest, authenticatesOnlySbeAndRedactsKeyFromLogsAndGrouping) {
  std::string secret = "do-not-log-this-key";
  Subscription subscription("binance", "BTCUSDT", CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE", "auth", {{CCAPI_BINANCE_SBE_API_KEY, secret}});
  auto group = service->getInstrumentGroup(subscription);
  auto ws = std::make_shared<WsConnection>(UtilString::split(group, "|")[0], group, std::vector<Subscription>{subscription}, subscription.getCredential());
  service->setWsConnectionStream(ws);
  service->prepareConnect(ws);
  EXPECT_EQ(ws->headers.at("X-MBX-APIKEY"), secret);
  EXPECT_EQ(ws->headers.size(), 1);
  for (auto text : {group, ws->toString(), ws->longId, subscription.toString()}) EXPECT_EQ(text.find(secret.substr(0, 4)), std::string::npos);
  EXPECT_EQ(service->credentialDefault.count(CCAPI_BINANCE_SBE_API_KEY), 0);
  auto stream = std::get<std::shared_ptr<beast::websocket::stream<beast::ssl_stream<beast::tcp_stream>>>>(ws->streamPtr);
  EXPECT_EQ(SSL_get_verify_mode(stream->next_layer().native_handle()), SSL_VERIFY_PEER);
}

TEST_F(BinanceSbeServiceTest, normalizesTradesWithoutLosingMicrosecondsIdsOrAggressor) {
  auto ws = connection(CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE");
  receive(ws, tradeFrame);
  ASSERT_EQ(events.size(), 1);
  const auto& message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getType(), Message::Type::MARKET_DATA_EVENTS_TRADE);
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::microseconds(eventUs - 123)));
  EXPECT_EQ(message.getTimeReceived(), now);
  EXPECT_EQ(message.getCorrelationIdList()[0], "correlation");
  ASSERT_EQ(message.getElementList().size(), 2);
  const auto& first = message.getElementList()[0];
  EXPECT_EQ(first.getValue(CCAPI_LAST_PRICE), "90071992.54740993");
  EXPECT_EQ(first.getValue(CCAPI_TRADE_ID), "9007199254740993");
  EXPECT_EQ(first.getValue(CCAPI_IS_BUYER_MAKER), "1");
  EXPECT_EQ(first.getValue(CCAPI_EVENT_TIME_MICROSECONDS), std::to_string(eventUs));
}

TEST_F(BinanceSbeServiceTest, tickerReplacesBothSidesWithoutRequiringContiguousIds) {
  auto ws = connection(CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE&BINANCE_MARKET_DATA_STREAM=bestBidAsk");
  receive(ws, bboFrame);
  auto next = bboFrame;
  put(next, 16, 200);
  put(next, 26, 8007199254740993);
  receive(ws, next);
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["bestBidAsk"]["BTCUSDT"].size(), 1);
  const auto& message = events.back().getMessageList()[0];
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::microseconds(eventUs)));
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_BEST_BID_N_PRICE), "80071992.54740993");
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_SEQUENCE_NUMBER), "200");
  EXPECT_TRUE(service->restartReason.empty());
}

TEST_F(BinanceSbeServiceTest, buffersDiffsDiscardsOldOnesAndBridgesSnapshot) {
  auto ws = connection();
  receive(ws, diff(90, 99));
  receive(ws, diff(100, 101));
  receive(ws, diff(101, 102));
  EXPECT_TRUE(events.empty());
  EXPECT_EQ(service->fetches, 1);
  snapshot(ws, 100);
  auto book = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  EXPECT_EQ(book->version, 102);
  EXPECT_TRUE(book->buffer.empty());
  const auto& bids = service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["depth"]["BTCUSDT"];
  EXPECT_EQ(bids.count(Decimal("90071992.54740993")), 0);
  EXPECT_EQ(bids.at(Decimal("90071992.54740992")), "3");
  ASSERT_GE(events.size(), 2);
  EXPECT_EQ(events[0].getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
  EXPECT_EQ(events[1].getMessageList()[0].getTimeReceived(), now);
  EXPECT_EQ(events[1].getMessageList()[0].getTime(), TimePoint(std::chrono::microseconds(eventUs)));
  EXPECT_EQ(events[1].getMessageList()[0].getElementList()[0].getValue(CCAPI_FIRST_SEQUENCE_NUMBER), "100");
  EXPECT_EQ(events[1].getMessageList()[0].getCorrelationIdList()[0], "correlation");
  auto count = events.size();
  receive(ws, diff(90, 102));
  EXPECT_EQ(events.size(), count);
  receive(ws, diff(103, 104, 10));
  EXPECT_EQ(book->version, 104);
}

TEST_F(BinanceSbeServiceTest, snapshotAheadWaitsForTheNextDiffAndAcceptsUEqualLastPlusOne) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 110);
  EXPECT_TRUE(events.empty());
  receive(ws, diff(103, 109));
  EXPECT_TRUE(events.empty());
  receive(ws, diff(111, 112));
  EXPECT_EQ(service->sbeBooks.at(ws->id).at("btcusdt@depth")->version, 112);
  EXPECT_EQ(service->fetches, 1);
}

TEST_F(BinanceSbeServiceTest, staleSnapshotSchedulesRetryAndContinuesBuffering) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 50);
  auto book = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  EXPECT_FALSE(book->snapshot);
  EXPECT_TRUE(book->fetching);
  EXPECT_EQ(book->version, -1);
  EXPECT_EQ(events.back().getType(), Event::Type::SUBSCRIPTION_STATUS);
  EXPECT_TRUE(service->fetchMarketDepthInitialSnapshotTimerByConnectionIdExchangeSubscriptionIdMap[ws->id].count("btcusdt@depth"));
  receive(ws, diff(103, 104));
  EXPECT_EQ(book->buffer.size(), 2);
  snapshot(ws, 101);
  EXPECT_EQ(book->version, 104);
  service->clearStates(ws);
}

TEST_F(BinanceSbeServiceTest, gapDuringBufferingPublishesNoBookAndReconnects) {
  auto ws = connection();
  receive(ws, depthFrame);
  receive(ws, diff(104, 105));
  snapshot(ws, 100);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getType(), Event::Type::SUBSCRIPTION_STATUS);
  EXPECT_NE(service->restartReason.find("gap"), std::string::npos);
  EXPECT_EQ(service->sbeBooks.count(ws->id), 0);
}

TEST_F(BinanceSbeServiceTest, liveGapAndReconnectInvalidateOldCallbacksAndRequireNewSnapshot) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto oldBook = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  snapshot(ws, 100);
  receive(ws, diff(104, 105));
  EXPECT_NE(service->restartReason.find("gap"), std::string::npos);
  auto fresh = connection();
  receive(fresh, diff(200, 201));
  auto count = events.size();
  http::response<http::string_body> response(http::status::ok, 11);
  response.body() = R"({"lastUpdateId":999,"bids":[],"asks":[]})";
  service->onSbeSnapshot(ws, "btcusdt@depth", oldBook, response);
  EXPECT_EQ(events.size(), count);
  snapshot(fresh, 200);
  EXPECT_EQ(service->sbeBooks.at(fresh->id).at("btcusdt@depth")->version, 201);
  EXPECT_EQ(service->fetches, 2);
}

TEST_F(BinanceSbeServiceTest, restFailureAndMalformedLevelsAreExplicitAndRetryable) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto book = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  for (const auto& body : {"not-json", R"({"lastUpdateId":100,"bids":[[null,1]],"asks":[]})", R"({"lastUpdateId":100,"bids":[["1x","2"]],"asks":[]})"}) {
    http::response<http::string_body> response(http::status::ok, 11);
    response.body() = body;
    service->onSbeSnapshot(ws, "btcusdt@depth", book, response);
    EXPECT_FALSE(book->snapshot);
    EXPECT_EQ(book->version, -1);
  }
  http::response<http::string_body> failed(http::status::too_many_requests, 11);
  service->onSbeSnapshot(ws, "btcusdt@depth", book, failed);
  ASSERT_EQ(events.size(), 4);
  EXPECT_NE(events.back().getMessageList()[0].getElementList()[0].getValue(CCAPI_ERROR_MESSAGE).find("429"), std::string::npos);
  service->clearStates(ws);
}

TEST_F(BinanceSbeServiceTest, controlAcksStayJsonAndAreCorrelated) {
  auto ws = connection();
  service->onTextMessage(ws, R"({"result":null,"id":0})", now);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_STARTED);
  EXPECT_EQ(events[0].getMessageList()[0].getCorrelationIdList()[0], "correlation");
}

TEST_F(BinanceSbeServiceTest, singleLevelIncrementalPromotesDeeperLevelsAndReportsUpdates) {
  auto ws = connection(CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE&MARKET_DEPTH_MAX=1&MARKET_DEPTH_RETURN_UPDATE=1");
  receive(ws, diff(101, 102, 100));
  snapshot(ws, 100);
  receive(ws, diff(103, 103));
  const auto& bids = service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["depth"]["BTCUSDT"];
  EXPECT_EQ(bids.size(), 1);
  EXPECT_EQ(bids.at(Decimal("90071992.54740992")), "3");
  const auto& message = events.back().getMessageList()[0];
  EXPECT_EQ(message.getRecapType(), Message::RecapType::NONE);
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_SEQUENCE_NUMBER), "103");
}

TEST_F(BinanceSbeServiceTest, emptySnapshotAndEmptyDiffStillSynchronize) {
  auto frame = depthFrame.substr(0, 34) + UtilAlgorithm::hexToString("10000000100000000742544355534454");
  auto ws = connection();
  receive(ws, frame);
  snapshot(ws, 100, R"("bids":[],"asks":[])");
  EXPECT_EQ(service->sbeBooks.at(ws->id).at("btcusdt@depth")->version, 102);
  ASSERT_FALSE(events.empty());
  EXPECT_EQ(events[0].getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
}

TEST_F(BinanceSbeServiceTest, bufferLimitTriggersResyncAndStopInvalidatesPendingSnapshot) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto book = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  book->buffer.resize(10000);
  receive(ws, diff(103, 104));
  EXPECT_NE(service->restartReason.find("10000"), std::string::npos);
  auto fresh = connection();
  receive(fresh, depthFrame);
  auto count = events.size();
  service->shouldContinue = false;
  snapshot(fresh, 100);
  service->prepareConnect(fresh);
  EXPECT_EQ(service->connections, 0);
  EXPECT_EQ(events.size(), count);
  EXPECT_EQ(service->sbeBooks.at(fresh->id).at("btcusdt@depth")->version, -1);
}

TEST_F(BinanceSbeServiceTest, reusedConnectionCannotAcceptSnapshotFromItsPreviousGeneration) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto oldBook = service->sbeBooks.at(ws->id).at("btcusdt@depth");
  service->clearStates(ws);
  service->prepareSubscription(ws, ws->subscriptionList.front());
  service->createSendStringList(ws);
  receive(ws, diff(201, 202));
  http::response<http::string_body> response(http::status::ok, 11);
  response.body() = R"({"lastUpdateId":200,"bids":[],"asks":[]})";
  service->onSbeSnapshot(ws, "btcusdt@depth", oldBook, response);
  EXPECT_TRUE(events.empty());
  EXPECT_EQ(service->sbeBooks.at(ws->id).at("btcusdt@depth")->version, -1);
  snapshot(ws, 200);
  EXPECT_EQ(service->sbeBooks.at(ws->id).at("btcusdt@depth")->version, 202);
}

TEST(BinanceSbeSubscription, marketDataOptionsCannotEnableSbeForPrivateOrOtherExchangeSubscriptions) {
  for (auto field : {CCAPI_EM_ORDER_UPDATE, CCAPI_EM_PRIVATE_TRADE, CCAPI_EM_BALANCE_UPDATE, CCAPI_EM_WEBSOCKET_ORDER_ENTRY}) {
    Subscription subscription("binance", "BTCUSDT", field, "BINANCE_MARKET_DATA_FORMAT=SBE");
    EXPECT_EQ(subscription.getServiceName(), CCAPI_EXECUTION_MANAGEMENT);
    EXPECT_FALSE(subscription.isBinanceSpotSbe());
  }
  for (auto exchange : {"okx", "binance-usds-futures", "binance-coin-futures", "binance-us"}) {
    EXPECT_FALSE(Subscription(exchange, "BTCUSDT", CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_FORMAT=SBE").isBinanceSpotSbe());
  }
}

TEST_F(BinanceSbeServiceTest, rejectsInvalidFramesAndControlErrorsWithoutFallback) {
  for (const auto& text : {R"({"code":-2015,"msg":"Invalid API-key","id":0})", "bad-json", R"({"stream":"btcusdt@depth20","data":{}})",
                           R"({"e":"serverShutdown","E":1720000000123456})"}) {
    auto ws = connection();
    service->onTextMessage(ws, text, now);
    EXPECT_EQ(ws->status, WsConnection::Status::CLOSING);
    EXPECT_EQ(events.back().getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
    EXPECT_EQ(events.back().getMessageList()[0].getCorrelationIdList()[0], "correlation");
  }
  for (auto frame : {depthFrame.substr(0, 20), tradeFrame}) {
    auto ws = connection();
    receive(ws, frame);
    EXPECT_EQ(ws->status, WsConnection::Status::CLOSING);
  }
}

TEST_F(BinanceSbeServiceTest, snapshotRequestIsPublicJsonAndUses5000Levels) {
  http::request<http::string_body> request;
  service->createFetchOrderBookInitialReq(request, "BTCUSDT", now, {});
  EXPECT_EQ(request.target(), "/api/v3/depth?symbol=BTCUSDT&limit=5000");
  EXPECT_EQ(request.method(), http::verb::get);
  EXPECT_EQ(request.find("X-MBX-APIKEY"), request.end());
  EXPECT_EQ(request.find("X-MBX-SBE"), request.end());
}

class LocalSbeServer {
 public:
  using Stream = beast::websocket::stream<beast::tcp_stream>;

  explicit LocalSbeServer(net::io_context& io) : io(io), acceptor(io, {net::ip::make_address("127.0.0.1"), 0}) {}

  void accept() {
    peer = std::make_shared<Stream>(io);
    acceptor.async_accept(beast::get_lowest_layer(*peer).socket(), [this, socket = peer](const ErrorCode& ec) {
      ASSERT_FALSE(ec) << ec.message();
      auto request = std::make_shared<http::request<http::string_body>>();
      auto buffer = std::make_shared<beast::flat_buffer>();
      http::async_read(socket->next_layer(), *buffer, *request, [this, socket, request, buffer](const ErrorCode& ec, size_t) {
        ASSERT_FALSE(ec) << ec.message();
        keys.push_back(std::string((*request)["X-MBX-APIKEY"]));
        socket->async_accept(*request, [this, socket, request, buffer](const ErrorCode& ec) {
          ASSERT_FALSE(ec) << ec.message();
          socket->async_read(*buffer, [this, socket, buffer](const ErrorCode& ec, size_t) {
            ASSERT_FALSE(ec) << ec.message();
            EXPECT_TRUE(socket->got_text());
            subscriptions.push_back(beast::buffers_to_string(buffer->data()));
            buffer->consume(buffer->size());
            socket->text(true);
            auto control = std::make_shared<std::string>(keys.size() == 1 ? R"({"e":"serverShutdown","E":1720000000123456})" : R"({"result":null,"id":0})");
            socket->async_write(net::buffer(*control), [this, socket, buffer, control](const ErrorCode& ec, size_t) {
              ASSERT_FALSE(ec) << ec.message();
              if (keys.size() == 1) {
                readClose(socket, buffer, true);
              } else {
                socket->control_callback([this](beast::websocket::frame_type kind, beast::string_view payload) {
                  if (kind == beast::websocket::frame_type::pong) {
                    pong = std::string(payload);
                    if (onPong) onPong();
                  }
                });
                socket->async_ping("sbe-heartbeat", [this, socket, buffer](const ErrorCode& ec) {
                  ASSERT_FALSE(ec) << ec.message();
                  socket->binary(true);
                  socket->async_write_some(false, net::buffer(tradeFrame.data(), 11), [this, socket, buffer](const ErrorCode& ec, size_t) {
                    ASSERT_FALSE(ec) << ec.message();
                    socket->async_write_some(true, net::buffer(tradeFrame.data() + 11, tradeFrame.size() - 11),
                                             [this, socket, buffer](const ErrorCode& ec, size_t) {
                                               ASSERT_FALSE(ec) << ec.message();
                                               readClose(socket, buffer, false);
                                             });
                  });
                });
              }
            });
          });
        });
      });
    });
  }

  void readClose(std::shared_ptr<Stream> socket, std::shared_ptr<beast::flat_buffer> buffer, bool reconnect) {
    socket->async_read(*buffer, [this, socket, buffer, reconnect](const ErrorCode& ec, size_t) {
      EXPECT_EQ(ec, beast::websocket::error::closed);
      ++closes;
      if (reconnect) accept();
    });
  }

  using ErrorCode = boost::system::error_code;
  net::io_context& io;
  tcp::acceptor acceptor;
  std::shared_ptr<Stream> peer;
  std::vector<std::string> keys;
  std::vector<std::string> subscriptions;
  std::string pong;
  std::function<void()> onPong;
  int closes{};
};

TEST(BinanceSbeTransport, handshakeJsonControlsFragmentedBinaryPingPongReconnectAndStop) {
  ServiceContext context;
  auto& io = *context.ioContextPtr;
  LocalSbeServer server(io);
  std::vector<Message> trades;
  bool stopped = false;
  std::shared_ptr<MarketDataServiceBinance> service;
  auto stopWhenComplete = [&] {
    if (!stopped && !trades.empty() && !server.pong.empty()) {
      stopped = true;
      service->stop();
    }
  };
  server.onPong = stopWhenComplete;
  service = std::make_shared<MarketDataServiceBinance>(
      [&](Event& event, Queue<Event>*) {
        for (const auto& message : event.getMessageList()) {
          if (message.getType() == Message::Type::MARKET_DATA_EVENTS_TRADE) {
            trades.push_back(message);
            stopWhenComplete();
          }
        }
      },
      SessionOptions{}, SessionConfigs{{{CCAPI_BINANCE_SBE_API_KEY, "local-test-key"}}}, &context);
  Subscription subscription("binance", "BTCUSDT", CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE", "wire-correlation");
  auto url = "ws://127.0.0.1:" + std::to_string(server.acceptor.local_endpoint().port()) + "/ws";
  auto connection = std::make_shared<WsConnection>(url, service->getInstrumentGroup(subscription), std::vector<Subscription>{subscription},
                                                   std::map<std::string, std::string>{});
  service->setWsConnectionStream(connection);
  server.accept();
  service->prepareConnect(connection);
  io.run_for(std::chrono::seconds(3));
  service->shouldContinue = false;
  service->clearStates(connection);
  EXPECT_TRUE(stopped);
  EXPECT_EQ(server.keys, (std::vector<std::string>{"local-test-key", "local-test-key"}));
  ASSERT_EQ(server.subscriptions.size(), 2);
  for (const auto& text : server.subscriptions) {
    rj::Document control;
    control.Parse(text.c_str());
    EXPECT_STREQ(control["method"].GetString(), "SUBSCRIBE");
    EXPECT_STREQ(control["params"][0].GetString(), "btcusdt@trade");
  }
  EXPECT_EQ(server.pong, "sbe-heartbeat");
  EXPECT_EQ(server.closes, 2);
  EXPECT_EQ(connection->status, WsConnection::Status::CLOSED);
  ASSERT_EQ(trades.size(), 1);
  EXPECT_EQ(trades[0].getCorrelationIdList()[0], "wire-correlation");
  EXPECT_EQ(trades[0].getTime(), TimePoint(std::chrono::microseconds(eventUs - 123)));
  EXPECT_EQ(trades[0].getElementList().size(), 2);
}

TEST(BinanceSbeTransport, rejectedUpgradeReportsAuthenticationStatusAndCorrelation) {
  ServiceContext context;
  auto& io = *context.ioContextPtr;
  tcp::acceptor acceptor(io, {net::ip::make_address("127.0.0.1"), 0});
  beast::tcp_stream socket(io);
  beast::flat_buffer buffer;
  http::request<http::string_body> request;
  http::response<http::string_body> response(http::status::unauthorized, 11);
  response.prepare_payload();
  std::vector<Message> errors;
  auto service = std::make_shared<MarketDataServiceBinance>(
      [&](Event& event, Queue<Event>*) {
        for (const auto& message : event.getMessageList()) {
          if (message.getType() == Message::Type::SUBSCRIPTION_FAILURE) {
            errors.push_back(message);
            io.stop();
          }
        }
      },
      SessionOptions{}, SessionConfigs{{{CCAPI_BINANCE_SBE_API_KEY, "invalid-test-key"}}}, &context);
  acceptor.async_accept(socket.socket(), [&](const boost::system::error_code& ec) {
    ASSERT_FALSE(ec);
    http::async_read(socket, buffer, request, [&](const boost::system::error_code& ec, size_t) {
      ASSERT_FALSE(ec);
      http::async_write(socket, response, [&](const boost::system::error_code& ec, size_t) { EXPECT_FALSE(ec); });
    });
  });
  Subscription subscription("binance", "BTCUSDT", CCAPI_TRADE, "BINANCE_MARKET_DATA_FORMAT=SBE", "auth-correlation");
  auto connection =
      std::make_shared<WsConnection>("ws://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/ws", service->getInstrumentGroup(subscription),
                                     std::vector<Subscription>{subscription}, std::map<std::string, std::string>{});
  service->setWsConnectionStream(connection);
  service->prepareConnect(connection);
  io.run_for(std::chrono::seconds(3));
  service->shouldContinue = false;
  service->clearStates(connection);
  ASSERT_EQ(errors.size(), 1);
  EXPECT_EQ(errors[0].getCorrelationIdList()[0], "auth-correlation");
  auto reason = errors[0].getElementList()[0].getValue(CCAPI_ERROR_MESSAGE);
  EXPECT_NE(reason.find("authentication/access rejected"), std::string::npos);
  EXPECT_NE(reason.find("401"), std::string::npos);
  EXPECT_EQ(reason.find("invalid-test-key"), std::string::npos);
}
}  // namespace
}  // namespace ccapi
