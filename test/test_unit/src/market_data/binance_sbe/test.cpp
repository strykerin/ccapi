#include "ccapi_cpp/service/ccapi_market_data_service_binance.h"
#include "ccapi_cpp/service/ccapi_market_data_service_binance_usds_futures.h"
#include "gtest/gtest.h"

namespace ccapi {
namespace {

constexpr int64_t eventUs = 1720000000000123;
constexpr int64_t tradeUs = 1720000000000011;

void integer(std::string& out, uint64_t value, size_t width) {
  for (size_t i = 0; i < width; ++i) out.push_back(static_cast<char>(value >> (8 * i)));
}

void symbol(std::string& out) {
  out.push_back(7);
  out += "BTCUSDT";
}

void level(std::string& out, int64_t price, int64_t qty) {
  integer(out, price, 8);
  integer(out, qty, 8);
}

std::string header(uint16_t id, uint16_t size) {
  std::string out;
  integer(out, size, 2);
  integer(out, id, 2);
  integer(out, 1, 2);
  integer(out, 0, 2);
  integer(out, eventUs, 8);
  return out;
}

std::string bbo(int64_t qty = 250) {
  auto out = header(10001, 50);
  integer(out, 100, 8);
  integer(out, -2, 1);
  integer(out, -3, 1);
  level(out, 6000010, qty);
  level(out, 6000020, 500);
  symbol(out);
  return out;
}

std::string depth(uint16_t id = 10003, int64_t first = 101, int64_t last = 102, int64_t qty = 250) {
  auto out = header(id, id == 10003 ? 26 : 18);
  integer(out, first, 8);
  if (id == 10003) integer(out, last, 8);
  integer(out, -2, 1);
  integer(out, -3, 1);
  integer(out, 16, 2);
  integer(out, 1, 2);
  level(out, 6000010, qty);
  integer(out, 16, 2);
  integer(out, 1, 2);
  level(out, 6000020, 500);
  symbol(out);
  return out;
}

std::string trades() {
  auto out = header(10000, 18);
  integer(out, tradeUs, 8);
  integer(out, -2, 1);
  integer(out, -3, 1);
  integer(out, 25, 2);
  integer(out, 2, 4);
  integer(out, 901, 8);
  level(out, 6000010, 250);
  integer(out, 1, 1);
  integer(out, 902, 8);
  level(out, 6000020, 500);
  integer(out, 0, 1);
  symbol(out);
  return out;
}

class ServiceForTest : public MarketDataServiceBinance {
 public:
  using MarketDataServiceBinance::MarketDataServiceBinance;

  void connect(std::shared_ptr<WsConnection> connection) override { connected = true; }

  void failSbe(std::shared_ptr<WsConnection> connection, const std::string& reason) override {
    failure = reason;
    shouldProcessRemainingMessageOnClosingByConnectionIdMap[connection->id] = false;
  }

  bool connected{};
  std::string failure;
};

class BinanceSbeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    service = std::make_shared<ServiceForTest>([this](Event& event, Queue<Event>*) { events.push_back(event); }, SessionOptions(), configs, &context);
  }

  Subscription subscription(const std::string& field = CCAPI_MARKET_DEPTH, const std::string& options = "MARKET_DEPTH_MAX=20") {
    return Subscription(CCAPI_EXCHANGE_NAME_BINANCE, "BTCUSDT", field, "BINANCE_MARKET_DATA_ENCODING=SBE&MARKET_DEPTH_RETURN_UPDATE=1&" + options, "corr",
                        {{CCAPI_BINANCE_API_KEY, "test-ed25519-key"}});
  }

  std::shared_ptr<WsConnection> prepare(const Subscription& sub) {
    auto group = service->getInstrumentGroup(sub);
    auto connection = std::make_shared<WsConnection>(group.substr(0, group.find('|')), group, std::vector<Subscription>{sub}, sub.getCredential());
    service->prepareSubscription(connection, sub);
    requests = service->createSendStringList(connection);
    return connection;
  }

  void ready(std::shared_ptr<WsConnection> connection) {
    service->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[connection->id]["depth"]["BTCUSDT"] = true;
    service->orderbookVersionIdByConnectionIdExchangeSubscriptionIdMap[connection->id]["btcusdt@depth"] = 100;
    service->snapshotBidByConnectionIdChannelIdSymbolIdMap[connection->id]["depth"]["BTCUSDT"][Decimal("60000.1")] = "0.2";
    service->snapshotAskByConnectionIdChannelIdSymbolIdMap[connection->id]["depth"]["BTCUSDT"][Decimal("60000.2")] = "0.4";
  }

  SessionConfigs configs;
  ServiceContext context;
  std::shared_ptr<ServiceForTest> service;
  std::vector<Event> events;
  std::vector<std::string> requests;
};

TEST(BinanceSbeDecoder, decodesAllTemplatesAndExactDecimals) {
  auto quote = BinanceSbe::decode(bbo());
  EXPECT_EQ(quote.eventTime, eventUs);
  EXPECT_EQ(quote.symbol, "BTCUSDT");
  EXPECT_EQ(quote.data.at(MarketDataMessage::DataType::BID)[0].at(MarketDataMessage::DataFieldType::PRICE), "60000.1");
  EXPECT_EQ(quote.data.at(MarketDataMessage::DataType::BID)[0].at(MarketDataMessage::DataFieldType::SIZE), "0.25");
  auto diff = BinanceSbe::decode(depth());
  EXPECT_EQ(diff.firstUpdateId, 101);
  EXPECT_EQ(diff.lastUpdateId, 102);
  EXPECT_EQ(BinanceSbe::decode(depth(10002)).lastUpdateId, 101);
  auto trade = BinanceSbe::decode(trades());
  EXPECT_EQ(trade.transactTime, tradeUs);
  const auto& points = trade.data.at(MarketDataMessage::DataType::TRADE);
  ASSERT_EQ(points.size(), 2);
  EXPECT_EQ(points[0].at(MarketDataMessage::DataFieldType::TRADE_ID), "901");
  EXPECT_EQ(points[0].at(MarketDataMessage::DataFieldType::IS_BUYER_MAKER), "1");
  EXPECT_EQ(points[1].at(MarketDataMessage::DataFieldType::IS_BUYER_MAKER), "0");
  EXPECT_EQ(BinanceSbe::decimal(123, -8), "0.00000123");
  EXPECT_EQ(BinanceSbe::decimal(1200, -2), "12");
  EXPECT_EQ(BinanceSbe::decimal(123, 2), "12300");
  EXPECT_EQ(BinanceSbe::decimal(0, -128), "0");
  EXPECT_EQ(BinanceSbe::decimal(std::numeric_limits<int64_t>::min(), 0), "-9223372036854775808");
  EXPECT_EQ(BinanceSbe::decimal(std::numeric_limits<int64_t>::max(), -8), "92233720368.54775807");
}

TEST(BinanceSbeDecoder, rejectsEveryTruncatedPrefixAndTrailingBytes) {
  for (const auto& bytes : {bbo(), depth(), depth(10002), trades()}) {
    for (size_t i = 0; i < bytes.size(); ++i) {
      EXPECT_THROW(BinanceSbe::decode(std::string_view(bytes.data(), i)), std::runtime_error) << i;
    }
    EXPECT_THROW(BinanceSbe::decode(bytes + "x"), std::runtime_error);
  }
}

TEST(BinanceSbeDecoder, rejectsInvalidHeadersGroupsAndValues) {
  for (const auto offset : {0, 2, 4, 6}) {
    auto bytes = bbo();
    bytes[offset] = 0x7f;
    EXPECT_THROW(BinanceSbe::decode(bytes), std::runtime_error);
  }
  auto bytes = trades();
  bytes[28] = '\xff';
  bytes[29] = '\xff';
  bytes[30] = '\xff';
  bytes[31] = '\x7f';
  EXPECT_THROW(BinanceSbe::decode(bytes), std::runtime_error);
  bytes = trades();
  bytes[56] = 2;
  EXPECT_THROW(BinanceSbe::decode(bytes), std::runtime_error);
  bytes = depth();
  bytes[34] = 1;
  EXPECT_THROW(BinanceSbe::decode(bytes), std::runtime_error);
  EXPECT_THROW(BinanceSbe::decode(depth(10003, 105, 104)), std::runtime_error);
  EXPECT_THROW(BinanceSbe::decode(bbo(-1)), std::runtime_error);
  EXPECT_EQ(BinanceSbe::decode(depth(10003, 101, 102, 0)).data.at(MarketDataMessage::DataType::BID)[0].at(MarketDataMessage::DataFieldType::SIZE), "0");
}

TEST_F(BinanceSbeTest, routesStreamsAndAuthenticatesWithoutLeakingHeader) {
  auto conn = prepare(subscription());
  EXPECT_EQ(conn->url, "wss://stream-sbe.binance.com/stream");
  ASSERT_EQ(requests.size(), 1);
  EXPECT_NE(requests[0].find("btcusdt@depth\""), std::string::npos);
  EXPECT_EQ(requests[0].find("@100ms"), std::string::npos);
  service->prepareConnect(conn);
  EXPECT_TRUE(service->connected);
  EXPECT_EQ(conn->headers.at("X-MBX-APIKEY"), "test-ed25519-key");
  EXPECT_EQ(conn->toString().find("test-ed25519-key"), std::string::npos);
  EXPECT_NE(conn->toString().find("<redacted>"), std::string::npos);
  conn = prepare(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=1"));
  EXPECT_NE(requests[0].find("btcusdt@bestBidAsk"), std::string::npos);
  conn = prepare(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=5&BINANCE_SBE_DEPTH_MODE=partial"));
  EXPECT_NE(requests[0].find("btcusdt@depth20"), std::string::npos);
  conn = prepare(subscription(CCAPI_TRADE));
  EXPECT_NE(requests[0].find("btcusdt@trade"), std::string::npos);
}

TEST_F(BinanceSbeTest, rejectsUnsupportedSubscriptionsAndMissingKey) {
  service->credentialDefault.clear();
  Subscription missing(CCAPI_EXCHANGE_NAME_BINANCE, "BTCUSDT", CCAPI_TRADE, "BINANCE_MARKET_DATA_ENCODING=SBE");
  EXPECT_THROW(service->getInstrumentGroup(missing), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(subscription(CCAPI_AGG_TRADE)), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(subscription(CCAPI_CANDLESTICK)), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=21&BINANCE_SBE_DEPTH_MODE=partial")), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(subscription(CCAPI_MARKET_DEPTH, "BINANCE_SBE_DEPTH_MODE=other")), std::invalid_argument);
  auto future = std::make_shared<MarketDataServiceBinanceUsdsFutures>([](Event&, Queue<Event>*) {}, SessionOptions(), configs, &context);
  EXPECT_THROW(
      future->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARKET_DEPTH, "BINANCE_MARKET_DATA_ENCODING=SBE")),
      std::invalid_argument);
}

TEST_F(BinanceSbeTest, preservesJsonDefaultAndKeepsConnectionsSeparate) {
  auto sub = Subscription(CCAPI_EXCHANGE_NAME_BINANCE, "BTCUSDT", CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=20", "json");
  auto conn = prepare(sub);
  EXPECT_EQ(conn->url, configs.getUrlWebsocketBase().at(CCAPI_EXCHANGE_NAME_BINANCE) + "/stream");
  EXPECT_NE(requests[0].find("btcusdt@depth20@100ms"), std::string::npos);
  EXPECT_NE(service->getInstrumentGroup(sub), service->getInstrumentGroup(subscription()));
  service->prepareConnect(conn);
  EXPECT_EQ(conn->headers.count("X-MBX-APIKEY"), 0);
}

TEST_F(BinanceSbeTest, publishesNormalizedTradesWithMicrosecondTimestamp) {
  auto conn = prepare(subscription(CCAPI_TRADE));
  service->onBinaryMessage(conn, trades(), TimePoint(std::chrono::microseconds(eventUs + 10)));
  ASSERT_TRUE(service->failure.empty()) << service->failure;
  ASSERT_EQ(events.size(), 1);
  const auto& messages = events[0].getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::MARKET_DATA_EVENTS_TRADE);
  EXPECT_EQ(messages[0].getTime(), TimePoint(std::chrono::microseconds(tradeUs)));
  EXPECT_EQ(messages[0].getCorrelationIdList(), std::vector<std::string>{"corr"});
  ASSERT_EQ(messages[0].getElementList().size(), 2);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_LAST_PRICE), "60000.1");
  EXPECT_EQ(messages[0].getElementList()[1].getValue(CCAPI_TRADE_ID), "902");
}

TEST_F(BinanceSbeTest, publishesBboAndPartialSnapshots) {
  auto conn = prepare(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=1"));
  service->onBinaryMessage(conn, bbo(), TimePoint{});
  ASSERT_TRUE(service->failure.empty()) << service->failure;
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events.back().getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
  EXPECT_EQ(events.back().getMessageList()[0].getTime(), TimePoint(std::chrono::microseconds(eventUs)));
  service->onBinaryMessage(conn, bbo(300), TimePoint{});
  EXPECT_EQ(events.back().getMessageList()[0].getRecapType(), Message::RecapType::NONE);
  conn = prepare(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=20&BINANCE_SBE_DEPTH_MODE=partial"));
  service->onBinaryMessage(conn, depth(10002), TimePoint{});
  EXPECT_TRUE(service->failure.empty()) << service->failure;
  EXPECT_EQ(events.back().getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
}

TEST_F(BinanceSbeTest, checksLiveSequenceDropsDuplicatesAndAppliesDeletions) {
  auto conn = prepare(subscription());
  ready(conn);
  service->onBinaryMessage(conn, depth(10003, 99, 102), TimePoint{});
  EXPECT_TRUE(service->failure.empty()) << service->failure;
  ASSERT_EQ(events.size(), 1);
  service->onBinaryMessage(conn, depth(10003, 99, 102), TimePoint{});
  EXPECT_EQ(events.size(), 1);
  service->onBinaryMessage(conn, depth(10003, 103, 104, 0), TimePoint{});
  EXPECT_TRUE(service->snapshotBidByConnectionIdChannelIdSymbolIdMap[conn->id]["depth"]["BTCUSDT"].empty());
  service->onBinaryMessage(conn, depth(10003, 106, 107), TimePoint{});
  EXPECT_NE(service->failure.find("sequence gap"), std::string::npos);
  EXPECT_EQ(service->orderbookVersionIdByConnectionIdExchangeSubscriptionIdMap[conn->id]["btcusdt@depth"], 104);
}

TEST_F(BinanceSbeTest, buffersOwnedDataAndClearsRecoveryOnDisconnect) {
  auto conn = prepare(subscription(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=20&FETCH_MARKET_DEPTH_INITIAL_SNAPSHOT_DELAY_MILLISECONDS=10000"));
  auto bytes = depth();
  service->onBinaryMessage(conn, bytes, TimePoint{});
  EXPECT_TRUE(service->failure.empty()) << service->failure;
  EXPECT_TRUE(events.empty());
  bytes.assign(bytes.size(), 'x');
  auto& buffer = service->marketDataMessageDataBufferByConnectionIdExchangeSubscriptionIdVersionIdMap[conn->id]["btcusdt@depth"];
  ASSERT_EQ(buffer.size(), 1);
  EXPECT_EQ(buffer.at(102).data.at(MarketDataMessage::DataType::BID)[0].at(MarketDataMessage::DataFieldType::PRICE), "60000.1");
  service->clearStates(conn);
  EXPECT_EQ(service->marketDataMessageDataBufferByConnectionIdExchangeSubscriptionIdVersionIdMap.count(conn->id), 0);
  EXPECT_EQ(service->orderBookRecoveryGenerationByConnectionIdExchangeSubscriptionIdMap.count(conn->id), 0);
}

TEST_F(BinanceSbeTest, snapshotBridgeRequiresSnapshotPlusOneAndContiguousRanges) {
  std::map<int64_t, MarketDataService::BufferedOrderBookUpdate> buffer;
  buffer[100] = {90, 100, -1, {}};
  buffer[104] = {101, 104, -1, {}};
  buffer[110] = {103, 110, -1, {}};
  EXPECT_EQ(service->findOrderBookSnapshotBridge(101, buffer), 104);
  EXPECT_TRUE(service->isOrderBookUpdateChainContiguous(104, buffer, true));
  EXPECT_FALSE(service->isOrderBookUpdateChainContiguous(104, buffer));
  buffer[110].firstUpdateId = 106;
  EXPECT_FALSE(service->isOrderBookUpdateChainContiguous(104, buffer, true));
  EXPECT_EQ(service->findOrderBookSnapshotBridge(111, buffer), -1);
}

TEST_F(BinanceSbeTest, controlResponsesStayJsonAndFailuresNeverPublishData) {
  auto conn = prepare(subscription(CCAPI_TRADE));
  service->onTextMessage(conn, R"({"result":null,"id":0})", TimePoint{});
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_STARTED);
  EXPECT_EQ(events[0].getMessageList()[0].getCorrelationIdList(), std::vector<std::string>{"corr"});
  service->onTextMessage(conn, R"({"code":2,"msg":"invalid request","id":9})", TimePoint{});
  EXPECT_EQ(events.back().getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
  service->onTextMessage(conn, R"({"e":"serverShutdown","E":1720000000000})", TimePoint{});
  EXPECT_NE(service->failure.find("shutdown"), std::string::npos);
  const auto size = events.size();
  service->onBinaryMessage(conn, "broken", TimePoint{});
  EXPECT_NE(service->failure.find("invalid Binance SBE"), std::string::npos);
  EXPECT_EQ(events.size(), size);
  service->onBinaryMessage(conn, bbo(), TimePoint{});
  EXPECT_NE(service->failure.find("invalid Binance SBE"), std::string::npos);
  EXPECT_EQ(events.size(), size);
}

class LoopbackService : public MarketDataServiceBinance {
 public:
  using MarketDataServiceBinance::MarketDataServiceBinance;
  std::string endpoint;

  std::string getInstrumentGroup(const Subscription& subscription) override {
    auto group = MarketDataServiceBinance::getInstrumentGroup(subscription);
    return endpoint + "/stream" + group.substr(group.find('|'));
  }
};

TEST(BinanceSbeLoopback, handshakeBinaryDispatchSnapshotGapAndReconnect) {
  const auto* port = std::getenv("CCAPI_SBE_TEST_PORT");
  if (!port) GTEST_SKIP() << "Run test/run_binance_sbe_loopback.py with this executable";
  ServiceContext context;
  SessionConfigs configs;
  auto roots = configs.getUrlRestBase();
  roots[CCAPI_EXCHANGE_NAME_BINANCE] = std::string("https://127.0.0.1:") + port;
  configs.setUrlRestBase(roots);
  std::vector<Event> events;
  auto service = std::make_shared<LoopbackService>([&](Event& event, Queue<Event>*) { events.push_back(event); }, SessionOptions(), configs, &context);
  service->endpoint = std::string("wss://127.0.0.1:") + port;
  std::vector<Subscription> subscriptions{Subscription(CCAPI_EXCHANGE_NAME_BINANCE, "BTCUSDT", CCAPI_MARKET_DEPTH,
                                                       "BINANCE_MARKET_DATA_ENCODING=SBE&MARKET_DEPTH_MAX=20&MARKET_DEPTH_RETURN_UPDATE=1", "loopback",
                                                       {{CCAPI_BINANCE_API_KEY, "loopback-key"}})};
  service->subscribe(subscriptions);
  context.ioContextPtr->run_for(std::chrono::seconds(4));
  size_t snapshots = 0, updates = 0, gaps = 0;
  for (const auto& event : events) {
    for (const auto& message : event.getMessageList()) {
      if (message.getType() == Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH) {
        EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"loopback"});
        if (message.getRecapType() == Message::RecapType::SOLICITED)
          ++snapshots;
        else
          ++updates;
      } else if (message.getType() == Message::Type::SUBSCRIPTION_FAILURE) {
        for (const auto& element : message.getElementList()) {
          if (element.getValue(CCAPI_ERROR_MESSAGE).find("sequence gap") != std::string::npos) ++gaps;
        }
      }
    }
  }
  service->stop();
  context.ioContextPtr->run_for(std::chrono::milliseconds(50));
  EXPECT_EQ(snapshots, 2);
  EXPECT_GE(updates, 1);
  EXPECT_EQ(gaps, 1);
}

}  // namespace
}  // namespace ccapi
