#include "ccapi_cpp/service/ccapi_market_data_service_okx.h"
#include "gtest/gtest.h"

namespace ccapi {
namespace {
using D = MarketDataMessage::DataType;
using F = MarketDataMessage::DataFieldType;
constexpr int64_t eventUs = 1720000000123000;
const std::string streamId = "books-l2-tbt:211874";
const std::string options = "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=211874";
const std::map<std::string, std::string> credentials{
    {CCAPI_OKX_API_KEY, "test-key"}, {CCAPI_OKX_API_SECRET, "test-secret"}, {CCAPI_OKX_API_PASSPHRASE, "test-passphrase"}};
const std::string bboFrame = UtilAlgorithm::hexToString(
    "4a00e80301000000a23b030000000000786005b5541c0600fd5f05b5541c0600660000000000000002000000000020000200000000000000010000000000200015cd5b07000000000200000003"
    "000000f8f8");
const std::string tradeFrame = UtilAlgorithm::hexToString(
    "3e00ed0301000000a23b030000000000786005b5541c0600fd5f05b5541c06006600000000000000010000000000200015cd5b07000000000100000000002000030000f8f801");
const std::string depthFrame = UtilAlgorithm::hexToString(
    "2a00e90301000000a23b030000000000786005b5541c0600fd5f05b5541c060066000000000000006400000000000000f8f8140001000200000000002000020000000000000002000000140001"
    "000100000000002000000000000000000002000000");
const std::string exponentFrame =
    UtilAlgorithm::hexToString("2a00ea0301000000a23b030000000000786005b5541c0600fd5f05b5541c060067000000000000006600000000000000f9fa");
const std::string snapshotFrame = UtilAlgorithm::hexToString(
    "1a00ee0301000000a23b030000000000905c05b5541c06006400000000000000f8f81400010002000000000020000084d717000000000200000014000200010000000000200000c2eb0b000000"
    "0002000000000000000000200000a3e1110000000002000000");

void put(std::string& frame, size_t offset, uint64_t value, size_t size = 8) {
  for (size_t i = 0; i < size; ++i) frame.at(offset + i) = static_cast<char>(value >> (i * 8));
}

std::string diff(int64_t previous, int64_t sequence, int64_t quantity = 0) {
  auto frame = depthFrame;
  put(frame, 32, sequence);
  put(frame, 40, previous);
  put(frame, 86, quantity);
  return frame;
}

std::string heartbeat(int64_t sequence) {
  auto frame = diff(sequence, sequence).substr(0, 50);
  return frame + UtilAlgorithm::hexToString("1400000014000000");
}

TEST(OkxSbeDecoder, decodesAllFiveMarketDataTemplates) {
  auto trade = OkxSbe::decode(tradeFrame);
  EXPECT_EQ(trade.templateId, 1005);
  EXPECT_EQ(trade.instIdCode, 211874);
  EXPECT_EQ(trade.eventTime, eventUs);
  EXPECT_EQ(trade.outTime, eventUs - 123);
  EXPECT_EQ(trade.sequence, 102);
  EXPECT_EQ(trade.tradeCount, 3);
  EXPECT_EQ(trade.tradeSource, 1);
  const auto& point = trade.data.at(D::TRADE)[0];
  EXPECT_EQ(point.at(F::PRICE), "90071992.54740993");
  EXPECT_EQ(point.at(F::SIZE), "1.23456789");
  EXPECT_EQ(point.at(F::TRADE_ID), "9007199254740993");
  EXPECT_EQ(point.at(F::IS_BUYER_MAKER), "1");
  auto buy = tradeFrame;
  put(buy, 66, 1, 1);
  EXPECT_EQ(OkxSbe::decode(buy).data.at(D::TRADE)[0].at(F::IS_BUYER_MAKER), "0");
  auto bbo = OkxSbe::decode(bboFrame);
  EXPECT_EQ(bbo.data.at(D::ASK)[0].at(F::PRICE), "90071992.54740994");
  EXPECT_EQ(bbo.data.at(D::BID)[0].at(F::SIZE), "1.23456789");
  auto depth = OkxSbe::decode(depthFrame);
  EXPECT_EQ(depth.previousSequence, 100);
  EXPECT_EQ(depth.data.at(D::BID)[0].at(F::SIZE), "0");
  auto exponent = OkxSbe::decode(exponentFrame);
  EXPECT_EQ(exponent.previousSequence, 102);
  EXPECT_EQ(exponent.sequence, 103);
  EXPECT_EQ(exponent.priceExponent, -7);
  EXPECT_EQ(exponent.sizeExponent, -6);
  EXPECT_TRUE(exponent.data.empty());
  auto snapshot = OkxSbe::decode(snapshotFrame);
  EXPECT_EQ(snapshot.sequence, 100);
  EXPECT_EQ(snapshot.eventTime, eventUs - 1000);
  EXPECT_EQ(snapshot.data.at(D::BID).size(), 2);
  EXPECT_EQ(snapshot.data.at(D::ASK)[0].at(F::SIZE), "4");
}

TEST(OkxSbeDecoder, decimalConversionNeverUsesFloatingPoint) {
  EXPECT_EQ(OkxSbe::decimal(std::numeric_limits<int64_t>::min(), 0), "-9223372036854775808");
  EXPECT_EQ(OkxSbe::decimal(std::numeric_limits<int64_t>::max(), -8), "92233720368.54775807");
  EXPECT_EQ(OkxSbe::decimal(12300, -4), "1.23");
  EXPECT_EQ(OkxSbe::decimal(-1, -8), "-0.00000001");
  for (int exponent = -127; exponent <= 127; ++exponent) {
    EXPECT_EQ(OkxSbe::decimal(0, exponent), "0");
    EXPECT_EQ(OkxSbe::decimal(1, exponent), exponent < 0 ? "0." + std::string(-exponent - 1, '0') + "1" : "1" + std::string(exponent, '0'));
  }
  EXPECT_THROW(OkxSbe::decimal(1, -128), std::runtime_error);
}

TEST(OkxSbeDecoder, rejectsEveryTruncatedPrefixAndTrailingBytes) {
  for (const auto& frame : {tradeFrame, bboFrame, depthFrame, exponentFrame, snapshotFrame}) {
    for (size_t size = 0; size < frame.size(); ++size) EXPECT_THROW(OkxSbe::decode(std::string_view(frame.data(), size)), std::runtime_error) << size;
    EXPECT_THROW(OkxSbe::decode(frame + '\0'), std::runtime_error);
  }
}

TEST(OkxSbeDecoder, rejectsInvalidSchemaTemplateBlockGroupEnumsAndNumericFields) {
  for (auto offset : {0, 2, 4, 6, 50, 52}) {
    auto frame = depthFrame;
    put(frame, offset, 65535, 2);
    EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error) << offset;
  }
  for (auto offset : {8, 16, 24, 32, 40, 54, 62}) {
    auto frame = depthFrame;
    put(frame, offset, uint64_t(1) << 63);
    EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error) << offset;
  }
  for (auto offset : {66, 69}) {
    auto frame = tradeFrame;
    put(frame, offset, 2, 1);
    EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error);
  }
  auto frame = tradeFrame;
  put(frame, 64, 0, 2);
  EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error);
  frame = depthFrame;
  put(frame, 16, std::numeric_limits<int64_t>::max());
  EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error);
  put(frame, 48, 128, 1);
  EXPECT_THROW(OkxSbe::decode(frame), std::runtime_error);
  EXPECT_THROW(OkxSbe::decode(diff(100, 100)), std::runtime_error);
  EXPECT_NO_THROW(OkxSbe::decode(heartbeat(100)));
  EXPECT_NO_THROW(OkxSbe::decode(diff(100, 3)));
}

class TestService : public MarketDataServiceOkx {
 public:
  using MarketDataServiceOkx::MarketDataServiceOkx;

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

class OkxSbeServiceTest : public ::testing::Test {
 public:
  void SetUp() override {
    service = std::make_shared<TestService>([this](Event& event, Queue<Event>*) { events.push_back(event); }, SessionOptions{}, SessionConfigs{credentials},
                                            &context);
  }

  std::shared_ptr<WsConnection> connection(const std::string& field = CCAPI_MARKET_DEPTH, const std::string& settings = options + "&MARKET_DEPTH_MAX=20") {
    Subscription subscription("okx", "BTC-USDT", field, settings, "correlation");
    auto group = service->getInstrumentGroup(subscription);
    auto ws = std::make_shared<WsConnection>(UtilString::split(group, "|")[0], group, std::vector<Subscription>{subscription}, credentials);
    ws->status = WsConnection::Status::OPEN;
    service->prepareSubscription(ws, subscription);
    service->createSendStringList(ws);
    return ws;
  }

  void snapshot(std::shared_ptr<WsConnection> ws, int64_t sequence) {
    http::response<http::string_body> response(http::status::ok, 11);
    response.set(http::field::content_type, "application/sbe");
    response.body() = snapshotFrame;
    put(response.body(), 24, sequence);
    service->onSbeSnapshot(ws, streamId, service->sbeBooks.at(ws->id).at(streamId), response);
  }

  void receive(std::shared_ptr<WsConnection> ws, const std::string& frame) { EXPECT_TRUE(service->onBinaryMessage(ws, frame, now)); }

  ServiceContext context;
  std::shared_ptr<TestService> service;
  std::vector<Event> events;
  TimePoint now{std::chrono::microseconds(eventUs + 100)};
};

TEST_F(OkxSbeServiceTest, incrementalSelectionIsIndependentOfLocalDepthAndConflation) {
  for (int levels : {1, 5, 20, 50, 400}) {
    auto ws = connection(CCAPI_MARKET_DEPTH, options + "&MARKET_DEPTH_MAX=" + std::to_string(levels) + "&CONFLATE_INTERVAL_MILLISECONDS=100");
    rj::Document document;
    document.Parse(service->createSendStringList(ws).front().c_str());
    EXPECT_STREQ(document["op"].GetString(), "subscribe");
    ASSERT_EQ(document["args"].Size(), 1);
    EXPECT_STREQ(document["args"][0]["channel"].GetString(), "books-l2-tbt");
    EXPECT_EQ(document["args"][0]["instIdCode"].GetInt64(), 211874);
    EXPECT_FALSE(document["args"][0].HasMember("instId"));
    EXPECT_EQ(ws->url, "wss://ws.okx.com/ws/v5/public-sbe");
  }
}

TEST_F(OkxSbeServiceTest, selectsRealtimeBboAndTradesExplicitly) {
  auto bbo = connection(CCAPI_MARKET_DEPTH, options + "&OKX_MARKET_DATA_STREAM=bbo-tbt");
  EXPECT_NE(service->createSendStringList(bbo).front().find("bbo-tbt"), std::string::npos);
  auto trade = connection(CCAPI_TRADE, options);
  EXPECT_NE(service->createSendStringList(trade).front().find("trades"), std::string::npos);
  EXPECT_NE(bbo->group, trade->group);
}

TEST_F(OkxSbeServiceTest, jsonDefaultChannelsLoginAndNormalizationRemainUnchanged) {
  auto json = connection(CCAPI_MARKET_DEPTH, "MARKET_DEPTH_MAX=20");
  EXPECT_FALSE(json->isOkxSbe());
  EXPECT_EQ(json->url, "wss://ws.okx.com/ws/v5/public");
  EXPECT_NE(json->group, connection()->group);
  auto request = service->createSendStringList(json).front();
  EXPECT_NE(request.find("books50-l2-tbt"), std::string::npos);
  EXPECT_NE(request.find("\"instId\":\"BTC-USDT\""), std::string::npos);
  service->prepareConnect(json);
  EXPECT_TRUE(json->headers.empty());
  EXPECT_FALSE(service->onBinaryMessage(json, depthFrame, now));
  auto login = service->createSendStringListFromSubscriptionList(json, json->subscriptionList, now, credentials).front();
  EXPECT_NE(login.find("\"op\":\"login\""), std::string::npos);
  auto ticker = connection(CCAPI_MARKET_DEPTH, "");
  EXPECT_NE(service->createSendStringList(ticker).front().find("bbo-tbt"), std::string::npos);
  auto trades = connection(CCAPI_TRADE, "");
  service->onTextMessage(
      trades, R"({"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"ts":"1720000000123","px":"1.2300","sz":"2.00","tradeId":"91","side":"sell"}]})",
      now);
  ASSERT_EQ(events.size(), 1);
  auto message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::milliseconds(1720000000123)));
  EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"correlation"});
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_LAST_PRICE), "1.23");
}

TEST_F(OkxSbeServiceTest, rejectsInvalidFormatStreamDepthCodeAndMissingCredentials) {
  for (const auto& settings :
       {"OKX_MARKET_DATA_FORMAT=bad", "OKX_MARKET_DATA_FORMAT=SBE", "OKX_MARKET_DATA_STREAM=books-l2-tbt",
        "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=211874&OKX_MARKET_DATA_STREAM=books50-l2-tbt",
        "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=211874&OKX_MARKET_DATA_STREAM=bbo-tbt&MARKET_DEPTH_MAX=20", "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=-1",
        "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=1x", "OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=211874&MARKET_DEPTH_MAX=401"}) {
    EXPECT_THROW(service->getInstrumentGroup(Subscription("okx", "BTC-USDT", CCAPI_MARKET_DEPTH, settings)), std::invalid_argument);
  }
  EXPECT_THROW(service->getInstrumentGroup(Subscription("okx", "BTC-USDT", CCAPI_CANDLESTICK, options)), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(Subscription("okx", "BTC-USDT,ETH-USDT", CCAPI_TRADE, options)), std::invalid_argument);
  EXPECT_THROW(service->getInstrumentGroup(Subscription("okx", "BTC-USDT", CCAPI_TRADE, options + "&OKX_MARKET_DATA_STREAM=bbo-tbt")), std::invalid_argument);
  service->credentialDefault.clear();
  std::vector<Subscription> subscriptions{Subscription("okx", "BTC-USDT", CCAPI_TRADE, options, "missing")};
  service->subscribe(subscriptions);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
  EXPECT_EQ(events[0].getMessageList()[0].getCorrelationIdList()[0], "missing");
}

TEST_F(OkxSbeServiceTest, signsHandshakeVerifiesTlsAndRedactsAllCredentials) {
  Subscription subscription("okx", "BTC-USDT", CCAPI_TRADE, options, "auth", credentials);
  auto group = service->getInstrumentGroup(subscription);
  auto ws = std::make_shared<WsConnection>(UtilString::split(group, "|")[0], group, std::vector<Subscription>{subscription}, credentials);
  service->setWsConnectionStream(ws);
  service->prepareConnect(ws);
  service->setSbeHeaders(ws, TimePoint(std::chrono::seconds(1720000000)));
  EXPECT_EQ(ws->headers.at("OK-ACCESS-KEY"), "test-key");
  EXPECT_EQ(ws->headers.at("OK-ACCESS-PASSPHRASE"), "test-passphrase");
  EXPECT_EQ(ws->headers.at("OK-ACCESS-TIMESTAMP"), "1720000000");
  EXPECT_EQ(ws->headers.at("OK-ACCESS-SIGN"), "epWudKPrxMj8KEhjyZZA3etxMBcB5CgX7uniaNdnWok=");
  EXPECT_EQ(ws->headers.size(), 4);
  for (const auto& text : {group, ws->toString(), ws->longId, subscription.toString()}) {
    EXPECT_EQ(text.find("test-"), std::string::npos);
    EXPECT_EQ(text.find("epWudK"), std::string::npos);
  }
  auto socket = std::get<std::shared_ptr<beast::websocket::stream<beast::ssl_stream<beast::tcp_stream>>>>(ws->streamPtr);
  EXPECT_EQ(SSL_get_verify_mode(socket->next_layer().native_handle()), SSL_VERIFY_PEER);
}

TEST_F(OkxSbeServiceTest, normalizesTradesWithExactPricesTimesIdsAggressorSequenceAndSource) {
  receive(connection(CCAPI_TRADE, options), tradeFrame);
  ASSERT_EQ(events.size(), 1);
  const auto& message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getType(), Message::Type::MARKET_DATA_EVENTS_TRADE);
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::microseconds(eventUs)));
  EXPECT_EQ(message.getTimeReceived(), now);
  EXPECT_EQ(message.getCorrelationIdList()[0], "correlation");
  const auto& point = message.getElementList()[0];
  EXPECT_EQ(point.getValue(CCAPI_LAST_PRICE), "90071992.54740993");
  EXPECT_EQ(point.getValue(CCAPI_LAST_SIZE), "1.23456789");
  EXPECT_EQ(point.getValue(CCAPI_TRADE_ID), "9007199254740993");
  EXPECT_EQ(point.getValue(CCAPI_IS_BUYER_MAKER), "1");
  EXPECT_EQ(point.getValue(CCAPI_SEQUENCE_NUMBER), "102");
  EXPECT_EQ(point.getValue(CCAPI_OUT_TIME_MICROSECONDS), std::to_string(eventUs - 123));
  EXPECT_EQ(point.getValue(CCAPI_TRADE_COUNT), "3");
  EXPECT_EQ(point.getValue(CCAPI_TRADE_SOURCE), "1");
}

TEST_F(OkxSbeServiceTest, bboReplacesBothSidesAndAllowsSequenceJumps) {
  auto ws = connection(CCAPI_MARKET_DEPTH, options + "&OKX_MARKET_DATA_STREAM=bbo-tbt");
  receive(ws, bboFrame);
  auto next = bboFrame;
  put(next, 32, 200);
  put(next, 56, 8007199254740993);
  receive(ws, next);
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["bbo-tbt"]["211874"].size(), 1);
  auto element = events.back().getMessageList()[0].getElementList()[0];
  EXPECT_EQ(element.getValue(CCAPI_BEST_BID_N_PRICE), "80071992.54740993");
  EXPECT_EQ(element.getValue(CCAPI_SEQUENCE_NUMBER), "200");
  EXPECT_TRUE(service->restartReason.empty());
}

TEST_F(OkxSbeServiceTest, buffersDiscardsOldDiffsAndBridgesSnapshotInsideSequenceRange) {
  auto ws = connection();
  receive(ws, diff(90, 99));
  receive(ws, diff(99, 102));
  receive(ws, diff(102, 105, 100));
  EXPECT_TRUE(events.empty());
  EXPECT_EQ(service->fetches, 1);
  snapshot(ws, 100);
  auto book = service->sbeBooks.at(ws->id).at(streamId);
  EXPECT_EQ(book->sequence, 105);
  EXPECT_TRUE(book->buffer.empty());
  const auto& bids = service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["books-l2-tbt"]["211874"];
  EXPECT_EQ(bids.at(Decimal("90071992.54740993")), "0.000001");
  EXPECT_EQ(bids.at(Decimal("90071992.54740992")), "3");
  ASSERT_EQ(events.size(), 3);
  EXPECT_EQ(events[0].getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
  EXPECT_EQ(events[0].getMessageList()[0].getTime(), TimePoint(std::chrono::microseconds(eventUs - 1000)));
  EXPECT_EQ(events[1].getMessageList()[0].getElementList()[0].getValue(CCAPI_PREVIOUS_SEQUENCE_NUMBER), "99");
  EXPECT_EQ(events[1].getMessageList()[0].getCorrelationIdList()[0], "correlation");
  EXPECT_EQ(events[1].getMessageList()[0].getTimeReceived(), now);
}

TEST_F(OkxSbeServiceTest, processesExponentUpdatesWithoutEmittingOrRescalingExistingLevels) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 100);
  auto count = events.size();
  receive(ws, exponentFrame);
  auto book = service->sbeBooks.at(ws->id).at(streamId);
  EXPECT_EQ(book->sequence, 103);
  EXPECT_EQ(book->priceExponent, -7);
  EXPECT_EQ(book->sizeExponent, -6);
  EXPECT_EQ(events.size(), count);
  auto frame = diff(103, 104, 123);
  put(frame, 48, uint8_t(-7), 1);
  put(frame, 49, uint8_t(-6), 1);
  put(frame, 78, 10000000);
  receive(ws, frame);
  const auto& bids = service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["books-l2-tbt"]["211874"];
  EXPECT_EQ(bids.at(Decimal("1")), "0.000123");
  EXPECT_EQ(bids.at(Decimal("90071992.54740992")), "3");
  EXPECT_EQ(book->sequence, 104);
}

TEST_F(OkxSbeServiceTest, exponentEventCanBridgeSnapshotAndPreventFalseGap) {
  auto ws = connection();
  receive(ws, exponentFrame);
  receive(ws, diff(103, 104));
  snapshot(ws, 102);
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 104);
  EXPECT_TRUE(service->restartReason.empty());
}

TEST_F(OkxSbeServiceTest, heartbeatAndMaintenanceSequenceResetAreValidAfterSynchronization) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 100);
  auto count = events.size();
  receive(ws, heartbeat(102));
  EXPECT_EQ(events.size(), count);
  receive(ws, diff(102, 3, 7));
  receive(ws, diff(3, 5, 8));
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 5);
  EXPECT_TRUE(service->restartReason.empty());
}

TEST_F(OkxSbeServiceTest, resetWhileFetchingSnapshotResynchronizesSafely) {
  auto ws = connection();
  receive(ws, depthFrame);
  receive(ws, diff(102, 3));
  EXPECT_NE(service->restartReason.find("reset during snapshot"), std::string::npos);
  EXPECT_EQ(service->sbeBooks.count(ws->id), 0);
}

TEST_F(OkxSbeServiceTest, snapshotAheadWaitsForABridgingDiff) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 110);
  EXPECT_TRUE(events.empty());
  receive(ws, diff(102, 109));
  EXPECT_TRUE(events.empty());
  receive(ws, diff(109, 112));
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 112);
  EXPECT_EQ(service->fetches, 1);
}

TEST_F(OkxSbeServiceTest, staleSnapshotRetriesWithBackoffAndKeepsBuffering) {
  auto ws = connection();
  receive(ws, depthFrame);
  snapshot(ws, 50);
  auto book = service->sbeBooks.at(ws->id).at(streamId);
  EXPECT_FALSE(book->snapshot);
  EXPECT_TRUE(book->fetching);
  EXPECT_EQ(book->retryMilliseconds, 2000);
  EXPECT_EQ(book->sequence, -1);
  EXPECT_EQ(events.back().getType(), Event::Type::SUBSCRIPTION_STATUS);
  receive(ws, diff(102, 104));
  EXPECT_EQ(book->buffer.size(), 2);
  snapshot(ws, 101);
  EXPECT_EQ(book->sequence, 104);
  service->clearStates(ws);
}

TEST_F(OkxSbeServiceTest, matchingSnapshotPublishesWithoutWaitingForAnotherChange) {
  auto ws = connection();
  receive(ws, heartbeat(100));
  snapshot(ws, 100);
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 100);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getRecapType(), Message::RecapType::SOLICITED);
  receive(ws, diff(100, 103));
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 103);
}

TEST_F(OkxSbeServiceTest, emptySnapshotAndHeartbeatKeepBothSidesEmpty) {
  auto ws = connection();
  receive(ws, heartbeat(100));
  http::response<http::string_body> response(http::status::ok, 11);
  response.set(http::field::content_type, "application/sbe");
  response.body() = snapshotFrame.substr(0, 34) + UtilAlgorithm::hexToString("1400000014000000");
  service->onSbeSnapshot(ws, streamId, service->sbeBooks.at(ws->id).at(streamId), response);
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 100);
  EXPECT_TRUE(service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["books-l2-tbt"]["211874"].empty());
  EXPECT_TRUE(service->snapshotAskByConnectionIdChannelIdSymbolIdMap[ws->id]["books-l2-tbt"]["211874"].empty());
  ASSERT_EQ(events.size(), 1);
  receive(ws, heartbeat(100));
  EXPECT_EQ(events.size(), 1);
}

TEST_F(OkxSbeServiceTest, snapshotRequestUsesPublicSbeEndpointAndNormalSource) {
  auto request = service->createSbeSnapshotRequest("211874");
  EXPECT_EQ(request.target(), "/api/v5/market/books-sbe?instIdCode=211874&source=0");
  EXPECT_EQ(request.method(), http::verb::get);
  EXPECT_EQ(request[http::field::host], "www.okx.com");
  EXPECT_EQ(request.find("OK-ACCESS-KEY"), request.end());
  EXPECT_EQ(request.find("OK-ACCESS-SIGN"), request.end());
}

TEST_F(OkxSbeServiceTest, bufferingGapPublishesNoBook) {
  auto ws = connection();
  receive(ws, depthFrame);
  receive(ws, diff(104, 105));
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getType(), Event::Type::SUBSCRIPTION_STATUS);
  EXPECT_NE(service->restartReason.find("gap"), std::string::npos);
}

TEST_F(OkxSbeServiceTest, gapReconnectInvalidatesOldSnapshotCallbacks) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto oldBook = service->sbeBooks.at(ws->id).at(streamId);
  snapshot(ws, 100);
  receive(ws, diff(104, 105));
  EXPECT_NE(service->restartReason.find("gap"), std::string::npos);
  auto fresh = connection();
  receive(fresh, diff(200, 201));
  auto count = events.size();
  http::response<http::string_body> response(http::status::ok, 11);
  response.set(http::field::content_type, "application/sbe");
  response.body() = snapshotFrame;
  service->onSbeSnapshot(ws, streamId, oldBook, response);
  EXPECT_EQ(events.size(), count);
  snapshot(fresh, 200);
  EXPECT_EQ(service->sbeBooks.at(fresh->id).at(streamId)->sequence, 201);
  EXPECT_EQ(service->fetches, 2);
}

TEST_F(OkxSbeServiceTest, reusedConnectionRejectsSnapshotFromItsPreviousGeneration) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto oldBook = service->sbeBooks.at(ws->id).at(streamId);
  service->clearStates(ws);
  service->prepareSubscription(ws, ws->subscriptionList.front());
  service->createSendStringList(ws);
  receive(ws, diff(200, 201));
  http::response<http::string_body> response(http::status::ok, 11);
  response.set(http::field::content_type, "application/sbe");
  response.body() = snapshotFrame;
  service->onSbeSnapshot(ws, streamId, oldBook, response);
  EXPECT_TRUE(events.empty());
  snapshot(ws, 200);
  EXPECT_EQ(service->sbeBooks.at(ws->id).at(streamId)->sequence, 201);
}

TEST_F(OkxSbeServiceTest, restJsonErrorsRateLimitsAndBadBinaryAreExplicit) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto book = service->sbeBooks.at(ws->id).at(streamId);
  http::response<http::string_body> response(http::status::too_many_requests, 11);
  response.set(http::field::content_type, "application/json");
  response.body() = R"({"code":"50011","msg":"redact-this-server-text"})";
  service->onSbeSnapshot(ws, streamId, book, response);
  EXPECT_EQ(book->sequence, -1);
  ASSERT_EQ(events.size(), 1);
  auto reason = events.back().getMessageList()[0].getElementList()[0].getValue(CCAPI_ERROR_MESSAGE);
  EXPECT_NE(reason.find("429, code=50011"), std::string::npos);
  EXPECT_EQ(reason.find("redact-this"), std::string::npos);
  response.result(http::status::ok);
  response.set(http::field::content_type, "application/sbe");
  response.body() = snapshotFrame.substr(0, 50);
  service->onSbeSnapshot(ws, streamId, book, response);
  EXPECT_NE(service->restartReason.find("truncated"), std::string::npos);
}

TEST_F(OkxSbeServiceTest, singleLocalLevelKeepsDeeperLevelsForPromotion) {
  auto ws = connection(CCAPI_MARKET_DEPTH, options + "&MARKET_DEPTH_MAX=1&MARKET_DEPTH_RETURN_UPDATE=1");
  receive(ws, diff(100, 102, 100));
  snapshot(ws, 100);
  receive(ws, diff(102, 103));
  const auto& bids = service->snapshotBidByConnectionIdChannelIdSymbolIdMap[ws->id]["books-l2-tbt"]["211874"];
  EXPECT_EQ(bids.size(), 1);
  EXPECT_EQ(bids.at(Decimal("90071992.54740992")), "3");
  ASSERT_FALSE(events.empty());
  EXPECT_EQ(events.back().getMessageList()[0].getElementList()[0].getValue(CCAPI_SEQUENCE_NUMBER), "103");
}

TEST_F(OkxSbeServiceTest, boundsBufferAndStopsPendingSnapshotAndReconnect) {
  auto ws = connection();
  receive(ws, depthFrame);
  auto book = service->sbeBooks.at(ws->id).at(streamId);
  book->buffer.resize(10000, OkxSbe::decode(depthFrame));
  receive(ws, diff(102, 104));
  EXPECT_NE(service->restartReason.find("10000"), std::string::npos);
  auto fresh = connection();
  receive(fresh, depthFrame);
  auto count = events.size();
  service->shouldContinue = false;
  snapshot(fresh, 100);
  receive(fresh, diff(102, 104));
  service->prepareConnect(fresh);
  EXPECT_EQ(service->connections, 0);
  EXPECT_EQ(events.size(), count);
}

TEST_F(OkxSbeServiceTest, acknowledgementsIdentifyActualStreamAndCorrelation) {
  auto ws = connection();
  service->onTextMessage(ws, R"({"event":"subscribe","arg":{"channel":"books-l2-tbt","instIdCode":211874},"connId":"connection"})", now);
  ASSERT_EQ(events.size(), 1);
  const auto& message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getType(), Message::Type::SUBSCRIPTION_STARTED);
  EXPECT_EQ(message.getCorrelationIdList()[0], "correlation");
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_INFO_MESSAGE), "OKX SBE subscribed books-l2-tbt:211874");
}

TEST_F(OkxSbeServiceTest, rejectsControlErrorsAndInvalidOrUnsubscribedFramesWithoutFallback) {
  for (const auto& text : {R"({"event":"error","code":"64003","msg":"secret-server-text"})", "bad-json", R"({"arg":{"channel":"books"},"data":[]})",
                           R"({"event":"notice","code":"64008"})", R"({"event":"subscribe","arg":{"channel":"trades","instIdCode":211874}})"}) {
    auto ws = connection();
    service->onTextMessage(ws, text, now);
    EXPECT_EQ(ws->status, WsConnection::Status::CLOSING);
    EXPECT_EQ(events.back().getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
    EXPECT_EQ(events.back().getMessageList()[0].getCorrelationIdList()[0], "correlation");
    EXPECT_EQ(service->restartReason.find("secret-server-text"), std::string::npos);
  }
  auto wrongInstrument = depthFrame;
  put(wrongInstrument, 8, 211875);
  for (const auto& frame : {depthFrame.substr(0, 20), tradeFrame, snapshotFrame, wrongInstrument}) {
    auto ws = connection();
    receive(ws, frame);
    EXPECT_EQ(ws->status, WsConnection::Status::CLOSING);
  }
}

TEST(OkxSbeSubscription, marketDataOptionsNeverEnableSbeForPrivateOrOtherExchangeSubscriptions) {
  for (auto field : {CCAPI_EM_ORDER_UPDATE, CCAPI_EM_PRIVATE_TRADE, CCAPI_EM_BALANCE_UPDATE, CCAPI_EM_WEBSOCKET_ORDER_ENTRY}) {
    Subscription subscription("okx", "BTC-USDT", field, options);
    EXPECT_EQ(subscription.getServiceName(), CCAPI_EXECUTION_MANAGEMENT);
    EXPECT_FALSE(subscription.isOkxSbe());
  }
  for (auto exchange : {"binance", "binance-usds-futures", "binance-coin-futures"}) {
    EXPECT_FALSE(Subscription(exchange, "BTC-USDT", CCAPI_MARKET_DEPTH, options).isOkxSbe());
  }
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
        keys.push_back(std::string((*request)["OK-ACCESS-KEY"]));
        EXPECT_EQ((*request)["OK-ACCESS-PASSPHRASE"], "test-passphrase");
        auto timestamp = std::string((*request)["OK-ACCESS-TIMESTAMP"]);
        EXPECT_FALSE(timestamp.empty());
        EXPECT_EQ((*request)["OK-ACCESS-SIGN"],
                  UtilAlgorithm::base64Encode(Hmac::hmac(Hmac::ShaVersion::SHA256, "test-secret", timestamp + "GET/users/self/verify")));
        socket->async_accept(*request, [this, socket, request, buffer](const ErrorCode& ec) {
          ASSERT_FALSE(ec) << ec.message();
          socket->async_read(*buffer, [this, socket, buffer](const ErrorCode& ec, size_t) {
            ASSERT_FALSE(ec) << ec.message();
            EXPECT_TRUE(socket->got_text());
            subscriptions.push_back(beast::buffers_to_string(buffer->data()));
            buffer->consume(buffer->size());
            socket->text(true);
            auto control = std::make_shared<std::string>(keys.size() == 1 ? R"({"event":"notice","code":"64008"})"
                                                                          : R"({"event":"subscribe","arg":{"channel":"trades","instIdCode":211874}})");
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

TEST(OkxSbeTransport, handshakeJsonControlsFragmentedBinaryPingPongReconnectAndStop) {
  ServiceContext context;
  auto& io = *context.ioContextPtr;
  LocalSbeServer server(io);
  std::vector<Message> trades;
  bool stopped = false;
  std::shared_ptr<MarketDataServiceOkx> service;
  auto stopWhenComplete = [&] {
    if (!stopped && !trades.empty() && !server.pong.empty()) {
      stopped = true;
      service->stop();
    }
  };
  server.onPong = stopWhenComplete;
  service = std::make_shared<MarketDataServiceOkx>(
      [&](Event& event, Queue<Event>*) {
        for (const auto& message : event.getMessageList()) {
          if (message.getType() == Message::Type::MARKET_DATA_EVENTS_TRADE) {
            trades.push_back(message);
            stopWhenComplete();
          }
          if (message.getType() == Message::Type::SUBSCRIPTION_STARTED) {
            EXPECT_EQ(service->pingTimerByMethodByConnectionIdMap.begin()->second.count(Service::PingPongMethod::WEBSOCKET_APPLICATION_LEVEL), 0);
          }
        }
      },
      SessionOptions{}, SessionConfigs{credentials}, &context);
  Subscription subscription("okx", "BTC-USDT", CCAPI_TRADE, options, "wire-correlation");
  auto url = "ws://127.0.0.1:" + std::to_string(server.acceptor.local_endpoint().port()) + "/ws";
  auto connection = std::make_shared<WsConnection>(url, service->getInstrumentGroup(subscription), std::vector<Subscription>{subscription}, credentials);
  service->setWsConnectionStream(connection);
  server.accept();
  service->prepareConnect(connection);
  io.run_for(std::chrono::seconds(3));
  service->shouldContinue = false;
  service->clearStates(connection);
  EXPECT_TRUE(stopped);
  EXPECT_EQ(server.keys, (std::vector<std::string>{"test-key", "test-key"}));
  ASSERT_EQ(server.subscriptions.size(), 2);
  for (const auto& text : server.subscriptions) {
    rj::Document control;
    control.Parse(text.c_str());
    EXPECT_STREQ(control["op"].GetString(), "subscribe");
    EXPECT_STREQ(control["args"][0]["channel"].GetString(), "trades");
    EXPECT_EQ(control["args"][0]["instIdCode"].GetInt64(), 211874);
  }
  EXPECT_EQ(server.pong, "sbe-heartbeat");
  EXPECT_EQ(server.closes, 2);
  EXPECT_EQ(connection->status, WsConnection::Status::CLOSED);
  ASSERT_EQ(trades.size(), 1);
  EXPECT_EQ(trades[0].getCorrelationIdList()[0], "wire-correlation");
  EXPECT_EQ(trades[0].getTime(), TimePoint(std::chrono::microseconds(eventUs)));
  EXPECT_EQ(trades[0].getElementList().size(), 1);
}

TEST(OkxSbeTransport, rejectedUpgradeReportsAuthenticationStatusAndCorrelation) {
  ServiceContext context;
  auto& io = *context.ioContextPtr;
  tcp::acceptor acceptor(io, {net::ip::make_address("127.0.0.1"), 0});
  beast::tcp_stream socket(io);
  beast::flat_buffer buffer;
  http::request<http::string_body> request;
  http::response<http::string_body> response(http::status::unauthorized, 11);
  response.prepare_payload();
  std::vector<Message> errors;
  auto service = std::make_shared<MarketDataServiceOkx>(
      [&](Event& event, Queue<Event>*) {
        for (const auto& message : event.getMessageList()) {
          if (message.getType() == Message::Type::SUBSCRIPTION_FAILURE) {
            errors.push_back(message);
            io.stop();
          }
        }
      },
      SessionOptions{}, SessionConfigs{credentials}, &context);
  acceptor.async_accept(socket.socket(), [&](const boost::system::error_code& ec) {
    ASSERT_FALSE(ec);
    http::async_read(socket, buffer, request, [&](const boost::system::error_code& ec, size_t) {
      ASSERT_FALSE(ec);
      http::async_write(socket, response, [&](const boost::system::error_code& ec, size_t) { EXPECT_FALSE(ec); });
    });
  });
  Subscription subscription("okx", "BTC-USDT", CCAPI_TRADE, options, "auth-correlation");
  auto connection = std::make_shared<WsConnection>("ws://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/ws",
                                                   service->getInstrumentGroup(subscription), std::vector<Subscription>{subscription}, credentials);
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
  EXPECT_EQ(reason.find("test-key"), std::string::npos);
}

TEST(OkxSbeTransport, stopDuringConnectionUpClosesWithoutSubscribing) {
  ServiceContext context;
  auto& io = *context.ioContextPtr;
  tcp::acceptor acceptor(io, {net::ip::make_address("127.0.0.1"), 0});
  beast::websocket::stream<beast::tcp_stream> peer(io);
  beast::flat_buffer buffer;
  bool closed = false;
  std::shared_ptr<MarketDataServiceOkx> service;
  service = std::make_shared<MarketDataServiceOkx>(
      [&](Event& event, Queue<Event>*) {
        for (const auto& message : event.getMessageList()) {
          if (message.getType() == Message::Type::SESSION_CONNECTION_UP) service->stop();
        }
      },
      SessionOptions{}, SessionConfigs{credentials}, &context);
  acceptor.async_accept(beast::get_lowest_layer(peer).socket(), [&](const boost::system::error_code& ec) {
    ASSERT_FALSE(ec);
    peer.async_accept([&](const boost::system::error_code& ec) {
      ASSERT_FALSE(ec);
      peer.async_read(buffer, [&](const boost::system::error_code& ec, size_t) {
        EXPECT_EQ(ec, beast::websocket::error::closed);
        closed = ec == beast::websocket::error::closed;
      });
    });
  });
  Subscription subscription("okx", "BTC-USDT", CCAPI_TRADE, options, "stop-correlation");
  auto connection = std::make_shared<WsConnection>("ws://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/ws/v5/public-sbe",
                                                   service->getInstrumentGroup(subscription), std::vector<Subscription>{subscription}, credentials);
  service->setWsConnectionStream(connection);
  service->prepareConnect(connection);
  io.run_for(std::chrono::seconds(1));
  EXPECT_TRUE(closed);
  EXPECT_EQ(connection->status, WsConnection::Status::CLOSED);
  EXPECT_FALSE(service->shouldContinue.load());
  EXPECT_TRUE(service->subscriptionListByConnectionIdChannelIdSymbolIdMap.empty());
  service->clearStates(connection);
}
}  // namespace
}  // namespace ccapi
