#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE_USDS_FUTURES
#include "gtest/gtest.h"

#include "ccapi_cpp/service/ccapi_market_data_service_binance_usds_futures.h"

namespace ccapi {

class MarketDataServiceBinanceUsdsFuturesTest : public ::testing::Test {
 public:
  void SetUp() override {
    service = std::make_shared<MarketDataServiceBinanceUsdsFutures>([](Event&, Queue<Event>*) {}, SessionOptions(), sessionConfigs, &serviceContext);
  }

  std::shared_ptr<WsConnection> prepareConnection(const std::string& channelId, const std::string& exchangeSubscriptionId,
                                                  const std::string& symbolId = "btcusdt") {
    auto wsConnectionPtr = std::make_shared<WsConnection>();
    wsConnectionPtr->id = "connection";
    service->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id][exchangeSubscriptionId][CCAPI_CHANNEL_ID] = channelId;
    service->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id][exchangeSubscriptionId][CCAPI_SYMBOL_ID] = symbolId;
    return wsConnectionPtr;
  }

  SessionConfigs sessionConfigs;
  ServiceContext serviceContext;
  std::shared_ptr<MarketDataServiceBinanceUsdsFutures> service;
  TimePoint now{UtilTime::makeTimePointFromMilliseconds(1720000000000)};
};

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, routesEverySupportedFieldByTrafficClass) {
  EXPECT_TRUE(UtilString::startsWith(service->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARKET_DEPTH)),
                                     "wss://fstream.binance.com/public/stream|"));
  for (const auto& field : {CCAPI_AGG_TRADE, CCAPI_CANDLESTICK, CCAPI_MARK_PRICE}) {
    EXPECT_TRUE(UtilString::startsWith(service->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", field)),
                                       "wss://fstream.binance.com/market/stream|"));
  }
  EXPECT_EQ(sessionConfigs.getExchangeFieldWebsocketChannelMap().at(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES).count(CCAPI_TRADE), 0);
  EXPECT_THROW(service->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_TRADE)), std::invalid_argument);
}

TEST(MarketDataServiceBinanceUsdsFuturesConfigTest, customRootIsPreserved) {
  SessionConfigs configs;
  auto roots = configs.getUrlWebsocketBase();
  roots[CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES] = "wss://demo.example.test/root/";
  configs.setUrlWebsocketBase(roots);
  ServiceContext serviceContext;
  auto service = std::make_shared<MarketDataServiceBinanceUsdsFutures>([](Event&, Queue<Event>*) {}, SessionOptions(), configs, &serviceContext);
  EXPECT_TRUE(UtilString::startsWith(service->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARKET_DEPTH)),
                                     "wss://demo.example.test/root/public/stream|"));
  EXPECT_TRUE(UtilString::startsWith(service->getInstrumentGroup(Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_AGG_TRADE)),
                                     "wss://demo.example.test/root/market/stream|"));
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, aggregateTradeParsingPreservesDocumentedFields) {
  auto wsConnectionPtr = prepareConnection("aggTrade", "btcusdt@aggTrade");
  Event event;
  std::vector<MarketDataMessage> list;
  service->processTextMessage(wsConnectionPtr,
                              R"({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1720000000001,"s":"BTCUSDT","a":91,"p":"60000.10","q":"0.25","T":1720000000000,"m":true}})",
                              now, event, list);
  ASSERT_EQ(list.size(), 1);
  EXPECT_EQ(list[0].type, MarketDataMessage::Type::MARKET_DATA_EVENTS_AGG_TRADE);
  const auto& point = list[0].data.at(MarketDataMessage::DataType::AGG_TRADE).at(0);
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::PRICE), "60000.1");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::SIZE), "0.25");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::AGG_TRADE_ID), "91");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::IS_BUYER_MAKER), "1");
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, markPriceParsingSupportsEstimatedSettlementPrice) {
  auto wsConnectionPtr = prepareConnection("markPrice", "btcusdt@markPrice");
  Event event;
  std::vector<MarketDataMessage> list;
  service->processTextMessage(wsConnectionPtr,
                              R"({"stream":"btcusdt@markPrice","data":{"e":"markPriceUpdate","E":1720000000001,"s":"BTCUSDT","p":"60000.1","i":"59990.2","P":"59995.3","r":"0.0001","T":1720003600000}})",
                              now, event, list);
  ASSERT_EQ(list.size(), 1);
  const auto& point = list[0].data.at(MarketDataMessage::DataType::MARK_PRICE).at(0);
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::SYMBOL), "BTCUSDT");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::MARK_PRICE), "60000.1");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::INDEX_PRICE), "59990.2");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::ESTIMATED_SETTLEMENT_PRICE), "59995.3");
  EXPECT_EQ(point.at(MarketDataMessage::DataFieldType::FUNDING_RATE), "0.0001");

  list.clear();
  service->processTextMessage(wsConnectionPtr,
                              R"({"stream":"btcusdt@markPrice","data":{"e":"markPriceUpdate","E":1720000000002,"s":"BTCUSDT","p":"60000.2","i":"59990.3","r":"0.0002","T":1720003600000}})",
                              now, event, list);
  ASSERT_EQ(list.size(), 1);
  const auto& pointWithoutSettlement = list[0].data.at(MarketDataMessage::DataType::MARK_PRICE).at(0);
  EXPECT_EQ(pointWithoutSettlement.count(MarketDataMessage::DataFieldType::ESTIMATED_SETTLEMENT_PRICE), 0);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, markPriceCadenceSupportsNormalAndOneSecond) {
  auto normal = std::make_shared<WsConnection>();
  normal->id = "normal";
  std::string normalChannel = CCAPI_WEBSOCKET_BINANCE_BASE_CHANNEL_MARK_PRICE;
  std::string symbol = "btcusdt";
  Subscription normalSubscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARK_PRICE);
  service->prepareSubscriptionDetail(normalChannel, symbol, CCAPI_MARK_PRICE, normal, normalSubscription, normalSubscription.getOptionMap());
  EXPECT_EQ(normalChannel, "markPrice");

  auto oneSecond = std::make_shared<WsConnection>();
  oneSecond->id = "one-second";
  std::string oneSecondChannel = CCAPI_WEBSOCKET_BINANCE_BASE_CHANNEL_MARK_PRICE;
  Subscription oneSecondSubscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARK_PRICE,
                                     std::string(CCAPI_MARK_PRICE_UPDATE_SPEED_MILLISECONDS) + "=1000");
  service->prepareSubscriptionDetail(oneSecondChannel, symbol, CCAPI_MARK_PRICE, oneSecond, oneSecondSubscription,
                                     oneSecondSubscription.getOptionMap());
  EXPECT_EQ(oneSecondChannel, "markPrice@1s");
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, subscriptionErrorIsCorrelated) {
  auto wsConnectionPtr = prepareConnection("aggTrade", "btcusdt@aggTrade");
  service->correlationIdListByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id]["aggTrade"]["btcusdt"] = {"corr"};
  service->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap[wsConnectionPtr->id][7] = {"btcusdt@aggTrade"};
  Event event;
  std::vector<MarketDataMessage> list;
  service->processTextMessage(wsConnectionPtr, R"({"code":2,"msg":"Invalid request","id":7})", now, event, list);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
  ASSERT_EQ(event.getMessageList()[0].getCorrelationIdList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getCorrelationIdList()[0], "corr");
  EXPECT_EQ(service->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap[wsConnectionPtr->id].count(7), 0);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, subscriptionAcknowledgementIsCorrelated) {
  auto wsConnectionPtr = prepareConnection("aggTrade", "btcusdt@aggTrade");
  service->correlationIdListByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id]["aggTrade"]["btcusdt"] = {"corr"};
  service->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap[wsConnectionPtr->id][8] = {"btcusdt@aggTrade"};
  Event event;
  std::vector<MarketDataMessage> list;
  service->processTextMessage(wsConnectionPtr, R"({"result":null,"id":8})", now, event, list);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_STARTED);
  EXPECT_EQ(event.getMessageList()[0].getCorrelationIdList().at(0), "corr");
  EXPECT_EQ(service->exchangeSubscriptionIdListByConnectionIdExchangeJsonPayloadIdMap[wsConnectionPtr->id].count(8), 0);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, liveDepthRequiresContiguousPreviousUpdateId) {
  auto wsConnectionPtr = std::make_shared<WsConnection>();
  wsConnectionPtr->id = "depth";
  const std::string channel = "depth";
  const std::string symbol = "btcusdt";
  const std::string exchangeSubscriptionId = "btcusdt@depth";
  service->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channel][symbol] = true;
  service->orderbookVersionIdByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id][exchangeSubscriptionId] = 100;
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARKET_DEPTH);
  MarketDataMessage message;
  std::vector<MarketDataMessage> output;
  EXPECT_TRUE(service->processOrderBookWithVersionId(101, wsConnectionPtr, channel, symbol, exchangeSubscriptionId, subscription.getOptionMap(), output,
                                                     message, 101, 100));
  ASSERT_EQ(output.size(), 1);
  output.clear();
  EXPECT_FALSE(service->processOrderBookWithVersionId(103, wsConnectionPtr, channel, symbol, exchangeSubscriptionId, subscription.getOptionMap(), output,
                                                      message, 103, 102));
  EXPECT_TRUE(output.empty());
  EXPECT_FALSE(service->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channel][symbol]);

  Subscription recoverySubscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_MARKET_DEPTH,
                                    std::string(CCAPI_FETCH_MARKET_DEPTH_INITIAL_SNAPSHOT_DELAY_MILLISECONDS) + "=60000");
  EXPECT_TRUE(service->processOrderBookWithVersionId(104, wsConnectionPtr, channel, symbol, exchangeSubscriptionId,
                                                     recoverySubscription.getOptionMap(), output, message, 104, 103));
  EXPECT_TRUE(output.empty());
  EXPECT_FALSE(service->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[wsConnectionPtr->id][channel][symbol]);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, snapshotBridgeDiscardsStaleRangesAndValidatesThePuChain) {
  std::map<int64_t, MarketDataService::BufferedOrderBookUpdate> buffer;
  buffer[90] = {80, 90, 79, {}};
  buffer[105] = {95, 105, 90, {}};
  buffer[110] = {106, 110, 105, {}};
  EXPECT_EQ(service->findOrderBookSnapshotBridge(100, buffer), 105);
  EXPECT_TRUE(service->isOrderBookUpdateChainContiguous(105, buffer));
  buffer[110].previousFinalUpdateId = 104;
  EXPECT_FALSE(service->isOrderBookUpdateChainContiguous(105, buffer));

  buffer.clear();
  buffer[110] = {105, 110, 100, {}};
  buffer[115] = {95, 115, 110, {}};
  EXPECT_EQ(service->findOrderBookSnapshotBridge(100, buffer), -1);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, snapshotWithoutBridgeIsRejected) {
  std::map<int64_t, MarketDataService::BufferedOrderBookUpdate> buffer;
  buffer[99] = {90, 99, 89, {}};
  buffer[110] = {105, 110, 99, {}};
  EXPECT_EQ(service->findOrderBookSnapshotBridge(100, buffer), -1);
}

TEST_F(MarketDataServiceBinanceUsdsFuturesTest, oldRecoveryGenerationCannotInitializeReplacementConnection) {
  auto wsConnectionPtr = std::make_shared<WsConnection>();
  wsConnectionPtr->id = "generation";
  service->orderBookRecoveryGenerationByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id]["btcusdt@depth"] = 11;
  EXPECT_TRUE(service->isOrderBookRecoveryGenerationCurrent(wsConnectionPtr->id, "btcusdt@depth", 11));
  service->orderBookRecoveryGenerationByConnectionIdExchangeSubscriptionIdMap[wsConnectionPtr->id]["btcusdt@depth"] = 12;
  EXPECT_FALSE(service->isOrderBookRecoveryGenerationCurrent(wsConnectionPtr->id, "btcusdt@depth", 11));
}

}  // namespace ccapi
#endif
#endif
