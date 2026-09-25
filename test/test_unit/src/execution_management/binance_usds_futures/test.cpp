#ifdef CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE_USDS_FUTURES
// clang-format off
#include <algorithm>

#include "gtest/gtest.h"
#include "ccapi_cpp/ccapi_test_execution_management_helper.h"
#include "ccapi_cpp/service/ccapi_execution_management_service_binance_usds_futures.h"

// clang-format on

namespace ccapi {

class ExecutionManagementServiceBinanceUsdsFuturesTest : public ::testing::Test {
 public:
  typedef Service::ServiceContextPtr ServiceContextPtr;

  void SetUp() override {
    this->service =
        std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>([](Event&, Queue<Event>*) {}, SessionOptions(), SessionConfigs(), &this->serviceContext);
    this->credential = {
        {CCAPI_BINANCE_USDS_FUTURES_API_KEY, "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"},
        {CCAPI_BINANCE_USDS_FUTURES_API_SECRET, "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"},
    };
    this->timestamp = 1499827319559;
    this->now = UtilTime::makeTimePointFromMilliseconds(this->timestamp);
  }

  ServiceContext serviceContext;
  std::shared_ptr<ExecutionManagementServiceBinanceUsdsFutures> service{nullptr};
  std::map<std::string, std::string> credential;
  long long timestamp{};
  TimePoint now{};
};

void verifyApiKey(const http::request<http::string_body>& req, const std::string& apiKey) { EXPECT_EQ(std::string(req.base().at("X-MBX-APIKEY")), apiKey); }

void verifySignature(const std::string& paramString, const std::string& apiSecret) {
  auto pos = paramString.find_last_of("&");
  auto paramStringWithoutSignature = paramString.substr(0, pos);
  auto signature = paramString.substr(pos + 11, paramString.length() - pos - 1);
  EXPECT_EQ(Hmac::hmac(Hmac::ShaVersion::SHA256, apiSecret, paramStringWithoutSignature, true), signature);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, convertTextMessageToMessageRestGetOrder) {
  Request request(Request::Operation::GET_ORDER, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "foo", this->credential);
  std::string textMessage =
      R"(
  {
    "avgPrice": "0.00000",
    "clientOrderId": "abc",
    "cumQuote": "0.01",
    "executedQty": "0",
    "orderId": 1917641,
    "origQty": "0.40",
    "origType": "TRAILING_STOP_MARKET",
    "price": "0",
    "reduceOnly": false,
    "side": "BUY",
    "positionSide": "SHORT",
    "status": "NEW",
    "stopPrice": "9300",
    "closePosition": false,
    "symbol": "BTCUSDT",
    "time": 1579276756075,
    "timeInForce": "GTC",
    "type": "TRAILING_STOP_MARKET",
    "activatePrice": "9020",
    "priceRate": "0.3",
    "updateTime": 1579276756075,
    "workingType": "CONTRACT_PRICE",
    "priceProtect": false
  }
  )";
  auto messageList = this->service->convertTextMessageToMessageRest(request, textMessage, this->now);
  EXPECT_EQ(messageList.size(), 1);
  auto message = messageList.at(0);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 1);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY), "0.01");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, createEventExecutionTypeTrade) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_EM_PRIVATE_TRADE);
  std::string textMessage = R"(
    {
  "e": "ORDER_TRADE_UPDATE",
  "T": 1625560320441,
  "E": 1625560320443,
  "o": {
    "s": "BTCUSDT",
    "c": "LyfbomrL94skVnLNNCsoNr",
    "S": "SELL",
    "o": "LIMIT",
    "f": "GTC",
    "q": "1",
    "p": "30000",
    "ap": "34704.74000",
    "sp": "0",
    "x": "TRADE",
    "X": "FILLED",
    "i": 2732542295,
    "l": "1",
    "z": "1",
    "L": "34704.74",
    "n": "13.88189600",
    "N": "USDT",
    "T": 1625560320441,
    "t": 185523342,
    "b": "0",
    "a": "0",
    "m": false,
    "R": false,
    "wt": "CONTRACT_PRICE",
    "ot": "LIMIT",
    "ps": "BOTH",
    "cp": false,
    "rp": "0",
    "pP": false,
    "si": 0,
    "ss": 0
  }
}
)";
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), wspp::lib::weak_ptr<void>(), subscription, textMessage, document, this->now)
                         .getMessageList();
#else
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), subscription, textMessage, document, this->now).getMessageList();
#endif

  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, subscription.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_PRIVATE_TRADE);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 1);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_TRADE_ID), "185523342");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE), "34704.74");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE), "1");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_SIDE), CCAPI_EM_ORDER_SIDE_SELL);
  EXPECT_EQ(element.getValue(CCAPI_IS_MAKER), "0");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_ID), "2732542295");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_INSTRUMENT), "BTCUSDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_FEE_QUANTITY), "13.88189600");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_FEE_ASSET), "USDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_REALIZED_PNL), "0");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_REDUCE_ONLY), "0");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_SIDE), "BOTH");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_EXECUTION_TYPE), "TRADE");
  EXPECT_EQ(element.getValue(CCAPI_EVENT_TIME_MILLISECONDS), "1625560320443");
  EXPECT_EQ(element.getValue(CCAPI_TRANSACTION_TIME_MILLISECONDS), "1625560320441");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, createEventBalanceUpdate) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_EM_BALANCE_UPDATE);
  std::string textMessage = R"(
    {
  "e": "ACCOUNT_UPDATE",
  "E": 1564745798939,
  "T": 1564745798938,
  "a": {
    "m": "ORDER",
    "B": [
      {
        "a": "USDT",
        "wb": "122624.12345678",
        "cw": "100.12345678",
        "bc": "50.12345678"
      },
      {
        "a": "BUSD",
        "wb": "1.00000000",
        "cw": "0.00000000",
        "bc": "-49.12345678"
      }
    ]
  }
}
)";
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), subscription, textMessage, document, this->now).getMessageList();
  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, subscription.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_BALANCE_UPDATE);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 2);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_EM_ASSET), "USDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_QUANTITY_TOTAL), "122624.12345678");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, createEventPositionUpdate) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_EM_POSITION_UPDATE);
  std::string textMessage = R"(
    {
  "e": "ACCOUNT_UPDATE",
  "E": 1564745798939,
  "T": 1564745798938,
  "a": {
    "m": "ORDER",
    "P": [
      {
        "s": "BTCUSDT",
        "pa": "0",
        "ep": "0.00000",
        "bep": "0",
        "cr": "200",
        "up": "0",
        "mt": "isolated",
        "iw": "0.00000000",
        "ps": "BOTH"
      },
      {
        "s": "BTCUSDT",
        "pa": "20",
        "ep": "6563.66500",
        "bep": "6563.6",
        "cr": "0",
        "up": "2850.21200",
        "mt": "isolated",
        "iw": "13200.70726908",
        "ps": "LONG"
      },
      {
        "s": "BTCUSDT",
        "pa": "-10",
        "ep": "6563.86000",
        "bep": "6563.6",
        "cr": "-45.04000000",
        "up": "-1423.15600",
        "mt": "isolated",
        "iw": "6570.42511771",
        "ps": "SHORT"
      }
    ]
  }
}
)";
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), subscription, textMessage, document, this->now).getMessageList();
  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, subscription.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_POSITION_UPDATE);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 3);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_INSTRUMENT), "BTCUSDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_SIDE), "BOTH");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_QUANTITY), "0");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_ENTRY_PRICE), "0.00000");
  EXPECT_EQ(element.getValue(CCAPI_EM_UNREALIZED_PNL), "0");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, createEventExecutionTypeNew) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_EM_ORDER_UPDATE);
  std::string textMessage = R"(
    {
      "e": "ORDER_TRADE_UPDATE",
      "T": 1625559810396,
      "E": 1625559810398,
      "o": {
        "s": "BTCUSDT",
        "c": "swEILRb7r1nZaIg3jeMljV",
        "S": "SELL",
        "o": "LIMIT",
        "f": "GTC",
        "q": "1",
        "p": "30000",
        "ap": "0",
        "sp": "0",
        "x": "NEW",
        "X": "NEW",
        "i": 2732541457,
        "l": "0",
        "z": "0",
        "L": "0",
        "T": 1625559810396,
        "t": 0,
        "b": "0",
        "a": "0",
        "m": false,
        "R": false,
        "wt": "CONTRACT_PRICE",
        "ot": "LIMIT",
        "ps": "BOTH",
        "cp": false,
        "rp": "0",
        "pP": false,
        "si": 0,
        "ss": 0
      }
    }
)";
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), wspp::lib::weak_ptr<void>(), subscription, textMessage, document, this->now)
                         .getMessageList();
#else
  auto messageList = this->service->createEvent(std::make_shared<WsConnection>(), subscription, textMessage, document, this->now).getMessageList();
#endif
  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, subscription.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 1);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_ID), "2732541457");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_SIDE), CCAPI_EM_ORDER_SIDE_SELL);
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_LIMIT_PRICE), "30000");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_QUANTITY), "1");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY), "0");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_STATUS), "NEW");
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_INSTRUMENT), "BTCUSDT");
  EXPECT_TRUE(element.getValue(CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY).empty());
  EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_AVERAGE_FILLED_PRICE), "0");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, convertRequestGetAccountBalances) {
  Request request(Request::Operation::GET_ACCOUNT_BALANCES, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", "foo", this->credential);
  auto req = this->service->convertRequest(request, this->now);
  EXPECT_EQ(req.method(), http::verb::get);
  verifyApiKey(req, this->credential.at(CCAPI_BINANCE_USDS_FUTURES_API_KEY));
  auto splitted = UtilString::split(std::string(req.target()), "?");
  EXPECT_EQ(splitted.at(0), "/fapi/v3/account");
  auto paramMap = Url::convertQueryStringToMap(splitted.at(1));
  EXPECT_EQ(paramMap.at("timestamp"), std::to_string(this->timestamp));
  verifySignature(splitted.at(1), this->credential.at(CCAPI_BINANCE_USDS_FUTURES_API_SECRET));
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, convertTextMessageToMessageRestGetAccountBalances) {
  Request request(Request::Operation::GET_ACCOUNT_BALANCES, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", "foo", this->credential);
  std::string textMessage =
      R"(
        {
            "feeTier": 0,
            "canTrade": true,
            "canDeposit": true,
            "canWithdraw": true,
            "updateTime": 0,
            "totalInitialMargin": "0.00000000",
            "totalMaintMargin": "0.00000000",
            "totalWalletBalance": "23.72469206",
            "totalUnrealizedProfit": "0.00000000",
            "totalMarginBalance": "23.72469206",
            "totalPositionInitialMargin": "0.00000000",
            "totalOpenOrderInitialMargin": "0.00000000",
            "totalCrossWalletBalance": "23.72469206",
            "totalCrossUnPnl": "0.00000000",
            "availableBalance": "23.72469206",
            "maxWithdrawAmount": "23.72469206",
            "assets": [
                {
                    "asset": "USDT",
                    "walletBalance": "23.72469206",
                    "unrealizedProfit": "0.00000000",
                    "marginBalance": "23.72469206",
                    "maintMargin": "0.00000000",
                    "initialMargin": "0.00000000",
                    "positionInitialMargin": "0.00000000",
                    "openOrderInitialMargin": "0.00000000",
                    "crossWalletBalance": "23.72469206",
                    "crossUnPnl": "0.00000000",
                    "availableBalance": "23.72469206",
                    "maxWithdrawAmount": "23.72469206",
                    "marginAvailable": true,
                    "updateTime": 1625474304765
                },
                {
                    "asset": "BUSD",
                    "walletBalance": "103.12345678",
                    "unrealizedProfit": "0.00000000",
                    "marginBalance": "103.12345678",
                    "maintMargin": "0.00000000",
                    "initialMargin": "0.00000000",
                    "positionInitialMargin": "0.00000000",
                    "openOrderInitialMargin": "0.00000000",
                    "crossWalletBalance": "103.12345678",
                    "crossUnPnl": "0.00000000",
                    "availableBalance": "103.12345678",
                    "maxWithdrawAmount": "103.12345678",
                    "marginAvailable": true,
                    "updateTime": 1625474304765
                }
            ],
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "initialMargin": "0",
                    "maintMargin": "0",
                    "unrealizedProfit": "0.00000000",
                    "positionInitialMargin": "0",
                    "openOrderInitialMargin": "0",
                    "leverage": "100",
                    "isolated": true,
                    "entryPrice": "0.00000",
                    "maxNotional": "250000",
                    "positionSide": "BOTH",
                    "positionAmt": "0",
                    "updateTime": 0
                }
            ]
        }
  )";
  auto messageList = this->service->convertTextMessageToMessageRest(request, textMessage, this->now);
  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, request.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::GET_ACCOUNT_BALANCES);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 2);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_EM_ASSET), "USDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING), "23.72469206");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, convertRequestGetAccountPositions) {
  Request request(Request::Operation::GET_ACCOUNT_POSITIONS, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", "foo", this->credential);
  auto req = this->service->convertRequest(request, this->now);
  EXPECT_EQ(req.method(), http::verb::get);
  verifyApiKey(req, this->credential.at(CCAPI_BINANCE_USDS_FUTURES_API_KEY));
  auto splitted = UtilString::split(std::string(req.target()), "?");
  EXPECT_EQ(splitted.at(0), "/fapi/v3/positionRisk");
  auto paramMap = Url::convertQueryStringToMap(splitted.at(1));
  EXPECT_EQ(paramMap.at("timestamp"), std::to_string(this->timestamp));
  verifySignature(splitted.at(1), this->credential.at(CCAPI_BINANCE_USDS_FUTURES_API_SECRET));
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, convertTextMessageToMessageRestGetAccountPositions) {
  Request request(Request::Operation::GET_ACCOUNT_POSITIONS, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", "foo", this->credential);
  std::string textMessage =
      R"(
        [
            {
                "symbol": "BTCUSDT",
                "initialMargin": "0",
                "maintMargin": "0",
                "unrealizedProfit": "0.00000000",
                "positionInitialMargin": "0",
                "openOrderInitialMargin": "0",
                "isolated": true,
                "entryPrice": "0.00000",
                "maxNotional": "250000",
                "positionSide": "BOTH",
                "positionAmt": "10",
                "updateTime": 0
            }
        ]
  )";
  auto messageList = this->service->convertTextMessageToMessageRest(request, textMessage, this->now);
  EXPECT_EQ(messageList.size(), 1);
  verifyCorrelationId(messageList, request.getCorrelationId());
  auto message = messageList.at(0);
  EXPECT_EQ(message.getType(), Message::Type::GET_ACCOUNT_POSITIONS);
  auto elementList = message.getElementList();
  EXPECT_EQ(elementList.size(), 1);
  Element element = elementList.at(0);
  EXPECT_EQ(element.getValue(CCAPI_INSTRUMENT), "BTCUSDT");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_SIDE), "BOTH");
  EXPECT_EQ(element.getValue(CCAPI_EM_POSITION_QUANTITY), "10");
  EXPECT_DOUBLE_EQ(std::stod(element.getValue(CCAPI_EM_POSITION_ENTRY_PRICE)), 0);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, productionRoutesSeparatePrivateAndOrderEntrySockets) {
  EXPECT_EQ(service->baseUrlWs, "wss://fstream.binance.com/private/ws");
  EXPECT_EQ(service->createListenKeyWebsocketUrl("listen-key"), "wss://fstream.binance.com/private/ws/listen-key");
  EXPECT_EQ(service->baseUrlWsOrderEntry, "wss://ws-fapi.binance.com/ws-fapi/v1");
  EXPECT_FALSE(service->useWebsocketOrderEntryConnection({CCAPI_EM_ORDER_UPDATE, CCAPI_EM_PRIVATE_TRADE, CCAPI_EM_PRIVATE_TRADE_LITE,
                                                          CCAPI_EM_BALANCE_UPDATE, CCAPI_EM_POSITION_UPDATE}));
  EXPECT_TRUE(service->useWebsocketOrderEntryConnection({CCAPI_EM_WEBSOCKET_ORDER_ENTRY}));
  Subscription accountSubscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_ORDER_UPDATE, "", "account-events");
  auto privateConnection = std::make_shared<WsConnection>(service->createListenKeyWebsocketUrl("listen-key"), "",
                                                          std::vector<Subscription>{accountSubscription}, credential);
  service->connectionRoleByConnectionIdMap[privateConnection->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::PRIVATE_LISTEN_KEY;
  EXPECT_FALSE(service->shouldRegisterWebsocketConnectionOnOpen(privateConnection));
}

TEST(ExecutionManagementServiceBinanceUsdsFuturesConfigTest, customEndpointsRemainIndependentAndOrderEntryPathIsNotDuplicated) {
  SessionConfigs configs;
  auto websocketRoots = configs.getUrlWebsocketBase();
  websocketRoots[CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES] = "wss://testnet.binancefuture.com";
  configs.setUrlWebsocketBase(websocketRoots);
  auto orderEntryRoots = configs.getUrlWebsocketOrderEntryBase();
  orderEntryRoots[CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES] = "wss://testnet.binancefuture.com/ws-fapi/v1";
  configs.setUrlWebsocketOrderEntryBase(orderEntryRoots);
  auto restRoots = configs.getUrlRestBase();
  restRoots[CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES] = "https://demo-fapi.example.test";
  configs.setUrlRestBase(restRoots);
  ServiceContext serviceContext;
  auto service = std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>([](Event&, Queue<Event>*) {}, SessionOptions(), configs, &serviceContext);
  EXPECT_EQ(service->baseUrlWs, "wss://testnet.binancefuture.com/private/ws");
  EXPECT_EQ(service->baseUrlWsOrderEntry, "wss://testnet.binancefuture.com/ws-fapi/v1");
  EXPECT_EQ(service->baseUrlRest, "https://demo-fapi.example.test");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, authorizationControlsOrderEntryReadiness) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY, "", "order-entry");
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  service->correlationIdByConnectionIdMap[wsConnectionPtr->id] = subscription.getCorrelationId();
  EXPECT_FALSE(service->shouldRegisterWebsocketConnectionOnOpen(wsConnectionPtr));
  EXPECT_EQ(service->wsConnectionPtrByCorrelationIdMap.count(subscription.getCorrelationId()), 0);

  rj::Document success;
  success.Parse<rj::kParseNumbersAsStringsFlag>(R"({"id":"session_logon","status":200,"result":{"apiKey":"key"}})");
  auto event = service->createEvent(wsConnectionPtr, subscription, "", success, now);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::AUTHORIZATION_SUCCESS);
  EXPECT_EQ(service->wsConnectionPtrByCorrelationIdMap.count(subscription.getCorrelationId()), 1);

  rj::Document revoked;
  revoked.Parse<rj::kParseNumbersAsStringsFlag>(R"({"id":null,"status":401,"error":{"code":-2015,"msg":"revoked"}})");
  event = service->createEvent(wsConnectionPtr, subscription, "", revoked, now);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::AUTHORIZATION_FAILURE);
  EXPECT_EQ(service->wsConnectionPtrByCorrelationIdMap.count(subscription.getCorrelationId()), 0);

  rj::Document rejected;
  rejected.Parse<rj::kParseNumbersAsStringsFlag>(R"({"id":"session_logon","status":401,"error":{"code":-2015,"msg":"bad key"}})");
  event = service->createEvent(wsConnectionPtr, subscription, "", rejected, now);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::AUTHORIZATION_FAILURE);
}

TEST(ExecutionManagementServiceBinanceUsdsFuturesLifecycleTest, invalidOrderEntryKeyMaterialFailsAuthorizationCleanly) {
  for (const auto& credential :
       {std::map<std::string, std::string>{},
        std::map<std::string, std::string>{{CCAPI_BINANCE_USDS_FUTURES_WEBSOCKET_ORDER_ENTRY_API_KEY, "key"},
                                           {CCAPI_BINANCE_USDS_FUTURES_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PATH,
                                            "/path/that/does/not/exist"}}}) {
    std::vector<Message> messages;
    ServiceContext serviceContext;
    auto service = std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>(
        [&messages](Event& event, Queue<Event>*) { messages.insert(messages.end(), event.getMessageList().begin(), event.getMessageList().end()); },
        SessionOptions(), SessionConfigs(), &serviceContext);
    Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY, "", "order-entry");
    auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
    service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;

    EXPECT_NO_THROW(service->onOpen(wsConnectionPtr));
    auto failureIt = std::find_if(messages.begin(), messages.end(), [](const Message& message) {
      return message.getType() == Message::Type::AUTHORIZATION_FAILURE;
    });
    ASSERT_NE(failureIt, messages.end());
    EXPECT_EQ(failureIt->getCorrelationIdList().at(0), "order-entry");
    EXPECT_EQ(service->wsConnectionPtrByCorrelationIdMap.count("order-entry"), 0);
  }
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, orderEntryResponseIsCorrelatedAndReleasedExactlyOnce) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY, "", "order-entry");
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  service->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id][1] = "request-correlation";
  rj::Document response;
  response.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"id":"order_place1","status":200,"result":{"orderId":123,"clientOrderId":"client-1","symbol":"BTCUSDT","side":"BUY","origQty":"0.1","price":"60000","executedQty":"0","cumQuote":"0","status":"NEW","updateTime":1720000000000}})");
  auto event = service->createEvent(wsConnectionPtr, subscription, "", response, now);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::CREATE_ORDER);
  EXPECT_EQ(event.getMessageList()[0].getCorrelationIdList().at(0), "request-correlation");
  EXPECT_EQ(event.getMessageList()[0].getElementList().at(0).getValue(CCAPI_EM_CLIENT_ORDER_ID), "client-1");
  EXPECT_EQ(service->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id].count(1), 0);
  EXPECT_TRUE(service->createEvent(wsConnectionPtr, subscription, "", response, now).getMessageList().empty());
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, unknownAndMalformedOrderEntryResponsesDoNotCreateEmptyMessages) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY);
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  for (const auto& json : {R"({"id":99,"status":200,"result":{}})", R"({"id":"order_place-not-a-number","status":200,"result":{}})"}) {
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(json);
    EXPECT_TRUE(service->createEvent(wsConnectionPtr, subscription, json, document, now).getMessageList().empty());
  }
  rj::Document malformed;
  malformed.Parse<rj::kParseNumbersAsStringsFlag>(R"([])");
  auto event = service->createEvent(wsConnectionPtr, subscription, "[]", malformed, now);
  ASSERT_EQ(event.getMessageList().size(), 1);
  EXPECT_EQ(event.getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, accountUpdateToleratesMissingArrays) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", std::string(CCAPI_EM_BALANCE_UPDATE) + "," + CCAPI_EM_POSITION_UPDATE);
  rj::Document neither;
  neither.Parse<rj::kParseNumbersAsStringsFlag>(R"({"e":"ACCOUNT_UPDATE","E":1720000000001,"T":1720000000000,"a":{"m":"FUNDING_FEE"}})");
  EXPECT_TRUE(service->createEvent(std::make_shared<WsConnection>(), subscription, "", neither, now).getMessageList().empty());

  rj::Document both;
  both.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"e":"ACCOUNT_UPDATE","E":1720000000001,"T":1720000000000,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"10","cw":"8","bc":"2"}],"P":[{"s":"BTCUSDT","ps":"BOTH","pa":"1","ep":"60000","bep":"59900","cr":"5","up":"3","mt":"isolated","iw":"100"}]}})");
  auto messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", both, now).getMessageList();
  ASSERT_EQ(messages.size(), 2);
  const auto& balance = messages[0].getElementList().at(0);
  EXPECT_EQ(balance.getValue(CCAPI_EM_ACCOUNT_UPDATE_REASON), "ORDER");
  EXPECT_EQ(balance.getValue(CCAPI_EM_CROSS_WALLET_BALANCE), "8");
  EXPECT_EQ(balance.getValue(CCAPI_EM_BALANCE_CHANGE), "2");
  EXPECT_EQ(balance.getValue(CCAPI_TRANSACTION_TIME_MILLISECONDS), "1720000000000");
  const auto& position = messages[1].getElementList().at(0);
  EXPECT_EQ(position.getValue(CCAPI_EM_POSITION_BREAK_EVEN_PRICE), "59900");
  EXPECT_EQ(position.getValue(CCAPI_EM_POSITION_ACCUMULATED_REALIZED_PNL), "5");
  EXPECT_EQ(position.getValue(CCAPI_EM_POSITION_ISOLATED_WALLET), "100");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, accountConfigAndMarginCallAreSurfaced) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_POSITION_UPDATE);
  rj::Document config;
  config.Parse<rj::kParseNumbersAsStringsFlag>(R"({"e":"ACCOUNT_CONFIG_UPDATE","E":1720000000001,"T":1720000000000,"ac":{"s":"BTCUSDT","l":20}})");
  auto messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", config, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_ACCOUNT_CONFIG_UPDATE);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_EM_POSITION_LEVERAGE), "20");

  rj::Document multiAssets;
  multiAssets.Parse<rj::kParseNumbersAsStringsFlag>(R"({"e":"ACCOUNT_CONFIG_UPDATE","E":1720000000001,"T":1720000000000,"ai":{"j":true}})");
  messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", multiAssets, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_EM_MULTI_ASSETS_MODE), "1");

  rj::Document marginCall;
  marginCall.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"e":"MARGIN_CALL","E":1720000000001,"cw":"100","p":[{"s":"BTCUSDT","ps":"BOTH","pa":"1","mt":"cross","iw":"0","mp":"59000","up":"-100"}]})");
  messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", marginCall, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_MARGIN_CALL);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_MARK_PRICE_VALUE), "59000");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, restBuildersPreserveRecvWindowAndDoNotSendSymbolsToAccount) {
  Request getOpen(Request::Operation::GET_OPEN_ORDERS, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "get", credential);
  getOpen.appendParam({{"recvWindow", "5000"}});
  auto req = service->convertRequest(getOpen, now);
  EXPECT_EQ(Url::convertQueryStringToMap(UtilString::split(std::string(req.target()), "?").at(1)).at("recvWindow"), "5000");

  Request cancelAll(Request::Operation::CANCEL_OPEN_ORDERS, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "cancel", credential);
  cancelAll.appendParam({{"recvWindow", "6000"}});
  req = service->convertRequest(cancelAll, now);
  EXPECT_EQ(Url::convertQueryStringToMap(UtilString::split(std::string(req.target()), "?").at(1)).at("recvWindow"), "6000");

  Request account(Request::Operation::GET_ACCOUNT_BALANCES, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "account", credential);
  req = service->convertRequest(account, now);
  EXPECT_EQ(std::string(req.target()).find("symbols="), std::string::npos);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, cancelAllResponseHasOperationSpecificShape) {
  Request request(Request::Operation::CANCEL_OPEN_ORDERS, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "cancel-all", credential);
  auto messages = service->convertTextMessageToMessageRest(request, R"({"code":200,"msg":"The operation of cancel all open order is done."})", now);
  ASSERT_EQ(messages.size(), 1);
  ASSERT_EQ(messages[0].getElementList().size(), 1);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_HTTP_STATUS_CODE), "200");
  EXPECT_FALSE(messages[0].getElementList()[0].getValue(CCAPI_INFO_MESSAGE).empty());
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, nonIdempotentRestOperationsAreNeverAutomaticallyRetried) {
  for (const auto operation : {Request::Operation::CREATE_ORDER, Request::Operation::CANCEL_ORDER, Request::Operation::CANCEL_OPEN_ORDERS}) {
    Request request(operation, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "corr", credential);
    EXPECT_FALSE(service->shouldRetryRequest(request));
    EXPECT_EQ(service->requestFailureMessageType(request), Message::Type::REQUEST_OUTCOME_UNKNOWN);
  }
}

TEST(ExecutionManagementServiceBinanceUsdsFuturesLifecycleTest, mixedOrderEntryAndAccountSubscriptionFailsBeforeConnecting) {
  std::vector<Message> messages;
  ServiceContext serviceContext;
  auto service = std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>(
      [&messages](Event& event, Queue<Event>*) { messages.insert(messages.end(), event.getMessageList().begin(), event.getMessageList().end()); },
      SessionOptions(), SessionConfigs(), &serviceContext);
  std::vector<Subscription> subscriptions{Subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT",
                                                       std::string(CCAPI_EM_WEBSOCKET_ORDER_ENTRY) + "," + CCAPI_EM_ORDER_UPDATE, "", "mixed")};
  service->subscribe(subscriptions);
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
  EXPECT_EQ(messages[0].getCorrelationIdList().at(0), "mixed");
}

TEST(ExecutionManagementServiceBinanceUsdsFuturesLifecycleTest, sendBeforeAuthorizationFailsWithRequestCorrelation) {
  std::vector<Message> messages;
  ServiceContext serviceContext;
  auto service = std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>(
      [&messages](Event& event, Queue<Event>*) { messages.insert(messages.end(), event.getMessageList().begin(), event.getMessageList().end()); },
      SessionOptions(), SessionConfigs(), &serviceContext);
  Request request(Request::Operation::CREATE_ORDER, CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", "order-request");
  service->sendRequestByWebsocket("order-entry-subscription", request, UtilTime::now());
  serviceContext.ioContextPtr->poll();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::REQUEST_FAILURE);
  EXPECT_EQ(messages[0].getCorrelationIdList().at(0), "order-request");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, terminalOrderEntryErrorReleasesCorrelation) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY);
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  service->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id][4] = "cancel-request";
  rj::Document response;
  response.Parse<rj::kParseNumbersAsStringsFlag>(R"({"id":"order_cancel4","status":400,"error":{"code":-2011,"msg":"Unknown order"}})");
  auto messages = service->createEvent(wsConnectionPtr, subscription, "", response, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::RESPONSE_ERROR);
  EXPECT_EQ(messages[0].getCorrelationIdList().at(0), "cancel-request");
  EXPECT_EQ(service->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id].count(4), 0);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, cancelResponseUsesCurrentClientOrderId) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY);
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  service->requestCorrelationIdByWsRequestIdByConnectionIdMap[wsConnectionPtr->id][5] = "cancel-request";
  rj::Document response;
  response.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"id":"order_cancel5","status":200,"result":{"orderId":123,"clientOrderId":"current-client","origClientOrderId":"original-client","symbol":"BTCUSDT","side":"BUY","origQty":"0.1","price":"60000","executedQty":"0","cumQuote":"0","status":"CANCELED","updateTime":1720000000000}})");
  auto messages = service->createEvent(wsConnectionPtr, subscription, "", response, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].getType(), Message::Type::CANCEL_ORDER);
  EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_EM_CLIENT_ORDER_ID), "current-client");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, partialFillEmitsExactTradeBeforeOrderDelta) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT",
                            std::string(CCAPI_EM_PRIVATE_TRADE) + "," + CCAPI_EM_ORDER_UPDATE);
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"e":"ORDER_TRADE_UPDATE","E":1720000000002,"T":1720000000001,"o":{"s":"BTCUSDT","c":"client","C":"original","S":"BUY","q":"2","p":"60000","ap":"60001","x":"TRADE","X":"PARTIALLY_FILLED","i":123,"l":"0.25","z":"0.25","L":"60001","Z":"15000.25","n":"1.2","N":"USDT","T":1720000000001,"t":456,"m":true,"R":true,"ps":"LONG","rp":"2.5"}})");
  auto messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", document, now).getMessageList();
  ASSERT_EQ(messages.size(), 2);
  EXPECT_EQ(messages[0].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_PRIVATE_TRADE);
  EXPECT_EQ(messages[1].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
  const auto& fill = messages[0].getElementList().at(0);
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE), "60001");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE), "0.25");
  EXPECT_EQ(fill.getValue(CCAPI_TRADE_ID), "456");
  EXPECT_EQ(fill.getValue(CCAPI_EM_CLIENT_ORDER_ID), "client");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORIGINAL_CLIENT_ORDER_ID), "original");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_REALIZED_PNL), "2.5");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_REDUCE_ONLY), "1");
  const auto& order = messages[1].getElementList().at(0);
  EXPECT_EQ(order.getValue(CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUOTE_QUANTITY), "15000.25");
  EXPECT_EQ(order.getValue(CCAPI_EM_ORDER_AVERAGE_FILLED_PRICE), "60001");
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, nonTradeOrderLifecycleEventsRemainOrderUpdates) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT",
                            std::string(CCAPI_EM_PRIVATE_TRADE) + "," + CCAPI_EM_ORDER_UPDATE);
  for (const auto& lifecycle : std::vector<std::pair<std::string, std::string>>{{"NEW", "NEW"},
                                                                               {"CANCELED", "CANCELED"},
                                                                               {"EXPIRED", "EXPIRED"},
                                                                               {"AMENDMENT", "NEW"}}) {
    std::string json = std::string(R"({"e":"ORDER_TRADE_UPDATE","E":1720000000002,"T":1720000000001,"o":{"s":"BTCUSDT","c":"client","S":"SELL","q":"1","p":"60000","ap":"0","x":")") +
                       lifecycle.first + R"(","X":")" + lifecycle.second +
                       R"(","i":123,"l":"0","z":"0","L":"0","T":1720000000001,"t":0,"m":false,"R":false,"ps":"BOTH","rp":"0"}})";
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(json.c_str());
    auto messages = service->createEvent(std::make_shared<WsConnection>(), subscription, json, document, now).getMessageList();
    ASSERT_EQ(messages.size(), 1) << lifecycle.first;
    EXPECT_EQ(messages[0].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
    EXPECT_EQ(messages[0].getElementList()[0].getValue(CCAPI_EM_ORDER_EXECUTION_TYPE), lifecycle.first);
  }
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, tradeLitePreservesExactAvailableFields) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "BTCUSDT", CCAPI_EM_PRIVATE_TRADE_LITE);
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(
      R"({"e":"TRADE_LITE","E":1720000000002,"T":1720000000001,"s":"BTCUSDT","q":"1","p":"60000","m":true,"c":"client","S":"BUY","L":"60001","l":"0.2","t":456,"i":123})");
  auto messages = service->createEvent(std::make_shared<WsConnection>(), subscription, "", document, now).getMessageList();
  ASSERT_EQ(messages.size(), 1);
  const auto& fill = messages[0].getElementList().at(0);
  EXPECT_EQ(fill.getValue(CCAPI_TRADE_ID), "456");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE), "60001");
  EXPECT_EQ(fill.getValue(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE), "0.2");
  EXPECT_EQ(fill.getValue(CCAPI_EM_CLIENT_ORDER_ID), "client");
  EXPECT_EQ(fill.getValue(CCAPI_EVENT_TIME_MILLISECONDS), "1720000000002");
  EXPECT_EQ(fill.getValue(CCAPI_TRANSACTION_TIME_MILLISECONDS), "1720000000001");
}

TEST(ExecutionManagementServiceBinanceUsdsFuturesLifecycleTest, listenKeyExpirySignalsRecoveryAndInvalidatesOldGeneration) {
  std::vector<Message> messages;
  ServiceContext serviceContext;
  auto service = std::make_shared<ExecutionManagementServiceBinanceUsdsFutures>(
      [&messages](Event& event, Queue<Event>*) { messages.insert(messages.end(), event.getMessageList().begin(), event.getMessageList().end()); },
      SessionOptions(), SessionConfigs(), &serviceContext);
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_BALANCE_UPDATE, "", "account");
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWs, "", std::vector<Subscription>{subscription}, std::map<std::string, std::string>{});
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::PRIVATE_LISTEN_KEY;
  auto oldGeneration = service->beginConnectionGeneration(wsConnectionPtr);
  rj::Document expired;
  expired.Parse<rj::kParseNumbersAsStringsFlag>(R"({"e":"listenKeyExpired","E":1720000000000})");
  service->createEvent(wsConnectionPtr, subscription, "", expired, UtilTime::now());
  EXPECT_FALSE(service->isConnectionGenerationCurrent(wsConnectionPtr, oldGeneration));
  ASSERT_FALSE(messages.empty());
  EXPECT_EQ(messages.front().getType(), Message::Type::SUBSCRIPTION_FAILURE);
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, listenKeyTimerIsSingletonAndCleanupCancelsIt) {
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE_USDS_FUTURES, "", CCAPI_EM_BALANCE_UPDATE);
  auto wsConnectionPtr = std::make_shared<WsConnection>(service->baseUrlWs, "", std::vector<Subscription>{subscription}, credential);
  service->connectionRoleByConnectionIdMap[wsConnectionPtr->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::PRIVATE_LISTEN_KEY;
  service->beginConnectionGeneration(wsConnectionPtr);
  service->pingListenKeyIntervalSeconds = 1;
  service->setPingListenKeyTimer(wsConnectionPtr);
  service->setPingListenKeyTimer(wsConnectionPtr);
  EXPECT_EQ(service->pingListenKeyTimerMapByConnectionIdMap.size(), 1);
  service->clearStates(wsConnectionPtr);
  EXPECT_TRUE(service->pingListenKeyTimerMapByConnectionIdMap.empty());
}

TEST_F(ExecutionManagementServiceBinanceUsdsFuturesTest, listenKeyHttpBodiesRecognizeExpiredKeyError) {
  EXPECT_FALSE(service->doesHttpBodyContainError(R"({"listenKey":"abc"})"));
  EXPECT_FALSE(service->doesHttpBodyContainError(R"({})"));
  EXPECT_FALSE(service->doesHttpBodyContainError(R"({"code":200,"msg":"ok"})"));
  EXPECT_TRUE(service->doesHttpBodyContainError(R"({"code":-1125,"msg":"This listenKey does not exist."})"));
}

} /* namespace ccapi */
#endif
#endif
