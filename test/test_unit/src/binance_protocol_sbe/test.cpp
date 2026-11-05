#include "gtest/gtest.h"
#include "ccapi_cpp/service/ccapi_fix_service_binance.h"
#include "ccapi_cpp/service/ccapi_execution_management_service_binance.h"
#include <fstream>
#include <random>

namespace ccapi {
namespace codec = BinanceProtocolSbe;

std::string fixture(const std::string& name) {
  std::string path = std::string(__FILE__);
  path = path.substr(0, path.find_last_of('/')) + "/fixtures/" + name + ".bin";
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("Missing fixture: " + path);
  return {std::istreambuf_iterator<char>(input), {}};
}

TEST(BinanceProtocolSbe, ExactDecimals) {
  EXPECT_EQ(codec::decimal(-123456789, -8), "-1.23456789");
  EXPECT_EQ(codec::decimal(INT64_MIN, 0), "-9223372036854775808");
  EXPECT_EQ(codec::decimalMantissa("123456789.12345678", -8), 12345678912345678LL);
  EXPECT_EQ(codec::decimalMantissa("0", 3), 0);
  EXPECT_THROW(codec::decimalMantissa("1.000000001", -8), std::runtime_error);
  EXPECT_THROW(codec::decimalMantissa("9223372036854775808", 0), std::runtime_error);
  EXPECT_THROW(codec::timestampUs("9223372036854775807"), std::runtime_error);
}
TEST(BinanceProtocolSbe, OrderEncodingMatchesIndependentFixture) {
  codec::Params params{{35,"D"},{25054,"-2"},{25055,"-8"},{38,"0.00010000"},{40,"2"},{54,"1"},{44,"350000.25"},
                       {59,"1"},{11,"benchmark-1"},{55,"BTCBRL"}};
  EXPECT_EQ(codec::Fix::encode(params, 7, 1720000000000123LL), fixture("fix_new_order"));
  auto bytes = codec::Fix::encode(params, 7, 1720000000000123LL);
  rapidjson::Document allocator;
  auto decoded = codec::Fix::decode(bytes, allocator.GetAllocator());
  EXPECT_STREQ(decoded["44"].GetString(), "350000.25");
  EXPECT_STREQ(decoded["38"].GetString(), "0.00010000");
  params.emplace_back(9999, "unsupported");
  EXPECT_THROW(codec::Fix::encode(params, 7, 0), std::runtime_error);
}
TEST(BinanceProtocolSbe, ProductionRequestVersionAndBlockLengths) {
  codec::Params logon{{35,"A"},{98,"0"},{108,"30"},{141,"Y"},{25035,"1"},
                      {49,"1"},{56,"SPOT"},{96,"signature"},{553,"key"}};
  auto bytes = codec::Fix::encode(logon, 1, 1720000000000123LL);
  EXPECT_EQ(bytes, fixture("fix_logon"));
  codec::Reader reader(bytes);
  EXPECT_EQ(reader.at<uint16_t>(6), 14);
  EXPECT_EQ(reader.at<uint16_t>(8), 20008);
  EXPECT_EQ(reader.at<uint16_t>(10), 1);
  EXPECT_EQ(reader.at<uint16_t>(12), 1);
  codec::Params order{{35,"D"},{25054,"-2"},{25055,"-8"},{38,"0.00010000"},{40,"2"},
                      {54,"1"},{44,"350000.25"},{59,"1"},{11,"benchmark-1"},{55,"BTCBRL"}};
  bytes = codec::Fix::encode(order, 7, 1720000000000123LL);
  EXPECT_EQ(codec::Reader(bytes).at<uint16_t>(6), 76);
  order.emplace_back(25057, "100");
  EXPECT_THROW(codec::Fix::encode(order, 7, 0), std::runtime_error);
}
TEST(BinanceProtocolSbe, MarketDataRequestGroupsAndCancel) {
  codec::Params params{{35,"V"},{263,"1"},{264,"1"},{146,"1"},{55,"BTCBRL"},{267,"2"},{269,"0"},{269,"1"},{262,"bbo"}};
  EXPECT_EQ(codec::Fix::encode(params, 2, 1720000000000123LL), fixture("fix_md_request"));
  codec::Params cancel{{35,"F"},{37,"12345"},{11,"cancel-1"},{55,"BTCBRL"}};
  EXPECT_EQ(codec::Fix::encode(cancel, 8, 1720000000000123LL), fixture("fix_cancel"));
  params[3].second = "2";
  EXPECT_THROW(codec::Fix::encode(params, 2, 0), std::runtime_error);
}
TEST(BinanceProtocolSbe, MissingDuplicateAndPrecisionLosingFieldsFail) {
  EXPECT_THROW(codec::Fix::encode({{35,"D"}}, 1, 0), std::runtime_error);
  EXPECT_THROW(codec::Fix::encode({{35,"0"},{112,"a"},{112,"b"}}, 1, 0), std::runtime_error);
  EXPECT_THROW(codec::Fix::encode({{35,"D"},{25054,"-2"},{25055,"-8"},{38,"1"},{40,"2"},{54,"1"},{44,"1.123"},{11,"a"},{55,"BTCBRL"}}, 1, 0), std::runtime_error);
}
TEST(BinanceProtocolSbe, DecodeTradesTickerAndDepth) {
  rapidjson::Document document;
  for (const auto& name : {"fix_trade", "fix_bbo", "fix_depth", "fix_snapshot"}) {
    auto decoded = codec::Fix::decode(fixture(name), document.GetAllocator());
    EXPECT_STREQ(decoded["55"].GetString(), "BTCBRL");
    if (decoded.HasMember("268")) {
      ASSERT_EQ(decoded["268"].Size(), 2);
      EXPECT_STREQ(decoded["268"][0]["1003"].GetString(), "101");
      EXPECT_STREQ(decoded["268"][1]["1003"].GetString(), "102");
    } else {
      ASSERT_EQ(decoded["25047"].Size(), 1);
      EXPECT_STREQ(decoded["25047"][0]["270"].GetString(), "350000.25");
      if (std::string(name) == "fix_depth") EXPECT_FALSE(decoded["25047"][0].HasMember("271"));
    }
  }
}
TEST(BinanceProtocolSbe, TruncationSchemaAndLengthRejection) {
  for (const auto& name : {"fix_trade", "fix_bbo", "fix_depth", "fix_execution", "ws_ack", "ws_full", "ws_execution"}) {
    auto bytes = fixture(name);
    bool fix = std::string(name).find("fix_") == 0;
    for (size_t n = 0; n < bytes.size(); ++n) {
      rapidjson::Document doc;
      EXPECT_THROW((fix ? codec::Fix::decode(std::string_view(bytes).substr(0,n), doc.GetAllocator()) :
                           codec::Ws::decode(std::string_view(bytes).substr(0,n), doc.GetAllocator())), std::runtime_error) << name << " at " << n;
    }
    rapidjson::Document doc;
    bytes[fix ? 10 : 4] = 99;
    EXPECT_THROW((fix ? codec::Fix::decode(bytes,doc.GetAllocator()) : codec::Ws::decode(bytes,doc.GetAllocator())), std::runtime_error);
  }
}
TEST(BinanceProtocolSbe, OlderFixSchemaAndNullCharacter) {
  rapidjson::Document doc;
  auto versionOne = codec::Fix::decode(fixture("fix_execution_v1"), doc.GetAllocator());
  EXPECT_FALSE(versionOne.HasMember("25057"));
  EXPECT_FALSE(versionOne.HasMember("25058"));
  EXPECT_STREQ(versionOne["39"].GetString(), "0");
  auto decoded = codec::Fix::decode(fixture("fix_execution_v0"), doc.GetAllocator());
  EXPECT_FALSE(decoded.HasMember("18"));
  EXPECT_FALSE(decoded.HasMember("25056"));
  EXPECT_STREQ(decoded["39"].GetString(), "0");
}
TEST(BinanceProtocolSbe, WebsocketSchemaFiveAndPreviousVersion) {
  for (const auto& name : {"ws_logon", "ws_logon_v4", "ws_ack", "ws_ack_v4"}) {
    rapidjson::Document doc;
    auto decoded = codec::Ws::decode(fixture(name), doc.GetAllocator());
    EXPECT_STREQ(decoded["status"].GetString(), "200");
    ASSERT_TRUE(decoded.HasMember("result"));
    if (std::string(name).find("logon") != std::string::npos)
      EXPECT_STREQ(decoded["result"]["apiKey"].GetString(), "test-key");
    else EXPECT_STREQ(decoded["result"]["orderId"].GetString(), "12345");
  }
  EXPECT_EQ(codec::Reader(fixture("ws_logon")).at<uint16_t>(6), 5);
}
class BinanceProtocolServiceTest : public ::testing::Test {
 public:
  ServiceContext context;
  std::vector<Event> events;
  std::shared_ptr<ExecutionManagementServiceBinance> ws;
  std::shared_ptr<FixServiceBinance> fix;
  TimePoint received = TimePoint(std::chrono::microseconds(1720000000000500LL));
  void SetUp() override {
    auto callback = [this](Event& e, Queue<Event>*) { events.push_back(e); };
    ws = std::make_shared<ExecutionManagementServiceBinance>(callback, SessionOptions{}, SessionConfigs{}, &context);
    fix = std::make_shared<FixServiceBinance>(callback, SessionOptions{}, SessionConfigs{}, &context);
  }
  std::shared_ptr<WsConnection> wsConnection(std::string fields = CCAPI_EM_WEBSOCKET_ORDER_ENTRY) {
    Subscription sub("binance", "BTCBRL", fields, "BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING=SBE", "ws-sbe");
    return std::make_shared<WsConnection>("wss://example.test/ws-api/v3", "", std::vector<Subscription>{sub}, std::map<std::string,std::string>{});
  }
};
TEST_F(BinanceProtocolServiceTest, UnsupportedWebsocketSchemaReportsBothVersions) {
  auto bytes = fixture("ws_logon");
  bytes[6] = 6;
  ws->onBinaryMessage(wsConnection(), bytes, received);
  ASSERT_EQ(events.size(), 1U);
  const auto& message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getType(), Message::Type::SUBSCRIPTION_FAILURE);
  EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"ws-sbe"});
  const auto reason = message.getElementList()[0].getValue(CCAPI_ERROR_MESSAGE);
  EXPECT_NE(reason.find("received 3:6, supported through 3:5"), std::string::npos);
}
TEST_F(BinanceProtocolServiceTest, WsAckAndCancelKeepRequestCorrelationAndReceiveTime) {
  auto connection = wsConnection();
  ws->requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id][7] = "request-7";
  auto bytes = fixture("ws_ack");
  ws->onBinaryMessage(connection, bytes, received);
  ASSERT_EQ(events.size(), 1);
  auto message = events[0].getMessageList()[0];
  EXPECT_EQ(message.getType(), Message::Type::CREATE_ORDER);
  EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"request-7"});
  EXPECT_EQ(message.getTimeReceived(), received);
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::microseconds(1720000000000123LL)));
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_EM_ORDER_ID), "12345");
  EXPECT_TRUE(ws->requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id].empty());
  ws->requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id][8] = "request-8";
  ws->onBinaryMessage(connection, fixture("ws_cancel"), received);
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[1].getMessageList()[0].getType(), Message::Type::CANCEL_ORDER);
}
TEST_F(BinanceProtocolServiceTest, WsResultFullAndError) {
  auto connection = wsConnection();
  for (const auto& name : {"ws_result", "ws_full", "ws_error"}) {
    ws->requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id][7] = "request-7";
    ws->onBinaryMessage(connection, fixture(name), received);
  }
  ASSERT_EQ(events.size(), 3);
  EXPECT_EQ(events[0].getMessageList()[0].getElementList()[0].getValue(CCAPI_EM_ORDER_LIMIT_PRICE), "350000.25");
  EXPECT_EQ(events[1].getMessageList()[0].getElementList()[0].getValue(CCAPI_EM_ORDER_QUANTITY), "0.00010000");
  EXPECT_EQ(events[2].getMessageList()[0].getType(), Message::Type::RESPONSE_ERROR);
  EXPECT_NE(events[2].getMessageList()[0].getElementList()[0].getValue(CCAPI_ERROR_MESSAGE).find("Insufficient balance"), std::string::npos);
}
TEST_F(BinanceProtocolServiceTest, WsSessionLogonAndUserData) {
  auto connection = wsConnection();
  ws->onBinaryMessage(connection, fixture("ws_logon"), received);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::AUTHORIZATION_SUCCESS);
  auto privateConnection = wsConnection(std::string(CCAPI_EM_ORDER_UPDATE) + "," + CCAPI_EM_PRIVATE_TRADE + "," + CCAPI_EM_BALANCE_UPDATE);
  ws->onBinaryMessage(privateConnection, fixture("ws_execution"), received);
  ASSERT_EQ(events.size(), 2);
  ASSERT_EQ(events.back().getMessageList().size(), 2);
  auto message = events.back().getMessageList().back();
  EXPECT_EQ(message.getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
  EXPECT_EQ(message.getTime(), TimePoint(std::chrono::microseconds(1720000000000123LL)));
  EXPECT_EQ(message.getElementList()[0].getValue(CCAPI_EVENT_TIME_MILLISECONDS), "1720000000000");
  ws->onBinaryMessage(privateConnection, fixture("ws_balance"), received);
  ASSERT_EQ(events.size(), 3);
  EXPECT_EQ(events.back().getMessageList()[0].getType(), Message::Type::EXECUTION_MANAGEMENT_EVENTS_BALANCE_UPDATE);
}
TEST_F(BinanceProtocolServiceTest, FixAuthorizationSequenceAndGroupPreservation) {
  Subscription sub("binance", "", CCAPI_FIX_MARKET_DATA, "BINANCE_FIX_ENCODING=SBE", "fix-sbe");
  auto connection = std::make_shared<FixConnection>("tcp://localhost:9002", sub, std::map<std::string,std::string>{});
  fix->sbeStateByConnection[connection->id] = std::make_shared<FixServiceBinance::SbeState>();
  fix->processSbeFrame(connection, fixture("fix_logon_ack"), received);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::AUTHORIZATION_SUCCESS);
  fix->processSbeFrame(connection, fixture("fix_trade"), received);
  auto message = events.back().getMessageList()[0];
  EXPECT_EQ(message.getTimeReceived(), received);
  EXPECT_EQ(message.getElementList()[0].getValue("BINANCE_SBE_TEMPLATE_ID"), "205");
  size_t trades = 0;
  for (const auto& tag : message.getElementList()[0].getTagValueList()) if (tag.first == 1003) ++trades;
  EXPECT_EQ(trades, 2);
  EXPECT_THROW(fix->processSbeFrame(connection, fixture("fix_trade"), received), std::runtime_error);
}
TEST_F(BinanceProtocolServiceTest, RejectedLogonDoesNotReconnectInATightLoop) {
  Subscription sub("binance", "", CCAPI_FIX_MARKET_DATA, "BINANCE_FIX_ENCODING=SBE", "rejected");
  auto connection = std::make_shared<FixConnection>("tcp://localhost:9002", sub, std::map<std::string,std::string>{});
  fix->sbeStateByConnection[connection->id] = std::make_shared<FixServiceBinance::SbeState>();
  EXPECT_THROW(fix->processSbeFrame(connection, fixture("fix_logon_reject"), received), std::runtime_error);
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::AUTHORIZATION_FAILURE);
  EXPECT_EQ(fix->rejectedSbeConnections.count(connection->id), 1U);
  fix->sbeStateByConnection.erase(connection->id);
  fix->connect(connection);
  EXPECT_TRUE(fix->sbeStateByConnection.empty());
}
TEST_F(BinanceProtocolServiceTest, JsonRemainsDefaultAndConnectionsAreIndependent) {
  EXPECT_FALSE(ExecutionManagementServiceBinance::usesSbeOrderEntry(Subscription{}));
  EXPECT_FALSE(FixServiceBinance::usesSbe(Subscription{}));
  EXPECT_EQ(ws->sbeOrderEntryUrl("wss://example.test/ws-api/v3"), "wss://example.test/ws-api/v3?responseFormat=sbe&sbeSchemaId=3&sbeSchemaVersion=5");
  EXPECT_THROW(ws->sbeOrderEntryUrl("wss://example.test/?responseFormat=json"), std::runtime_error);
  auto connection = wsConnection();
  ws->onBinaryMessage(connection, "bad", received);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(), Message::Type::SUBSCRIPTION_FAILURE);
}
TEST_F(BinanceProtocolServiceTest, SbeDoesNotSilentlyAcceptJson) {
  auto connection = wsConnection();
  ws->onTextMessage(connection, connection->subscriptionList[0], R"({"id":"session_logon","status":200,"result":{}})", received);
  ASSERT_EQ(events.size(),1);
  EXPECT_EQ(events[0].getMessageList()[0].getType(),Message::Type::SUBSCRIPTION_FAILURE);
}
TEST(BinanceProtocolSbe, NestedEnvelopeIsBounded) {
  auto payload = fixture("ws_ack");
  for (int i=0;i<8;++i) {
    codec::Writer writer;
    writer.write<uint16_t>(3); writer.write<uint16_t>(50); writer.write<uint16_t>(3); writer.write<uint16_t>(4);
    writer.write<uint8_t>(0); writer.write<uint16_t>(200);
    writer.write<uint16_t>(18); writer.write<uint16_t>(0);
    writer.string<uint8_t>("nested"); writer.string<uint32_t>(payload);
    payload = writer.bytes;
  }
  rapidjson::Document doc;
  EXPECT_THROW(codec::Ws::decode(payload,doc.GetAllocator()),std::runtime_error);
}

} // namespace ccapi

namespace ccapi {
TEST(BinanceProtocolLoopback, WebsocketJsonSbeAndFixSbe) {
  const char* fixPort = std::getenv("CCAPI_PROTOCOL_FIX_PORT");
  const char* wsPort = std::getenv("CCAPI_PROTOCOL_WS_PORT");
  const char* keyPath = std::getenv("CCAPI_PROTOCOL_KEY_PATH");
  const char* caPath = std::getenv("CCAPI_PROTOCOL_CA_PATH");
  if (!fixPort || !wsPort || !keyPath || !caPath) GTEST_SKIP() << "Run test/run_binance_protocol_loopback.py";
  ServiceContext context;
  context.sslContextPtr->load_verify_file(caPath);
  SessionConfigs configs;
  configs.setUrlFixBase({{"binance",std::string("tcp+tls://127.0.0.1:")+fixPort}});
  configs.setUrlFixMarketDataBase(configs.getUrlFixBase());
  configs.setUrlWebsocketOrderEntryBase({{"binance",std::string("wss://127.0.0.1:")+wsPort}});
  SessionOptions options;
  options.enableCheckPingPongWebsocketProtocolLevel = false;
  options.enableCheckPingPongWebsocketApplicationLevel = false;
  std::mutex mutex;
  std::condition_variable condition;
  unsigned wsResponses=0, fixResponses=0, marketMessages=0;
  std::vector<std::string> failures;
  std::shared_ptr<ExecutionManagementServiceBinance> ws;
  std::shared_ptr<FixServiceBinance> fix;
  auto callback = [&](Event& event, Queue<Event>*) {
    for (const auto& message : event.getMessageList()) {
      const auto correlation = message.getCorrelationIdList().at(0);
      if (message.getType() == Message::Type::AUTHORIZATION_SUCCESS) {
        if (correlation == "fix-md") {
          for (const auto& stream : {"trade", "bbo", "depth"}) {
            Request request(Request::Operation::FIX, "binance");
            request.appendFixParam({{35,"V"},{263,"1"},{264,std::string(stream)=="bbo"?"1":"20"},{146,"1"},{55,"BTCBRL"},
                                   {267,"1"},{269,std::string(stream)=="trade"?"2":"0"},{262,stream}});
            fix->sendRequestByFix(correlation,request,UtilTime::now());
          }
        } else if (correlation == "fix-oe") {
          Request request(Request::Operation::FIX, "binance");
          request.appendFixParam({{35,"D"},{25054,"-2"},{25055,"-8"},{38,"0.00010000"},{40,"2"},{54,"1"},{44,"350000.25"},
                                  {59,"1"},{11,"benchmark-1"},{55,"BTCBRL"}});
          fix->sendRequestByFix(correlation,request,UtilTime::now());
        } else {
          Request request(Request::Operation::CREATE_ORDER,"binance","BTCBRL",correlation+"-create");
          request.appendParam({{CCAPI_EM_ORDER_SIDE,"BUY"},{CCAPI_EM_ORDER_QUANTITY,"0.00010000"},{CCAPI_EM_ORDER_LIMIT_PRICE,"350000.25"},
                               {CCAPI_EM_CLIENT_ORDER_ID,"benchmark-1"},{"newOrderRespType","ACK"}});
          ws->sendRequestByWebsocket(correlation,request,UtilTime::now());
        }
      } else if (message.getType() == Message::Type::CREATE_ORDER) {
        auto connection = correlation.substr(0,correlation.find("-create"));
        Request request(Request::Operation::CANCEL_ORDER,"binance","BTCBRL",connection+"-cancel");
        request.appendParam({{CCAPI_EM_ORDER_ID,"12345"}});
        ws->sendRequestByWebsocket(connection,request,UtilTime::now());
        std::lock_guard<std::mutex> lock(mutex); ++wsResponses;
      } else if (message.getType() == Message::Type::CANCEL_ORDER) {
        std::lock_guard<std::mutex> lock(mutex); ++wsResponses;
      } else if (message.getType() == Message::Type::FIX) {
        if (correlation == "fix-md") {
          std::lock_guard<std::mutex> lock(mutex); ++marketMessages;
        } else {
          if (message.getElementList()[0].getValue(39)=="0") {
            Request request(Request::Operation::FIX,"binance");
            request.appendFixParam({{35,"F"},{37,"12345"},{11,"cancel-1"},{55,"BTCBRL"}});
            fix->sendRequestByFix(correlation,request,UtilTime::now());
          }
          std::lock_guard<std::mutex> lock(mutex); ++fixResponses;
        }
      } else if (event.getType()==Event::Type::FIX_STATUS || event.getType()==Event::Type::REQUEST_STATUS ||
                 message.getType()==Message::Type::AUTHORIZATION_FAILURE || message.getType()==Message::Type::SUBSCRIPTION_FAILURE) {
        std::lock_guard<std::mutex> lock(mutex); failures.push_back(event.toString());
      }
    }
    condition.notify_one();
  };
  ws=std::make_shared<ExecutionManagementServiceBinance>(callback, options, configs, &context);
  fix=std::make_shared<FixServiceBinance>(callback, options, configs, &context);
  std::map<std::string,std::string> credential{{CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_API_KEY,"test-key"},
      {CCAPI_BINANCE_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PATH,keyPath},{CCAPI_BINANCE_FIX_API_KEY,"test-key"},{CCAPI_BINANCE_FIX_API_PRIVATE_KEY_PATH,keyPath}};
  std::vector<Subscription> wsSubs{
    Subscription("binance","BTCBRL",CCAPI_EM_WEBSOCKET_ORDER_ENTRY,"","ws-json",credential),
    Subscription("binance","BTCBRL",CCAPI_EM_WEBSOCKET_ORDER_ENTRY,"BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING=SBE","ws-sbe",credential)};
  std::vector<Subscription> fixSubs{
    Subscription("binance","",CCAPI_FIX,"BINANCE_FIX_ENCODING=SBE","fix-oe",credential),
    Subscription("binance","",CCAPI_FIX_MARKET_DATA,"BINANCE_FIX_ENCODING=SBE","fix-md",credential)};
  context.start();
  ws->subscribe(wsSubs);
  fix->subscribe(fixSubs);
  bool completed;
  {
    std::unique_lock<std::mutex> lock(mutex);
    completed=condition.wait_for(lock,std::chrono::seconds(15),[&] {return (wsResponses==4 && fixResponses==2 && marketMessages==6) || !failures.empty();});
  }
  context.stop();
  EXPECT_TRUE(completed);
  EXPECT_EQ(wsResponses,4);
  EXPECT_EQ(fixResponses,2);
  EXPECT_EQ(marketMessages,6);
  EXPECT_TRUE(failures.empty()) << (failures.empty()?"":failures.front());
}
} // namespace ccapi
