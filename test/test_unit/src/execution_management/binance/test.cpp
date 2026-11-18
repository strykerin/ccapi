#include <tuple>

// clang-format off
#include "gtest/gtest.h"
#include "ccapi_cpp/service/ccapi_execution_management_service_binance.h"
// clang-format on

namespace ccapi {
namespace {

// Exercise the production parser and callback, including correlation and the existing order/error fields.
class BinanceOrderEntryRateLimitsTest : public ::testing::TestWithParam<std::tuple<bool, bool, std::string>> {};

TEST_P(BinanceOrderEntryRateLimitsTest, deliversResponseMetadata) {
  const auto [cancel, success, rateLimits] = GetParam();
  ServiceContext context;
  std::vector<Event> events;
  auto servicePtr = std::make_shared<ExecutionManagementServiceBinance>([&](Event& event, Queue<Event>*) { events.push_back(event); }, SessionOptions(),
                                                                      SessionConfigs(), &context);
  auto& service = *servicePtr;
  Subscription subscription(CCAPI_EXCHANGE_NAME_BINANCE, "", CCAPI_EM_WEBSOCKET_ORDER_ENTRY, "", "order-entry");
  auto connection =
      std::make_shared<WsConnection>(service.baseUrlWsOrderEntry, "", std::vector<Subscription>{subscription}, std::map<std::string, std::string>{});
  service.connectionRoleByConnectionIdMap[connection->id] = ExecutionManagementServiceBinanceBase::ConnectionRole::ORDER_ENTRY;
  service.requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id][1] = "request-correlation";
  const auto now = UtilTime::makeTimePointFromMilliseconds(1720000000000);
  std::string payload =
      std::string(R"({"id":")") + (cancel ? "order_cancel1" : "order_place1") +
      (success
           ? R"(","status":200,"result":{"orderId":123,"clientOrderId":"client-1","symbol":"BTCUSDT","side":"BUY","origQty":"0.1","price":"60000","executedQty":"0","cummulativeQuoteQty":"0","status":"NEW","transactTime":1720000000000})"
           : R"(","status":429,"error":{"code":-1015,"msg":"Too many new orders"})");
  if (!rateLimits.empty()) {
    payload += R"(,"rateLimits":)" + rateLimits;
  }
  payload += "}";
  service.onTextMessage(connection, subscription, payload, now);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events.front().getType(), Event::Type::RESPONSE);
  ASSERT_EQ(events.front().getMessageList().size(), 1);
  const auto& message = events.front().getMessageList().front();
  EXPECT_EQ(message.getType(), success ? (cancel ? Message::Type::CANCEL_ORDER : Message::Type::CREATE_ORDER) : Message::Type::RESPONSE_ERROR);
  EXPECT_EQ(message.getCorrelationIdList(), std::vector<std::string>{"request-correlation"});
  EXPECT_EQ(message.getTimeReceived(), now);
  ASSERT_EQ(message.getElementList().size(), 1);
  const auto& element = message.getElementList().front();
  if (success) {
    EXPECT_EQ(element.getValue(CCAPI_EM_ORDER_ID), "123");
    EXPECT_EQ(element.getValue(CCAPI_EM_CLIENT_ORDER_ID), "client-1");
  } else {
    EXPECT_EQ(element.getValue(CCAPI_ERROR_MESSAGE), payload);
  }
  if (rateLimits.empty() || rateLimits == "null" || rateLimits == "{}") {
    EXPECT_FALSE(element.has(CCAPI_EM_RATE_LIMITS));
  } else {
    ASSERT_TRUE(element.has(CCAPI_EM_RATE_LIMITS));
    rj::Document actual;
    actual.Parse(element.getValue(CCAPI_EM_RATE_LIMITS).c_str());
    rj::Document expected;
    expected.Parse(rateLimits.c_str());
    ASSERT_FALSE(actual.HasParseError());
    EXPECT_EQ(actual, expected);  // Includes numeric types, array order and every interval/field.
  }
  EXPECT_EQ(service.requestCorrelationIdByWsRequestIdByConnectionIdMap[connection->id].count(1), 0);
  service.onTextMessage(connection, subscription, payload, now);
  EXPECT_EQ(events.size(), 1);  // Duplicate responses remain suppressed.
}

INSTANTIATE_TEST_SUITE_P(
    BinanceSpot, BinanceOrderEntryRateLimitsTest,
    ::testing::Combine(
        ::testing::Bool(), ::testing::Bool(),
        ::testing::Values(
            std::string(), std::string("[]"), std::string("null"), std::string("{}"),
            std::string(
                R"([{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":50,"count":0},{"rateLimitType":"ORDERS","interval":"DAY","intervalNum":1,"limit":160000,"count":123},{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000,"count":9007199254740993}])"))));

}  // namespace
}  // namespace ccapi
