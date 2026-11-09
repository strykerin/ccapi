#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "ccapi_cpp/ccapi_market_data_message.h"

namespace ccapi {

// OKX market data schema 1, version 0 (okx_sbe_1_0.xml).
class OkxSbe {
 public:
  struct Event {
    uint16_t templateId{};
    int64_t instIdCode{};
    int64_t eventTime{};
    int64_t outTime{};
    int64_t sequence{};
    int64_t previousSequence{-1};
    int priceExponent{};
    int sizeExponent{};
    int tradeCount{};
    int tradeSource{};
    MarketDataMessage::TypeForOwingData data;
    TimePoint timeReceived{};
  };

  static std::string decimal(int64_t mantissa, int exponent) {
    if (exponent < -127 || exponent > 127) throw std::runtime_error("OKX SBE: exponent out of range");
    if (!mantissa) return "0";
    auto value = std::to_string(mantissa);
    bool negative = value.front() == '-';
    if (negative) value.erase(0, 1);
    if (exponent >= 0) {
      value.append(exponent, '0');
    } else {
      auto places = static_cast<size_t>(-exponent);
      if (value.size() <= places) value.insert(0, places + 1 - value.size(), '0');
      value.insert(value.size() - places, 1, '.');
      while (value.back() == '0') value.pop_back();
      if (value.back() == '.') value.pop_back();
    }
    return negative ? "-" + value : value;
  }

  static Event decode(std::string_view frame) {
    Reader r(frame);
    auto blockLength = r.integer(2);
    Event event;
    event.templateId = r.integer(2);
    auto schema = r.integer(2);
    auto version = r.integer(2);
    if (schema != 1 || version != 0) throw std::runtime_error("OKX SBE: unsupported market data schema/version");
    size_t expected = event.templateId == 1000                               ? 74
                      : event.templateId == 1001 || event.templateId == 1002 ? 42
                      : event.templateId == 1005                             ? 62
                      : event.templateId == 1006                             ? 26
                                                                             : 0;
    if (!expected) throw std::runtime_error("OKX SBE: unsupported market data template");
    if (blockLength != expected) throw std::runtime_error("OKX SBE: invalid root block length");
    event.instIdCode = r.nonnegative();
    event.eventTime = r.timestamp();
    if (event.templateId != 1006) event.outTime = r.timestamp();
    event.sequence = r.nonnegative();
    using D = MarketDataMessage::DataType;
    using F = MarketDataMessage::DataFieldType;
    auto exponents = [&]() {
      event.priceExponent = r.exponent();
      event.sizeExponent = r.exponent();
    };
    auto point = [&](int64_t price, int64_t size) {
      return MarketDataMessage::TypeForOwingDataPoint{{F::PRICE, decimal(price, event.priceExponent)}, {F::SIZE, decimal(size, event.sizeExponent)}};
    };
    if (event.templateId == 1000) {
      auto askPrice = r.nonnegative(), askSize = r.nonnegative(), bidPrice = r.nonnegative(), bidSize = r.nonnegative();
      r.nonnegative(4);
      r.nonnegative(4);
      exponents();
      event.data[D::ASK].push_back(point(askPrice, askSize));
      event.data[D::BID].push_back(point(bidPrice, bidSize));
    } else if (event.templateId == 1005) {
      auto price = r.nonnegative(), size = r.nonnegative(), tradeId = r.nonnegative();
      event.tradeCount = r.nonnegative(2);
      auto side = r.integer(1);
      if (side > 1 || !event.tradeCount) throw std::runtime_error("OKX SBE: invalid trade side/count");
      exponents();
      event.tradeSource = r.integer(1);
      if (event.tradeSource > 1) throw std::runtime_error("OKX SBE: invalid trade source");
      auto trade = point(price, size);
      trade[F::TRADE_ID] = std::to_string(tradeId);
      trade[F::IS_BUYER_MAKER] = side == 0 ? "1" : "0";
      event.data[D::TRADE].push_back(std::move(trade));
    } else {
      if (event.templateId != 1006) event.previousSequence = r.nonnegative();
      exponents();
      if (event.templateId != 1002) {
        for (auto side : {D::ASK, D::BID}) {
          auto count = r.group();
          auto& levels = event.data[side];
          for (uint64_t i = 0; i < count; ++i) {
            auto price = r.nonnegative(), size = r.nonnegative();
            r.nonnegative(4);
            levels.push_back(point(price, size));
          }
        }
        if (event.templateId == 1001 && event.sequence == event.previousSequence && (!event.data[D::ASK].empty() || !event.data[D::BID].empty()))
          throw std::runtime_error("OKX SBE: nonempty order book heartbeat");
      }
    }
    if (!r.empty()) throw std::runtime_error("OKX SBE: trailing bytes");
    return event;
  }

 private:
  class Reader {
   public:
    explicit Reader(std::string_view bytes) : bytes(bytes) {}

    uint64_t integer(size_t size) {
      if (size > bytes.size() - position) throw std::runtime_error("OKX SBE: truncated frame");
      uint64_t value = 0;
      for (size_t i = 0; i < size; ++i) value |= uint64_t(static_cast<unsigned char>(bytes[position++])) << (8 * i);
      return value;
    }

    int64_t nonnegative(size_t size = 8) {
      auto value = integer(size);
      if (value >> (size * 8 - 1)) throw std::runtime_error("OKX SBE: negative/null numeric field");
      return static_cast<int64_t>(value);
    }

    int64_t timestamp() {
      auto value = nonnegative();
      if (value > std::numeric_limits<int64_t>::max() / 1000) throw std::runtime_error("OKX SBE: timestamp out of range");
      return value;
    }

    int exponent() {
      auto value = integer(1);
      if (value == 128) throw std::runtime_error("OKX SBE: null exponent");
      return value < 128 ? static_cast<int>(value) : static_cast<int>(value) - 256;
    }

    uint64_t group() {
      auto length = integer(2), count = integer(2);
      if (length != 20 || count == 65535 || count > (bytes.size() - position) / 20) throw std::runtime_error("OKX SBE: invalid/truncated group");
      return count;
    }

    bool empty() const { return position == bytes.size(); }

   private:
    std::string_view bytes;
    size_t position{};
  };
};

} /* namespace ccapi */
