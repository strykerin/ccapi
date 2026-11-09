#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "ccapi_cpp/ccapi_market_data_message.h"

namespace ccapi {

// Binance spot_stream schema 1, version 0 (stream_1_0.xml).
class BinanceSbe {
 public:
  struct Event {
    uint16_t templateId{};
    int64_t eventTime{};
    int64_t transactTime{};
    int64_t firstUpdateId{};
    int64_t lastUpdateId{};
    std::string symbol;
    MarketDataMessage::TypeForOwingData data;
    TimePoint timeReceived{};
  };

  static std::string decimal(int64_t mantissa, int exponent) {
    if (exponent < -127 || exponent > 127) throw std::runtime_error("Binance SBE: exponent out of range");
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
    auto blockLength = r.unsignedInteger(2);
    Event event;
    event.templateId = r.unsignedInteger(2);
    auto schema = r.unsignedInteger(2);
    auto version = r.unsignedInteger(2);
    if (schema != 1 || version != 0) throw std::runtime_error("Binance SBE: unsupported schema/version");
    size_t expected = event.templateId == 10000 ? 18 : event.templateId == 10001 ? 50 : event.templateId == 10003 ? 26 : 0;
    if (!expected) throw std::runtime_error("Binance SBE: unsupported template (depth20 snapshots are not supported)");
    if (blockLength != expected) throw std::runtime_error("Binance SBE: invalid root block length");
    event.eventTime = r.timestamp();
    if (event.templateId == 10000) {
      event.transactTime = r.timestamp();
    } else {
      event.firstUpdateId = r.nonnegative();
      event.lastUpdateId = event.templateId == 10003 ? r.nonnegative() : event.firstUpdateId;
      if (event.firstUpdateId > event.lastUpdateId) throw std::runtime_error("Binance SBE: invalid update ID range");
    }
    int priceExponent = r.exponent();
    int qtyExponent = r.exponent();
    using D = MarketDataMessage::DataType;
    using F = MarketDataMessage::DataFieldType;
    auto level = [&]() {
      MarketDataMessage::TypeForOwingDataPoint point;
      point[F::PRICE] = decimal(r.nonnegative(), priceExponent);
      point[F::SIZE] = decimal(r.nonnegative(), qtyExponent);
      return point;
    };
    if (event.templateId == 10000) {
      auto count = r.group(25, 4);
      for (uint64_t i = 0; i < count; ++i) {
        auto id = r.nonnegative();
        auto point = level();
        point[F::TRADE_ID] = std::to_string(id);
        auto maker = r.unsignedInteger(1);
        if (maker > 1) throw std::runtime_error("Binance SBE: invalid isBuyerMaker");
        point[F::IS_BUYER_MAKER] = maker ? "1" : "0";
        event.data[D::TRADE].push_back(std::move(point));
      }
    } else if (event.templateId == 10001) {
      event.data[D::BID].push_back(level());
      event.data[D::ASK].push_back(level());
    } else {
      for (auto side : {D::BID, D::ASK}) {
        auto count = r.group(16, 2);
        auto& levels = event.data[side];
        for (uint64_t i = 0; i < count; ++i) levels.push_back(level());
      }
    }
    event.symbol = r.symbol();
    if (!r.empty()) throw std::runtime_error("Binance SBE: trailing bytes");
    return event;
  }

 private:
  class Reader {
   public:
    explicit Reader(std::string_view bytes) : bytes(bytes) {}

    uint64_t unsignedInteger(size_t size) {
      require(size);
      uint64_t value = 0;
      for (size_t i = 0; i < size; ++i) value |= uint64_t(static_cast<unsigned char>(bytes[position++])) << (8 * i);
      return value;
    }

    int64_t nonnegative() {
      auto value = unsignedInteger(8);
      if (value > uint64_t(std::numeric_limits<int64_t>::max())) throw std::runtime_error("Binance SBE: negative/null numeric field");
      return static_cast<int64_t>(value);
    }

    int64_t timestamp() {
      auto value = nonnegative();
      if (value > std::numeric_limits<int64_t>::max() / 1000) throw std::runtime_error("Binance SBE: timestamp out of range");
      return value;
    }

    int exponent() {
      auto value = unsignedInteger(1);
      if (value == 128) throw std::runtime_error("Binance SBE: null exponent");
      return value < 128 ? static_cast<int>(value) : static_cast<int>(value) - 256;
    }

    uint64_t group(size_t expected, size_t countSize) {
      auto length = unsignedInteger(2);
      auto count = unsignedInteger(countSize);
      if (length != expected || count > 2147483647 || count > (bytes.size() - position) / expected)
        throw std::runtime_error("Binance SBE: invalid/truncated group");
      return count;
    }

    std::string symbol() {
      auto length = unsignedInteger(1);
      require(length);
      std::string value(bytes.substr(position, length));
      if (value.empty() || value.find('\0') != std::string::npos) throw std::runtime_error("Binance SBE: invalid symbol");
      position += length;
      return value;
    }

    bool empty() const { return position == bytes.size(); }

   private:
    void require(size_t size) const {
      if (size > bytes.size() - position) throw std::runtime_error("Binance SBE: truncated frame");
    }

    std::string_view bytes;
    size_t position{};
  };
};

} /* namespace ccapi */
