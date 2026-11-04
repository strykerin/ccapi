#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "ccapi_cpp/ccapi_market_data_message.h"

namespace ccapi {

class BinanceSbe {
 public:
  struct Message {
    uint16_t templateId{};
    int64_t eventTime{}, transactTime{}, firstUpdateId{}, lastUpdateId{};
    std::string symbol;
    MarketDataMessage::TypeForOwingData data;
  };

  static std::string decimal(int64_t mantissa, int exponent) {
    if (mantissa == 0) return "0";
    bool negative = mantissa < 0;
    std::string value = std::to_string(mantissa);
    if (negative) value.erase(0, 1);
    if (exponent >= 0) {
      value.append(static_cast<size_t>(exponent), '0');
    } else {
      const auto places = static_cast<size_t>(-exponent);
      if (places >= value.size()) value.insert(0, places - value.size() + 1, '0');
      value.insert(value.size() - places, 1, '.');
      while (value.back() == '0') value.pop_back();
      if (value.back() == '.') value.pop_back();
    }
    if (negative) value.insert(0, 1, '-');
    return value;
  }

  static Message decode(std::string_view bytes) {
    Reader reader(bytes);
    const auto blockLength = reader.u16();
    Message message;
    message.templateId = reader.u16();
    const auto schema = reader.u16();
    const auto version = reader.u16();
    if (schema != 1 || version != 0) throw std::runtime_error("unsupported Binance SBE schema/version");
    size_t minimumBlock;
    switch (message.templateId) {
      case 10000:
        minimumBlock = 18;
        break;
      case 10001:
        minimumBlock = 50;
        break;
      case 10002:
        minimumBlock = 18;
        break;
      case 10003:
        minimumBlock = 26;
        break;
      default:
        throw std::runtime_error("unsupported Binance SBE template");
    }
    if (blockLength < minimumBlock) throw std::runtime_error("short Binance SBE root block");
    reader.require(blockLength);
    const auto rootEnd = reader.position + blockLength;
    message.eventTime = reader.i64();
    validateTime(message.eventTime);
    if (message.templateId == 10000) {
      message.transactTime = reader.i64();
      validateTime(message.transactTime);
    } else {
      message.firstUpdateId = reader.i64();
      message.lastUpdateId = message.templateId == 10003 ? reader.i64() : message.firstUpdateId;
      if (message.firstUpdateId <= 0 || message.lastUpdateId < message.firstUpdateId || message.lastUpdateId == std::numeric_limits<int64_t>::max()) {
        throw std::runtime_error("invalid Binance SBE update IDs");
      }
    }
    const auto priceExponent = reader.i8();
    const auto qtyExponent = reader.i8();
    if (message.templateId == 10001) {
      message.data[MarketDataMessage::DataType::BID].push_back(level(reader, priceExponent, qtyExponent));
      message.data[MarketDataMessage::DataType::ASK].push_back(level(reader, priceExponent, qtyExponent));
    }
    reader.position = rootEnd;
    if (message.templateId == 10000) {
      const auto length = reader.u16();
      const auto count = reader.u32();
      reader.group(length, count, 25);
      auto& trades = message.data[MarketDataMessage::DataType::TRADE];
      trades.reserve(count);
      for (uint32_t i = 0; i < count; ++i) {
        const auto end = reader.position + length;
        const auto id = reader.i64();
        if (id < 0) throw std::runtime_error("invalid Binance SBE trade ID");
        auto point = level(reader, priceExponent, qtyExponent);
        const auto maker = reader.u8();
        if (maker > 1) throw std::runtime_error("invalid Binance SBE boolean");
        point.emplace(MarketDataMessage::DataFieldType::TRADE_ID, std::to_string(id));
        point.emplace(MarketDataMessage::DataFieldType::IS_BUYER_MAKER, maker ? "1" : "0");
        trades.push_back(std::move(point));
        reader.position = end;
      }
    } else if (message.templateId != 10001) {
      for (auto side : {MarketDataMessage::DataType::BID, MarketDataMessage::DataType::ASK}) {
        const auto length = reader.u16();
        const auto count = reader.u16();
        reader.group(length, count, 16);
        auto& levels = message.data[side];
        levels.reserve(count);
        for (uint16_t i = 0; i < count; ++i) {
          const auto end = reader.position + length;
          levels.push_back(level(reader, priceExponent, qtyExponent));
          reader.position = end;
        }
      }
    }
    const auto length = reader.u8();
    reader.require(length);
    message.symbol = std::string(bytes.substr(reader.position, length));
    reader.position += length;
    if (message.symbol.empty() || message.symbol.find('\0') != std::string::npos || reader.position != bytes.size()) {
      throw std::runtime_error("invalid Binance SBE symbol/message length");
    }
    return message;
  }

 private:
  class Reader {
   public:
    explicit Reader(std::string_view bytes) : bytes(bytes) {}

    void require(size_t size) const {
      if (size > bytes.size() - position) throw std::runtime_error("truncated Binance SBE message");
    }

    uint64_t number(size_t size) {
      require(size);
      uint64_t value = 0;
      for (size_t i = 0; i < size; ++i) value |= uint64_t(static_cast<unsigned char>(bytes[position++])) << (8 * i);
      return value;
    }

    uint8_t u8() { return static_cast<uint8_t>(number(1)); }

    uint16_t u16() { return static_cast<uint16_t>(number(2)); }

    uint32_t u32() { return static_cast<uint32_t>(number(4)); }

    int i8() {
      const auto value = u8();
      return value < 128 ? value : int(value) - 256;
    }

    int64_t i64() {
      const auto value = number(8);
      int64_t result;
      std::memcpy(&result, &value, sizeof(result));
      return result;
    }

    void group(size_t blockLength, size_t count, size_t minimum) const {
      if (blockLength < minimum || count > (bytes.size() - position) / blockLength || count > 2147483647) {
        throw std::runtime_error("invalid Binance SBE group dimensions");
      }
    }

    size_t position{};
    std::string_view bytes;
  };

  static void validateTime(int64_t value) {
    if (value < 0 || value > std::numeric_limits<int64_t>::max() / 1000) throw std::runtime_error("invalid Binance SBE timestamp");
  }

  static MarketDataMessage::TypeForOwingDataPoint level(Reader& reader, int priceExponent, int qtyExponent) {
    const auto price = reader.i64();
    const auto qty = reader.i64();
    if (price <= 0 || qty < 0) throw std::runtime_error("invalid Binance SBE price/quantity");
    return {{MarketDataMessage::DataFieldType::PRICE, decimal(price, priceExponent)}, {MarketDataMessage::DataFieldType::SIZE, decimal(qty, qtyExponent)}};
  }
};

}  // namespace ccapi
