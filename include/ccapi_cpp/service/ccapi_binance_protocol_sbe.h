#pragma once

#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <type_traits>
#include "rapidjson/document.h"

namespace ccapi { namespace BinanceProtocolSbe {
namespace rj = rapidjson;
using Allocator = rj::Document::AllocatorType;
using Params = std::vector<std::pair<int, std::string>>;

template<class T> constexpr T nullValue() {
  return std::numeric_limits<T>::is_signed ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
}
inline void requireBlock(size_t actual, size_t required) {
  if (actual < required) throw std::runtime_error("Truncated SBE fixed block");
}
template<class T> T checkedSize(size_t size) {
  if (size >= std::numeric_limits<T>::max()) throw std::runtime_error("SBE length overflow");
  return static_cast<T>(size);
}
struct Reader {
  std::string_view bytes;
  size_t position = 0;
  explicit Reader(std::string_view bytes) : bytes(bytes) {}
  void require(size_t size) const {
    if (position > bytes.size() || size > bytes.size() - position) throw std::runtime_error("Truncated SBE message");
  }
  template<class T> T at(size_t offset) const {
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset) throw std::runtime_error("Truncated SBE field");
    using U = std::make_unsigned_t<T>;
    U value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) value |= U(static_cast<unsigned char>(bytes[offset + i])) << (8 * i);
    T result;
    std::memcpy(&result, &value, sizeof(T));
    return result;
  }
  template<class T> T read() { auto result = at<T>(position); position += sizeof(T); return result; }
  template<class T> std::string_view string() {
    size_t size = read<T>(); require(size);
    auto result = bytes.substr(position, size); position += size; return result;
  }
  void checkCount(uint32_t count, size_t block) const {
    require(0);
    if (count > 100000 || count > (bytes.size() - position) / (block ? block : 1)) throw std::runtime_error("Invalid SBE group count");
  }
};
inline void put(rj::Value& object, const char* key, std::string_view value, Allocator& allocator) {
  object.AddMember(rj::Value(key, allocator), rj::Value(value.data(), static_cast<rj::SizeType>(value.size()), allocator), allocator);
}
inline void put(rj::Value& object, const char* key, const std::string& value, Allocator& allocator) {
  put(object, key, std::string_view(value), allocator);
}
inline void put(rj::Value& object, const char* key, const char* value, Allocator& allocator) {
  put(object, key, std::string_view(value), allocator);
}
inline void put(rj::Value& object, const char* key, bool value, Allocator& allocator) {
  object.AddMember(rj::Value(key, allocator), rj::Value(value), allocator);
}
inline std::string decimal(int64_t mantissa, int exponent) {
  if (exponent < -18 || exponent > 18) throw std::runtime_error("Unsupported SBE decimal exponent");
  bool negative = mantissa < 0;
  uint64_t magnitude = negative ? uint64_t(-(mantissa + 1)) + 1 : uint64_t(mantissa);
  std::string value = std::to_string(magnitude);
  if (exponent >= 0) value.append(exponent, '0');
  else {
    size_t places = size_t(-exponent);
    if (value.size() <= places) value.insert(0, places + 1 - value.size(), '0');
    value.insert(value.size() - places, 1, '.');
  }
  if (negative) value.insert(0, 1, '-');
  return value;
}
template<class T> T integer(std::string_view value) {
  T result{};
  auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc() || parsed.ptr != value.data() + value.size() || value.empty()) throw std::runtime_error("Invalid SBE integer");
  return result;
}
inline int64_t timestampUs(std::string_view value) {
  auto result = integer<int64_t>(value);
  if (result < 0 || result > std::numeric_limits<int64_t>::max() / 1000) throw std::runtime_error("SBE timestamp out of range");
  return result;
}
inline uint8_t character(std::string_view value) {
  if (value.size() != 1) throw std::runtime_error("Invalid FIX character");
  return static_cast<uint8_t>(value[0]);
}
inline int64_t decimalMantissa(std::string_view value, int exponent) {
  if (exponent < -18 || exponent > 18 || value.empty()) throw std::runtime_error("Invalid SBE decimal");
  bool negative = value.front() == '-';
  size_t offset = negative ? 1 : 0;
  std::string digits;
  int places = 0;
  bool dot = false;
  for (; offset < value.size(); ++offset) {
    char c = value[offset];
    if (c == '.' && !dot) { dot = true; continue; }
    if (c < '0' || c > '9') throw std::runtime_error("Invalid SBE decimal");
    digits += c;
    if (dot) ++places;
  }
  if (digits.empty()) throw std::runtime_error("Invalid SBE decimal");
  if (digits.find_first_not_of('0') == std::string::npos) return 0;
  int shift = -exponent - places;
  while (shift < 0) {
    if (digits.empty() || digits.back() != '0') throw std::runtime_error("SBE decimal loses precision");
    digits.pop_back(); ++shift;
  }
  digits.append(shift, '0');
  auto first = digits.find_first_not_of('0');
  digits = first == std::string::npos ? "0" : digits.substr(first);
  if (negative) digits.insert(0, 1, '-');
  auto result = integer<int64_t>(digits);
  if (result == nullValue<int64_t>()) throw std::runtime_error("Reserved SBE null mantissa");
  return result;
}
struct Writer {
  std::string bytes;
  template<class T> void write(T value) {
    using U = std::make_unsigned_t<T>;
    for (size_t i = 0; i < sizeof(T); ++i) bytes.push_back(static_cast<char>((U(value) >> (8 * i)) & 255));
  }
  template<class T> void string(std::string_view value) {
    write<T>(checkedSize<T>(value.size())); bytes.append(value);
  }
  void header(uint16_t block, uint16_t type, uint16_t schema, uint16_t version, uint32_t sequence, int64_t time) {
    write<uint32_t>(0); write<uint16_t>(0xeb50); write<uint16_t>(block); write<uint16_t>(type);
    write<uint16_t>(schema); write<uint16_t>(version); write<uint32_t>(sequence); write<int64_t>(time);
  }
  std::string finish() {
    auto size = checkedSize<uint32_t>(bytes.size());
    for (size_t i = 0; i < 4; ++i) bytes[i] = static_cast<char>((size >> (8 * i)) & 255);
    return std::move(bytes);
  }
};
class Parameters {
 public:
  explicit Parameters(const Params& values) : values(values), used(values.size(), false) {}
  std::string string(int tag, bool optional) {
    const std::string* result = nullptr;
    for (size_t i = 0; i < values.size(); ++i) if (values[i].first == tag) {
      if (result) throw std::runtime_error("Duplicate FIX SBE tag: " + std::to_string(tag));
      used[i] = true; result = &values[i].second;
    }
    if (!result && !optional) throw std::runtime_error("Missing FIX SBE tag: " + std::to_string(tag));
    if (result && result->empty() && !optional) throw std::runtime_error("Empty FIX SBE tag");
    return result ? *result : "";
  }
  template<class T> T number(int tag, bool optional, bool isChar = false, bool isBool = false) {
    auto value = string(tag, optional);
    if (value.empty() && optional) return isChar ? T(0) : nullValue<T>();
    T result;
    if (isBool) {
      if (value == "Y" || value == "1") result = 1;
      else if (value == "N" || value == "0") result = 0;
      else throw std::runtime_error("Invalid FIX SBE boolean");
    } else result = isChar ? static_cast<T>(character(value)) : integer<T>(value);
    if (result == (isChar ? T(0) : nullValue<T>())) throw std::runtime_error("Explicit SBE null value is not allowed");
    return result;
  }
  int64_t mantissa(int tag, int exponentTag, bool optional) {
    auto value = string(tag, optional);
    if (optional && value.empty()) return nullValue<int64_t>();
    return decimalMantissa(value, number<int8_t>(exponentTag, false));
  }
  std::vector<std::string> group(int tag, int childTag) {
    auto countText = string(tag, false);
    auto count = integer<uint32_t>(countText);
    if (count > values.size()) throw std::runtime_error("Invalid FIX SBE group count");
    size_t i = 0;
    while (i < values.size() && values[i].first != tag) ++i;
    std::vector<std::string> result;
    for (uint32_t j = 0; j < count; ++j) {
      ++i;
      if (i >= values.size() || values[i].first != childTag || used[i]) throw std::runtime_error("Invalid FIX SBE group layout");
      used[i] = true; result.push_back(values[i].second);
    }
    return result;
  }
  void finish() const {
    for (size_t i = 0; i < values.size(); ++i) {
      // Header tags and RawDataLength are carried by the SBE envelope.
      int tag = values[i].first;
      if (!used[i] && tag != 35 && tag != 34 && tag != 52 && tag != 49 && tag != 56 && tag != 95)
        throw std::runtime_error("Unsupported FIX SBE tag: " + std::to_string(tag));
    }
  }
 private:
  const Params& values;
  std::vector<bool> used;
};
}} // namespace ccapi::BinanceProtocolSbe
