#include "util/logging.h"

#include <cstdio>
#include <limits>

namespace db {

void AppendNumberTo(std::string* dst, uint64_t number) {
  char buffer[30];
  std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(number));
  dst->append(buffer);
}

void AppendEscapedStringTo(std::string* dst, const Slice& value) {
  for (std::size_t index = 0; index < value.Size(); ++index) {
    const unsigned char byte = static_cast<unsigned char>(value[index]);
    if (byte >= ' ' && byte <= '~') {
      dst->push_back(static_cast<char>(byte));
      continue;
    }

    char buffer[10];
    std::snprintf(buffer, sizeof(buffer), "\\x%02x", static_cast<unsigned int>(byte));
    dst->append(buffer);
  }
}

std::string NumberToString(uint64_t number) {
  std::string result;
  AppendNumberTo(&result, number);
  return result;
}

std::string EscapeString(const Slice& value) {
  std::string result;
  AppendEscapedStringTo(&result, value);
  return result;
}

bool ConsumeDecimalNumber(Slice* input, uint64_t* value) {
  constexpr uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
  constexpr char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10U);

  uint64_t parsed_value = 0;
  const auto* const start = reinterpret_cast<const uint8_t*>(input->Data());
  const auto* const end = start + input->Size();

  const uint8_t* current = start;
  for (; current != end; ++current) {
    const uint8_t digit = *current;
    if (digit < static_cast<uint8_t>('0') || digit > static_cast<uint8_t>('9')) {
      break;
    }

    if (parsed_value > kMaxUint64 / 10U ||
        (parsed_value == kMaxUint64 / 10U && digit > static_cast<uint8_t>(kLastDigitOfMaxUint64))) {
      return false;
    }

    parsed_value = (parsed_value * 10U) + static_cast<uint64_t>(digit - static_cast<uint8_t>('0'));
  }

  const std::size_t digits_consumed = static_cast<std::size_t>(current - start);
  *value = parsed_value;
  input->RemovePrefix(digits_consumed);
  return digits_consumed != 0;
}

}  // namespace db
