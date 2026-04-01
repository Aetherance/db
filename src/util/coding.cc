#include "util/coding.h"

#include <cassert>
#include <cstddef>
#include <limits>

namespace db {
namespace {

constexpr uint8_t kVarintContinuationBit = 128U;
constexpr uint8_t kVarintValueMask = 127U;

}  // namespace

void PutFixed32(std::string* dst, uint32_t value) {
  char buffer[sizeof(uint32_t)];
  EncodeFixed32(buffer, value);
  dst->append(buffer, sizeof(buffer));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buffer[sizeof(uint64_t)];
  EncodeFixed64(buffer, value);
  dst->append(buffer, sizeof(buffer));
}

void PutVarint32(std::string* dst, uint32_t value) {
  char buffer[5];
  char* const end = EncodeVarint32(buffer, value);
  dst->append(buffer, static_cast<std::size_t>(end - buffer));
}

void PutVarint64(std::string* dst, uint64_t value) {
  char buffer[10];
  char* const end = EncodeVarint64(buffer, value);
  dst->append(buffer, static_cast<std::size_t>(end - buffer));
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  assert(value.Size() <= static_cast<std::size_t>(std::numeric_limits<uint32_t>::max()));
  PutVarint32(dst, static_cast<uint32_t>(value.Size()));
  dst->append(value.Data(), value.Size());
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* const start = input->Data();
  const char* const end = start + input->Size();
  const char* const parsed = GetVarint32Ptr(start, end, value);
  if (parsed == nullptr) {
    return false;
  }
  input->RemovePrefix(static_cast<std::size_t>(parsed - start));
  return true;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* const start = input->Data();
  const char* const end = start + input->Size();
  const char* const parsed = GetVarint64Ptr(start, end, value);
  if (parsed == nullptr) {
    return false;
  }
  input->RemovePrefix(static_cast<std::size_t>(parsed - start));
  return true;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t length = 0;
  if (!GetVarint32(input, &length)) {
    return false;
  }
  if (input->Size() < static_cast<std::size_t>(length)) {
    return false;
  }
  *result = Slice(input->Data(), static_cast<std::size_t>(length));
  input->RemovePrefix(static_cast<std::size_t>(length));
  return true;
}

int VarintLength(uint64_t value) {
  int length = 1;
  while (value >= static_cast<uint64_t>(kVarintContinuationBit)) {
    value >>= 7;
    ++length;
  }
  return length;
}

char* EncodeVarint32(char* dst, uint32_t value) {
  auto* buffer = reinterpret_cast<uint8_t*>(dst);
  while (value >= static_cast<uint32_t>(kVarintContinuationBit)) {
    *buffer++ = static_cast<uint8_t>(value & static_cast<uint32_t>(kVarintValueMask)) |
                kVarintContinuationBit;
    value >>= 7;
  }
  *buffer++ = static_cast<uint8_t>(value);
  return reinterpret_cast<char*>(buffer);
}

char* EncodeVarint64(char* dst, uint64_t value) {
  auto* buffer = reinterpret_cast<uint8_t*>(dst);
  while (value >= static_cast<uint64_t>(kVarintContinuationBit)) {
    *buffer++ = static_cast<uint8_t>(value & static_cast<uint64_t>(kVarintValueMask)) |
                kVarintContinuationBit;
    value >>= 7;
  }
  *buffer++ = static_cast<uint8_t>(value);
  return reinterpret_cast<char*>(buffer);
}

const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28U && p < limit; shift += 7U) {
    const uint32_t byte = static_cast<uint32_t>(*reinterpret_cast<const uint8_t*>(p));
    ++p;
    result |= (byte & static_cast<uint32_t>(kVarintValueMask)) << shift;
    if ((byte & static_cast<uint32_t>(kVarintContinuationBit)) == 0U) {
      *value = result;
      return p;
    }
  }
  return nullptr;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63U && p < limit; shift += 7U) {
    const uint64_t byte = static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(p));
    ++p;
    if (shift == 63U && byte > 1U) {
      return nullptr;
    }
    result |= (byte & static_cast<uint64_t>(kVarintValueMask)) << shift;
    if ((byte & static_cast<uint64_t>(kVarintContinuationBit)) == 0U) {
      *value = result;
      return p;
    }
  }
  return nullptr;
}

}  // namespace db
