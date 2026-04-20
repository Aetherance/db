#pragma once

#include <cstddef>
#include <cstdint>

namespace db {

inline uint32_t Hash(const char* data, std::size_t n, uint32_t seed) {
  constexpr uint32_t kMul = 0xc6a4a793U;
  constexpr int kRightShift = 24;

  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  uint32_t hash = seed ^ (static_cast<uint32_t>(n) * kMul);

  while (n >= 4) {
    const uint32_t word = static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
                          (static_cast<uint32_t>(bytes[2]) << 16) |
                          (static_cast<uint32_t>(bytes[3]) << 24);
    hash += word;
    hash *= kMul;
    hash ^= (hash >> 16);
    bytes += 4;
    n -= 4;
  }

  switch (n) {
    case 3:
      hash += static_cast<uint32_t>(bytes[2]) << 16;
      [[fallthrough]];
    case 2:
      hash += static_cast<uint32_t>(bytes[1]) << 8;
      [[fallthrough]];
    case 1:
      hash += static_cast<uint32_t>(bytes[0]);
      hash *= kMul;
      hash ^= (hash >> kRightShift);
      [[fallthrough]];
    case 0:
      return hash;
  }

  return hash;
}

}  // namespace db
