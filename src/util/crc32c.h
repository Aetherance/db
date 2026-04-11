#pragma once

#include <cstddef>
#include <cstdint>

namespace db {
namespace crc32c {

uint32_t Extend(uint32_t init_crc, const char* data, size_t n);
uint32_t Value(const char* data, size_t n);
uint32_t Mask(uint32_t crc);
uint32_t Unmask(uint32_t masked_crc);

}  // namespace crc32c
}  // namespace db
