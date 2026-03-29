#include "util/arena.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "util/random.h"

namespace db {

TEST(ArenaTest, Empty) {
  Arena arena;
  ASSERT_EQ(std::size_t{0}, arena.MemoryUsage());
}

TEST(ArenaTest, Simple) {
  std::vector<std::pair<std::size_t, char*>> allocated;
  Arena arena;
  constexpr int kNumAllocations = 100000;
  std::size_t bytes = 0;
  Random rnd(301);

  allocated.reserve(static_cast<std::size_t>(kNumAllocations));

  for (int i = 0; i < kNumAllocations; ++i) {
    std::size_t size = 0;
    if (i % (kNumAllocations / 10) == 0) {
      size = static_cast<std::size_t>(i);
    } else {
      size = rnd.OneIn(4000) ? static_cast<std::size_t>(rnd.Uniform(6000))
                             : (rnd.OneIn(10) ? static_cast<std::size_t>(rnd.Uniform(100))
                                              : static_cast<std::size_t>(rnd.Uniform(20)));
    }

    if (size == 0) {
      size = 1;
    }

    char* const allocation = rnd.OneIn(10) ? arena.AllocateAligned(size) : arena.Allocate(size);
    const unsigned char pattern = static_cast<unsigned char>(i % 256);

    for (std::size_t b = 0; b < size; ++b) {
      allocation[b] = static_cast<char>(pattern);
    }

    bytes += size;
    allocated.emplace_back(size, allocation);
    ASSERT_GE(arena.MemoryUsage(), bytes);
    if (i > kNumAllocations / 10) {
      ASSERT_LE(arena.MemoryUsage() * 10, bytes * 11);
    }
  }

  for (std::size_t i = 0; i < allocated.size(); ++i) {
    const std::size_t num_bytes = allocated[i].first;
    const char* const data = allocated[i].second;
    const unsigned char pattern = static_cast<unsigned char>(i % 256U);

    for (std::size_t b = 0; b < num_bytes; ++b) {
      ASSERT_EQ(pattern, static_cast<unsigned char>(data[b]));
    }
  }
}

TEST(ArenaTest, AllocateAlignedReturnsAlignedPointers) {
  Arena arena;
  const std::size_t alignment = sizeof(void*) > 8 ? sizeof(void*) : 8;

  for (std::size_t size : {1U, 7U, 8U, 15U, 64U, 256U, 1024U, 2048U}) {
    char* const allocation = arena.AllocateAligned(size);
    const auto address = reinterpret_cast<std::uintptr_t>(allocation);

    ASSERT_EQ(std::uintptr_t{0}, address % alignment);
  }
}

}  // namespace db
