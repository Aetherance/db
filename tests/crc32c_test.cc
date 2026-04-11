#include "util/crc32c.h"

#include <string>

#include "gtest/gtest.h"

namespace db {
namespace crc32c {
namespace {

TEST(Crc32cTest, ValueMatchesKnownVector) {
  EXPECT_EQ(0xe3069283U, Value("123456789", 9));
}

TEST(Crc32cTest, ExtendMatchesConcatenation) {
  const std::string prefix = "hello ";
  const std::string suffix = "world";
  const std::string full = prefix + suffix;

  const uint32_t prefix_crc = Value(prefix.data(), prefix.size());
  EXPECT_EQ(Value(full.data(), full.size()), Extend(prefix_crc, suffix.data(), suffix.size()));
}

TEST(Crc32cTest, MaskAndUnmaskRoundTrip) {
  const uint32_t crc = Value("abc", 3);
  EXPECT_EQ(crc, Unmask(Mask(crc)));
}

}  // namespace
}  // namespace crc32c
}  // namespace db
