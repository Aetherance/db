#include "util/coding.h"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace db {

TEST(CodingTest, Fixed32RoundTripsLittleEndianBytes) {
  char buffer[sizeof(uint32_t)];
  EncodeFixed32(buffer, 0x04030201U);

  EXPECT_EQ(0x01U, static_cast<unsigned char>(buffer[0]));
  EXPECT_EQ(0x02U, static_cast<unsigned char>(buffer[1]));
  EXPECT_EQ(0x03U, static_cast<unsigned char>(buffer[2]));
  EXPECT_EQ(0x04U, static_cast<unsigned char>(buffer[3]));
  EXPECT_EQ(0x04030201U, DecodeFixed32(buffer));
}

TEST(CodingTest, Fixed64RoundTripsLittleEndianBytes) {
  char buffer[sizeof(uint64_t)];
  EncodeFixed64(buffer, 0x0807060504030201ULL);

  EXPECT_EQ(0x01U, static_cast<unsigned char>(buffer[0]));
  EXPECT_EQ(0x02U, static_cast<unsigned char>(buffer[1]));
  EXPECT_EQ(0x03U, static_cast<unsigned char>(buffer[2]));
  EXPECT_EQ(0x04U, static_cast<unsigned char>(buffer[3]));
  EXPECT_EQ(0x05U, static_cast<unsigned char>(buffer[4]));
  EXPECT_EQ(0x06U, static_cast<unsigned char>(buffer[5]));
  EXPECT_EQ(0x07U, static_cast<unsigned char>(buffer[6]));
  EXPECT_EQ(0x08U, static_cast<unsigned char>(buffer[7]));
  EXPECT_EQ(0x0807060504030201ULL, DecodeFixed64(buffer));
}

TEST(CodingTest, Varint32RoundTripsRepresentativeValues) {
  const std::vector<uint32_t> values = {
      0U, 1U, 127U, 128U, 255U, 300U, 16384U, std::numeric_limits<uint32_t>::max(),
  };

  for (const uint32_t value : values) {
    std::string encoded;
    PutVarint32(&encoded, value);

    Slice input(encoded);
    uint32_t decoded = 0;

    EXPECT_TRUE(GetVarint32(&input, &decoded));
    EXPECT_EQ(value, decoded);
    EXPECT_TRUE(input.Empty());
    EXPECT_EQ(static_cast<std::size_t>(VarintLength(value)), encoded.size());
  }
}

TEST(CodingTest, Varint64RoundTripsRepresentativeValues) {
  const std::vector<uint64_t> values = {
      0ULL,   1ULL,     127ULL,     128ULL,     255ULL,
      300ULL, 16384ULL, 1ULL << 32, 1ULL << 63, std::numeric_limits<uint64_t>::max(),
  };

  for (const uint64_t value : values) {
    std::string encoded;
    PutVarint64(&encoded, value);

    Slice input(encoded);
    uint64_t decoded = 0;

    EXPECT_TRUE(GetVarint64(&input, &decoded));
    EXPECT_EQ(value, decoded);
    EXPECT_TRUE(input.Empty());
    EXPECT_EQ(static_cast<std::size_t>(VarintLength(value)), encoded.size());
  }
}

TEST(CodingTest, LengthPrefixedSliceRoundTripsSequentialValues) {
  std::string encoded;
  PutLengthPrefixedSlice(&encoded, Slice("value"));
  PutLengthPrefixedSlice(&encoded, Slice("tail"));

  Slice input(encoded);
  Slice first;
  Slice second;

  EXPECT_TRUE(GetLengthPrefixedSlice(&input, &first));
  EXPECT_EQ(std::string("value"), first.ToString());

  EXPECT_TRUE(GetLengthPrefixedSlice(&input, &second));
  EXPECT_EQ(std::string("tail"), second.ToString());
  EXPECT_TRUE(input.Empty());
}

TEST(CodingTest, TruncatedVarintDoesNotDecode) {
  std::string encoded(1, static_cast<char>(0x80));
  Slice input(encoded);
  uint32_t decoded = 0;

  EXPECT_FALSE(GetVarint32(&input, &decoded));
  EXPECT_EQ(encoded.size(), input.Size());
}

TEST(CodingTest, TruncatedVarint64DoesNotDecode) {
  std::string encoded(9, static_cast<char>(0x80));
  Slice input(encoded);
  uint64_t decoded = 99;

  EXPECT_FALSE(GetVarint64(&input, &decoded));
  EXPECT_EQ(99U, decoded);
  EXPECT_EQ(encoded.size(), input.Size());
}

TEST(CodingTest, OverflowVarint64DoesNotDecode) {
  std::string encoded(9, static_cast<char>(0x80));
  encoded.push_back(static_cast<char>(0x02));

  Slice input(encoded);
  uint64_t decoded = 7;

  EXPECT_FALSE(GetVarint64(&input, &decoded));
  EXPECT_EQ(7U, decoded);
  EXPECT_EQ(encoded.size(), input.Size());
}

TEST(CodingTest, LengthPrefixedSliceRejectsTruncatedPayload) {
  std::string encoded;
  PutVarint32(&encoded, 5);
  encoded.append("abc");

  Slice input(encoded);
  Slice result("unchanged");

  EXPECT_FALSE(GetLengthPrefixedSlice(&input, &result));
  EXPECT_EQ("unchanged", result.ToString());
}

}  // namespace db
