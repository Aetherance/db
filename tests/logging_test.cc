#include "util/logging.h"

#include <cstdint>
#include <limits>
#include <string>

#include "gtest/gtest.h"

namespace db {

TEST(LoggingTest, NumberFormattingMatchesDecimalRepresentation) {
  std::string formatted = "prefix:";
  AppendNumberTo(&formatted, 42);

  EXPECT_EQ("prefix:42", formatted);
  EXPECT_EQ("0", NumberToString(0));
  EXPECT_EQ(std::to_string(std::numeric_limits<uint64_t>::max()),
            NumberToString(std::numeric_limits<uint64_t>::max()));
}

TEST(LoggingTest, EscapeStringLeavesPrintableBytesAndEscapesBinaryData) {
  std::string value = "A";
  value.push_back('\n');
  value.push_back('\0');
  value.push_back(static_cast<char>(0xff));
  value.push_back('~');

  std::string escaped = "value=";
  AppendEscapedStringTo(&escaped, Slice(value));

  EXPECT_EQ("value=A\\x0a\\x00\\xff~", escaped);
  EXPECT_EQ("A\\x0a\\x00\\xff~", EscapeString(Slice(value)));
}

TEST(LoggingTest, ConsumeDecimalNumberParsesPrefixAndAdvancesSlice) {
  Slice input("12345tail");
  uint64_t parsed = 0;

  EXPECT_TRUE(ConsumeDecimalNumber(&input, &parsed));
  EXPECT_EQ(12345U, parsed);
  EXPECT_EQ("tail", input.ToString());
}

TEST(LoggingTest, ConsumeDecimalNumberRejectsMissingDigits) {
  Slice input("tail");
  uint64_t parsed = 99;

  EXPECT_FALSE(ConsumeDecimalNumber(&input, &parsed));
  EXPECT_EQ(0U, parsed);
  EXPECT_EQ("tail", input.ToString());
}

TEST(LoggingTest, ConsumeDecimalNumberRejectsOverflowWithoutAdvancing) {
  std::string value = std::to_string(std::numeric_limits<uint64_t>::max());
  value.push_back('0');

  Slice input(value);
  uint64_t parsed = 7;

  EXPECT_FALSE(ConsumeDecimalNumber(&input, &parsed));
  EXPECT_EQ(7U, parsed);
  EXPECT_EQ(value, input.ToString());
}

}  // namespace db
