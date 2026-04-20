#include "util/hash.h"

#include <string>

#include "gtest/gtest.h"

namespace db {

TEST(HashTest, EmptyInputReturnsSeed) {
  EXPECT_EQ(0xbc9f1d34U, Hash("", 0, 0xbc9f1d34U));
}

TEST(HashTest, HashesAsciiInputDeterministically) {
  EXPECT_EQ(0xf795964eU, Hash("hello", 5, 0xbc9f1d34U));
}

TEST(HashTest, HashesBinaryInputWithoutSignedCharIssues) {
  const std::string value("\x62\xc3\x97\xe2\x99\xa5", 6);
  EXPECT_EQ(0x728a4af8U, Hash(value.data(), value.size(), 0xbc9f1d34U));
}

}  // namespace db
