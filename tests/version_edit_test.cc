#include "db/version_edit.h"

#include <string>

#include "db/dbformat.h"
#include "gtest/gtest.h"
#include "util/coding.h"

namespace db {
namespace {

TEST(VersionEditTest, IgnoresLegacyCompactPointer) {
  constexpr uint32_t kLegacyCompactPointerTag = 5;
  const InternalKey key("middle", 100, kTypeValue);

  std::string encoded;
  PutVarint32(&encoded, kLegacyCompactPointerTag);
  PutVarint32(&encoded, 2);
  PutLengthPrefixedSlice(&encoded, key.Encode());

  VersionEdit edit;
  EXPECT_TRUE(edit.DecodeFrom(encoded).Ok());

  std::string rewritten;
  edit.EncodeTo(&rewritten);
  EXPECT_TRUE(rewritten.empty());
}

TEST(VersionEditTest, RejectsMalformedLegacyCompactPointer) {
  constexpr uint32_t kLegacyCompactPointerTag = 5;

  std::string encoded;
  PutVarint32(&encoded, kLegacyCompactPointerTag);
  PutVarint32(&encoded, 2);

  VersionEdit edit;
  EXPECT_EQ(Status::Code::kCorruption, edit.DecodeFrom(encoded).GetCode());
}

}  // namespace
}  // namespace db
