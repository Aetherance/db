#include "slice.h"

#include <string>

#include "gtest/gtest.h"

namespace db {

TEST(SliceTest, DefaultConstructedSliceIsEmpty) {
  Slice slice;

  EXPECT_TRUE(slice.Empty());
  EXPECT_EQ(std::size_t{0}, slice.Size());
  EXPECT_EQ(std::string{}, slice.ToString());
}

TEST(SliceTest, ConstructorsExposeReferencedBytes) {
  static constexpr char kData[] = {'a', 'b', 'c', 'd', 'e'};
  Slice raw_slice(kData, 3);
  std::string value = "value";
  Slice string_slice(value);
  Slice c_string_slice("hello");

  EXPECT_EQ('a', raw_slice[0]);
  EXPECT_EQ('c', raw_slice[2]);
  EXPECT_EQ(std::string{"val"}, Slice(value.data(), 3).ToString());
  EXPECT_EQ(value.data(), string_slice.Data());
  EXPECT_EQ(std::string{"hello"}, c_string_slice.ToString());
}

TEST(SliceTest, ClearAndRemovePrefixAdjustTheView) {
  Slice slice("prefix-data");

  slice.RemovePrefix(7);
  EXPECT_EQ(std::string{"data"}, slice.ToString());

  slice.Clear();
  EXPECT_TRUE(slice.Empty());
  EXPECT_EQ(std::size_t{0}, slice.Size());
}

TEST(SliceTest, CompareIsLexicographical) {
  const Slice alpha("alpha");
  const Slice alphabet("alphabet");
  const Slice bravo("bravo");
  const Slice alpha_copy("alpha");

  EXPECT_LT(alpha.Compare(alphabet), 0);
  EXPECT_LT(alpha.Compare(bravo), 0);
  EXPECT_GT(bravo.Compare(alpha), 0);
  EXPECT_EQ(0, alpha.Compare(alpha_copy));
}

TEST(SliceTest, StartsWithMatchesPrefixesOnly) {
  const Slice slice("database");

  EXPECT_TRUE(slice.StartsWith(Slice("data")));
  EXPECT_TRUE(slice.StartsWith(Slice("database")));
  EXPECT_FALSE(slice.StartsWith(Slice("base")));
  EXPECT_FALSE(slice.StartsWith(Slice("database-engine")));
}

TEST(SliceTest, EqualityChecksContentAndSize) {
  const Slice left("db");
  const Slice same("db");
  const Slice different_content("da");
  const Slice different_size("dbx");

  EXPECT_TRUE(left == same);
  EXPECT_FALSE(left != same);
  EXPECT_FALSE(left == different_content);
  EXPECT_FALSE(left == different_size);
  EXPECT_TRUE(left != different_content);
  EXPECT_TRUE(left != different_size);
}

}  // namespace db
