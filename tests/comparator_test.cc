#include "comparator.h"

#include <string>

#include "gtest/gtest.h"

namespace db {
namespace {

TEST(ComparatorTest, BytewiseComparatorUsesLexicographicalOrdering) {
  const Comparator* comparator = BytewiseComparator();

  ASSERT_NE(nullptr, comparator);
  EXPECT_STREQ("db.BytewiseComparator", comparator->Name());
  EXPECT_LT(comparator->Compare("alpha", "alphabet"), 0);
  EXPECT_LT(comparator->Compare("alphabet", "bravo"), 0);
  EXPECT_GT(comparator->Compare("bravo", "alphabet"), 0);
  EXPECT_EQ(0, comparator->Compare("same", "same"));
}

TEST(ComparatorTest, FindShortestSeparatorShortensWhenGapExists) {
  const Comparator* comparator = BytewiseComparator();
  std::string start = "alpha1zz";

  comparator->FindShortestSeparator(&start, "alpha3");

  EXPECT_EQ("alpha2", start);
}

TEST(ComparatorTest, FindShortestSeparatorLeavesPrefixAndTightGapUntouched) {
  const Comparator* comparator = BytewiseComparator();

  std::string prefix_case = "alpha";
  comparator->FindShortestSeparator(&prefix_case, "alphabet");
  EXPECT_EQ("alpha", prefix_case);

  std::string tight_gap = "alpha1";
  comparator->FindShortestSeparator(&tight_gap, "alpha2");
  EXPECT_EQ("alpha1", tight_gap);
}

TEST(ComparatorTest, FindShortSuccessorShortensAtFirstNonMaxByte) {
  const Comparator* comparator = BytewiseComparator();

  std::string key("\xff\x01\x02", 3);
  comparator->FindShortSuccessor(&key);

  EXPECT_EQ(std::string("\xff\x02", 2), key);
}

TEST(ComparatorTest, FindShortSuccessorLeavesAllMaxBytesUntouched) {
  const Comparator* comparator = BytewiseComparator();
  std::string key("\xff\xff", 2);

  comparator->FindShortSuccessor(&key);

  EXPECT_EQ(std::string("\xff\xff", 2), key);
}

}  // namespace
}  // namespace db
