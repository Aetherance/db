#include <cstdint>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "gtest/gtest.h"
#include "table/filter_block.h"
#include "util/coding.h"

namespace db {
namespace {

class TestFilterPolicy : public FilterPolicy {
public:
  const char* Name() const override {
    return "db.TestFilterPolicy";
  }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    for (int i = 0; i < n; ++i) {
      PutLengthPrefixedSlice(dst, keys[i]);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    Slice input = filter;
    Slice entry;
    while (GetLengthPrefixedSlice(&input, &entry)) {
      if (entry == key) {
        return true;
      }
    }
    return false;
  }
};

std::string BuildFilter(const FilterPolicy& policy, const std::vector<Slice>& keys) {
  std::string filter;
  policy.CreateFilter(keys.data(), static_cast<int>(keys.size()), &filter);
  return filter;
}

TEST(FilterTest, InternalFilterPolicyBuildsFiltersOverUserKeys) {
  TestFilterPolicy user_policy;
  InternalFilterPolicy internal_policy(&user_policy);

  const InternalKey alpha_value("alpha", 100, kTypeValue);
  const InternalKey beta_delete("beta", 90, kTypeDeletion);
  std::vector<Slice> keys = {alpha_value.Encode(), beta_delete.Encode()};

  const std::string filter = BuildFilter(internal_policy, keys);

  EXPECT_STREQ(user_policy.Name(), internal_policy.Name());
  EXPECT_TRUE(internal_policy.KeyMayMatch(InternalKey("alpha", 1, kTypeDeletion).Encode(), filter));
  EXPECT_TRUE(internal_policy.KeyMayMatch(InternalKey("beta", 999, kTypeValue).Encode(), filter));
  EXPECT_FALSE(internal_policy.KeyMayMatch(InternalKey("gamma", 1, kTypeValue).Encode(), filter));
}

TEST(FilterTest, FilterBlockReaderMatchesKeysInTheirOwnBlockRange) {
  constexpr uint64_t kFilterBase = 1ULL << 11;

  TestFilterPolicy policy;
  FilterBlockBuilder builder(&policy);
  builder.StartBlock(0);
  builder.AddKey("alpha");
  builder.AddKey("beta");
  builder.StartBlock(kFilterBase);
  builder.AddKey("gamma");
  builder.StartBlock(2 * kFilterBase);
  builder.AddKey("delta");

  const Slice contents = builder.Finish();
  FilterBlockReader reader(&policy, contents);

  EXPECT_TRUE(reader.KeyMayMatch(0, "alpha"));
  EXPECT_TRUE(reader.KeyMayMatch(0, "beta"));
  EXPECT_FALSE(reader.KeyMayMatch(0, "gamma"));

  EXPECT_TRUE(reader.KeyMayMatch(kFilterBase, "gamma"));
  EXPECT_FALSE(reader.KeyMayMatch(kFilterBase, "alpha"));

  EXPECT_TRUE(reader.KeyMayMatch(2 * kFilterBase, "delta"));
  EXPECT_FALSE(reader.KeyMayMatch(2 * kFilterBase, "gamma"));
}

TEST(FilterTest, FilterBlockReaderTreatsSkippedBlocksAsEmptyAndUnknownBlocksConservatively) {
  constexpr uint64_t kFilterBase = 1ULL << 11;

  TestFilterPolicy policy;
  FilterBlockBuilder builder(&policy);
  builder.StartBlock(0);
  builder.AddKey("alpha");
  builder.StartBlock(2 * kFilterBase);
  builder.AddKey("beta");

  const Slice contents = builder.Finish();
  FilterBlockReader reader(&policy, contents);

  EXPECT_TRUE(reader.KeyMayMatch(0, "alpha"));
  EXPECT_FALSE(reader.KeyMayMatch(kFilterBase, "alpha"));
  EXPECT_TRUE(reader.KeyMayMatch(2 * kFilterBase, "beta"));

  // Unknown blocks outside the encoded filter array must return "may match".
  EXPECT_TRUE(reader.KeyMayMatch(10 * kFilterBase, "anything"));
}

TEST(FilterTest, FilterBlockReaderTreatsCorruptContentsConservatively) {
  TestFilterPolicy policy;
  FilterBlockReader reader(&policy, Slice("bad", 3));

  EXPECT_TRUE(reader.KeyMayMatch(0, "alpha"));
}

}  // namespace
}  // namespace db
