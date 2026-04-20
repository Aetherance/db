#include <memory>
#include <string>
#include <vector>

#include "filter_policy.h"
#include "gtest/gtest.h"
#include "slice.h"

namespace db {
namespace {

std::string BuildFilter(const FilterPolicy& policy, const std::vector<std::string>& keys) {
  std::vector<Slice> key_slices;
  key_slices.reserve(keys.size());
  for (const std::string& key : keys) {
    key_slices.emplace_back(key);
  }

  std::string filter;
  policy.CreateFilter(key_slices.data(), static_cast<int>(key_slices.size()), &filter);
  return filter;
}

TEST(BloomTest, NewBloomFilterPolicyExposesExpectedName) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));

  ASSERT_NE(nullptr, policy);
  EXPECT_STREQ("db.BuiltinBloomFilter2", policy->Name());
}

TEST(BloomTest, CreateFilterAppendsToDestination) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));
  const std::vector<std::string> keys = {"alpha", "beta"};
  std::vector<Slice> key_slices;
  key_slices.reserve(keys.size());
  for (const std::string& key : keys) {
    key_slices.emplace_back(key);
  }

  std::string filter = "prefix:";
  policy->CreateFilter(key_slices.data(), static_cast<int>(key_slices.size()), &filter);

  EXPECT_TRUE(filter.starts_with("prefix:"));
  EXPECT_GT(filter.size(), std::string("prefix:").size());
}

TEST(BloomTest, InsertedKeysAlwaysMatch) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));
  const std::vector<std::string> keys = {"alpha", "beta", "delta", "epsilon"};
  const std::string filter = BuildFilter(*policy, keys);

  for (const std::string& key : keys) {
    EXPECT_TRUE(policy->KeyMayMatch(key, filter)) << key;
  }
}

TEST(BloomTest, MissingKeysCanBeRejected) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));
  const std::vector<std::string> keys = {"alpha", "beta", "delta", "epsilon"};
  const std::string filter = BuildFilter(*policy, keys);

  EXPECT_FALSE(policy->KeyMayMatch("omega", filter));
  EXPECT_FALSE(policy->KeyMayMatch("theta", filter));
}

TEST(BloomTest, SmallFilterUsesMinimumSizeAndRejectsShortEncoding) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));
  const std::vector<std::string> keys = {"a"};
  const std::string filter = BuildFilter(*policy, keys);

  EXPECT_EQ(9U, filter.size());
  EXPECT_FALSE(policy->KeyMayMatch("a", Slice()));
  EXPECT_FALSE(policy->KeyMayMatch("a", Slice("x", 1)));
}

TEST(BloomTest, UnknownProbeCountFallsBackToMatch) {
  std::unique_ptr<const FilterPolicy> policy(NewBloomFilterPolicy(10));

  std::string filter(8, '\0');
  filter.push_back(static_cast<char>(31));

  EXPECT_TRUE(policy->KeyMayMatch("anything", filter));
}

}  // namespace
}  // namespace db
