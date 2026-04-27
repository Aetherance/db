#include "db/dbformat.h"

#include <string>

#include "gtest/gtest.h"
#include "util/coding.h"

namespace db {
namespace {

TEST(DbFormatTest, AppendAndParseInternalKeyRoundTrips) {
  const ParsedInternalKey input("alpha", 123, kTypeValue);
  std::string encoded;

  AppendInternalKey(&encoded, input);

  ParsedInternalKey parsed;
  ASSERT_TRUE(ParseInternalKey(encoded, &parsed));
  EXPECT_EQ("alpha", parsed.user_key.ToString());
  EXPECT_EQ(123, parsed.sequence);
  EXPECT_EQ(kTypeValue, parsed.type);
}

TEST(DbFormatTest, ParseInternalKeyRejectsTooShortAndBadType) {
  ParsedInternalKey parsed;
  EXPECT_FALSE(ParseInternalKey(Slice("short", 5), &parsed));

  std::string encoded = "alpha";
  PutFixed64(&encoded, (100ULL << 8) | 2ULL);
  EXPECT_FALSE(ParseInternalKey(encoded, &parsed));
}

TEST(DbFormatTest, InternalKeyComparatorOrdersByUserKeyThenDescendingTag) {
  InternalKeyComparator comparator(BytewiseComparator());

  const InternalKey apple("apple", 80, kTypeValue);
  const InternalKey banana("banana", 10, kTypeValue);
  const InternalKey key_seq100("key", 100, kTypeValue);
  const InternalKey key_seq90("key", 90, kTypeValue);
  const InternalKey key_del100("key", 100, kTypeDeletion);

  EXPECT_LT(comparator.Compare(apple, banana), 0);
  EXPECT_LT(comparator.Compare(key_seq100, key_seq90), 0);
  EXPECT_LT(comparator.Compare(key_seq100, key_del100), 0);
  EXPECT_GT(comparator.Compare(key_seq90, key_seq100), 0);
}

TEST(DbFormatTest, InternalKeyExposesUserKeyAndDecodeFromRoundTrips) {
  const ParsedInternalKey parsed("user-key", 77, kTypeDeletion);
  InternalKey key;

  key.SetFrom(parsed);

  EXPECT_EQ("user-key", key.user_key().ToString());

  InternalKey decoded;
  ASSERT_TRUE(decoded.DecodeFrom(key.Encode()));
  EXPECT_EQ("user-key", decoded.user_key().ToString());
  EXPECT_EQ("'user-key' @77 : 0", decoded.DebugString());
}

TEST(DbFormatTest, LookupKeyEncodesMemtableAndInternalViews) {
  LookupKey lookup("hello", 321);

  EXPECT_EQ("hello", lookup.user_key().ToString());
  EXPECT_EQ("hello", ExtractUserKey(lookup.internal_key()).ToString());
  EXPECT_EQ(lookup.memtable_key().Size(), lookup.internal_key().Size() + 1);

  uint32_t internal_key_size = 0;
  Slice memtable_key = lookup.memtable_key();
  const char* parsed =
      GetVarint32Ptr(memtable_key.Data(), memtable_key.Data() + 5, &internal_key_size);
  ASSERT_NE(nullptr, parsed);
  EXPECT_EQ(lookup.internal_key().Size(), internal_key_size);
  EXPECT_EQ(lookup.internal_key().Data(), parsed);

  ParsedInternalKey decoded;
  ASSERT_TRUE(ParseInternalKey(lookup.internal_key(), &decoded));
  EXPECT_EQ("hello", decoded.user_key.ToString());
  EXPECT_EQ(321, decoded.sequence);
  EXPECT_EQ(kTypeValue, decoded.type);
}

TEST(DbFormatTest, LookupKeySupportsHeapAllocatedUserKeys) {
  const std::string long_key(256, 'x');
  LookupKey lookup(long_key, 999);

  ParsedInternalKey decoded;
  ASSERT_TRUE(ParseInternalKey(lookup.internal_key(), &decoded));
  EXPECT_EQ(long_key, decoded.user_key.ToString());
  EXPECT_EQ(999, decoded.sequence);
  EXPECT_EQ(kTypeValue, decoded.type);
}

TEST(DbFormatTest, InternalKeyComparatorFindShortestSeparatorUsesUserComparator) {
  InternalKeyComparator comparator(BytewiseComparator());
  const std::string limit = InternalKey("alpha3", 90, kTypeValue).Encode().ToString();
  std::string start = InternalKey("alpha1zz", 100, kTypeValue).Encode().ToString();
  const std::string original = start;

  comparator.FindShortestSeparator(&start, limit);

  ParsedInternalKey parsed;
  ASSERT_TRUE(ParseInternalKey(start, &parsed));
  EXPECT_EQ("alpha2", parsed.user_key.ToString());
  EXPECT_EQ(kMaxSequenceNumber, parsed.sequence);
  EXPECT_EQ(kTypeValue, parsed.type);
  EXPECT_LT(comparator.Compare(original, start), 0);
  EXPECT_LT(comparator.Compare(start, limit), 0);
}

TEST(DbFormatTest, InternalKeyComparatorFindShortSuccessorUsesUserComparator) {
  InternalKeyComparator comparator(BytewiseComparator());
  std::string key = InternalKey("bravo", 100, kTypeDeletion).Encode().ToString();
  const std::string original = key;

  comparator.FindShortSuccessor(&key);

  ParsedInternalKey parsed;
  ASSERT_TRUE(ParseInternalKey(key, &parsed));
  EXPECT_EQ("c", parsed.user_key.ToString());
  EXPECT_EQ(kMaxSequenceNumber, parsed.sequence);
  EXPECT_EQ(kTypeValue, parsed.type);
  EXPECT_LT(comparator.Compare(original, key), 0);
}

TEST(DbFormatTest, InternalKeyDebugStringMarksBadEncodings) {
  InternalKey key;
  ASSERT_TRUE(key.DecodeFrom(Slice("bad", 3)));

  EXPECT_EQ("(bad)bad", key.DebugString());
}

}  // namespace
}  // namespace db
