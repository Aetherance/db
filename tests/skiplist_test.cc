#include "db/skiplist.h"

#include <array>
#include <set>
#include <vector>

#include "gtest/gtest.h"
#include "util/arena.h"

namespace db {

namespace {

struct IntComparator {
  int operator()(int lhs, int rhs) const {
    if (lhs < rhs) {
      return -1;
    }
    if (lhs > rhs) {
      return 1;
    }
    return 0;
  }
};

using IntSkipList = SkipList<int, IntComparator>;

std::vector<int> CollectForward(IntSkipList::Iterator* iter) {
  std::vector<int> values;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    values.push_back(iter->key());
  }
  return values;
}

std::vector<int> CollectBackward(IntSkipList::Iterator* iter) {
  std::vector<int> values;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    values.push_back(iter->key());
  }
  return values;
}

}  // namespace

TEST(SkipListTest, EmptyListBehavesAsExpected) {
  Arena arena;
  IntSkipList list(IntComparator{}, &arena);
  IntSkipList::Iterator iter(&list);

  EXPECT_FALSE(list.Contains(0));
  EXPECT_FALSE(list.Contains(42));

  iter.SeekToFirst();
  EXPECT_FALSE(iter.Valid());

  iter.SeekToLast();
  EXPECT_FALSE(iter.Valid());

  iter.Seek(10);
  EXPECT_FALSE(iter.Valid());
}

TEST(SkipListTest, InsertContainsAndIterationProduceSortedOrder) {
  Arena arena;
  IntSkipList list(IntComparator{}, &arena);
  constexpr std::array<int, 8> kValues = {8, 1, 5, 3, 12, 7, 9, 2};

  for (int value : kValues) {
    list.Insert(value);
  }

  for (int value : kValues) {
    EXPECT_TRUE(list.Contains(value));
  }

  EXPECT_FALSE(list.Contains(-1));
  EXPECT_FALSE(list.Contains(4));
  EXPECT_FALSE(list.Contains(13));

  IntSkipList::Iterator iter(&list);
  EXPECT_EQ((std::vector<int>{1, 2, 3, 5, 7, 8, 9, 12}), CollectForward(&iter));
  EXPECT_EQ((std::vector<int>{12, 9, 8, 7, 5, 3, 2, 1}), CollectBackward(&iter));
}

TEST(SkipListTest, SeekFindsExactAndNextGreaterKeys) {
  Arena arena;
  IntSkipList list(IntComparator{}, &arena);

  for (int value : {10, 20, 30, 40}) {
    list.Insert(value);
  }

  IntSkipList::Iterator iter(&list);

  iter.Seek(10);
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(10, iter.key());

  iter.Seek(25);
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(30, iter.key());

  iter.Seek(40);
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(40, iter.key());

  iter.Seek(41);
  EXPECT_FALSE(iter.Valid());

  iter.Seek(-100);
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(10, iter.key());
}

TEST(SkipListTest, PrevAndNextRespectBoundaries) {
  Arena arena;
  IntSkipList list(IntComparator{}, &arena);

  for (int value : {15, 5, 25}) {
    list.Insert(value);
  }

  IntSkipList::Iterator iter(&list);

  iter.SeekToFirst();
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(5, iter.key());
  iter.Prev();
  EXPECT_FALSE(iter.Valid());

  iter.Seek(15);
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(15, iter.key());
  iter.Next();
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(25, iter.key());
  iter.Next();
  EXPECT_FALSE(iter.Valid());

  iter.SeekToLast();
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(25, iter.key());
  iter.Prev();
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(15, iter.key());
}

TEST(SkipListTest, IteratorMatchesReferenceSetAcrossManyInsertions) {
  Arena arena;
  IntSkipList list(IntComparator{}, &arena);
  std::set<int> reference;

  for (int value = 0; value < 200; ++value) {
    const int key = (value * 37) % 200;
    reference.insert(key);
    list.Insert(key);
  }

  for (int key = 0; key < 200; ++key) {
    EXPECT_EQ(reference.contains(key), list.Contains(key));
  }

  IntSkipList::Iterator iter(&list);
  const std::vector<int> actual = CollectForward(&iter);
  const std::vector<int> expected(reference.begin(), reference.end());
  EXPECT_EQ(expected, actual);
}

}  // namespace db
