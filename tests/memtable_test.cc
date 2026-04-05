#include "db/memtable.h"

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"
#include "iterator.h"

namespace db {
namespace {

class TestComparator : public Comparator {
public:
  int Compare(const Slice& lhs, const Slice& rhs) const override {
    return lhs.Compare(rhs);
  }

  const char* Name() const override {
    return "db.TestComparator";
  }

  void FindShortestSeparator(std::string* start, const Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

struct LookupResult {
  bool found = false;
  Status status = Status::OkStatus();
  std::string value;
};

class MemTableTest : public ::testing::Test {
protected:
  MemTableTest() : comparator_(&user_comparator_), table_(new MemTable(comparator_)) {
    table_->Ref();
  }

  ~MemTableTest() override {
    table_->Unref();
  }

  void Add(SequenceNumber sequence, ValueType type, const std::string& user_key,
           const std::string& value = "") {
    table_->Add(sequence, type, Slice(user_key), Slice(value));
  }

  LookupResult Get(const std::string& user_key, SequenceNumber sequence) {
    LookupResult result;
    LookupKey key(user_key, sequence);
    result.found = table_->Get(key, &result.value, &result.status);
    return result;
  }

  ParsedInternalKey ParseKey(const Slice& internal_key) {
    ParsedInternalKey parsed;
    EXPECT_TRUE(ParseInternalKey(internal_key, &parsed));
    return parsed;
  }

  TestComparator user_comparator_;
  InternalKeyComparator comparator_;
  MemTable* table_;
};

TEST_F(MemTableTest, GetReturnsInsertedValue) {
  Add(100, kTypeValue, "alpha", "one");

  const LookupResult result = Get("alpha", 100);

  EXPECT_TRUE(result.found);
  EXPECT_TRUE(result.status.Ok());
  EXPECT_EQ("one", result.value);
}

TEST_F(MemTableTest, GetReturnsNewestVisibleVersion) {
  Add(100, kTypeValue, "key", "v100");
  Add(90, kTypeValue, "key", "v90");
  Add(80, kTypeValue, "key", "v80");

  EXPECT_EQ("v100", Get("key", 120).value);
  EXPECT_EQ("v90", Get("key", 95).value);
  EXPECT_EQ("v80", Get("key", 80).value);

  const LookupResult missing = Get("key", 79);
  EXPECT_FALSE(missing.found);
  EXPECT_TRUE(missing.status.Ok());
}

TEST_F(MemTableTest, DeletionHidesOlderValueAtNewerSnapshot) {
  Add(100, kTypeValue, "key", "value");
  Add(105, kTypeDeletion, "key");

  const LookupResult deleted = Get("key", 110);
  EXPECT_TRUE(deleted.found);
  EXPECT_EQ(Status::Code::kNotFound, deleted.status.GetCode());
  EXPECT_TRUE(deleted.value.empty());

  const LookupResult visible = Get("key", 100);
  EXPECT_TRUE(visible.found);
  EXPECT_TRUE(visible.status.Ok());
  EXPECT_EQ("value", visible.value);
}

TEST_F(MemTableTest, IteratorYieldsEntriesInInternalKeyOrder) {
  Add(90, kTypeValue, "key", "v90");
  Add(50, kTypeValue, "apple", "fruit");
  Add(100, kTypeValue, "key", "v100");
  Add(105, kTypeValue, "zoo", "animal");

  std::unique_ptr<Iterator> iter(table_->NewIterator());
  std::vector<std::tuple<std::string, SequenceNumber, std::string>> entries;

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    const ParsedInternalKey parsed = ParseKey(iter->Key());
    entries.emplace_back(parsed.user_key.ToString(), parsed.sequence, iter->Value().ToString());
  }

  const std::vector<std::tuple<std::string, SequenceNumber, std::string>> expected = {
      {"apple", 50, "fruit"},
      {"key", 100, "v100"},
      {"key", 90, "v90"},
      {"zoo", 105, "animal"},
  };

  EXPECT_EQ(expected, entries);
}

TEST_F(MemTableTest, IteratorSeekUsesInternalKeys) {
  Add(100, kTypeValue, "key", "v100");
  Add(90, kTypeValue, "key", "v90");
  Add(80, kTypeValue, "other", "other-value");

  std::unique_ptr<Iterator> iter(table_->NewIterator());
  InternalKey target("key", 95, kTypeValue);

  iter->Seek(target.Encode());
  ASSERT_TRUE(iter->Valid());

  const ParsedInternalKey parsed = ParseKey(iter->Key());
  EXPECT_EQ("key", parsed.user_key.ToString());
  EXPECT_EQ(90, parsed.sequence);
  EXPECT_EQ("v90", iter->Value().ToString());
}

}  // namespace
}  // namespace db
