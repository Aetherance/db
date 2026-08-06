#include "db/db_impl.h"

#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "db.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "env.h"
#include "gtest/gtest.h"
#include "write_batch.h"

namespace db {
namespace {

std::string MakeDBName(const std::string& name) {
  Env* env = Env::Default();
  std::string root;
  EXPECT_TRUE(env->GetTestDirectory(&root).Ok());
  return root + "/db-impl-test-" + std::to_string(env->NowMicros()) + "-" + name;
}

class ManualScheduleEnv : public EnvWrapper {
public:
  explicit ManualScheduleEnv(Env* target)
      : EnvWrapper(target), function_(nullptr), argument_(nullptr) {}

  void Schedule(void (*function)(void*), void* argument) override {
    std::lock_guard<std::mutex> lock(mutex_);
    EXPECT_EQ(nullptr, function_);
    function_ = function;
    argument_ = argument;
  }

  bool HasScheduledWork() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return function_ != nullptr;
  }

  bool RunScheduledWork() {
    void (*function)(void*) = nullptr;
    void* argument = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      function = function_;
      argument = argument_;
      function_ = nullptr;
      argument_ = nullptr;
    }
    if (function == nullptr) {
      return false;
    }
    function(argument);
    return true;
  }

private:
  mutable std::mutex mutex_;
  void (*function_)(void*);
  void* argument_;
};

class DBImplTest : public ::testing::Test {
protected:
  void SetUp() override {
    options_.env = Env::Default();
    options_.create_if_missing = true;
    dbname_ = MakeDBName(::testing::UnitTest::GetInstance()->current_test_info()->name());
    DestroyDB(dbname_, options_);
  }

  void TearDown() override {
    db_.reset();
    DestroyDB(dbname_, options_);
  }

  void Open() {
    DB* db = nullptr;
    Status status = DB::Open(options_, dbname_, &db);
    ASSERT_TRUE(status.Ok()) << status.ToString();
    db_.reset(db);
  }

  void Reopen() {
    db_.reset();
    options_.create_if_missing = false;
    Open();
  }

  Status Get(const std::string& key, std::string* value) {
    return db_->Get(ReadOptions(), key, value);
  }

  int NumFilesAtLevel(int level) {
    std::string value;
    const std::string property = "db.num-files-at-level" + std::to_string(level);
    if (!db_->GetProperty(property, &value)) {
      ADD_FAILURE() << "missing property " << property;
      return -1;
    }
    return std::stoi(value);
  }

  std::set<uint64_t> TableFiles() {
    std::vector<std::string> children;
    EXPECT_TRUE(options_.env->GetChildren(dbname_, &children).Ok());
    std::set<uint64_t> files;
    for (const std::string& child : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(child, &number, &type) && type == kTableFile) {
        files.insert(number);
      }
    }
    return files;
  }

  Options options_;
  std::string dbname_;
  std::unique_ptr<DB> db_;
};

TEST_F(DBImplTest, PutGetDeleteAndReopen) {
  Open();

  ASSERT_TRUE(db_->Put(WriteOptions(), "alpha", "one").Ok());
  std::string value;
  ASSERT_TRUE(Get("alpha", &value).Ok());
  EXPECT_EQ("one", value);

  ASSERT_TRUE(db_->Delete(WriteOptions(), "alpha").Ok());
  EXPECT_TRUE(Get("alpha", &value).IsNotFound());

  ASSERT_TRUE(db_->Put(WriteOptions(), "beta", "two").Ok());
  Reopen();

  EXPECT_TRUE(Get("alpha", &value).IsNotFound());
  ASSERT_TRUE(Get("beta", &value).Ok());
  EXPECT_EQ("two", value);
}

TEST_F(DBImplTest, SnapshotsReadStableSequence) {
  Open();

  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "v1").Ok());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "v2").Ok());

  ReadOptions snapshot_read;
  snapshot_read.snapshot = snapshot;
  std::string value;
  ASSERT_TRUE(db_->Get(snapshot_read, "key", &value).Ok());
  EXPECT_EQ("v1", value);

  ASSERT_TRUE(Get("key", &value).Ok());
  EXPECT_EQ("v2", value);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBImplTest, WriteBatchPersistsAtomically) {
  Open();

  WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("a");
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).Ok());

  Reopen();

  std::string value;
  EXPECT_TRUE(Get("a", &value).IsNotFound());
  ASSERT_TRUE(Get("b", &value).Ok());
  EXPECT_EQ("2", value);
}

TEST_F(DBImplTest, NullWriteBatchIsRejected) {
  Open();

  const Status status = db_->Write(WriteOptions(), nullptr);
  EXPECT_EQ(Status::Code::kInvalidArgument, status.GetCode());
}

TEST_F(DBImplTest, DatabaseLockExcludesOtherOpensAndDestroy) {
  Open();
  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "value").Ok());
  EXPECT_TRUE(options_.env->FileExists(LockFileName(dbname_)));

  DB* second = nullptr;
  const Status second_open = DB::Open(options_, dbname_, &second);
  EXPECT_FALSE(second_open.Ok());
  EXPECT_EQ(nullptr, second);

  const Status destroy_while_open = DestroyDB(dbname_, options_);
  EXPECT_FALSE(destroy_while_open.Ok());

  std::string value;
  ASSERT_TRUE(Get("key", &value).Ok());
  EXPECT_EQ("value", value);

  db_.reset();
  Open();
  ASSERT_TRUE(Get("key", &value).Ok());
  EXPECT_EQ("value", value);
}

TEST_F(DBImplTest, ForcedMemTableFlushBuildsRecoverableTable) {
  options_.write_buffer_size = 1024;
  Open();

  const std::string large_value(70 * 1024, 'x');
  ASSERT_TRUE(db_->Put(WriteOptions(), "large", large_value).Ok());
  db_->CompactRange(nullptr, nullptr);

  std::vector<std::string> children;
  ASSERT_TRUE(options_.env->GetChildren(dbname_, &children).Ok());
  bool saw_table = false;
  for (const std::string& child : children) {
    if (child.size() > 4 && child.substr(child.size() - 4) == ".sst") {
      saw_table = true;
      break;
    }
  }
  EXPECT_TRUE(saw_table);

  Reopen();

  std::string value;
  ASSERT_TRUE(Get("large", &value).Ok());
  EXPECT_EQ(large_value, value);
}

TEST_F(DBImplTest, MultipleManualFlushesRemainReadable) {
  Open();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int i = 0; i < 6; i++) {
    keys.push_back("key-" + std::to_string(i));
    values.push_back(std::string(1024, static_cast<char>('a' + i)));
    ASSERT_TRUE(db_->Put(WriteOptions(), keys.back(), values.back()).Ok());
    db_->CompactRange(nullptr, nullptr);
  }

  EXPECT_EQ(0, NumFilesAtLevel(0));
  EXPECT_EQ(6, NumFilesAtLevel(1));
  Reopen();
  for (size_t i = 0; i < keys.size(); i++) {
    std::string value;
    ASSERT_TRUE(Get(keys[i], &value).Ok()) << keys[i];
    EXPECT_EQ(values[i], value);
  }
}

TEST_F(DBImplTest, FlushTriggerRunsBackgroundCompaction) {
  options_.write_buffer_size = 1024;
  Open();

  const std::string value(70 * 1024, 'x');
  for (int i = 0; i < config::kL0_CompactionTrigger + 2; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), "key-" + std::to_string(i), value).Ok());
  }
  db_->CompactRange(nullptr, nullptr);

  EXPECT_LT(NumFilesAtLevel(0), config::kL0_CompactionTrigger);
  EXPECT_GT(NumFilesAtLevel(1), 0);
  Reopen();
  for (int i = 0; i < config::kL0_CompactionTrigger + 2; ++i) {
    std::string actual;
    ASSERT_TRUE(Get("key-" + std::to_string(i), &actual).Ok());
    EXPECT_EQ(value, actual);
  }
}

TEST_F(DBImplTest, CompactionPreservesSnapshotVersions) {
  Open();

  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "old").Ok());
  const Snapshot* snapshot = db_->GetSnapshot();
  db_->CompactRange(nullptr, nullptr);
  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "new").Ok());
  db_->CompactRange(nullptr, nullptr);

  ReadOptions read;
  read.snapshot = snapshot;
  std::string value;
  ASSERT_TRUE(db_->Get(read, "key", &value).Ok());
  EXPECT_EQ("old", value);
  ASSERT_TRUE(Get("key", &value).Ok());
  EXPECT_EQ("new", value);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBImplTest, IteratorMergesMemTableAndLevel0Tables) {
  Open();

  ASSERT_TRUE(db_->Put(WriteOptions(), "alpha", "one").Ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "charlie", "three").Ok());
  db_->CompactRange(nullptr, nullptr);
  ASSERT_TRUE(db_->Put(WriteOptions(), "bravo", "two").Ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "delta", "four").Ok());

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  std::vector<std::string> keys;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys.push_back(iter->Key().ToString());
  }
  EXPECT_TRUE(iter->GetStatus().Ok());
  EXPECT_EQ((std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}), keys);
}

TEST_F(DBImplTest, IteratorPinsItsVersionFiles) {
  Open();

  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "old").Ok());
  db_->CompactRange(nullptr, nullptr);
  const std::set<uint64_t> original_files = TableFiles();
  ASSERT_EQ(1U, original_files.size());
  const uint64_t original_file = *original_files.begin();

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_TRUE(db_->Put(WriteOptions(), "key", "new").Ok());
  db_->CompactRange(nullptr, nullptr);

  // The iterator still references the previous Version, so both its table and
  // the replacement table must remain live.
  const std::set<uint64_t> pinned_files = TableFiles();
  EXPECT_EQ(2U, pinned_files.size());
  EXPECT_EQ(1U, pinned_files.count(original_file));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("key", iter->Key());
  EXPECT_EQ("old", iter->Value());

  iter.reset();
  ASSERT_TRUE(db_->Put(WriteOptions(), "other", "value").Ok());
  db_->CompactRange(nullptr, nullptr);
  EXPECT_EQ(0U, TableFiles().count(original_file));
}

TEST(DBImplAsyncFlushTest, ReadsFromMutableAndImmutableMemTablesWhileFlushIsQueued) {
  ManualScheduleEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.write_buffer_size = 64 * 1024;
  const std::string dbname = MakeDBName("manual-background-flush");
  ASSERT_TRUE(DestroyDB(dbname, options).Ok());

  DB* raw_db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &raw_db).Ok());
  std::unique_ptr<DB> db(raw_db);

  const std::string large_value(70 * 1024, 'x');
  ASSERT_TRUE(db->Put(WriteOptions(), "immutable", large_value).Ok());
  ASSERT_TRUE(env.HasScheduledWork());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "immutable", &value).Ok());
  EXPECT_EQ(large_value, value);

  ASSERT_TRUE(db->Put(WriteOptions(), "mutable", "new-value").Ok());
  ASSERT_TRUE(db->Get(ReadOptions(), "mutable", &value).Ok());
  EXPECT_EQ("new-value", value);

  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
  std::vector<std::string> keys;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys.push_back(iter->Key().ToString());
  }
  ASSERT_TRUE(iter->GetStatus().Ok());
  EXPECT_EQ((std::vector<std::string>{"immutable", "mutable"}), keys);
  iter.reset();

  ASSERT_TRUE(env.RunScheduledWork());
  EXPECT_FALSE(env.HasScheduledWork());

  db.reset();
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, dbname, &raw_db).Ok());
  db.reset(raw_db);
  ASSERT_TRUE(db->Get(ReadOptions(), "immutable", &value).Ok());
  EXPECT_EQ(large_value, value);
  ASSERT_TRUE(db->Get(ReadOptions(), "mutable", &value).Ok());
  EXPECT_EQ("new-value", value);

  db.reset();
  ASSERT_TRUE(DestroyDB(dbname, options).Ok());
}

}  // namespace
}  // namespace db
