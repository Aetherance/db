#include "table.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache.h"
#include "env.h"
#include "gtest/gtest.h"
#include "iterator.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include "table_builder.h"

namespace db {
namespace {

class StringWritableFile : public WritableFile {
public:
  Status Append(const Slice& data) override {
    contents_.append(data.Data(), data.Size());
    return Status::OkStatus();
  }

  Status Close() override {
    return Status::OkStatus();
  }
  Status Flush() override {
    return Status::OkStatus();
  }
  Status Sync() override {
    return Status::OkStatus();
  }

  const std::string& contents() const {
    return contents_;
  }

private:
  std::string contents_;
};

class StringRandomAccessFile : public RandomAccessFile {
public:
  explicit StringRandomAccessFile(std::string data) : data_(std::move(data)) {}

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
    if (offset > data_.size()) {
      *result = Slice();
      return Status::IOError("read past end of file");
    }
    size_t available = data_.size() - static_cast<size_t>(offset);
    if (n > available) n = available;
    if (n <= 32) {
      std::memcpy(scratch, data_.data() + offset, n);
      *result = Slice(scratch, n);
    } else {
      *result = Slice(data_.data() + offset, n);
    }
    return Status::OkStatus();
  }

private:
  std::string data_;
};

class CountingRandomAccessFile : public RandomAccessFile {
public:
  enum class ReadMode {
    kCopyToScratch,
    kReturnInternal,
  };

  CountingRandomAccessFile(std::string data, ReadMode mode) : data_(std::move(data)), mode_(mode) {}

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
    ++read_count_;
    if (offset > data_.size()) {
      *result = Slice();
      return Status::IOError("read past end of file");
    }

    size_t available = data_.size() - static_cast<size_t>(offset);
    if (n > available) n = available;
    if (mode_ == ReadMode::kReturnInternal) {
      *result = Slice(data_.data() + offset, n);
    } else {
      std::memcpy(scratch, data_.data() + offset, n);
      if (corrupt_reads_ && n > 0) {
        scratch[0] ^= 0x01;
      }
      *result = Slice(scratch, n);
    }
    return Status::OkStatus();
  }

  int read_count() const {
    return read_count_;
  }

  void set_corrupt_reads(bool value) {
    corrupt_reads_ = value;
  }

private:
  std::string data_;
  ReadMode mode_;
  mutable int read_count_ = 0;
  bool corrupt_reads_ = false;
};

std::string BuildTableData(const std::vector<std::pair<std::string, std::string>>& entries,
                           Options options = Options()) {
  options.compression = kNoCompression;
  options.block_size = 256;

  StringWritableFile file;
  TableBuilder builder(options, &file);

  for (const auto& [key, value] : entries) {
    builder.Add(key, value);
  }
  EXPECT_TRUE(builder.Finish().Ok());
  return file.contents();
}

Status OpenTable(const std::string& data, const Options& options, Table** table) {
  auto* file = new StringRandomAccessFile(data);
  Status s = Table::Open(options, file, data.size(), table);
  if (!s.Ok()) {
    delete file;
  }
  return s;
}

std::vector<std::pair<std::string, std::string>> IterateAll(Iterator* iter) {
  std::vector<std::pair<std::string, std::string>> result;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    result.push_back({iter->Key().ToString(), iter->Value().ToString()});
  }
  return result;
}

Status ReadTableValue(Table* table, const ReadOptions& options, const Slice& key,
                      std::string* value) {
  std::unique_ptr<Iterator> iter(table->NewIterator(options));
  iter->Seek(key);
  Status status = iter->GetStatus();
  if (!status.Ok()) {
    return status;
  }
  if (!iter->Valid()) {
    return Status::NotFound(key);
  }
  value->assign(iter->Value().Data(), iter->Value().Size());
  return Status::OkStatus();
}

TEST(TableTest, OpenValidTable) {
  std::string data = BuildTableData({{"key1", "val1"}, {"key2", "val2"}});

  Options options;
  Table* table = nullptr;
  Status s = OpenTable(data, options, &table);
  ASSERT_TRUE(s.Ok());
  ASSERT_NE(nullptr, table);
  delete table;
}

TEST(TableTest, OpenTooShortFile) {
  Options options;
  Table* table = nullptr;
  Status s = OpenTable("short", options, &table);
  EXPECT_FALSE(s.Ok());
  EXPECT_EQ(nullptr, table);
}

TEST(TableTest, OpenCorruptedFooter) {
  std::string data = BuildTableData({{"key1", "val1"}});

  Options options;
  Table* table = nullptr;
  // Overwrite the last byte (part of magic number) to corrupt footer.
  data.back() = '\x00';
  Status s = OpenTable(data, options, &table);
  EXPECT_FALSE(s.Ok());
  EXPECT_EQ(nullptr, table);
}

TEST(TableTest, IterateForward) {
  std::vector<std::pair<std::string, std::string>> entries = {
      {"alpha", "1"},
      {"beta", "2"},
      {"gamma", "3"},
  };
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));
  auto result = IterateAll(iter.get());
  EXPECT_EQ(entries, result);

  delete table;
}

TEST(TableTest, IterateBackward) {
  std::vector<std::pair<std::string, std::string>> entries = {
      {"alpha", "1"},
      {"beta", "2"},
      {"gamma", "3"},
  };
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));

  std::vector<std::pair<std::string, std::string>> result;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    result.push_back({iter->Key().ToString(), iter->Value().ToString()});
  }

  std::vector<std::pair<std::string, std::string>> expected = {
      {"gamma", "3"},
      {"beta", "2"},
      {"alpha", "1"},
  };
  EXPECT_EQ(expected, result);

  delete table;
}

TEST(TableTest, SeekKey) {
  std::vector<std::pair<std::string, std::string>> entries = {
      {"alpha", "1"},
      {"beta", "2"},
      {"gamma", "3"},
  };
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));

  iter->Seek("beta");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("beta", iter->Key());
  EXPECT_EQ("2", iter->Value());

  iter->Seek("gamma");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("gamma", iter->Key());
  EXPECT_EQ("3", iter->Value());

  delete table;
}

TEST(TableTest, SeekBeforeFirst) {
  std::vector<std::pair<std::string, std::string>> entries = {
      {"beta", "2"},
      {"gamma", "3"},
  };
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));

  iter->Seek("alpha");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("beta", iter->Key());

  delete table;
}

TEST(TableTest, SeekAfterLast) {
  std::vector<std::pair<std::string, std::string>> entries = {
      {"alpha", "1"},
      {"beta", "2"},
  };
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));

  iter->Seek("omega");
  EXPECT_FALSE(iter->Valid());

  delete table;
}

TEST(TableTest, ApproximateOffsetOf) {
  std::vector<std::pair<std::string, std::string>> entries;
  for (int i = 0; i < 100; ++i) {
    std::string key = "key" + std::to_string(1000 + i);
    std::string val = "value" + std::to_string(i);
    entries.push_back({key, val});
  }
  std::string data = BuildTableData(entries);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  uint64_t offset_first = table->ApproximateOffsetOf("key1000");
  uint64_t offset_mid = table->ApproximateOffsetOf("key1050");
  uint64_t offset_last = table->ApproximateOffsetOf("key1099");

  EXPECT_LT(offset_first, offset_mid);
  EXPECT_LT(offset_mid, offset_last);

  delete table;
}

TEST(TableTest, MultipleBlocks) {
  Options build_opts;
  build_opts.block_size = 1;
  build_opts.compression = kNoCompression;

  std::vector<std::pair<std::string, std::string>> entries = {
      {"a", "1"},
      {"b", "2"},
      {"c", "3"},
      {"d", "4"},
  };
  std::string data = BuildTableData(entries, build_opts);

  Options options;
  Table* table = nullptr;
  ASSERT_TRUE(OpenTable(data, options, &table).Ok());

  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_opts));

  auto result = IterateAll(iter.get());
  EXPECT_EQ(entries, result);

  delete table;
}

TEST(TableTest, BlockCacheAvoidsRepeatedReads) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());
  const int reads_after_open = file.read_count();

  ReadOptions read_options;
  std::string value;
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(std::string(100, 'v'), value);
  EXPECT_EQ(reads_after_open + 1, file.read_count());
  EXPECT_GT(cache->TotalCharge(), 0u);

  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 1, file.read_count());

  delete table;
}

TEST(TableTest, FillCacheFalseDoesNotPopulateBlockCache) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());
  const int reads_after_open = file.read_count();

  ReadOptions read_options;
  read_options.fill_cache = false;
  std::string value;
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 1, file.read_count());
  EXPECT_EQ(0u, cache->TotalCharge());

  read_options.fill_cache = true;
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 2, file.read_count());
  EXPECT_GT(cache->TotalCharge(), 0u);

  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 2, file.read_count());

  delete table;
}

TEST(TableTest, ZeroCapacityBlockCacheDoesNotRetainBlocks) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(0));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());
  const int reads_after_open = file.read_count();

  ReadOptions read_options;
  std::string value;
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 2, file.read_count());
  EXPECT_EQ(0u, cache->TotalCharge());

  delete table;
}

TEST(TableTest, SharedBlockCacheKeepsTablesIsolated) {
  std::string first_data = BuildTableData({{"key", std::string(100, 'a')}});
  std::string second_data = BuildTableData({{"key", std::string(100, 'b')}});
  CountingRandomAccessFile first_file(first_data,
                                      CountingRandomAccessFile::ReadMode::kCopyToScratch);
  CountingRandomAccessFile second_file(second_data,
                                       CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* first_table = nullptr;
  Table* second_table = nullptr;
  ASSERT_TRUE(Table::Open(options, &first_file, first_data.size(), &first_table).Ok());
  ASSERT_TRUE(Table::Open(options, &second_file, second_data.size(), &second_table).Ok());
  const int first_reads_after_open = first_file.read_count();
  const int second_reads_after_open = second_file.read_count();

  ReadOptions read_options;
  std::string value;
  ASSERT_TRUE(ReadTableValue(first_table, read_options, "key", &value).Ok());
  EXPECT_EQ(std::string(100, 'a'), value);
  ASSERT_TRUE(ReadTableValue(second_table, read_options, "key", &value).Ok());
  EXPECT_EQ(std::string(100, 'b'), value);

  ASSERT_TRUE(ReadTableValue(first_table, read_options, "key", &value).Ok());
  EXPECT_EQ(std::string(100, 'a'), value);
  ASSERT_TRUE(ReadTableValue(second_table, read_options, "key", &value).Ok());
  EXPECT_EQ(std::string(100, 'b'), value);
  EXPECT_EQ(first_reads_after_open + 1, first_file.read_count());
  EXPECT_EQ(second_reads_after_open + 1, second_file.read_count());

  delete first_table;
  delete second_table;
}

TEST(TableTest, MmapBlocksBypassBlockCache) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kReturnInternal);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());
  const int reads_after_open = file.read_count();

  ReadOptions read_options;
  std::string value;
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_EQ(reads_after_open + 2, file.read_count());
  EXPECT_EQ(0u, cache->TotalCharge());

  delete table;
}

TEST(TableTest, ActiveIteratorPinsCachedBlock) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());

  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(table->NewIterator(read_options));
  iter->Seek("key");
  ASSERT_TRUE(iter->Valid());
  ASSERT_GT(cache->TotalCharge(), 0u);

  cache->Prune();
  EXPECT_GT(cache->TotalCharge(), 0u);
  iter.reset();
  cache->Prune();
  EXPECT_EQ(0u, cache->TotalCharge());

  delete table;
}

TEST(TableTest, CorruptedBlockIsNotCached) {
  std::string data = BuildTableData({{"key", std::string(100, 'v')}});
  CountingRandomAccessFile file(data, CountingRandomAccessFile::ReadMode::kCopyToScratch);
  std::unique_ptr<Cache> cache(NewLRUCache(1 << 20));

  Options options;
  options.block_cache = cache.get();
  Table* table = nullptr;
  ASSERT_TRUE(Table::Open(options, &file, data.size(), &table).Ok());

  ReadOptions read_options;
  read_options.verify_checksums = true;
  file.set_corrupt_reads(true);
  std::string value;
  EXPECT_EQ(Status::Code::kCorruption,
            ReadTableValue(table, read_options, "key", &value).GetCode());
  EXPECT_EQ(0u, cache->TotalCharge());

  file.set_corrupt_reads(false);
  ASSERT_TRUE(ReadTableValue(table, read_options, "key", &value).Ok());
  EXPECT_GT(cache->TotalCharge(), 0u);

  delete table;
}

}  // namespace
}  // namespace db
