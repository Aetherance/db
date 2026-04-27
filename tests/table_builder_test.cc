#include "table_builder.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "env.h"
#include "filter_policy.h"
#include "gtest/gtest.h"
#include "options.h"
#include "slice.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace db {
namespace {

class StringWritableFile : public WritableFile {
public:
  Status Append(const Slice& data) override {
    contents_.append(data.Data(), data.Size());
    append_sizes_.push_back(data.Size());
    return Status::OkStatus();
  }

  Status Close() override {
    closed_ = true;
    return Status::OkStatus();
  }

  Status Flush() override {
    ++flush_count_;
    return Status::OkStatus();
  }

  Status Sync() override {
    ++sync_count_;
    return Status::OkStatus();
  }

  const std::string& contents() const {
    return contents_;
  }

  int flush_count() const {
    return flush_count_;
  }

  bool closed() const {
    return closed_;
  }

private:
  std::string contents_;
  std::vector<size_t> append_sizes_;
  int flush_count_ = 0;
  int sync_count_ = 0;
  bool closed_ = false;
};

class TrackingComparator : public Comparator {
public:
  int Compare(const Slice& lhs, const Slice& rhs) const override {
    return lhs.Compare(rhs);
  }

  const char* Name() const override {
    return "db.TrackingComparator";
  }

  void FindShortestSeparator(std::string* start, const Slice& limit) const override {
    separators.emplace_back(*start, limit.ToString());
    BytewiseComparator()->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    successors.push_back(*key);
    BytewiseComparator()->FindShortSuccessor(key);
  }

  mutable std::vector<std::pair<std::string, std::string>> separators;
  mutable std::vector<std::string> successors;
};

class TestFilterPolicy : public FilterPolicy {
public:
  const char* Name() const override {
    return "db.TableBuilderTestFilter";
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

struct ParsedBlockEntry {
  std::string key;
  std::string value;
};

std::vector<ParsedBlockEntry> ParseBlockEntries(const std::string& block_data) {
  std::vector<ParsedBlockEntry> entries;
  const Slice block(block_data);
  const uint32_t num_restarts = DecodeFixed32(block.Data() + block.Size() - sizeof(uint32_t));
  const size_t restart_offset =
      block.Size() - (static_cast<size_t>(num_restarts) + 1U) * sizeof(uint32_t);

  Slice input(block.Data(), restart_offset);
  std::string last_key;

  while (!input.Empty()) {
    uint32_t shared = 0;
    uint32_t non_shared = 0;
    uint32_t value_size = 0;

    EXPECT_TRUE(GetVarint32(&input, &shared));
    EXPECT_TRUE(GetVarint32(&input, &non_shared));
    EXPECT_TRUE(GetVarint32(&input, &value_size));

    ParsedBlockEntry entry;
    entry.key.assign(last_key.data(), shared);
    entry.key.append(input.Data(), non_shared);
    input.RemovePrefix(non_shared);

    entry.value.assign(input.Data(), value_size);
    input.RemovePrefix(value_size);

    entries.push_back(entry);
    last_key = entry.key;
  }

  return entries;
}

std::string ExtractBlockData(const std::string& file_contents, const BlockHandle& handle) {
  return file_contents.substr(static_cast<size_t>(handle.offset()),
                              static_cast<size_t>(handle.size()));
}

BlockHandle DecodeHandle(const std::string& encoded) {
  Slice input(encoded);
  BlockHandle handle;
  EXPECT_TRUE(handle.DecodeFrom(&input).Ok());
  EXPECT_TRUE(input.Empty());
  return handle;
}

void ExpectBlockTrailer(const std::string& file_contents, const BlockHandle& handle,
                        CompressionType expected_type) {
  const size_t offset = static_cast<size_t>(handle.offset());
  const size_t size = static_cast<size_t>(handle.size());
  const size_t trailer_offset = offset + size;

  ASSERT_LE(trailer_offset + kBlockTrailerSize, file_contents.size());
  EXPECT_EQ(static_cast<char>(expected_type), file_contents[trailer_offset]);

  uint32_t crc = crc32c::Value(file_contents.data() + offset, size);
  crc = crc32c::Extend(crc, file_contents.data() + trailer_offset, 1);
  EXPECT_EQ(crc32c::Mask(crc), DecodeFixed32(file_contents.data() + trailer_offset + 1));
}

Footer DecodeFooter(const std::string& file_contents) {
  Slice input(file_contents.data() + file_contents.size() - Footer::kEncodeLenght,
              Footer::kEncodeLenght);
  Footer footer;
  EXPECT_TRUE(footer.DecodeFrom(&input).Ok());
  EXPECT_TRUE(input.Empty());
  return footer;
}

TEST(TableBuilderTest, FinishWritesExpectedStructureAndBlockTrailers) {
  Options options;
  options.block_size = 1024;
  options.compression = kNoCompression;
  TestFilterPolicy filter_policy;
  options.filter_policy = &filter_policy;

  StringWritableFile file;
  TableBuilder builder(options, &file);

  builder.Add("alpha", "one");
  builder.Add("bravo", "two");

  EXPECT_EQ(2U, builder.NumEntries());
  EXPECT_EQ(0U, builder.FileSize());
  ASSERT_TRUE(builder.status().Ok());
  ASSERT_TRUE(builder.Finish().Ok());

  EXPECT_EQ(2U, builder.NumEntries());
  EXPECT_EQ(file.contents().size(), builder.FileSize());
  EXPECT_EQ(1, file.flush_count());

  const Footer footer = DecodeFooter(file.contents());
  const BlockHandle metaindex_handle = footer.metaindex_handle();
  const BlockHandle index_handle = footer.index_handle();

  ExpectBlockTrailer(file.contents(), metaindex_handle, kNoCompression);
  ExpectBlockTrailer(file.contents(), index_handle, kNoCompression);

  const std::vector<ParsedBlockEntry> metaindex_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), metaindex_handle));
  ASSERT_EQ(1U, metaindex_entries.size());
  EXPECT_EQ("filter.db.TableBuilderTestFilter", metaindex_entries[0].key);

  const BlockHandle filter_handle = DecodeHandle(metaindex_entries[0].value);
  ExpectBlockTrailer(file.contents(), filter_handle, kNoCompression);

  const std::vector<ParsedBlockEntry> index_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), index_handle));
  ASSERT_EQ(1U, index_entries.size());
  EXPECT_EQ("c", index_entries[0].key);

  const BlockHandle data_handle = DecodeHandle(index_entries[0].value);
  EXPECT_EQ(0U, data_handle.offset());
  ExpectBlockTrailer(file.contents(), data_handle, kNoCompression);

  const std::vector<ParsedBlockEntry> data_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), data_handle));
  ASSERT_EQ(2U, data_entries.size());
  EXPECT_EQ("alpha", data_entries[0].key);
  EXPECT_EQ("one", data_entries[0].value);
  EXPECT_EQ("bravo", data_entries[1].key);
  EXPECT_EQ("two", data_entries[1].value);

  FilterBlockReader reader(&filter_policy, ExtractBlockData(file.contents(), filter_handle));
  EXPECT_TRUE(reader.KeyMayMatch(0, "alpha"));
  EXPECT_TRUE(reader.KeyMayMatch(0, "bravo"));
}

TEST(TableBuilderTest, MultipleFlushesUseComparatorSeparatorAndSuccessor) {
  TrackingComparator comparator;

  Options options;
  options.comparator = &comparator;
  options.block_size = 1;
  options.compression = kNoCompression;

  StringWritableFile file;
  TableBuilder builder(options, &file);

  builder.Add("alpha1", "one");
  builder.Add("alpha3", "two");
  builder.Add("bravo", "three");

  ASSERT_TRUE(builder.Finish().Ok());
  EXPECT_EQ(3, file.flush_count());
  EXPECT_EQ((std::vector<std::pair<std::string, std::string>>{
                {"alpha1", "alpha3"},
                {"alpha3", "bravo"},
            }),
            comparator.separators);
  EXPECT_EQ((std::vector<std::string>{"bravo"}), comparator.successors);

  const Footer footer = DecodeFooter(file.contents());
  const std::vector<ParsedBlockEntry> index_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), footer.index_handle()));
  ASSERT_EQ(3U, index_entries.size());
  EXPECT_EQ("alpha2", index_entries[0].key);
  EXPECT_EQ("alpha3", index_entries[1].key);
  EXPECT_EQ("c", index_entries[2].key);

  const BlockHandle first = DecodeHandle(index_entries[0].value);
  const BlockHandle second = DecodeHandle(index_entries[1].value);
  const BlockHandle third = DecodeHandle(index_entries[2].value);
  EXPECT_LT(first.offset(), second.offset());
  EXPECT_LT(second.offset(), third.offset());

  const std::vector<ParsedBlockEntry> first_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), first));
  const std::vector<ParsedBlockEntry> second_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), second));
  const std::vector<ParsedBlockEntry> third_entries =
      ParseBlockEntries(ExtractBlockData(file.contents(), third));

  ASSERT_EQ(1U, first_entries.size());
  EXPECT_EQ("alpha1", first_entries[0].key);
  EXPECT_EQ("one", first_entries[0].value);

  ASSERT_EQ(1U, second_entries.size());
  EXPECT_EQ("alpha3", second_entries[0].key);
  EXPECT_EQ("two", second_entries[0].value);

  ASSERT_EQ(1U, third_entries.size());
  EXPECT_EQ("bravo", third_entries[0].key);
  EXPECT_EQ("three", third_entries[0].value);
}

TEST(TableBuilderTest, ChangeOptionsRejectsComparatorChanges) {
  Options options;
  StringWritableFile file;
  TableBuilder builder(options, &file);

  TrackingComparator comparator;
  Options changed = options;
  changed.comparator = &comparator;

  const Status status = builder.ChangeOptions(changed);
  EXPECT_EQ(Status::Code::kInvalidArgument, status.GetCode());
  EXPECT_EQ("changing comparator while building table", status.Message());

  builder.Abandon();
}

TEST(TableBuilderTest, AbandonAllowsDestructionWithoutFinishingTable) {
  Options options;
  options.block_size = 1024;
  StringWritableFile file;

  {
    TableBuilder builder(options, &file);
    builder.Add("alpha", "one");
    builder.Abandon();
  }

  EXPECT_TRUE(file.contents().empty());
}

}  // namespace
}  // namespace db
