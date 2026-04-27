#include "table/block_builder.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "options.h"
#include "slice.h"
#include "util/coding.h"

namespace db {
namespace {

struct ParsedBlockEntry {
  uint32_t shared = 0;
  uint32_t non_shared = 0;
  uint32_t value_size = 0;
  std::string key;
  std::string value;
};

struct ParsedBlock {
  std::vector<ParsedBlockEntry> entries;
  std::vector<uint32_t> restarts;
};

ParsedBlock ParseBlock(const Slice& block) {
  ParsedBlock parsed;

  const uint32_t num_restarts = DecodeFixed32(block.Data() + block.Size() - sizeof(uint32_t));
  const size_t restart_offset =
      block.Size() - (static_cast<size_t>(num_restarts) + 1U) * sizeof(uint32_t);

  for (uint32_t i = 0; i < num_restarts; ++i) {
    parsed.restarts.push_back(DecodeFixed32(block.Data() + restart_offset + i * sizeof(uint32_t)));
  }

  Slice input(block.Data(), restart_offset);
  std::string last_key;

  while (!input.Empty()) {
    ParsedBlockEntry entry;
    EXPECT_TRUE(GetVarint32(&input, &entry.shared));
    EXPECT_TRUE(GetVarint32(&input, &entry.non_shared));
    EXPECT_TRUE(GetVarint32(&input, &entry.value_size));

    entry.key.assign(last_key.data(), entry.shared);
    entry.key.append(input.Data(), entry.non_shared);
    input.RemovePrefix(entry.non_shared);

    entry.value.assign(input.Data(), entry.value_size);
    input.RemovePrefix(entry.value_size);

    parsed.entries.push_back(entry);
    last_key = entry.key;
  }

  return parsed;
}

TEST(BlockBuilderTest, EncodesPrefixCompressionAndRestartPoints) {
  Options options;
  options.block_restart_interval = 2;

  BlockBuilder builder(&options);
  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(sizeof(uint32_t) * 2U, builder.CurrentSizeEstimate());

  builder.Add("alpha1", "v1");
  builder.Add("alpha2", "v2");
  builder.Add("bravo", "v3");

  const size_t estimated_size = builder.CurrentSizeEstimate();
  const Slice block = builder.Finish();

  EXPECT_EQ(estimated_size, block.Size());

  const ParsedBlock parsed = ParseBlock(block);
  ASSERT_EQ(3U, parsed.entries.size());
  EXPECT_EQ((std::vector<uint32_t>{0U, 17U}), parsed.restarts);

  EXPECT_EQ(0U, parsed.entries[0].shared);
  EXPECT_EQ(6U, parsed.entries[0].non_shared);
  EXPECT_EQ(2U, parsed.entries[0].value_size);
  EXPECT_EQ("alpha1", parsed.entries[0].key);
  EXPECT_EQ("v1", parsed.entries[0].value);

  EXPECT_EQ(5U, parsed.entries[1].shared);
  EXPECT_EQ(1U, parsed.entries[1].non_shared);
  EXPECT_EQ(2U, parsed.entries[1].value_size);
  EXPECT_EQ("alpha2", parsed.entries[1].key);
  EXPECT_EQ("v2", parsed.entries[1].value);

  EXPECT_EQ(0U, parsed.entries[2].shared);
  EXPECT_EQ(5U, parsed.entries[2].non_shared);
  EXPECT_EQ(2U, parsed.entries[2].value_size);
  EXPECT_EQ("bravo", parsed.entries[2].key);
  EXPECT_EQ("v3", parsed.entries[2].value);
}

TEST(BlockBuilderTest, ResetClearsBufferedStateAndRestartArray) {
  Options options;
  options.block_restart_interval = 1;

  BlockBuilder builder(&options);
  builder.Add("alpha", "first");
  EXPECT_FALSE(builder.empty());
  (void)builder.Finish();

  builder.Reset();
  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(sizeof(uint32_t) * 2U, builder.CurrentSizeEstimate());

  builder.Add("beta", "second");
  const ParsedBlock parsed = ParseBlock(builder.Finish());

  ASSERT_EQ(1U, parsed.entries.size());
  EXPECT_EQ((std::vector<uint32_t>{0U}), parsed.restarts);
  EXPECT_EQ("beta", parsed.entries[0].key);
  EXPECT_EQ("second", parsed.entries[0].value);
}

}  // namespace
}  // namespace db
