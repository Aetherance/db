#include "table/format.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "env.h"
#include "gtest/gtest.h"
#include "options.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace db {

class MockPReadFile : public RandomAccessFile {
public:
  explicit MockPReadFile(std::string data) : data_(std::move(data)) {}

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
    if (offset >= data_.size()) {
      *result = Slice(scratch, 0);
      return Status::OkStatus();
    }
    size_t available = data_.size() - static_cast<size_t>(offset);
    size_t to_read = std::min(n, available);
    std::memcpy(scratch, data_.data() + static_cast<size_t>(offset), to_read);
    *result = Slice(scratch, to_read);
    return Status::OkStatus();
  }

private:
  std::string data_;
};

class MockMmapFile : public RandomAccessFile {
public:
  explicit MockMmapFile(std::string data) : data_(std::move(data)) {}

  Status Read(uint64_t offset, size_t n, Slice* result, char* /*scratch*/) const override {
    if (offset >= data_.size()) {
      *result = Slice();
      return Status::OkStatus();
    }
    size_t available = data_.size() - static_cast<size_t>(offset);
    size_t to_read = std::min(n, available);
    *result = Slice(data_.data() + static_cast<size_t>(offset), to_read);
    return Status::OkStatus();
  }

private:
  std::string data_;
};

class FailFile : public RandomAccessFile {
public:
  Status Read(uint64_t /*offset*/, size_t /*n*/, Slice* result, char* /*scratch*/) const override {
    *result = Slice();
    return Status::IOError("mock io error");
  }
};

ReadOptions WithVerify() {
  ReadOptions opts;
  opts.verify_checksums = true;
  return opts;
}

ReadOptions WithoutVerify() {
  return {};
}

std::string MakeBlock(const std::string& content, char type) {
  std::string block = content + type;
  uint32_t crc = crc32c::Mask(crc32c::Value(block.data(), block.size()));
  char buf[4];
  EncodeFixed32(buf, crc);
  block.append(buf, 4);
  return block;
}

TEST(BlockHandleTest, RoundTripZero) {
  BlockHandle h;
  h.set_offset(0);
  h.set_size(0);

  std::string encoded;
  h.EncodeTo(&encoded);

  Slice input(encoded);
  BlockHandle decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.Ok());
  EXPECT_EQ(0, decoded.offset());
  EXPECT_EQ(0, decoded.size());
}

TEST(BlockHandleTest, RoundTripBoundary) {
  BlockHandle h;
  h.set_offset(127);
  h.set_size(128);

  std::string encoded;
  h.EncodeTo(&encoded);

  Slice input(encoded);
  BlockHandle decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.Ok());
  EXPECT_EQ(127, decoded.offset());
  EXPECT_EQ(128, decoded.size());
}

TEST(BlockHandleTest, RoundTripLarge) {
  BlockHandle h;
  h.set_offset(1ULL << 32);
  h.set_size(1ULL << 63);

  std::string encoded;
  h.EncodeTo(&encoded);

  Slice input(encoded);
  BlockHandle decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.Ok());
  EXPECT_EQ(1ULL << 32, decoded.offset());
  EXPECT_EQ(1ULL << 63, decoded.size());
}

TEST(BlockHandleTest, DecodeFromTruncated) {
  Slice input("\x80");
  BlockHandle decoded;
  Status s = decoded.DecodeFrom(&input);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(FooterTest, RoundTrip) {
  BlockHandle meta;
  meta.set_offset(100);
  meta.set_size(50);

  BlockHandle index;
  index.set_offset(200);
  index.set_size(80);

  Footer footer;
  footer.set_metaindex_handle(meta);
  footer.set_index_handle(index);

  std::string encoded;
  footer.EncodeTo(&encoded);

  EXPECT_EQ(Footer::kEncodeLenght, encoded.size());

  Slice input(encoded);
  Footer decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.Ok());

  EXPECT_EQ(100, decoded.metaindex_handle().offset());
  EXPECT_EQ(50, decoded.metaindex_handle().size());
  EXPECT_EQ(200, decoded.index_handle().offset());
  EXPECT_EQ(80, decoded.index_handle().size());
}

TEST(FooterTest, DecodeFromTooShort) {
  std::string short_input(Footer::kEncodeLenght - 1, '\0');
  Slice input(short_input);
  Footer decoded;
  Status s = decoded.DecodeFrom(&input);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(FooterTest, DecodeFromBadMagic) {
  std::string bad_footer(Footer::kEncodeLenght, '\0');
  Slice input(bad_footer);
  Footer decoded;
  Status s = decoded.DecodeFrom(&input);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(FooterTest, DecodeFromAdvancesInput) {
  BlockHandle meta;
  meta.set_offset(100);
  meta.set_size(50);

  BlockHandle index;
  index.set_offset(200);
  index.set_size(80);

  Footer footer;
  footer.set_metaindex_handle(meta);
  footer.set_index_handle(index);

  std::string encoded;
  footer.EncodeTo(&encoded);

  std::string extra = "trailing data";
  std::string full = encoded + extra;

  Slice input(full);
  Footer decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.Ok());

  EXPECT_EQ(extra, input.ToString());
}

TEST(ReadBlockTest, TruncatedBlock) {
  std::string partial = "short";
  MockPReadFile file(partial);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(100);

  BlockContents contents;
  Status s = ReadBlock(&file, WithoutVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, TruncatedBlockMmap) {
  std::string partial = "short";
  MockMmapFile file(partial);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(100);

  BlockContents contents;
  Status s = ReadBlock(&file, WithoutVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, ReadError) {
  FailFile file;

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(10);

  BlockContents contents;
  Status s = ReadBlock(&file, WithoutVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kIOError);
}

TEST(ReadBlockTest, BadBlockType) {
  std::string block = MakeBlock("hello", static_cast<char>(0xFF));
  MockPReadFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, BadBlockTypeMmap) {
  std::string block = MakeBlock("hello", static_cast<char>(0xFF));
  MockMmapFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, ChecksumMismatch) {
  std::string good = MakeBlock("hello", kNoCompression);

  good.back() ^= static_cast<char>(0xFF);
  MockPReadFile file(good);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, ChecksumMismatchMmap) {
  std::string good = MakeBlock("hello", kNoCompression);
  good.back() ^= static_cast<char>(0xFF);
  MockMmapFile file(good);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, SkipChecksum) {
  std::string good = MakeBlock("hello", kNoCompression);
  good.back() ^= static_cast<char>(0xFF);
  MockPReadFile file(good);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithoutVerify(), handle, &contents);
  ASSERT_TRUE(s.Ok());
  EXPECT_EQ("hello", contents.data.ToString());
}

TEST(ReadBlockTest, SkipChecksumMmap) {
  std::string good = MakeBlock("hello", kNoCompression);
  good.back() ^= static_cast<char>(0xFF);
  MockMmapFile file(good);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(5);

  BlockContents contents;
  Status s = ReadBlock(&file, WithoutVerify(), handle, &contents);
  ASSERT_TRUE(s.Ok());
  EXPECT_EQ("hello", contents.data.ToString());
}

TEST(ReadBlockTest, NoCompressionPRead) {
  std::string content = "hello";
  std::string block = MakeBlock(content, kNoCompression);
  MockPReadFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(content.size());

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  ASSERT_TRUE(s.Ok());

  EXPECT_EQ(content, contents.data.ToString());
  EXPECT_TRUE(contents.cachable);
  EXPECT_TRUE(contents.heep_allocated);

  delete[] contents.data.Data();
}

TEST(ReadBlockTest, NoCompressionMmap) {
  std::string content = "world";
  std::string block = MakeBlock(content, kNoCompression);
  MockMmapFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(content.size());

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  ASSERT_TRUE(s.Ok());

  EXPECT_EQ(content, contents.data.ToString());
  EXPECT_FALSE(contents.cachable);
  EXPECT_FALSE(contents.heep_allocated);
}

TEST(ReadBlockTest, SnappyBadData) {
  std::string block = MakeBlock("not actually snappy data", kSnappyCompression);
  MockPReadFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(block.size() - kBlockTrailerSize);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, SnappyBadDataMmap) {
  std::string block = MakeBlock("not actually snappy data", kSnappyCompression);
  MockMmapFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(block.size() - kBlockTrailerSize);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, ZstdBadData) {
  std::string block = MakeBlock("not actually zstd data", kZstdCompression);
  MockPReadFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(block.size() - kBlockTrailerSize);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

TEST(ReadBlockTest, ZstdBadDataMmap) {
  std::string block = MakeBlock("not actually zstd data", kZstdCompression);
  MockMmapFile file(block);

  BlockHandle handle;
  handle.set_offset(0);
  handle.set_size(block.size() - kBlockTrailerSize);

  BlockContents contents;
  Status s = ReadBlock(&file, WithVerify(), handle, &contents);
  EXPECT_TRUE(s.GetCode() == Status::Code::kCorruption);
}

}  // namespace db
