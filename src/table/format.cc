#include "format.h"

#include <sys/types.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "status.h"
#include "util/coding.h"

namespace db {

void BlockHandle::EncodeTo(std::string* dst) const {
  assert(offset_ != ~static_cast<u_int64_t>(0));
  assert(size_ != ~static_cast<u_int64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OkStatus();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber) & 0xffffffffu);
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodeLenght);
  (void)original_size;
}

Status Footer::DecodeFrom(Slice* input) {
  if (input->Size() < kEncodeLenght) {
    return Status::Corruption("not an sstable (footer too short)");
  }

  const char* magic_ptr = input->Data() + kEncodeLenght - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const u_int64_t magic =
      ((static_cast<u_int64_t>(magic_hi) << 32) | (static_cast<u_int64_t>(magic_lo)));
  // magic num judge if the file valid
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.Ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.Ok()) {
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->Data() + input->Size() - end);
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle,
                 BlockContents* result) {
  // TODO:
  // 1. implement env.h
  // 2. implement this method
  return Status::OkStatus();
}

}  // namespace db