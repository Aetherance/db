#include "format.h"

#include <sys/types.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "env.h"
#include "options.h"
#include "port.h"
#include "slice.h"
#include "status.h"
#include "util/coding.h"
#include "util/crc32c.h"

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
  (void)file;
  (void)options;
  (void)handle;
  result->data = Slice();
  result->cachable = false;
  result->heep_allocated = false;

  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.Ok()) {
    delete[] buf;
    return s;
  }
  if (contents.Size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("read truncated block");
  }

  const char* data = contents.Data();
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block crc checksum missmatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression: {
      if (data != buf) {
        // ::mmap()
        // ::map() will return a memory which is managed by OS
        delete[] buf;
        result->data = Slice(data, n);
        result->heep_allocated = false;
        result->cachable = false;
      } else {
        // ::pread()
        result->data = Slice(buf, n);
        result->heep_allocated = true;
        result->cachable = true;
      }
      break;
    }

    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted snappy compressed block length");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted snappy compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heep_allocated = true;
      result->cachable = true;
      break;
    }

    case kZstdCompression: {
      size_t ulength = 0;
      if (!port::Zstd_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted zstd compressed block length");
      }
      char* ubuf = new char[ulength];
      if (!port::Zstd_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted zstd compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heep_allocated = true;
      result->cachable = true;
      break;
    }

    default: {
      delete[] buf;
      return Status::Corruption("bad block type");
    }
  }
  return Status::OkStatus();
}

}  // namespace db