#include <sys/types.h>

#include <cstddef>
#include <string>

#include "status.h"
namespace db {
class Block;
class RandomAccessFile;
struct ReadOptions;

// SSTable Format
//
//  +------------------+
//  | Data Blocks      |
//  +------------------+
//  | Filter Block ?   |
//  +------------------+
//  | Metaindex Block  |
//  +------------------+
//  | Index Block      |
//  +------------------+
//  | Footer           |
//  +------------------+
//
// The Filter Block is optional.

// descripe a block 's position and size
// BlockHanlde's max size is 10
class BlockHandle {
public:
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle();

  u_int64_t offset() const {
    return offset_;
  }
  void set_offset(u_int64_t offset) {
    offset_ = offset;
  }

  u_int64_t size() const {
    return size_;
  }
  void set_size(u_int64_t size) {
    size_ = size;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

private:
  u_int64_t offset_;
  u_int64_t size_;
};

//   Footer Format:
//
//   [ meta  BlockHandle ]
//   [ index BlockHandle ]
//   [      empty        ]
//   [     magic num     ]
//
// Note: Unused bytes in meta/index BlockHandle are left empty to keep Footer size fixed.
// Footer has a Fixed Size 48
class Footer {
public:
  // two of BlockHandle: index_handle and metaindex_hanle
  // 8 is the size of MagicNum
  enum { kEncodeLenght = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  const BlockHandle& metaindex_handle() const {
    return metaindex_handle_;
  }
  void set_metaindex_handle(const BlockHandle& h) {
    metaindex_handle_ = h;
  }

  const BlockHandle& index_handle() const {
    return index_handle_;
  }
  void set_index_handle(const BlockHandle& h) {
    index_handle_ = h;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

// https://github.com/aetherance/db | sha1sum
//
// This magic num will be written into Footer by Footer::EncodeTo()
// And will be used to judge if the footer is correct
static const u_int64_t kTableMagicNumber = 0x8c7f4e0c9d6a2b31ull;

// tail after block
static const size_t kBlockTrailerSize = 5;

// Block Readed into memory
struct BlockContents {
  Slice data;
  bool cachable;
  bool heep_allocated;
};

//  offset_  =  size_  = UINT64_MAX;
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle,
                 BlockContents* result);

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<u_int64_t>(0)), size_(~static_cast<u_int64_t>(0)) {}
}  // namespace db