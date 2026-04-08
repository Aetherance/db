#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "slice.h"

namespace db {

struct Options;

// * Block Format
//	[Data-Entry-A]
//	[Data-Entry-B]
//	[Data-Entry-C]
//	[RestartArray]
//	[Restart-Size]
class BlockBuilder {
public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset all data as if the BlockBuilder was just constructed.
  void Reset();

  // NOTICE: DO NOT call Add() after Finish() called
  // REQUIRES: key is larger than
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and returns a slice that refers to the
  // block contents. The returned slice will remain valid until Reset()
  // is called or the BlockBuilder deconstructed.
  //
  // Add() should not called if Finish() has been called. It is because Finish()
  // will append [restart array][restart count] at the end of the block.
  // If Add() called after Finish(), the Block's format will be wrong.
  Slice Finish();

  // Return an estimate of current size of the block
  size_t CurrentSizeEstimate() const;

  // return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

private:
  const Options* options_;
  std::string buffer_;
  std::vector<uint32_t> restarts_;
  int counter_;
  bool finished_;
  std::string last_key_;
};

}  // namespace db