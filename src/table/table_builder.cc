#include "table_builder.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "env.h"
#include "format.h"
#include "options.h"
#include "port.h"
#include "slice.h"
#include "status.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "util/coding.h"
#include "util/crc32c.h"

//	Data Block Format:
//
//	 ┌──────────────────────────────────────────┐
//	 │  Block Data                              │
//	 │  ┌────────────────────────────────────┐  │
//	 │  │ Entry 1                            │  │
//	 │  │   shared (varint)                  │  │
//	 │  │   non_shared (varint)              │  │
//	 │  │   value_size (varint)              │  │
//	 │  │   key_delta                        │  │
//	 │  │   value                            │  │
//	 │  ├────────────────────────────────────┤  │
//	 │  │ Entry 2 ... Entry N                │  │
//	 │  ├────────────────────────────────────┤  │
//	 │  │ Restart Point 1 (uint32)           │  │
//	 │  │ Restart Point 2 (uint32)           │  │
//	 │  │ ...                                │  │
//	 │  │ Num Restarts (uint32)              │  │
//	 │  └────────────────────────────────────┘  │
//	 ├──────────────────────────────────────────┤
//	 │  Compression Type (1 byte)               │
//	 ├──────────────────────────────────────────┤
//	 │  CRC (4 bytes)                           │
//	 └──────────────────────────────────────────┘

namespace db {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr ? nullptr
                                                  : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    // for index block, depression is disabled.
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;
  FilterBlockBuilder* filter_block;

  bool pending_index_entry;
  BlockHandle pending_handle;

  std::string compressed_ouput;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OkStatus();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // record the index of the last written wrriten data block
  if (r->pending_index_entry) {
    // assert the data block is not empty
    assert(r->data_block.empty());

    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.Data(), key.Size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // File Flush
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;

  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_ouput;
      if (port::Snappy_Compress(raw.Data(), raw.Size(), compressed) &&
          compressed->size() < raw.Size() - (raw.Size() / 8u)) {
        block_contents = *compressed;
      } else {
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_ouput;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.Data(), raw.Size(),
                              compressed) &&
          compressed->size() < raw.Size() - (raw.Size() / 8u)) {
        block_contents = *compressed;
      } else {
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_ouput.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.Size());
  // Add data into file memory buff
  r->status = r->file->Append(block_contents);
  if (r->status.Ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.Data(), block_contents.Size());
    // Add type
    crc = crc32c::Extend(crc, trailer, 1);
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.Ok()) {
      r->offset += block_contents.Size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Filter
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);
  }

  // MetaIndex
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Index
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.Ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace db