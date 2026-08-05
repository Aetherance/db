#include "table.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "cache.h"
#include "comparator.h"
#include "env.h"
#include "filter_block.h"
#include "iterator.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include "two_level_iterator.h"
#include "util/coding.h"

namespace db {
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodeLenght) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char foot_space[Footer::kEncodeLenght];
  Slice footer_input;
  Status s =
      file->Read(size - Footer::kEncodeLenght, Footer::kEncodeLenght, &footer_input, foot_space);
  if (!s.Ok()) {
    return s;
  }

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.Ok()) {
    return s;
  }

  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.Ok()) {
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;

    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = options.block_cache != nullptr ? options.block_cache->NewId() : 0;
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
};

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).Ok()) {
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->Key() == Slice(key)) {
    ReadFilter(iter->Value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).Ok()) {
    return;
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).Ok()) {
    return;
  }
  if (block.heep_allocated) {
    rep_->filter_data = block.data.Data();
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  (void)ignored;
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  (void)key;
  delete reinterpret_cast<Block*>(value);
}

static void ReleaseBlock(void* arg, void* handle) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  cache->Release(reinterpret_cast<Cache::Handle*>(handle));
}

Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Block* block = nullptr;
  Cache* block_cache = table->rep_->options.block_cache;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);

  if (s.Ok()) {
    char cache_key_buffer[16];
    Slice cache_key;
    if (block_cache != nullptr) {
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      cache_key = Slice(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(cache_key);
    }

    if (cache_handle != nullptr) {
      block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
    } else {
      BlockContents contents;
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.Ok()) {
        block = new Block(contents);
        if (contents.cachable && options.fill_cache && block_cache != nullptr) {
          cache_handle = block_cache->Insert(cache_key, block, block->size(), &DeleteCachedBlock);
        }
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator),
                             &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->Value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).Ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // NOT FOUND
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->Value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->Key(), block_iter->Value());
      }
      s = block_iter->GetStatus();
      delete block_iter;
    }
  }
  if (s.Ok()) {
    s = iiter->GetStatus();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->Value();
    Status s = handle.DecodeFrom(&input);
    if (s.Ok()) {
      result = handle.offset();
    } else {
      result = rep_->metaindex_handle.offset();
    }
  } else {
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace db