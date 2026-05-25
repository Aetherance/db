#include "write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "write_batch_internal.h"
namespace db {

//	WAL record:
//  ┌─ record header (7B) ─┬─ WriteBatch header (12B) ─┬─ records ──┐
//  │ CRC(4B)              │ sequence: fixed64(8B)     │ record0... │
//  │ length(2B)           │ count: fixed32(4B)        │ record1... │
//  │ type(1B)             │                           │            │
//  └──────────────────────┴───────────────────────────┴────────────┘

static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const {
  return rep_.size();
}

int WriteBatchInternal::Count(const WriteBatch* batch) {
  return DecodeFixed32(batch->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

Status WriteBatchInternal::InsertInto(const WriteBatch* batch, MemTable* memtable) {
  Slice input(batch->rep_);
  if (input.Size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.RemovePrefix(kHeader);
  Slice key, value;
  int found = 0;
  SequenceNumber sequence = WriteBatchInternal::Sequence(batch);
  while (!input.Empty()) {
    found++;
    const char tag = input[0];
    input.RemovePrefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value)) {
          memtable->Add(sequence, kTypeValue, key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          memtable->Add(sequence, kTypeDeletion, key, Slice());
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
    sequence++;
  }
  if (found != WriteBatchInternal::Count(batch)) {
    return Status::Corruption("WriteBatch has wrong count");
  }
  return Status::OkStatus();
}

void WriteBatchInternal::SetContents(WriteBatch* batch, const Slice& contents) {
  assert(contents.Size() >= kHeader);
  batch->rep_.assign(contents.Data(), contents.Size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace db
