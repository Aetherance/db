#include "db/memtable.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "iterator.h"
#include "slice.h"
#include "status.h"
#include "util/coding.h"

namespace db {

// This func only used in memtable.cc
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  return arena_.MemoryUsage();
}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const {
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.Size());
  scratch->append(target.Data(), target.Size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override {
    return iter_.Valid();
  }
  void Seek(const Slice& k) override {
    iter_.Seek(EncodeKey(&tmp_, k));
  }

  void SeekToFirst() override {
    iter_.SeekToFirst();
  }

  void SeekToLast() override {
    iter_.SeekToLast();
  }

  void Next() override {
    iter_.Next();
  }

  void Prev() override {
    iter_.Prev();
  }

  Slice Key() const override {
    return GetLengthPrefixedSlice(iter_.key());
  }
  Slice Value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.Data() + key_slice.Size());
  }

  Status GetStatus() const override {
    return Status::OkStatus();
  }

private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

// Memtable Add Entry Format:
//  KEY
//  internal_key size
//  internal_key
//    user_key
//    seq_num
//    tag
//  VAL
//  val_data
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value) {
  size_t key_size = key.Size();
  size_t val_size = value.Size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  // internal_key length header
  char* p = EncodeVarint32(buf, internal_key_size);
  // user_key
  std::memcpy(p, key.Data(), key_size);
  p += key_size;
  // tag = seq + type
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  // value length header
  p = EncodeVarint32(p, val_size);
  // value
  std::memcpy(p, value.Data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
}

// only with Key
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.Data());
  if (iter.Valid()) {
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8),
                                                          key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type = static_cast<ValueType>(tag & 0xff);
      switch (type) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.Data(), v.Size());
          return true;
        }
        case kTypeDeletion: {
          *s = Status::NotFound(Slice());
          return true;
        }
      }
    }
  }
  return false;
}
}  // namespace db