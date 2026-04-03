#pragma once

#include <string.h>

#include <cassert>
#include <cstddef>

#include "db/skiplist.h"
#include "dbformat.h"
#include "slice.h"
#include "status.h"
#include "util/arena.h"

namespace db {
class InternalKeyComparator;
class Iterator;
class MemTableIterator;

class MemTable {
public:
  explicit MemTable(const InternalKeyComparator& comparator);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // Increase reference count.
  void Ref() {
    ++refs_;
  }

  void Unref() {
    --refs_;
    // ref should not < 0
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // return approximate memory usage Memtable has used
  size_t ApproximateMemoryUsage();

  // Create a Iterator to iterate this memtable
  Iterator* NewIterator();

  // This method will insert an entry into memtable mapping key to a value with a
  // specified sequence num and type. If type == kTypeDeletion, the value usually is empty
  void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& val);

  // If memtable contains a value for key, store it in *value and return true
  // If memtable  contains a deletion for key, store a NotFound() error in *status
  // and return true. Else return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

private:
  friend class MemTableIterator;
  friend class MemTableBackWardIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;

  ~MemTable();

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;
};
}  // namespace db
