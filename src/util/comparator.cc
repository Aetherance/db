#include "comparator.h"

#include <sys/types.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "util/no_destructor.h"

namespace db {

Comparator::~Comparator() = default;

namespace {

class BytewiseComparatorImpl : public Comparator {
public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override {
    return "db.BytewiseComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.Compare(b);
  }

  void FindShortestSeparator(std::string* start, const Slice& limit) const override {
    size_t min_length = std::min(start->size(), limit.Size());
    size_t diff_index = 0;
    while ((diff_index < min_length) && ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // start is limit's prefix or limit is start's prefix
      // do not shorten at these situations
    } else {
      // shorten
      uint8_t diff_type = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_type < static_cast<uint8_t>(0xff) &&
          diff_type + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  void FindShortSuccessor(std::string* key) const override {
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace db
