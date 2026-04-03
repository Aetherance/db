#include "comparator.h"

#include <cstddef>
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
    // TODO: implement me
  }

  void FindShortSuccessor(std::string* key) const override {
    // TODO: implement me
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace db
