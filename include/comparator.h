#pragma once

#include <string>

#include "slice.h"

namespace db {

// Comparator defines the total ordering used for keys stored by the database.
// Implementations must be thread-safe.
class Comparator {
public:
  virtual ~Comparator();

  virtual int Compare(const Slice& lhs, const Slice& rhs) const = 0;

  // Names starting with "db." are reserved for builtin comparators.
  virtual const char* Name() const = 0;

  // Updates *start to a short key in the range [*start, limit) when possible.
  virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;

  // Updates *key to a short key that is greater than or equal to the original.
  virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Returns the builtin comparator that uses lexicographical byte ordering.
const Comparator* BytewiseComparator();

}  // namespace db
