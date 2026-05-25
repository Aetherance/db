#pragma once

#include <cstddef>
#include <string>

namespace db {
class Slice;

class WriteBatch {
public:
  WriteBatch();

  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  void Put(const Slice& key, const Slice& value);

  void Delete(const Slice& key);

  void Clear();

  size_t ApproximateSize() const;

  void Append(const WriteBatch& source);

private:
  friend class WriteBatchInternal;

  std::string rep_;
};
}  // namespace db
