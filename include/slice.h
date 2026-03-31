#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

namespace db {

// Slice is a non-owning view over a contiguous byte sequence.
class Slice {
public:
  Slice() : data_(""), size_(0) {}

  Slice(const char* data, size_t size) : data_(data), size_(size) {}

  Slice(const std::string& value) : data_(value.data()), size_(value.size()) {}

  Slice(const char* value) : data_(value), size_(std::strlen(value)) {}

  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;

  const char* Data() const {
    return data_;
  }

  size_t Size() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  const char* begin() const {
    return data_;
  }

  const char* end() const {
    return data_ + size_;
  }

  char operator[](size_t index) const {
    assert(index < size_);
    return data_[index];
  }

  void Clear() {
    data_ = "";
    size_ = 0;
  }

  void RemovePrefix(size_t bytes) {
    assert(bytes <= size_);
    data_ += bytes;
    size_ -= bytes;
  }

  std::string ToString() const {
    return std::string(data_, size_);
  }

  int Compare(const Slice& other) const {
    const size_t min_size = size_ < other.size_ ? size_ : other.size_;
    int result = std::memcmp(data_, other.data_, min_size);
    if (result == 0) {
      if (size_ < other.size_) {
        return -1;
      }
      if (size_ > other.size_) {
        return 1;
      }
    }
    return result;
  }

  bool StartsWith(const Slice& prefix) const {
    return size_ >= prefix.size_ && std::memcmp(data_, prefix.data_, prefix.size_) == 0;
  }

private:
  const char* data_;
  size_t size_;
};

inline bool operator==(const Slice& lhs, const Slice& rhs) {
  return lhs.Size() == rhs.Size() && std::memcmp(lhs.Data(), rhs.Data(), lhs.Size()) == 0;
}

inline bool operator!=(const Slice& lhs, const Slice& rhs) {
  return !(lhs == rhs);
}

}  // namespace db
