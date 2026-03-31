#pragma once

#include <cassert>

#include "slice.h"
#include "status.h"

namespace db {

class Iterator {
public:
  using CleanupFunction = void (*)(void* arg1, void* arg2);

  Iterator();

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  virtual ~Iterator();

  virtual bool Valid() const = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual void Seek(const Slice& target) = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual Slice Key() const = 0;

  virtual Slice Value() const = 0;

  virtual Status GetStatus() const = 0;

  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

private:
  struct CleanupNode {
    bool IsEmpty() const {
      return function == nullptr;
    }

    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    CleanupFunction function;
    void* arg1;
    void* arg2;
    CleanupNode* next;
  };

  CleanupNode cleanup_head_;
};

Iterator* NewEmptyIterator();

Iterator* NewErrorIterator(const Status& status);

}  // namespace db
