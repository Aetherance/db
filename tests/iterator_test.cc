#include "iterator.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

namespace db {
namespace {

class StubIterator : public Iterator {
public:
  explicit StubIterator(Status status = Status::OkStatus()) : status_(status) {}

  bool Valid() const override {
    return false;
  }

  void SeekToFirst() override {}

  void SeekToLast() override {}

  void Seek(const Slice& target) override {
    (void)target;
  }

  void Next() override {}

  void Prev() override {}

  Slice Key() const override {
    return Slice();
  }

  Slice Value() const override {
    return Slice();
  }

  Status GetStatus() const override {
    return status_;
  }

private:
  Status status_;
};

void RecordCleanup(void* arg1, void* arg2) {
  auto* order = static_cast<std::vector<int>*>(arg1);
  order->push_back(static_cast<int>(reinterpret_cast<std::intptr_t>(arg2)));
}

TEST(IteratorTest, RegisteredCleanupsRunOnDestruction) {
  std::vector<int> order;

  {
    auto iter = std::make_unique<StubIterator>();
    iter->RegisterCleanup(&RecordCleanup, &order, reinterpret_cast<void*>(1));
    iter->RegisterCleanup(&RecordCleanup, &order, reinterpret_cast<void*>(2));
    iter->RegisterCleanup(&RecordCleanup, &order, reinterpret_cast<void*>(3));
  }

  EXPECT_EQ((std::vector<int>{1, 3, 2}), order);
}

TEST(IteratorTest, NewEmptyIteratorIsInvalidAndReportsOk) {
  std::unique_ptr<Iterator> iter(NewEmptyIterator());

  ASSERT_NE(nullptr, iter);
  iter->SeekToFirst();
  iter->SeekToLast();
  iter->Seek("target");
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->GetStatus().Ok());
}

TEST(IteratorTest, NewErrorIteratorPreservesStatus) {
  const Status error = Status::IOError("read", "failed");
  std::unique_ptr<Iterator> iter(NewErrorIterator(error));

  ASSERT_NE(nullptr, iter);
  EXPECT_FALSE(iter->Valid());
  EXPECT_EQ(error, iter->GetStatus());
}

}  // namespace
}  // namespace db
