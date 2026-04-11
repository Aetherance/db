#include <thread>

#include "gtest/gtest.h"
#include "port.h"

namespace db {
namespace port {
namespace {

TEST(PortStdCxxTest, MutexAndCondVarCoordinateThreads) {
  Mutex mutex;
  CondVar condition(&mutex);
  bool ready = false;
  bool done = false;

  std::thread worker([&]() {
    mutex.Lock();
    ready = true;
    condition.Signal();
    while (!done) {
      condition.Wait();
    }
    mutex.Unlock();
  });

  mutex.Lock();
  while (!ready) {
    condition.Wait();
  }
  done = true;
  condition.Signal();
  mutex.Unlock();

  worker.join();
}

TEST(PortStdCxxTest, HeapProfileStubReturnsFalse) {
  EXPECT_FALSE(GetHeapProfile(nullptr, nullptr));
}

}  // namespace
}  // namespace port
}  // namespace db
