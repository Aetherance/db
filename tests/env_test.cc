#include "env.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace db {
namespace {

std::string MakeTestPath(const std::string& name) {
  Env* env = Env::Default();
  std::string root;
  EXPECT_TRUE(env->GetTestDirectory(&root).Ok());
  return root + "/env-test-" + std::to_string(env->NowMicros()) + "-" + name;
}

void RunScheduledWork(void* arg) {
  auto* value = static_cast<std::atomic<bool>*>(arg);
  value->store(true, std::memory_order_release);
}

}  // namespace

TEST(EnvTest, WriteReadAppendAndDeleteRoundTrip) {
  Env* env = Env::Default();
  const std::string dir = MakeTestPath("dir");
  const std::string file_path = dir + "/data.txt";

  ASSERT_TRUE(env->CreateDir(dir).Ok());
  ASSERT_TRUE(WriteStringToFile(env, Slice("hello"), file_path).Ok());
  ASSERT_TRUE(env->FileExists(file_path));

  std::string file_contents;
  ASSERT_TRUE(ReadFileToString(env, file_path, &file_contents).Ok());
  EXPECT_EQ("hello", file_contents);

  WritableFile* appendable = nullptr;
  ASSERT_TRUE(env->NewAppendableFile(file_path, &appendable).Ok());
  ASSERT_NE(nullptr, appendable);
  ASSERT_TRUE(appendable->Append(Slice(" world")).Ok());
  ASSERT_TRUE(appendable->Close().Ok());
  delete appendable;

  ASSERT_TRUE(ReadFileToString(env, file_path, &file_contents).Ok());
  EXPECT_EQ("hello world", file_contents);

  uint64_t file_size = 0;
  ASSERT_TRUE(env->GetFileSize(file_path, &file_size).Ok());
  EXPECT_EQ(static_cast<uint64_t>(11), file_size);

  std::vector<std::string> children;
  ASSERT_TRUE(env->GetChildren(dir, &children).Ok());
  EXPECT_FALSE(children.empty());

  ASSERT_TRUE(env->RemoveFile(file_path).Ok());
  ASSERT_TRUE(env->RemoveDir(dir).Ok());
}

TEST(EnvTest, ScheduleRunsBackgroundWork) {
  Env* env = Env::Default();

  std::atomic<bool> ran(false);
  env->Schedule(&RunScheduledWork, &ran);

  for (int attempt = 0; attempt < 100 && !ran.load(std::memory_order_acquire); ++attempt) {
    env->SleepForMicroseconds(1000);
  }

  EXPECT_TRUE(ran.load(std::memory_order_acquire));
}

TEST(EnvTest, LoggerWritesMessages) {
  Env* env = Env::Default();
  const std::string dir = MakeTestPath("logdir");
  const std::string file_path = dir + "/log.txt";

  ASSERT_TRUE(env->CreateDir(dir).Ok());

  Logger* logger = nullptr;
  ASSERT_TRUE(env->NewLogger(file_path, &logger).Ok());
  ASSERT_NE(nullptr, logger);
  Log(logger, "value=%d", 7);
  delete logger;

  std::string log_contents;
  ASSERT_TRUE(ReadFileToString(env, file_path, &log_contents).Ok());
  EXPECT_NE(std::string::npos, log_contents.find("value=7"));

  ASSERT_TRUE(env->RemoveFile(file_path).Ok());
  ASSERT_TRUE(env->RemoveDir(dir).Ok());
}

}  // namespace db
