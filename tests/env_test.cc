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

void RunThreadWork(void* arg) {
  auto* value = static_cast<std::atomic<bool>*>(arg);
  value->store(true, std::memory_order_release);
}

class DummySequentialFile : public SequentialFile {
public:
  Status Read(size_t n, Slice* result, char* scratch) override {
    (void)n;
    (void)scratch;
    *result = Slice();
    return Status::OkStatus();
  }

  Status Skip(uint64_t n) override {
    (void)n;
    return Status::OkStatus();
  }
};

class DummyRandomAccessFile : public RandomAccessFile {
public:
  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
    (void)offset;
    (void)n;
    (void)scratch;
    *result = Slice();
    return Status::OkStatus();
  }
};

class DummyWritableFile : public WritableFile {
public:
  Status Append(const Slice& data) override {
    (void)data;
    return Status::OkStatus();
  }

  Status Close() override {
    return Status::OkStatus();
  }

  Status Flush() override {
    return Status::OkStatus();
  }

  Status Sync() override {
    return Status::OkStatus();
  }
};

class DummyLogger : public Logger {
public:
  void Logv(const char* format, std::va_list ap) override {
    (void)format;
    (void)ap;
  }
};

class DummyFileLock : public FileLock {};

class RecordingEnv : public Env {
public:
  Status NewSequentialFile(const std::string& fname, SequentialFile** result) override {
    last_sequential_file = fname;
    *result = new DummySequentialFile();
    return Status::OkStatus();
  }

  Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) override {
    last_random_access_file = fname;
    *result = new DummyRandomAccessFile();
    return Status::OkStatus();
  }

  Status NewWritableFile(const std::string& fname, WritableFile** result) override {
    last_writable_file = fname;
    *result = new DummyWritableFile();
    return Status::OkStatus();
  }

  Status NewAppendableFile(const std::string& fname, WritableFile** result) override {
    last_appendable_file = fname;
    *result = new DummyWritableFile();
    return Status::OkStatus();
  }

  bool FileExists(const std::string& fname) override {
    last_exists_file = fname;
    return true;
  }

  Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {
    last_children_dir = dir;
    *result = {"a", "b"};
    return Status::OkStatus();
  }

  Status RemoveFile(const std::string& fname) override {
    last_removed_file = fname;
    return Status::OkStatus();
  }

  Status CreateDir(const std::string& dirname) override {
    last_created_dir = dirname;
    return Status::OkStatus();
  }

  Status RemoveDir(const std::string& dirname) override {
    last_removed_dir = dirname;
    return Status::OkStatus();
  }

  Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
    last_sized_file = fname;
    *file_size = 123;
    return Status::OkStatus();
  }

  Status RenameFile(const std::string& src, const std::string& target) override {
    last_rename = {src, target};
    return Status::OkStatus();
  }

  Status LockFile(const std::string& fname, FileLock** lock) override {
    last_locked_file = fname;
    *lock = new DummyFileLock();
    return Status::OkStatus();
  }

  Status UnlockFile(FileLock* lock) override {
    ++unlock_count;
    delete lock;
    return Status::OkStatus();
  }

  void Schedule(void (*function)(void* arg), void* arg) override {
    ++schedule_count;
    function(arg);
  }

  void StartThread(void (*function)(void* arg), void* arg) override {
    ++start_thread_count;
    function(arg);
  }

  Status GetTestDirectory(std::string* path) override {
    *path = "/tmp/recording-env";
    return Status::OkStatus();
  }

  Status NewLogger(const std::string& fname, Logger** result) override {
    last_logger_file = fname;
    *result = new DummyLogger();
    return Status::OkStatus();
  }

  uint64_t NowMicros() override {
    return 4242;
  }

  void SleepForMicroseconds(int micros) override {
    last_sleep = micros;
  }

  std::string last_sequential_file;
  std::string last_random_access_file;
  std::string last_writable_file;
  std::string last_appendable_file;
  std::string last_exists_file;
  std::string last_children_dir;
  std::string last_removed_file;
  std::string last_created_dir;
  std::string last_removed_dir;
  std::string last_sized_file;
  std::pair<std::string, std::string> last_rename;
  std::string last_locked_file;
  std::string last_logger_file;
  int unlock_count = 0;
  int schedule_count = 0;
  int start_thread_count = 0;
  int last_sleep = 0;
};

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

TEST(EnvTest, SequentialAndRandomAccessFilesReadExpectedRanges) {
  Env* env = Env::Default();
  const std::string dir = MakeTestPath("io");
  const std::string file_path = dir + "/data.txt";

  ASSERT_TRUE(env->CreateDir(dir).Ok());
  ASSERT_TRUE(WriteStringToFile(env, Slice("abcdef"), file_path).Ok());

  SequentialFile* sequential = nullptr;
  ASSERT_TRUE(env->NewSequentialFile(file_path, &sequential).Ok());
  ASSERT_NE(nullptr, sequential);

  char sequential_scratch[8];
  Slice sequential_result;
  ASSERT_TRUE(sequential->Read(3, &sequential_result, sequential_scratch).Ok());
  EXPECT_EQ("abc", sequential_result.ToString());
  ASSERT_TRUE(sequential->Skip(1).Ok());
  ASSERT_TRUE(
      sequential->Read(sizeof(sequential_scratch), &sequential_result, sequential_scratch).Ok());
  EXPECT_EQ("ef", sequential_result.ToString());
  delete sequential;

  RandomAccessFile* random = nullptr;
  ASSERT_TRUE(env->NewRandomAccessFile(file_path, &random).Ok());
  ASSERT_NE(nullptr, random);

  char random_scratch[8];
  Slice random_result;
  ASSERT_TRUE(random->Read(2, 3, &random_result, random_scratch).Ok());
  EXPECT_EQ("cde", random_result.ToString());
  delete random;

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

TEST(EnvTest, RenameLockUnlockAndStartThreadRoundTrip) {
  Env* env = Env::Default();
  const std::string dir = MakeTestPath("rename-lock");
  const std::string source = dir + "/source.txt";
  const std::string target = dir + "/target.txt";
  const std::string lock_path = dir + "/LOCK";

  ASSERT_TRUE(env->CreateDir(dir).Ok());
  ASSERT_TRUE(WriteStringToFile(env, Slice("payload"), source).Ok());
  ASSERT_TRUE(env->RenameFile(source, target).Ok());
  EXPECT_FALSE(env->FileExists(source));
  EXPECT_TRUE(env->FileExists(target));

  FileLock* first_lock = nullptr;
  ASSERT_TRUE(env->LockFile(lock_path, &first_lock).Ok());
  ASSERT_NE(nullptr, first_lock);

  FileLock* second_lock = nullptr;
  const Status second_status = env->LockFile(lock_path, &second_lock);
  EXPECT_FALSE(second_status.Ok());
  EXPECT_EQ(nullptr, second_lock);

  ASSERT_TRUE(env->UnlockFile(first_lock).Ok());

  FileLock* relocked = nullptr;
  ASSERT_TRUE(env->LockFile(lock_path, &relocked).Ok());
  ASSERT_NE(nullptr, relocked);
  ASSERT_TRUE(env->UnlockFile(relocked).Ok());

  std::atomic<bool> ran(false);
  env->StartThread(&RunThreadWork, &ran);
  for (int attempt = 0; attempt < 100 && !ran.load(std::memory_order_acquire); ++attempt) {
    env->SleepForMicroseconds(1000);
  }
  EXPECT_TRUE(ran.load(std::memory_order_acquire));

  ASSERT_TRUE(env->RemoveFile(target).Ok());
  ASSERT_TRUE(env->RemoveFile(lock_path).Ok());
  ASSERT_TRUE(env->RemoveDir(dir).Ok());
}

TEST(EnvTest, EnvWrapperForwardsCallsToTarget) {
  RecordingEnv target;
  EnvWrapper wrapper(&target);

  SequentialFile* sequential = nullptr;
  ASSERT_TRUE(wrapper.NewSequentialFile("seq", &sequential).Ok());
  ASSERT_NE(nullptr, sequential);
  delete sequential;
  EXPECT_EQ("seq", target.last_sequential_file);

  RandomAccessFile* random = nullptr;
  ASSERT_TRUE(wrapper.NewRandomAccessFile("rand", &random).Ok());
  ASSERT_NE(nullptr, random);
  delete random;
  EXPECT_EQ("rand", target.last_random_access_file);

  WritableFile* writable = nullptr;
  ASSERT_TRUE(wrapper.NewWritableFile("write", &writable).Ok());
  ASSERT_NE(nullptr, writable);
  delete writable;
  EXPECT_EQ("write", target.last_writable_file);

  WritableFile* appendable = nullptr;
  ASSERT_TRUE(wrapper.NewAppendableFile("append", &appendable).Ok());
  ASSERT_NE(nullptr, appendable);
  delete appendable;
  EXPECT_EQ("append", target.last_appendable_file);

  EXPECT_TRUE(wrapper.FileExists("exists"));
  EXPECT_EQ("exists", target.last_exists_file);

  std::vector<std::string> children;
  ASSERT_TRUE(wrapper.GetChildren("dir", &children).Ok());
  EXPECT_EQ((std::vector<std::string>{"a", "b"}), children);
  EXPECT_EQ("dir", target.last_children_dir);

  ASSERT_TRUE(wrapper.RemoveFile("file").Ok());
  EXPECT_EQ("file", target.last_removed_file);

  ASSERT_TRUE(wrapper.CreateDir("mkdir").Ok());
  EXPECT_EQ("mkdir", target.last_created_dir);

  ASSERT_TRUE(wrapper.RemoveDir("rmdir").Ok());
  EXPECT_EQ("rmdir", target.last_removed_dir);

  uint64_t file_size = 0;
  ASSERT_TRUE(wrapper.GetFileSize("size", &file_size).Ok());
  EXPECT_EQ(123U, file_size);
  EXPECT_EQ("size", target.last_sized_file);

  ASSERT_TRUE(wrapper.RenameFile("from", "to").Ok());
  EXPECT_EQ(std::make_pair(std::string("from"), std::string("to")), target.last_rename);

  FileLock* lock = nullptr;
  ASSERT_TRUE(wrapper.LockFile("lock", &lock).Ok());
  ASSERT_NE(nullptr, lock);
  EXPECT_EQ("lock", target.last_locked_file);
  ASSERT_TRUE(wrapper.UnlockFile(lock).Ok());
  EXPECT_EQ(1, target.unlock_count);

  std::atomic<bool> scheduled(false);
  wrapper.Schedule(&RunScheduledWork, &scheduled);
  EXPECT_TRUE(scheduled.load(std::memory_order_acquire));
  EXPECT_EQ(1, target.schedule_count);

  std::atomic<bool> threaded(false);
  wrapper.StartThread(&RunThreadWork, &threaded);
  EXPECT_TRUE(threaded.load(std::memory_order_acquire));
  EXPECT_EQ(1, target.start_thread_count);

  std::string path;
  ASSERT_TRUE(wrapper.GetTestDirectory(&path).Ok());
  EXPECT_EQ("/tmp/recording-env", path);

  Logger* logger = nullptr;
  ASSERT_TRUE(wrapper.NewLogger("log", &logger).Ok());
  ASSERT_NE(nullptr, logger);
  delete logger;
  EXPECT_EQ("log", target.last_logger_file);

  EXPECT_EQ(4242U, wrapper.NowMicros());
  wrapper.SleepForMicroseconds(7);
  EXPECT_EQ(7, target.last_sleep);
}

}  // namespace db
