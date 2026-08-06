#include "db/db_impl.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "cache.h"
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "env.h"
#include "table/merger.h"
#include "util/logging.h"
#include "write_batch.h"

namespace db {

namespace {

constexpr int kNumNonTableCacheFiles = 10;

Env* OptionsEnv(const Options& options) {
  return options.env != nullptr ? options.env : Env::Default();
}

const Comparator* OptionsComparator(const Options& options) {
  return options.comparator != nullptr ? options.comparator : BytewiseComparator();
}

int TableCacheSize(const Options& options) {
  return std::max(1, options.max_open_files - kNumNonTableCacheFiles);
}

struct IterState {
  IterState(std::mutex* db_mutex, MemTable* memtable, MemTable* immutable, Version* version)
      : mutex(db_mutex), mem(memtable), imm(immutable), current(version) {}

  std::mutex* const mutex;
  MemTable* const mem;
  MemTable* const imm;
  Version* const current;
};

void CleanupIteratorState(void* arg1, void* arg2) {
  (void)arg2;
  auto* state = static_cast<IterState*>(arg1);
  std::lock_guard<std::mutex> lock(*state->mutex);
  state->mem->Unref();
  if (state->imm != nullptr) {
    state->imm->Unref();
  }
  state->current->Unref();
  delete state;
}

}  // namespace

Options PrepareOptions(const InternalKeyComparator* comparator,
                       const InternalFilterPolicy* filter_policy, const Options& source) {
  Options result = source;
  result.env = OptionsEnv(source);
  result.comparator = comparator;
  result.filter_policy = source.filter_policy != nullptr ? filter_policy : nullptr;
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(OptionsEnv(raw_options)),
      internal_comparator_(OptionsComparator(raw_options)),
      internal_filter_policy_(raw_options.filter_policy),
      options_(PrepareOptions(&internal_comparator_, &internal_filter_policy_, raw_options)),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      imm_logfile_number_(0),
      log_(nullptr),
      background_flush_scheduled_(false),
      background_error_(Status::OkStatus()),
      versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_)) {}

DBImpl::~DBImpl() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    background_work_finished_.wait(lock, [this] { return !background_flush_scheduled_; });
  }

  delete log_;
  delete logfile_;
  if (mem_ != nullptr) {
    mem_->Unref();
  }
  if (imm_ != nullptr) {
    imm_->Unref();
  }
  delete versions_;
  delete table_cache_;
  if (owns_cache_) {
    delete options_.block_cache;
  }
  if (db_lock_ != nullptr) {
    const Status status = env_->UnlockFile(db_lock_);
    if (!status.Ok()) {
      Log(options_.info_log, "unlocking database: %s", status.ToString().c_str());
    }
  }
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  return DB::Put(options, key, value);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::NewDB() {
  VersionEdit edit;
  edit.SetComparatorName(user_comparator()->Name());
  edit.SetLogNumber(0);
  edit.SetNextFile(2);
  edit.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* raw_file = nullptr;
  Status status = env_->NewWritableFile(manifest, &raw_file);
  if (!status.Ok()) {
    return status;
  }

  std::unique_ptr<WritableFile> file(raw_file);
  log::Writer writer(file.get());
  std::string record;
  edit.EncodeTo(&record);
  status = writer.AddRecord(record);
  if (status.Ok()) {
    status = file->Sync();
  }
  if (status.Ok()) {
    status = file->Close();
  }

  if (status.Ok()) {
    status = SetCurrentFile(env_, dbname_, 1);
  }
  if (!status.Ok()) {
    env_->RemoveFile(manifest);
  }
  return status;
}

Status DBImpl::Recover(VersionEdit* edit, std::vector<uint64_t>* recovered_logs) {
  Status status;
  if (!env_->FileExists(dbname_)) {
    status = env_->CreateDir(dbname_);
    if (!status.Ok() && !env_->FileExists(dbname_)) {
      return status;
    }
  }

  status = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!status.Ok()) {
    return status;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (!options_.create_if_missing) {
      return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
    }
    status = NewDB();
    if (!status.Ok()) {
      return status;
    }
  } else if (options_.error_if_exists) {
    return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
  }

  status = versions_->Recover();
  if (!status.Ok()) {
    return status;
  }

  std::vector<std::string> filenames;
  status = env_->GetChildren(dbname_, &filenames);
  if (!status.Ok()) {
    return status;
  }

  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  uint64_t number = 0;
  FileType type = kLogFile;
  for (const std::string& filename : filenames) {
    if (!ParseFileName(filename, &number, &type)) {
      continue;
    }
    versions_->MarkFileNumberUsed(number);
    if (type == kLogFile && (number >= min_log || number == prev_log)) {
      recovered_logs->push_back(number);
    }
  }
  std::sort(recovered_logs->begin(), recovered_logs->end());

  auto* recovered_mem = new MemTable(internal_comparator_);
  recovered_mem->Ref();
  SequenceNumber max_sequence = versions_->LastSequence();
  bool has_entries = false;
  for (uint64_t log_number : *recovered_logs) {
    status = RecoverLogFile(log_number, recovered_mem, &max_sequence, &has_entries);
    if (!status.Ok()) {
      break;
    }
  }

  if (status.Ok() && has_entries) {
    status = WriteLevel0Table(recovered_mem, edit);
  }
  recovered_mem->Unref();

  if (status.Ok() && versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }
  return status;
}

Status DBImpl::RecoverLogFile(uint64_t log_number, MemTable* mem, SequenceNumber* max_sequence,
                              bool* has_entries) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;

    void Corruption(size_t bytes, const Status& corruption) override {
      (void)bytes;
      if (status->Ok()) {
        *status = corruption;
      }
    }
  };

  const std::string filename = LogFileName(dbname_, log_number);
  SequentialFile* raw_file = nullptr;
  Status status = env_->NewSequentialFile(filename, &raw_file);
  if (!status.Ok()) {
    return status;
  }

  std::unique_ptr<SequentialFile> file(raw_file);
  LogReporter reporter;
  reporter.status = &status;
  log::Reader reader(file.get(), &reporter, true, 0);
  std::string scratch;
  Slice record;
  WriteBatch batch;
  while (reader.ReadRecord(&record, &scratch) && status.Ok()) {
    if (record.Size() < 12) {
      status = Status::Corruption("log record too small");
      break;
    }

    WriteBatchInternal::SetContents(&batch, record);
    status = WriteBatchInternal::InsertInto(&batch, mem);
    if (!status.Ok()) {
      break;
    }

    const int count = WriteBatchInternal::Count(&batch);
    if (count > 0) {
      const SequenceNumber last_sequence =
          WriteBatchInternal::Sequence(&batch) + static_cast<SequenceNumber>(count) - 1;
      *max_sequence = std::max(*max_sequence, last_sequence);
      *has_entries = true;
    }
  }
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();

  std::unique_ptr<Iterator> iter(mem->NewIterator());
  Status status = BuildTable(dbname_, env_, options_, table_cache_, iter.get(), &meta);
  if (status.Ok() && meta.file_size > 0) {
    edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
  }
  return status;
}

Status DBImpl::MakeMemTableImmutable() {
  assert(mem_ != nullptr);
  assert(imm_ == nullptr);
  assert(log_ != nullptr);
  assert(logfile_ != nullptr);

  const uint64_t new_log_number = versions_->NewFileNumber();
  WritableFile* raw_file = nullptr;
  Status status = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &raw_file);
  if (!status.Ok()) {
    return status;
  }

  std::unique_ptr<WritableFile> new_logfile(raw_file);
  auto new_log = std::make_unique<log::Writer>(new_logfile.get());
  status = new_logfile->Sync();
  if (status.Ok()) {
    status = logfile_->Flush();
  }

  auto* new_mem = new MemTable(internal_comparator_);
  new_mem->Ref();
  if (status.Ok()) {
    VersionEdit edit;
    edit.SetPrevLogNumber(logfile_number_);
    edit.SetLogNumber(new_log_number);
    status = versions_->LogAndApply(&edit);
  }

  if (!status.Ok()) {
    new_log.reset();
    new_logfile.reset();
    new_mem->Unref();
    env_->RemoveFile(LogFileName(dbname_, new_log_number));
    return status;
  }

  delete log_;
  delete logfile_;
  log_ = new_log.release();
  logfile_ = new_logfile.release();
  imm_ = mem_;
  imm_logfile_number_ = logfile_number_;
  mem_ = new_mem;
  logfile_number_ = new_log_number;
  background_flush_scheduled_ = true;
  return Status::OkStatus();
}

Status DBImpl::FlushMemTable() {
  bool schedule_background_work = false;
  std::unique_lock<std::mutex> lock(mutex_);
  background_work_finished_.wait(lock,
                                 [this] { return imm_ == nullptr || !background_error_.Ok(); });
  if (!background_error_.Ok()) {
    return background_error_;
  }

  std::unique_ptr<Iterator> probe(mem_->NewIterator());
  probe->SeekToFirst();
  if (!probe->Valid()) {
    return probe->GetStatus();
  }

  Status status = MakeMemTableImmutable();
  if (!status.Ok()) {
    background_error_ = status;
    return status;
  }
  schedule_background_work = true;

  lock.unlock();
  if (schedule_background_work) {
    env_->Schedule(&DBImpl::BackgroundWork, this);
  }
  lock.lock();
  background_work_finished_.wait(lock, [this] { return !background_flush_scheduled_; });
  return background_error_;
}

void DBImpl::BackgroundWork(void* db) {
  static_cast<DBImpl*>(db)->BackgroundFlush();
}

void DBImpl::BackgroundFlush() {
  FileMetaData meta;
  MemTable* immutable = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(background_flush_scheduled_);
    assert(imm_ != nullptr);
    immutable = imm_;
    meta.number = versions_->NewFileNumber();
  }

  Status status;
  {
    std::unique_ptr<Iterator> iter(immutable->NewIterator());
    status = BuildTable(dbname_, env_, options_, table_cache_, iter.get(), &meta);
  }
  if (status.Ok() && meta.file_size == 0) {
    status = Status::Corruption("immutable MemTable produced an empty table");
  }

  std::unique_lock<std::mutex> lock(mutex_);
  bool installed = false;
  if (status.Ok()) {
    VersionEdit edit;
    edit.AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);
    status = versions_->LogAndApply(&edit);
    installed = status.Ok();
  }

  if (installed) {
    const uint64_t obsolete_log_number = imm_logfile_number_;
    imm_ = nullptr;
    imm_logfile_number_ = 0;
    immutable->Unref();

    if (obsolete_log_number != 0) {
      env_->RemoveFile(LogFileName(dbname_, obsolete_log_number));
    }
    status = CompactIfNeeded();
  } else {
    table_cache_->Evict(meta.number);
    env_->RemoveFile(TableFileName(dbname_, meta.number));
  }

  if (!status.Ok() && background_error_.Ok()) {
    background_error_ = status;
  }
  background_flush_scheduled_ = false;
  lock.unlock();
  background_work_finished_.notify_all();
}

Status DBImpl::RunCompaction(Compaction* compaction) {
  assert(compaction != nullptr);

  if (compaction->IsTrivialMove()) {
    FileMetaData* file = compaction->input(0, 0);
    compaction->edit()->RemoveFile(compaction->level(), file->number);
    compaction->edit()->AddFile(compaction->level() + 1, file->number, file->file_size,
                                file->smallest, file->largest);
    return versions_->LogAndApply(compaction->edit());
  }

  FileMetaData output;
  output.number = versions_->NewFileNumber();
  std::unique_ptr<Iterator> input(versions_->MakeInputIterator(compaction));
  Status status = BuildTable(dbname_, env_, options_, table_cache_, input.get(), &output);
  if (status.Ok() && output.file_size == 0) {
    status = Status::Corruption("compaction produced an empty table");
  }

  if (status.Ok()) {
    compaction->AddInputDeletions(compaction->edit());
    compaction->edit()->AddFile(compaction->level() + 1, output.number, output.file_size,
                                output.smallest, output.largest);
    status = versions_->LogAndApply(compaction->edit());
  }

  if (!status.Ok()) {
    table_cache_->Evict(output.number);
    env_->RemoveFile(TableFileName(dbname_, output.number));
  }
  return status;
}

void DBImpl::RemoveObsoleteTables() {
  std::set<uint64_t> live;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  if (!env_->GetChildren(dbname_, &filenames).Ok()) {
    return;
  }

  for (const std::string& filename : filenames) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(filename, &number, &type) && type == kTableFile && live.count(number) == 0) {
      table_cache_->Evict(number);
      env_->RemoveFile(dbname_ + "/" + filename);
    }
  }
}

Status DBImpl::CompactIfNeeded() {
  while (versions_->NeedsCompaction()) {
    std::unique_ptr<Compaction> compaction(versions_->PickCompaction());
    if (compaction == nullptr) {
      break;
    }
    Status status = RunCompaction(compaction.get());
    compaction.reset();
    if (!status.Ok()) {
      return status;
    }
    RemoveObsoleteTables();
  }
  return Status::OkStatus();
}

Status DBImpl::CompactLevel(int level, const Slice* begin, const Slice* end) {
  InternalKey begin_storage;
  InternalKey end_storage;
  const InternalKey* manual_begin = nullptr;
  const InternalKey* manual_end = nullptr;
  if (begin != nullptr) {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual_begin = &begin_storage;
  }
  if (end != nullptr) {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual_end = &end_storage;
  }

  while (true) {
    std::unique_ptr<Compaction> compaction(
        versions_->CompactRange(level, manual_begin, manual_end));
    if (compaction == nullptr) {
      return Status::OkStatus();
    }

    const InternalKey next_begin =
        compaction->input(0, compaction->num_input_files(0) - 1)->largest;
    Status status = RunCompaction(compaction.get());
    compaction.reset();
    if (!status.Ok()) {
      return status;
    }
    RemoveObsoleteTables();
    begin_storage = next_begin;
    manual_begin = &begin_storage;
  }
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  if (updates == nullptr) {
    return Status::InvalidArgument("WriteBatch must not be null");
  }

  bool schedule_background_work = false;
  std::unique_lock<std::mutex> lock(mutex_);
  if (!background_error_.Ok()) {
    return background_error_;
  }
  assert(mem_ != nullptr);
  assert(log_ != nullptr);

  const int count = WriteBatchInternal::Count(updates);
  if (count < 0) {
    return Status::Corruption("negative WriteBatch count");
  }

  const SequenceNumber first_sequence = versions_->LastSequence() + 1;
  WriteBatchInternal::SetSequence(updates, first_sequence);
  Status status = log_->AddRecord(WriteBatchInternal::Contents(updates));
  if (status.Ok() && options.sync) {
    status = logfile_->Sync();
  }
  if (status.Ok()) {
    status = WriteBatchInternal::InsertInto(updates, mem_);
  }
  if (!status.Ok()) {
    background_error_ = status;
    return status;
  }

  if (count > 0) {
    versions_->SetLastSequence(first_sequence + static_cast<SequenceNumber>(count) - 1);
  }
  while (mem_->ApproximateMemoryUsage() > options_.write_buffer_size) {
    if (imm_ != nullptr) {
      background_work_finished_.wait(lock,
                                     [this] { return imm_ == nullptr || !background_error_.Ok(); });
      if (!background_error_.Ok()) {
        return background_error_;
      }
      continue;
    }

    status = MakeMemTableImmutable();
    if (!status.Ok()) {
      background_error_ = status;
      return status;
    }
    schedule_background_work = true;
    break;
  }

  lock.unlock();
  if (schedule_background_work) {
    env_->Schedule(&DBImpl::BackgroundWork, this);
  }
  return Status::OkStatus();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  const SequenceNumber sequence =
      options.snapshot != nullptr
          ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
          : versions_->LastSequence();
  const LookupKey lookup_key(key, sequence);
  Status status;
  if (mem_->Get(lookup_key, value, &status)) {
    return status;
  }
  if (imm_ != nullptr && imm_->Get(lookup_key, value, &status)) {
    return status;
  }

  return versions_->current()->Get(options, lookup_key, value);
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, SequenceNumber* latest_sequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  *latest_sequence = versions_->LastSequence();

  std::vector<Iterator*> iterators;
  iterators.push_back(mem_->NewIterator());
  mem_->Ref();
  MemTable* immutable = imm_;
  if (immutable != nullptr) {
    iterators.push_back(immutable->NewIterator());
    immutable->Ref();
  }

  Version* current = versions_->current();
  current->AddIterators(options, &iterators);
  current->Ref();

  Iterator* result = NewMergingIterator(&internal_comparator_, iterators.data(),
                                        static_cast<int>(iterators.size()));
  result->RegisterCleanup(CleanupIteratorState, new IterState(&mutex_, mem_, immutable, current),
                          nullptr);
  return result;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_sequence = 0;
  Iterator* internal = NewInternalIterator(options, &latest_sequence);
  const SequenceNumber sequence =
      options.snapshot != nullptr
          ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
          : latest_sequence;
  return NewDBIterator(user_comparator(), internal, sequence);
}

const Snapshot* DBImpl::GetSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  std::lock_guard<std::mutex> lock(mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  value->clear();
  Slice input = property;
  const Slice prefix("db.");
  if (!input.StartsWith(prefix)) {
    return false;
  }
  input.RemovePrefix(prefix.Size());

  constexpr char kLevelFiles[] = "num-files-at-level";
  if (input.StartsWith(kLevelFiles)) {
    input.RemovePrefix(sizeof(kLevelFiles) - 1);
    uint64_t level = 0;
    if (!ConsumeDecimalNumber(&input, &level) || !input.Empty() || level >= config::kNumLevels) {
      return false;
    }
    *value = std::to_string(versions_->NumLevelFiles(static_cast<int>(level)));
    return true;
  }

  if (input == Slice("sstables")) {
    *value = versions_->current()->DebugString();
    return true;
  }

  if (input == Slice("approximate-memory-usage")) {
    size_t usage = options_.block_cache->TotalCharge() + mem_->ApproximateMemoryUsage();
    if (imm_ != nullptr) {
      usage += imm_->ApproximateMemoryUsage();
    }
    *value = std::to_string(usage);
    return true;
  }
  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  std::lock_guard<std::mutex> lock(mutex_);
  Version* current = versions_->current();
  for (int i = 0; i < n; ++i) {
    InternalKey start(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    const uint64_t start_offset = versions_->ApproximateOffsetOf(current, start);
    const uint64_t limit_offset = versions_->ApproximateOffsetOf(current, limit);
    sizes[i] = limit_offset >= start_offset ? limit_offset - start_offset : 0;
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  Status status = FlushMemTable();
  if (!status.Ok()) {
    Log(options_.info_log, "MemTable flush failed: %s", status.ToString().c_str());
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  int max_level_with_files = 1;
  Version* current = versions_->current();
  for (int level = 1; level < config::kNumLevels; ++level) {
    if (current->OverlapInLevel(level, begin, end)) {
      max_level_with_files = level;
    }
  }

  for (int level = 0; level < max_level_with_files; ++level) {
    status = CompactLevel(level, begin, end);
    if (!status.Ok()) {
      Log(options_.info_log, "synchronous compaction failed: %s", status.ToString().c_str());
      return;
    }
  }
}

Status DB::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(options, &batch);
}

Status DB::Delete(const WriteOptions& options, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(options, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
  *dbptr = nullptr;
  auto impl = std::make_unique<DBImpl>(options, name);

  VersionEdit edit;
  std::vector<uint64_t> recovered_logs;
  Status status = impl->Recover(&edit, &recovered_logs);
  if (status.Ok()) {
    const uint64_t log_number = impl->versions_->NewFileNumber();
    WritableFile* logfile = nullptr;
    status = impl->env_->NewWritableFile(LogFileName(name, log_number), &logfile);
    if (status.Ok()) {
      impl->logfile_ = logfile;
      impl->logfile_number_ = log_number;
      impl->log_ = new log::Writer(logfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();

      edit.SetPrevLogNumber(0);
      edit.SetLogNumber(log_number);
      status = impl->versions_->LogAndApply(&edit);
    }
  }

  if (!status.Ok()) {
    return status;
  }

  for (uint64_t log_number : recovered_logs) {
    impl->env_->RemoveFile(LogFileName(name, log_number));
  }
  status = impl->CompactIfNeeded();
  if (!status.Ok()) {
    return status;
  }
  *dbptr = impl.release();
  return Status::OkStatus();
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = OptionsEnv(options);
  if (!env->FileExists(dbname)) {
    return Status::OkStatus();
  }

  FileLock* db_lock = nullptr;
  Status status = env->LockFile(LockFileName(dbname), &db_lock);
  if (!status.Ok()) {
    return status;
  }

  std::vector<std::string> filenames;
  status = env->GetChildren(dbname, &filenames);
  if (!status.Ok()) {
    env->UnlockFile(db_lock);
    return status;
  }

  Status result;
  for (const std::string& filename : filenames) {
    if (filename == "." || filename == ".." || filename == "LOCK") {
      continue;
    }
    const Status remove_status = env->RemoveFile(dbname + "/" + filename);
    if (result.Ok() && !remove_status.Ok()) {
      result = remove_status;
    }
  }

  const Status unlock_status = env->UnlockFile(db_lock);
  if (result.Ok() && !unlock_status.Ok()) {
    result = unlock_status;
  }

  const Status remove_lock_status = env->RemoveFile(LockFileName(dbname));
  if (result.Ok() && !remove_lock_status.Ok()) {
    result = remove_lock_status;
  }
  const Status remove_dir_status = env->RemoveDir(dbname);
  if (result.Ok() && !remove_dir_status.Ok()) {
    result = remove_dir_status;
  }
  return result;
}

}  // namespace db
