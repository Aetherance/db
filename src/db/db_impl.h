#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "db.h"
#include "db/dbformat.h"
#include "db/snapshot.h"
#include "options.h"
#include "status.h"

namespace db {

namespace log {
class Writer;
}  // namespace log

class Compaction;
class FileLock;
class MemTable;
class TableCache;
class VersionEdit;
class VersionSet;
class WritableFile;
class WriteBatch;

Options PrepareOptions(const InternalKeyComparator* comparator,
                       const InternalFilterPolicy* filter_policy, const Options& source);

// A deliberately small DB implementation with a single immutable MemTable.
//
// DBImpl only wires the storage components together:
//   WriteBatch -> WAL -> MemTable -> immutable MemTable -> L0 table -> VersionSet.
// The public API remains single-threaded, while immutable MemTables are flushed
// through Env::Schedule. Writer grouping is not implemented.
class DBImpl : public DB {
public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
  Iterator* NewIterator(const ReadOptions& options) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

private:
  friend class DB;

  Status NewDB();
  Status Recover(VersionEdit* edit, std::vector<uint64_t>* recovered_logs);
  Status RecoverLogFile(uint64_t log_number, MemTable* mem, SequenceNumber* max_sequence,
                        bool* has_entries);
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit);
  Status MakeMemTableImmutable();
  Status FlushMemTable();
  static void BackgroundWork(void* db);
  void BackgroundFlush();
  Status CompactIfNeeded();
  Status CompactLevel(int level, const Slice* begin, const Slice* end);
  Status RunCompaction(Compaction* compaction);
  void RemoveObsoleteTables();
  Iterator* NewInternalIterator(const ReadOptions& options, SequenceNumber* latest_sequence);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;
  const bool owns_cache_;
  const std::string dbname_;
  TableCache* const table_cache_;
  mutable std::mutex mutex_;
  std::condition_variable background_work_finished_;
  FileLock* db_lock_;
  MemTable* mem_;
  MemTable* imm_;
  WritableFile* logfile_;
  uint64_t logfile_number_;
  uint64_t imm_logfile_number_;
  log::Writer* log_;
  bool background_flush_scheduled_;
  Status background_error_;
  SnapshotList snapshots_;
  VersionSet* const versions_;
};

}  // namespace db
