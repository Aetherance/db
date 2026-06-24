#pragma once

#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "options.h"

namespace db {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
             const Slice& key);

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files, const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
public:
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val);

  void Ref();
  void Unref();

  void GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end,
                            std::vector<FileMetaData*>* inputs);

  bool OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key);

  std::string DebugString() const;

private:
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;
  Version* next_;
  Version* prev_;
  int refs_;

  std::vector<FileMetaData*> files_[config::kNumLevels];

  double compaction_score_;
  int compaction_level_;
};

class VersionSet {
public:
  VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache,
             const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  Status LogAndApply(VersionEdit* edit);

  Status Recover();

  Version* current() const {
    return current_;
  }

  uint64_t NewFileNumber() {
    return next_file_number_++;
  }

  int NumLevelFiles(int level) const;

  uint64_t LastSequence() const {
    return last_sequence_;
  }

  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  void MarkFileNumberUsed(uint64_t number);

  uint64_t LogNumber() const {
    return log_number_;
  }

  uint64_t PrevLogNumber() const {
    return prev_log_number_;
  }

  Compaction* PickCompaction();

  Compaction* CompactRange(int level, const InternalKey* begin, const InternalKey* end);

  Iterator* MakeInputIterator(Compaction* c);

  bool NeedsCompaction() const {
    return current_->compaction_score_ >= 1;
  }

  void AddLiveFiles(std::set<uint64_t>* live);

  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

private:
  class Builder;

  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;

  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;
  Version* current_;
};

class Compaction {
public:
  ~Compaction();

  int level() const {
    return level_;
  }

  VersionEdit* edit() {
    return &edit_;
  }

  int num_input_files(int which) const {
    return inputs_[which].size();
  }

  FileMetaData* input(int which, int i) const {
    return inputs_[which][i];
  }

  bool IsTrivialMove() const;

  void AddInputDeletions(VersionEdit* edit);

private:
  friend class VersionSet;

  explicit Compaction(int level);

  int level_;
  Version* input_version_;
  VersionEdit edit_;

  std::vector<FileMetaData*> inputs_[2];
};

}  // namespace db
