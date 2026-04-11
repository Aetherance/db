#include <cstdint>

#include "comparator.h"
#include "options.h"
#include "status.h"
#include "table/block_builder.h"
namespace db {

class BlockBuilder;
class BlockHandle;
class WritableFile;

class TableBuilder {
public:
  TableBuilder(const Options& options, WritableFile* file);

  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  // Finish() or Abandon() must called before this
  ~TableBuilder();

  // Change options. But not all options could be change
  Status ChangeOptions(const Options& options);

  // add an entry into table
  // REQUIRES: entry should bigger than all entries added before and
  // did not call Finish() or Abandon()
  void Add(const Slice& key, const Slice& value);

  // flush any buffered key/val pairs to file
  // Each flush produces one block that is written into the SST file
  void Flush();

  // return non-ok iff some error has been detected
  Status status() const;

  // Finish will write Filter, Metaindex, Index and Footer Block into .sst
  // If Finish() called, Abandon should not be called. Once Finish() called
  // the file, the file will be unable to use.
  Status Finish();

  // Only sign the TableBuilder as "closed"
  // Abandon all data which TableBuilder has built;
  void Abandon();

  // returns the num of written entries
  uint64_t NumEntries() const;

  // returns the size of written data
  uint64_t FileSize() const;

private:
  bool ok() const {
    return status().Ok();
  }

  // Eventually call WriteRawBlock.
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);

  // no entry compression
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;
};
}  // namespace db