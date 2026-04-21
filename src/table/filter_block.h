#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "filter_policy.h"
#include "slice.h"
namespace db {

class FilterPolicy;

// Builder of Filter Block
class FilterBlockBuilder {
public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;
  std::vector<size_t> start_;
  std::string result_;
  std::vector<Slice> tmp_keys_;
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
public:
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

private:
  const FilterPolicy* policy_;
  const char* data_;
  const char* offset_;
  size_t num_;
  size_t base_lg_;
};

}  // namespace db