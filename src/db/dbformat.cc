#include "db/dbformat.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>

#include "slice.h"
#include "util/coding.h"
#include "util/logging.h"

namespace db {
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | static_cast<uint64_t>(t);
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.Data(), key.user_key.Size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key) << "' @" << sequence << " : " << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "db.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // compare user_key first
  // if user_key is same, compare tag (seq + type)
  // if seq is same, compare type

  // Compare user_key in ascending order first, then compare sequence in descending
  // order. if the sequences are equal, compare type in descending order.
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.Data() + akey.Size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.Data() + bkey.Size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

// make internalKey shorter and bigger than the old key, but not bigger than limit
void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const {
  Slice user_start = ExtractUserKey((*start));
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.Data(), user_start.Size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.Size() && user_comparator_->Compare(user_start, tmp) < 0) {
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  // TODO: implement me
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const {
  Slice* mkeys = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkeys[i] = ExtractUserKey(keys[i]);
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.Size();
  // 13 = varint32 + tag
  // varint32 is 5 and tag is 8
  size_t needed = usize + 13;
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.Data(), usize);
  dst += usize;

  // kValueTypeForSeek is the key's type which will be sorted to front (bigger)
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace db