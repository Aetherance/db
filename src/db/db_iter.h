#pragma once

#include <cstdint>

#include "db/dbformat.h"
#include "iterator.h"

namespace db {

Iterator* NewDBIterator(const Comparator* user_key_comparator, Iterator* internal_iter,
                        SequenceNumber sequence);

}  // namespace db
