#pragma once

#include <cstdint>
#include <string>

#include "slice.h"

namespace db {

void AppendNumberTo(std::string* dst, uint64_t number);
void AppendEscapedStringTo(std::string* dst, const Slice& value);

std::string NumberToString(uint64_t number);
std::string EscapeString(const Slice& value);

bool ConsumeDecimalNumber(Slice* input, uint64_t* value);

}  // namespace db
