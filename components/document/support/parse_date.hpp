#pragma once

#include "slice.hpp"
#include <chrono>
#include <ctime>

namespace document {

static constexpr int64_t invalid_date = INT64_MIN;

int64_t parse_iso8601_date(const char* date_str);
int64_t parse_iso8601_date(slice_t date_str);

static constexpr size_t formatted_iso8601_date_max_size = 40;

slice_t format_iso8601_date(char buf[], int64_t timestamp, bool utc);
struct tm from_timestamp(std::chrono::seconds timestamp);
std::chrono::seconds get_local_timezone_offset(struct tm* time, bool utc);

}
