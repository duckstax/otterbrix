#pragma once

#include "platform_compat.hpp"

#if __has_include("Error.hh")
#include "Error.hh"
#else
#include "exception.hpp"
#endif

#include <cstddef>
#include <cstdint>
#include <cinttypes>
#include <typeinfo>
#include <limits>

namespace document {

bool parse_integer(const char *str NONNULL, int64_t &result, bool allow_trailing = false);
bool parse_integer(const char *str NONNULL, uint64_t &result, bool allow_trailing = false);
static inline bool parse_unsigned_integer(const char *str NONNULL, uint64_t &r, bool t = false) { return parse_integer(str, r, t); }

double parse_double(const char *str NONNULL) noexcept;

size_t write_float(float f, char *dst, size_t capacity);
size_t write_float(double d, char *dst, size_t capacity);
static inline size_t write_double(double n, char *dst, size_t c)  { return write_float(n, dst, c); }

}
