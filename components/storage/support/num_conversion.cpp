#include "num_conversion.hpp"
#include "swift_dtoa.hpp"
#include <ctype.h>
#include <locale.h>
#include <stdlib.h>
#if !defined(_MSC_VER) && !defined(__GLIBC__)
#include <xlocale.h>
#endif

namespace storage {

static bool _parse_uint(const char *str NONNULL, uint64_t &result, bool allow_trailing) {
    uint64_t n = 0;
    if (!isdigit(*str))
        return false;
    while (isdigit(*str)) {
        int digit = (*str++ - '0');
        if (_usually_false(n > UINT64_MAX / 10))
            return false;
        n *= 10;
        if (_usually_false(n > UINT64_MAX - uint64_t(digit)))
            return false;
        n += uint64_t(digit);
    }
    if (!allow_trailing) {
        while (isspace(*str))
            ++str;
        if (_usually_false(*str != '\0'))
            return false;
    }
    result = n;
    return true;
}

bool parse_integer(const char *str NONNULL, uint64_t &result, bool allow_trailing) {
    while (isspace(*str))
        ++str;
    if (*str == '+')
        ++str;
    return _parse_uint(str, result, allow_trailing);
}

bool parse_integer(const char *str NONNULL, int64_t &result, bool allow_trailing) {
    while (isspace(*str))
        ++str;
    bool negative = (*str == '-');
    if (negative || *str == '+')
        ++str;
    uint64_t uresult;
    if (!_parse_uint(str, uresult, allow_trailing))
        return false;
    if (negative) {
        if (_usually_true(uresult <= uint64_t(INT64_MAX))) {
            result = -int64_t(uresult);
        } else if (uresult == uint64_t(INT64_MAX) + 1) {
            result = INT64_MIN;
        } else {
            return false;
        }
    } else {
        if (_usually_false(uresult > uint64_t(INT64_MAX)))
            return false;
        result = int64_t(uresult);
    }
    return true;
}

double parse_double(const char *str) noexcept {
#if defined(_MSC_VER)
    static _locale_t locale = _create_locale(LC_ALL, "C");
    return _strtod_l(str, nullptr, locale);
#else
    static locale_t locale = newlocale(LC_ALL_MASK, "C", nullptr);
    return strtod_l(str, nullptr, locale);
#endif
}

size_t write_float(float n, char *dst, size_t capacity) {
    return swift_format_float(n, dst, capacity);
}

size_t write_float(double n, char *dst, size_t capacity) {
    return swift_format_double(n, dst, capacity);
}

}
