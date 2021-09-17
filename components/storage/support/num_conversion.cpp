#include "num_conversion.hpp"
#include <ctype.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <iomanip>
#if !defined(_MSC_VER) && !defined(__GLIBC__)
#include <xlocale.h>
#endif


template <class Tfloat, class Tuint>
uint64_t convert_to_uint(Tfloat value) {
    union { Tfloat value; Tuint u; } convert;
    convert.value = value;
    return convert.u;
}

size_t copy_str_if_fit(char *dst, size_t len, const char *src) {
    const size_t src_len = strlen(src);
    if (len < src_len) {
        return 0;
    }
    strcpy(dst, src);
    return src_len;
}

template <class Tfloat>
size_t count_digit_before_dot(Tfloat value) {
    size_t count = 1;
    auto v = value / 10;
    while (v > 1) {
        ++count;
        v /= 10;
    }
    return count;
}

template <class Tfloat, class Tuint, int significand_count, int digits_count>
size_t write_float_to_str(Tfloat value, char *dst, size_t capacity) {
    if (!isfinite(value)) {
        if (isinf(value)) {
            if (signbit(value)) {
                return copy_str_if_fit(dst, capacity, "-inf");
            } else {
                return copy_str_if_fit(dst, capacity, "inf");
            }
        } else {
            auto raw = convert_to_uint<Tfloat, Tuint>(value);
            const char *sign = signbit(value) ? "-" : "";
            const char *signaling = ((raw >> (significand_count - 1)) & 1) ? "" : "s";
            auto payload = raw & ((Tuint(1) << (significand_count - 2)) - 1);
            char buff[32];
            if (payload != 0) {
                snprintf(buff, sizeof(buff), "%s%snan(0x%" PRIx64 ")", sign, signaling, payload);
            } else {
                snprintf(buff, sizeof(buff), "%s%snan", sign, signaling);
            }
            return copy_str_if_fit(dst, capacity, buff);
        }
    }

    if (value == 0.0) {
        if (signbit(value)) {
            return copy_str_if_fit(dst, capacity, "-0.0");
        } else {
            return copy_str_if_fit(dst, capacity, "0.0");
        }
    }
    std::stringstream stream;
    stream << std::setprecision(digits_count - count_digit_before_dot(value) + 1) << value;
    auto str = stream.str();
    copy_str_if_fit(dst, str.size(), str.c_str());
    return copy_str_if_fit(dst, str.size(), str.c_str());
}


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

template <class T>
size_t get_precision(T value, size_t capacity) {
    size_t count_dec = 0;
    auto v = value;
    while (v > 1) {
        ++count_dec;
        v /= 10;
    }
    return capacity - 2 - (count_dec > 0 ? count_dec : 1);
}

size_t write_float(float f, char *dst, size_t capacity) {
    return write_float_to_str<float, uint32_t, 23, 7>(f, dst, capacity);
}

size_t write_float(double d, char *dst, size_t capacity) {
    return write_float_to_str<double, uint64_t, 52, 16>(d, dst, capacity);
}

}
