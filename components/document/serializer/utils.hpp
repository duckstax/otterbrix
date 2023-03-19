#pragma once

#include <type_traits>

#define HEDLEY_UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define JSON_THROW(exception)                           \
    {std::clog << "Error in " << __FILE__ << ":" << __LINE__ \
               << " (function " << __FUNCTION__ << ") - "    \
               << (exception).what() << std::endl;           \
     std::abort();}

template<typename T, typename U, std::enable_if_t<!std::is_same<T, U>::value, int> = 0>
T conditional_static_cast(U value) {
    return static_cast<T>(value);
}

template<typename T, typename U, std::enable_if_t<std::is_same<T, U>::value, int> = 0>
T conditional_static_cast(U value) {
    return value;
}

static inline bool little_endianness(int num = 1) noexcept {
    return *reinterpret_cast<char*>(&num) == 1;
}