#pragma once

#include <cstdint>
#include <cstring>

#include <string>

#include "base.hpp"
#include "better_assert.hpp"

template<typename T>
CONST constexpr inline const T* offsetby(const T* t, std::ptrdiff_t offset) noexcept {
    return reinterpret_cast<const T*>(reinterpret_cast<const uint8_t*>(t) + offset);
}

template<typename T>
CONST constexpr inline T* offsetby(T* t, std::ptrdiff_t offset) noexcept {
    return reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(t) + offset);
}

inline std::ptrdiff_t diff_pointer(const void* bigger, const void* smaller) noexcept {
    return reinterpret_cast<const uint8_t*>(bigger) - reinterpret_cast<const uint8_t*>(smaller);
}

template <class T>
constexpr const uint8_t* to_const_uint8_t(const T* t) {
    return reinterpret_cast<const uint8_t*>(t);
}

inline void copy_to(const std::string& storage, void* dst) noexcept {
    if (!storage.empty())
        ::memcpy(dst, storage.data(), storage.size());
}

inline void copy_to(const std::string_view& storage, void* dst) noexcept {
    if (!storage.empty())
        ::memcpy(dst, storage.data(), storage.size());
}