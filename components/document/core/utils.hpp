#pragma once

#include <cstdint>
#include <cstring>

#include <string>

#include "base.hpp"
#include "better_assert.hpp"

template<typename T>
CONST constexpr inline const T* offsetby(const T* t, std::ptrdiff_t offset) noexcept {
    return reinterpret_cast<const T*>(reinterpret_cast<uint8_t*>(const_cast<T*>(t)) + offset);
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

class storage_view final {
public:
    template<class T>
    constexpr storage_view(T* ptr, std::size_t size)
        : ptr_(ptr)
        , size_(size) {}

    std::size_t size() const noexcept {
        return size_;
    }

    const void* ptr() const noexcept {
        return ptr_;
    }
    constexpr const uint8_t& operator[](size_t off) const noexcept {
        assert_precondition(off < size_);
        return to_const_uint8_t(ptr_)[off];
    }

    constexpr int compare(const storage_view &s) const noexcept {
        if (size_ == s.size_) {
            if (size_ == 0)
                return 0;
            return memcmp(ptr_, s.ptr_, size_);
        } else if (size_ < s.size_) {
            if (size_ == 0)
                return -1;
            int result = memcmp(ptr_, s.ptr_, size_);
            return result ? result : -1;
        } else {
            if (s.size_ == 0)
                return 1;
            int result = memcmp(ptr_, s.ptr_, s.size_);
            return result ? result : 1;
        }
    }

    constexpr bool operator==(const storage_view& s) const noexcept {
        return this->size_ == s.size_ && (this->size_ == 0 || memcmp(this->ptr_, s.ptr_, this->size_) == 0);
    }

    constexpr bool operator!=(const storage_view& s) const noexcept {
        return !(*this == s);
    }

    constexpr bool operator<(const storage_view& s) const noexcept {
        return compare(s) < 0;
    }

private:
    const void* ptr_;
    const std::size_t size_;
};

void copy_to(storage_view& storage, void* dst) noexcept {
    if (storage.size() > 0)
        ::memcpy(dst, storage.ptr(), storage.size());
}

void copy_to(const std::string& storage, void* dst) noexcept {
    if (!storage.empty())
        ::memcpy(dst, storage.data(), storage.size());
}

void copy_to(const std::string_view & storage, void* dst) noexcept {
    if (!storage.empty())
        ::memcpy(dst, storage.data(), storage.size());
}