#pragma once

#include <algorithm>
#include <atomic>
#include <components/document/core/base.hpp>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

#ifndef assert
#include <assert.h>
#endif
#ifndef assert_precondition
#define assert_precondition(e) assert(e)
#endif

namespace document {

    struct slice_t;
    struct alloc_slice_t;
    struct null_slice_t;

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


    struct pure_slice_t {
    private:
        inline constexpr size_t _strlen(const char *str) noexcept {
            if (!str)
                return 0;
            auto c = str;
            while (*c) ++c;
            return size_t(c - str);
        }

    public:
        const void* const buf;
        size_t const size;

        constexpr pure_slice_t(std::string_view str) noexcept
            : pure_slice_t(str.data(), str.length()) {}

        constexpr pure_slice_t(const pure_slice_t &other) noexcept
            : buf(other.buf)
            , size(other.size) {}

        constexpr pure_slice_t(std::nullptr_t) noexcept
            : pure_slice_t() {}

        constexpr pure_slice_t(const char* str) noexcept
            : buf(str)
            , size(_strlen(str)) {}

        pure_slice_t(const std::string& str) noexcept;

        bool empty() const noexcept PURE;
        explicit operator bool() const noexcept PURE;

        const uint8_t* begin() const noexcept PURE;
        const uint8_t* end() const noexcept PURE;

        bool is_valid_address(const void* addr) const noexcept PURE;
        bool is_contains_address(const void* addr) const noexcept PURE;
        bool is_contains_address_range(pure_slice_t s) const noexcept PURE;

        const void* offset(size_t o) const noexcept PURE;

        const uint8_t& operator[](size_t i) const noexcept PURE;
        slice_t operator()(size_t off, size_t sz) const noexcept PURE;

        slice_t find(pure_slice_t target) const noexcept PURE;

        int compare(const pure_slice_t &s) const noexcept PURE;

        bool operator==(const pure_slice_t& s) const noexcept PURE;
        bool operator!=(const pure_slice_t& s) const noexcept PURE;
        bool operator<(const pure_slice_t &s) const noexcept PURE;
        bool operator>(const pure_slice_t &s) const noexcept PURE;
        bool operator<=(const pure_slice_t &s) const noexcept PURE;
        bool operator>=(const pure_slice_t &s) const noexcept PURE;

        uint32_t hash() const noexcept PURE;

        void copy_to(void* dst) const noexcept;

        explicit operator std::string() const;
        explicit operator std::string_view() const noexcept STEPOVER;
        std::string as_string() const;

    protected:
        constexpr pure_slice_t() noexcept
            : buf(nullptr)
            , size(0) {}

        constexpr pure_slice_t(const void* b, size_t s) noexcept
            : buf(b), size(s) {
            check_valid_slice();
        }

        void set_buf(const void* b NONNULL) noexcept;
        void set_size(size_t s) noexcept;
        void set(const void* b, size_t s) noexcept;

        pure_slice_t& operator=(const pure_slice_t& s) noexcept;

        [[noreturn]] static void fail_bad_alloc();

        constexpr void check_valid_slice() const {
            assert_precondition(buf != nullptr || size == 0);
            assert_precondition(size < (1ull << (8*sizeof(void*)-1)));
        }

        size_t check(size_t offset) const;
    };


    struct slice_t : public pure_slice_t {
        constexpr slice_t() noexcept STEPOVER = default;

        constexpr slice_t(std::nullptr_t) noexcept STEPOVER
            : pure_slice_t() {}

        slice_t(null_slice_t) noexcept;

        constexpr slice_t(const void* b, size_t s) noexcept STEPOVER
            : pure_slice_t(b, s) {}

        slice_t(const void* start NONNULL, const void* end NONNULL) noexcept STEPOVER;

        slice_t(const alloc_slice_t& s) noexcept;

        slice_t(const std::string& str) noexcept STEPOVER;

        constexpr slice_t(const char* str) noexcept STEPOVER
            : pure_slice_t(str) {}

        slice_t& operator=(alloc_slice_t&&) = delete;
        slice_t& operator=(const alloc_slice_t& s) noexcept;
        slice_t& operator=(std::nullptr_t) noexcept;
        slice_t& operator=(null_slice_t) noexcept;

        constexpr slice_t(std::string_view str) noexcept STEPOVER
            : pure_slice_t(str) {}

        void release();
    };


    struct null_slice_t : public slice_t {
        constexpr null_slice_t() noexcept = default;
    };

    constexpr null_slice_t null_slice;


    struct alloc_slice_t : public pure_slice_t {
        constexpr alloc_slice_t() noexcept STEPOVER = default;
        constexpr alloc_slice_t(std::nullptr_t) noexcept STEPOVER {}
        constexpr alloc_slice_t(null_slice_t) noexcept STEPOVER {}
        explicit alloc_slice_t(size_t sz) STEPOVER;
        alloc_slice_t(const void* b, size_t s);
        alloc_slice_t(const void* start NONNULL, const void* end NONNULL);
        explicit alloc_slice_t(const char* str);
        explicit alloc_slice_t(const std::string& str);
        explicit alloc_slice_t(pure_slice_t s) STEPOVER;
        alloc_slice_t(const alloc_slice_t& s) noexcept STEPOVER;
        alloc_slice_t(alloc_slice_t&& s) noexcept STEPOVER;

        ~alloc_slice_t() STEPOVER;

        alloc_slice_t& operator=(const alloc_slice_t&) noexcept STEPOVER;
        alloc_slice_t& operator=(alloc_slice_t&& s) noexcept;

        alloc_slice_t& operator=(pure_slice_t s);
        alloc_slice_t& operator=(std::nullptr_t) noexcept;
        alloc_slice_t& operator=(const char* str NONNULL);
        alloc_slice_t& operator=(const std::string& str);
        void reset() noexcept;
        void reset(size_t sz);
        void resize(size_t new_size);
        void append(pure_slice_t source);

        explicit alloc_slice_t(std::string_view str) STEPOVER;
        alloc_slice_t& operator=(std::string_view str);

        alloc_slice_t& retain() noexcept;
        void release() noexcept;
        static void retain(slice_t s) noexcept;
        static void release(slice_t s) noexcept;

    private:
        void assign_from(pure_slice_t s);
    };

} // namespace document

namespace std {

    template<>
    struct hash<document::slice_t> {
        size_t operator()(document::pure_slice_t const& s) const { return s.hash(); }
    };

    template<>
    struct hash<document::alloc_slice_t> {
        size_t operator()(document::pure_slice_t const& s) const { return s.hash(); }
    };

} // namespace std
