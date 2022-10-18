#pragma once

#include <components/document/core/slice_core.hpp>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

#ifndef assert
#   include <assert.h>
#endif
#ifndef assert_precondition
#   define assert_precondition(e) assert(e)
#endif

namespace document {

struct slice_t;
struct alloc_slice_t;
struct null_slice_t;

template <typename T>
CONST constexpr inline const T* offsetby(const T *t, std::ptrdiff_t offset) noexcept {
    return (const T*)((uint8_t*)t + offset);
}

template <typename T>
CONST constexpr inline T* offsetby(T *t, std::ptrdiff_t offset) noexcept {
    return (T*)((uint8_t*)t + offset);
}

constexpr inline std::ptrdiff_t diff_pointer(const void *bigger, const void *smaller) noexcept {
    return (uint8_t*)bigger - (uint8_t*)smaller;
}


struct pure_slice_t
{
    const void* const buf;
    size_t const size;

    bool empty() const noexcept PURE                          { return size == 0; }
    explicit operator bool() const noexcept PURE              { return buf != nullptr; }

    constexpr const uint8_t* begin() const noexcept PURE      { return (uint8_t*)buf; }
    constexpr const uint8_t* end() const noexcept PURE        { return begin() + size; }

    inline bool is_valid_address(const void *addr) const noexcept PURE;
    inline bool is_contains_address(const void *addr) const noexcept PURE;
    inline bool is_contains_address_range(pure_slice_t s) const noexcept PURE;

    const void* offset(size_t o) const noexcept PURE;

    inline const uint8_t& operator[](size_t i) const noexcept PURE;
    inline slice_t operator()(size_t off, size_t sz) const noexcept PURE;

    inline slice_t find(pure_slice_t target) const noexcept PURE;

    inline int compare(pure_slice_t s) const noexcept PURE    { return compare_slice_c(*this, s); }

    bool operator==(const pure_slice_t &s) const noexcept PURE  { return is_equal_slice_c(*this, s); }
    bool operator!=(const pure_slice_t &s) const noexcept PURE  { return !(*this == s); }
    bool operator<(pure_slice_t s) const noexcept PURE          { return compare(s) < 0; }
    bool operator>(pure_slice_t s) const noexcept PURE          { return compare(s) > 0; }
    bool operator<=(pure_slice_t s) const noexcept PURE         { return compare(s) <= 0; }
    bool operator>=(pure_slice_t s) const noexcept PURE         { return compare(s) >= 0; }

    uint32_t hash() const noexcept PURE         { return hash_slice_c(*this); }

    void copy_to(void *dst) const noexcept      { if (size > 0) ::memcpy(dst, buf, size); }

    explicit operator std::string() const       { return std::string((const char*)buf, size); }
    std::string as_string() const               { return (std::string)*this; }

    operator slice_t_c () const noexcept        { return {buf, size}; }

    constexpr pure_slice_t(std::string_view str) noexcept : pure_slice_t(str.data(), str.length()) {}
    operator std::string_view() const noexcept STEPOVER { return std::string_view((const char*)buf, size); }
    constexpr pure_slice_t(std::nullptr_t) noexcept   : pure_slice_t() {}
    constexpr pure_slice_t(const char* str) noexcept  : buf(str), size(_strlen(str)) {}
    pure_slice_t(const std::string& str) noexcept     : buf(&str[0]), size(str.size()) {}

protected:
    constexpr pure_slice_t() noexcept                 : buf(nullptr), size(0) {}
    inline constexpr pure_slice_t(const void* b, size_t s) noexcept;

    inline void set_buf(const void *b NONNULL) noexcept;
    inline void set_size(size_t s) noexcept;
    inline void set(const void *b, size_t s) noexcept;

    pure_slice_t& operator=(const pure_slice_t &s) noexcept  { set(s.buf, s.size); return *this; }
    static inline constexpr size_t _strlen(const char *str) noexcept PURE;
    [[noreturn]] static void fail_bad_alloc();
    inline constexpr void check_valid_slice() const;
    inline size_t check(size_t offset) const;
};


struct slice_t : public pure_slice_t
{
    constexpr slice_t() noexcept STEPOVER                             : pure_slice_t() {}
    constexpr slice_t(std::nullptr_t) noexcept STEPOVER               : pure_slice_t() {}
    inline constexpr slice_t(null_slice_t) noexcept STEPOVER;
    constexpr slice_t(const void* b, size_t s) noexcept STEPOVER : pure_slice_t(b, s) {}
    inline constexpr slice_t(const void* start NONNULL, const void* end NONNULL) noexcept STEPOVER;
    inline constexpr slice_t(const alloc_slice_t& s) noexcept STEPOVER;

    slice_t(const std::string& str) noexcept STEPOVER                 : pure_slice_t(str) {}
    constexpr slice_t(const char* str) noexcept STEPOVER              : pure_slice_t(str) {}

    slice_t& operator= (alloc_slice_t&&) = delete;
    slice_t& operator= (const alloc_slice_t &s) noexcept  { return *this = slice_t(s); }
    slice_t& operator= (std::nullptr_t) noexcept          { set(nullptr, 0); return *this; }
    inline slice_t& operator= (null_slice_t) noexcept;

    constexpr slice_t(const slice_t_c &s) noexcept STEPOVER      : pure_slice_t(s.buf,s.size) { }
    inline explicit operator slice_result_t_c () const noexcept;
    explicit slice_t(const slice_result_t_c &sr) STEPOVER        : pure_slice_t(sr.buf, sr.size) { }
    slice_t& operator= (heap_slice_t_c s) noexcept        { set(s.buf, s.size); return *this; }

    constexpr slice_t(std::string_view str) noexcept STEPOVER    : pure_slice_t(str) {}
    void release();
};


struct null_slice_t : public slice_t {
    constexpr null_slice_t() noexcept : slice_t() {}
};
constexpr null_slice_t null_slice;


inline constexpr slice_t operator "" _sl (const char *str NONNULL, size_t length) noexcept {
    return slice_t(str, length);
}


struct alloc_slice_t : public pure_slice_t
{
    constexpr alloc_slice_t() noexcept STEPOVER                {}
    constexpr alloc_slice_t(std::nullptr_t) noexcept STEPOVER  {}
    constexpr alloc_slice_t(null_slice_t) noexcept STEPOVER    {}

    inline explicit alloc_slice_t(size_t sz) STEPOVER;

    alloc_slice_t(const void* b, size_t s)                       : alloc_slice_t(slice_t(b, s)) {}
    alloc_slice_t(const void* start NONNULL, const void* end NONNULL) : alloc_slice_t(slice_t(start, end)) {}
    explicit alloc_slice_t(const char *str)                           : alloc_slice_t(slice_t(str)) {}
    explicit alloc_slice_t(const std::string &str)                    : alloc_slice_t(slice_t(str)) {}

    inline explicit alloc_slice_t(pure_slice_t s) STEPOVER;
    explicit alloc_slice_t(slice_t_c s)                               : alloc_slice_t(s.buf, s.size) {}
    alloc_slice_t(const alloc_slice_t &s) noexcept STEPOVER           : pure_slice_t(s) { retain(); }
    alloc_slice_t(alloc_slice_t&& s) noexcept STEPOVER                : pure_slice_t(s) { s.set(nullptr, 0); }

    ~alloc_slice_t() STEPOVER  { release_buf_c(buf); }

    inline alloc_slice_t& operator=(const alloc_slice_t&) noexcept STEPOVER;
    inline alloc_slice_t& operator=(alloc_slice_t&& s) noexcept;

    alloc_slice_t& operator= (pure_slice_t s)           { return *this = alloc_slice_t(s); }
    alloc_slice_t& operator= (slice_t_c s)              { return operator=(slice_t(s.buf, s.size)); }
    alloc_slice_t& operator= (std::nullptr_t) noexcept  { reset(); return *this; }
    alloc_slice_t& operator= (const char *str NONNULL)  { *this = (slice_t)str; return *this; }
    alloc_slice_t& operator= (const std::string &str)   { *this = (slice_t)str; return *this; }
    void reset() noexcept                               { release(); assign_from(null_slice); }
    void reset(size_t sz)                          { *this = alloc_slice_t(sz); }
    inline void resize(size_t new_size);
    inline void append(pure_slice_t source);

    explicit alloc_slice_t(const slice_result_t_c &s) noexcept STEPOVER : pure_slice_t(s.buf, s.size) { retain(); }
    alloc_slice_t(slice_result_t_c &&sr) noexcept STEPOVER              : pure_slice_t(sr.buf, sr.size) {}
    explicit operator slice_result_t_c () & noexcept                    { retain(); return {(void*)buf, size}; }
    explicit operator slice_result_t_c () && noexcept                   { slice_result_t_c r {(void*)buf, size}; set(nullptr, 0); return r; }
    alloc_slice_t& operator= (slice_result_t_c &&sr) noexcept           { release(); set(sr.buf, sr.size); return *this; }
    alloc_slice_t(heap_slice_t_c s) noexcept STEPOVER                   : pure_slice_t(s.buf, s.size) { retain(); }
    alloc_slice_t& operator= (heap_slice_t_c) noexcept;
    operator heap_slice_t_c () const noexcept                           { return {buf, size}; }

    explicit alloc_slice_t(std::string_view str) STEPOVER               : alloc_slice_t(slice_t(str)) {}
    alloc_slice_t& operator=(std::string_view str)                      { *this = (slice_t)str; return *this; }

    alloc_slice_t& retain() noexcept         { retain_buf_c(buf); return *this; }
    inline void release() noexcept           { release_buf_c(buf); }
    static void retain(slice_t s) noexcept   { ((alloc_slice_t*)&s)->retain(); }
    static void release(slice_t s) noexcept  { ((alloc_slice_t*)&s)->release(); }

private:
    void assign_from(pure_slice_t s)         { set(s.buf, s.size); }
};


inline constexpr size_t pure_slice_t::_strlen(const char *str) noexcept {
    if (!str)
        return 0;
    auto c = str;
    while (*c) ++c;
    return c - str;
}

inline constexpr pure_slice_t::pure_slice_t(const void* b, size_t s) noexcept
    : buf(b), size(s) {
    check_valid_slice();
}

inline void pure_slice_t::set_buf(const void *b NONNULL) noexcept {
    const_cast<const void*&>(buf) = b;
    check_valid_slice();
}

inline void pure_slice_t::set_size(size_t s) noexcept {
    const_cast<size_t&>(size) = s;
    check_valid_slice();
}

inline void pure_slice_t::set(const void *b, size_t s) noexcept {
    const_cast<const void*&>(buf) = b;
    const_cast<size_t&>(size) = s;
    check_valid_slice();
}

inline bool pure_slice_t::is_valid_address(const void *addr) const noexcept {
    return size_t(diff_pointer(addr, buf)) <= size;
}

inline bool pure_slice_t::is_contains_address(const void *addr) const noexcept {
    return size_t(diff_pointer(addr, buf)) < size;
}

inline bool pure_slice_t::is_contains_address_range(pure_slice_t s) const noexcept {
    return s.buf >= buf && s.end() <= end();
}

inline constexpr void pure_slice_t::check_valid_slice() const {
    assert_precondition(buf != nullptr || size == 0);
    assert_precondition(size < (1ull << (8*sizeof(void*)-1)));
}

inline size_t pure_slice_t::check(size_t offset) const {
    assert_precondition(offset <= size);
    return offset;
}

inline const void* pure_slice_t::offset(size_t o) const noexcept {
    return (uint8_t*)buf + check(o);
}

inline const uint8_t& pure_slice_t::operator[](size_t off) const noexcept {
    assert_precondition(off < size);
    return ((const uint8_t*)buf)[off];
}

inline slice_t pure_slice_t::operator()(size_t off, size_t sz) const noexcept  {
    assert_precondition(off + sz <= size);
    return slice_t(offset(off), sz);
}

inline slice_t pure_slice_t::find(pure_slice_t target) const noexcept {
    char* src = (char *)buf;
    char* search = (char *)target.buf;
    char* found = std::search(src, src + size, search, search + target.size);
    if(found == src + size) {
        return null_slice;
    }
    return {found, target.size};
}

[[noreturn]] inline void pure_slice_t::fail_bad_alloc() {
#ifdef __cpp_exceptions
    throw std::bad_alloc();
#else
    ::fputs("*** FATAL ERROR: heap allocation failed (fleece/slice_t.cc) ***\n", stderr);
    std::terminate();
#endif
}


inline constexpr slice_t::slice_t(null_slice_t) noexcept
    : pure_slice_t()
{}

inline constexpr slice_t::slice_t(const alloc_slice_t &s) noexcept
    : pure_slice_t(s)
{}

inline constexpr slice_t::slice_t(const void* start NONNULL, const void* end NONNULL) noexcept
    : slice_t(start, diff_pointer(end, start))
{
    assert_precondition(end >= start);
}

inline slice_t& slice_t::operator= (null_slice_t) noexcept {
    set(nullptr, 0);
    return *this;
}

inline slice_t::operator slice_result_t_c () const noexcept {
    return slice_result_t_c(alloc_slice_t(*this));
}

inline void slice_t::release() {
    char *c = const_cast<char *>(static_cast<const char *>(buf));
    delete[] c;
}


inline alloc_slice_t::alloc_slice_t(size_t sz)
    : alloc_slice_t(create_slice_result_c(sz))
{
    if (_usually_false(!buf))
        fail_bad_alloc();
}

inline alloc_slice_t::alloc_slice_t(pure_slice_t s)
    : alloc_slice_t(copy_slice_c(s))
{
    if (_usually_false(!buf) && s.buf)
        fail_bad_alloc();
}

inline alloc_slice_t& alloc_slice_t::operator=(const alloc_slice_t& s) noexcept {
    if (_usually_true(s.buf != buf)) {
        release();
        assign_from(s);
        retain();
    }
    return *this;
}

inline alloc_slice_t &alloc_slice_t::operator=(alloc_slice_t &&s) noexcept {
    std::swap((slice_t&)*this, (slice_t&)s);
    return *this;
}

inline alloc_slice_t& alloc_slice_t::operator=(heap_slice_t_c s) noexcept {
    if (_usually_true(s.buf != buf)) {
        release();
        assign_from(slice_t(s));
        retain();
    }
    return *this;
}

inline void alloc_slice_t::resize(size_t new_size) {
    if (new_size == size) {
        return;
    } else if (buf == nullptr) {
        reset(new_size);
    } else {
        alloc_slice_t new_slice(new_size);
        ::memcpy((void*)new_slice.buf, buf, std::min(size, new_size));
        *this = std::move(new_slice);
    }
}

inline void alloc_slice_t::append(pure_slice_t source) {
    if (_usually_false(source.size == 0))
        return;
    size_t dst_off = size;
    size_t src_off = size_t(diff_pointer(source.buf, buf));

    resize(dst_off + source.size);

    const void *src;
    if (_usually_false(src_off < dst_off)) {
        src = offset(src_off);
    } else {
        src = source.buf;
    }

    ::memcpy((void*)offset(dst_off), src, source.size);
}

}


namespace std {

template<> struct hash<document::slice_t> {
    size_t operator() (document::pure_slice_t const& s) const { return s.hash(); }
};

template<> struct hash<document::alloc_slice_t> {
    size_t operator() (document::pure_slice_t const& s) const { return s.hash(); }
};

}
