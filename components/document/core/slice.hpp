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
    size_t offsetOf(const void* ptr NONNULL) const noexcept PURE;

    inline const uint8_t& operator[](size_t i) const noexcept PURE;
    inline slice_t operator()(size_t off, size_t sz) const noexcept PURE;

    inline slice_t up_to(const void* pos NONNULL) const noexcept PURE;
    inline slice_t from(const void* pos NONNULL) const noexcept PURE;
    inline slice_t up_to(size_t offset) const noexcept PURE;
    inline slice_t from(size_t offset) const noexcept PURE;

    inline bool contains_bytes(pure_slice_t bytes) const noexcept PURE;
    inline slice_t find(pure_slice_t target) const noexcept PURE;
    inline const uint8_t* find_byte(uint8_t b) const PURE;
    inline const uint8_t* find_byte_or_end(uint8_t byte) const noexcept PURE;
    inline const uint8_t* find_any_byte_of(pure_slice_t target_bytes) const noexcept PURE;
    inline const uint8_t* find_byte_not_in(pure_slice_t target_bytes) const noexcept PURE;

    inline int compare(pure_slice_t s) const noexcept PURE    { return compare_slice_c(*this, s); }
    inline int case_equivalent_compare(pure_slice_t s) const noexcept PURE;
    inline bool case_equivalent(pure_slice_t s) const noexcept PURE;

    bool operator==(const pure_slice_t &s) const noexcept PURE  { return is_equal_slice_c(*this, s); }
    bool operator!=(const pure_slice_t &s) const noexcept PURE  { return !(*this == s); }
    bool operator<(pure_slice_t s) const noexcept PURE          { return compare(s) < 0; }
    bool operator>(pure_slice_t s) const noexcept PURE          { return compare(s) > 0; }
    bool operator<=(pure_slice_t s) const noexcept PURE         { return compare(s) <= 0; }
    bool operator>=(pure_slice_t s) const noexcept PURE         { return compare(s) >= 0; }

    inline bool has_prefix(pure_slice_t s) const noexcept PURE;
    inline bool has_suffix(pure_slice_t s) const noexcept PURE;
    bool has_prefix(uint8_t b) const noexcept PURE   { return size > 0 && (*this)[0] == b; }
    bool has_suffix(uint8_t b) const noexcept PURE   { return size > 0 && (*this)[size-1] == b; }

    uint32_t hash() const noexcept PURE         { return hash_slice_c(*this); }

    void copy_to(void *dst) const noexcept      { if (size > 0) ::memcpy(dst, buf, size); }
    inline slice_t copy() const;

    explicit operator std::string() const       { return std::string((const char*)buf, size); }
    std::string as_string() const               { return (std::string)*this; }
    std::string hex_string() const;
    inline bool to_cstring(char *buf, size_t size) const noexcept;

    operator slice_t_c () const noexcept        { return {buf, size}; }

    constexpr pure_slice_t(std::string_view str) noexcept : pure_slice_t(str.data(), str.length()) {}
    operator std::string_view() const noexcept STEPOVER { return std::string_view((const char*)buf, size); }
    constexpr pure_slice_t(std::nullptr_t) noexcept   : pure_slice_t() {}
    constexpr pure_slice_t(const char* str) noexcept  : buf(str), size(_strlen(str)) {}
    pure_slice_t(const std::string& str) noexcept     : buf(&str[0]), size(str.size()) {}

    RETURNS_NONNULL inline static void* new_bytes(size_t size);
    template <typename T> RETURNS_NONNULL static inline T* realloc_bytes(T* bytes, size_t new_size);

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
    inline const void* check(const void *addr) const;
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

    void set_size(size_t s) noexcept                 { pure_slice_t::set_size(s); }
    inline void shorten(size_t s);

    void set_end(const void* e NONNULL) noexcept          { set_size(diff_pointer(e, buf)); }
    inline void set_start(const void* s NONNULL) noexcept;
    void move_start(std::ptrdiff_t delta) noexcept        { set(offsetby(buf, delta), size - delta); }
    bool checked_move_start(size_t delta) noexcept   { if (size < delta) return false;
        else { move_start(delta); return true; } }
    constexpr slice_t(const slice_t_c &s) noexcept STEPOVER      : pure_slice_t(s.buf,s.size) { }
    inline explicit operator slice_result_t_c () const noexcept;
    explicit slice_t(const slice_result_t_c &sr) STEPOVER        : pure_slice_t(sr.buf, sr.size) { }
    slice_t& operator= (heap_slice_t_c s) noexcept        { set(s.buf, s.size); return *this; }

    constexpr slice_t(std::string_view str) noexcept STEPOVER    : pure_slice_t(str) {}
    void release();
};


struct mutable_slice_t
{
    void* buf;
    size_t size;

    constexpr mutable_slice_t() noexcept                        : buf(nullptr), size(0) {}
    explicit constexpr mutable_slice_t(pure_slice_t s) noexcept : buf((void*)s.buf), size(s.size) {}
    constexpr mutable_slice_t(void* b, size_t s) noexcept  : buf(b), size(s) {}
    constexpr mutable_slice_t(void* b NONNULL, void* e NONNULL) noexcept
        : buf(b)
        , size(diff_pointer(e, b)) {}

    operator slice_t() const noexcept { return slice_t(buf, size); }
    void wipe() noexcept              { wipe_memory(buf, size); }
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

    inline static alloc_slice_t null_padded_string(pure_slice_t s);

    alloc_slice_t& operator= (pure_slice_t s)           { return *this = alloc_slice_t(s); }
    alloc_slice_t& operator= (slice_t_c s)              { return operator=(slice_t(s.buf, s.size)); }
    alloc_slice_t& operator= (std::nullptr_t) noexcept  { reset(); return *this; }
    alloc_slice_t& operator= (const char *str NONNULL)  { *this = (slice_t)str; return *this; }
    alloc_slice_t& operator= (const std::string &str)   { *this = (slice_t)str; return *this; }
    void reset() noexcept                               { release(); assign_from(null_slice); }
    void reset(size_t sz)                          { *this = alloc_slice_t(sz); }
    inline void resize(size_t new_size);
    inline void append(pure_slice_t source);
    inline void shorten(size_t sz);

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


struct nonnull_slice : public slice_t
{
    constexpr nonnull_slice(const void* b NONNULL, size_t s) : slice_t(b, s) {}
    constexpr nonnull_slice(slice_t s)                            : nonnull_slice(s.buf, s.size) {}
    constexpr nonnull_slice(slice_t_c s)                          : nonnull_slice(s.buf, s.size) {}
    constexpr nonnull_slice(const char *str NONNULL)              : slice_t(str) {}
    nonnull_slice(alloc_slice_t s)                                : nonnull_slice(s.buf, s.size) {}
    nonnull_slice(const std::string &str)                         : nonnull_slice(str.data(), str.size()) {}
    nonnull_slice(std::string_view str)                           : nonnull_slice(str.data(), str.size()) {}
    nonnull_slice(std::nullptr_t) = delete;
    nonnull_slice(null_slice_t)   = delete;
};


struct slice_hash_t
{
    size_t operator() (pure_slice_t const& s) const { return s.hash(); }
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

inline const void* pure_slice_t::check(const void *addr) const {
    assert_precondition(is_valid_address(addr));
    return addr;
}

inline size_t pure_slice_t::check(size_t offset) const {
    assert_precondition(offset <= size);
    return offset;
}

inline const void* pure_slice_t::offset(size_t o) const noexcept {
    return (uint8_t*)buf + check(o);
}

inline size_t pure_slice_t::offsetOf(const void* ptr NONNULL) const noexcept {
    return diff_pointer(check(ptr), buf);
}

inline slice_t pure_slice_t::up_to(const void* pos) const noexcept {
    return slice_t(buf, check(pos));
}

inline slice_t pure_slice_t::from(const void* pos) const noexcept {
    return slice_t(check(pos), end());
}

inline slice_t pure_slice_t::up_to(size_t off) const noexcept {
    return slice_t(buf, check(off));
}

inline slice_t pure_slice_t::from(size_t off) const noexcept {
    return slice_t(offset(check(off)), end());
}

inline const uint8_t& pure_slice_t::operator[](size_t off) const noexcept {
    assert_precondition(off < size);
    return ((const uint8_t*)buf)[off];
}

inline slice_t pure_slice_t::operator()(size_t off, size_t sz) const noexcept  {
    assert_precondition(off + sz <= size);
    return slice_t(offset(off), sz);
}

inline bool pure_slice_t::to_cstring(char *str, size_t size) const noexcept {
    size_t n = std::min(size, size-1);
    if (n > 0)
        ::memcpy(str, buf, n);
    str[n] = 0;
    return n == size;
}

inline std::string pure_slice_t::hex_string() const {
    static const char digits[17] = "0123456789abcdef";
    std::string result;
    result.reserve(2 * size);
    for (size_t i = 0; i < size; i++) {
        uint8_t byte = (*this)[(unsigned)i];
        result += digits[byte >> 4];
        result += digits[byte & 0xf];
    }
    return result;
}

inline int pure_slice_t::case_equivalent_compare(pure_slice_t s) const noexcept {
    size_t min_size = std::min(size, s.size);
    for (size_t i = 0; i < min_size; i++) {
        if ((*this)[i] != s[i]) {
            int cmp = ::tolower((*this)[i]) - ::tolower(s[i]);
            if (cmp != 0)
                return cmp;
        }
    }
    return (int)size - (int)s.size;
}

inline bool pure_slice_t::case_equivalent(pure_slice_t s) const noexcept {
    if (size != s.size)
        return false;
    for (size_t i = 0; i < size; i++)
        if (::tolower((*this)[i]) != ::tolower(s[i]))
            return false;
    return true;
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

inline bool pure_slice_t::contains_bytes(pure_slice_t bytes) const noexcept {
    return bool(find(bytes));
}

inline const uint8_t* pure_slice_t::find_byte(uint8_t b) const {
    if (_usually_false(size == 0))
        return nullptr;
    return (const uint8_t*)::memchr(buf, b, size);
}

inline const uint8_t* pure_slice_t::find_byte_or_end(uint8_t byte) const noexcept {
    auto result = find_byte(byte);
    return result ? result : (const uint8_t*)end();
}

inline const uint8_t* pure_slice_t::find_any_byte_of(pure_slice_t target_bytes) const noexcept {
    const void* result = nullptr;
    for (size_t i = 0; i < target_bytes.size; ++i) {
        auto r = find_byte(target_bytes[i]);
        if (r && (!result || r < result))
            result = r;
    }
    return (const uint8_t*)result;
}

inline const uint8_t* pure_slice_t::find_byte_not_in(pure_slice_t target_bytes) const noexcept {
    for (auto c = (const uint8_t*)buf; c != end(); ++c) {
        if (!target_bytes.find_byte(*c))
            return c;
    }
    return nullptr;
}

inline bool pure_slice_t::has_prefix(pure_slice_t s) const noexcept {
    return s.size > 0 && size >= s.size && ::memcmp(buf, s.buf, s.size) == 0;
}

inline bool pure_slice_t::has_suffix(pure_slice_t s) const noexcept {
    return s.size > 0 && size >= s.size
            && ::memcmp(offsetby(buf, size - s.size), s.buf, s.size) == 0;
}

RETURNS_NONNULL inline void* pure_slice_t::new_bytes(size_t size) {
    void* result = ::malloc(size);
    if (_usually_false(!result)) fail_bad_alloc();
    return result;
}

template <typename T>
RETURNS_NONNULL inline T* pure_slice_t::realloc_bytes(T* bytes, size_t new_size) {
    T* new_bytes = (T*)::realloc(bytes, new_size);
    if (_usually_false(!new_bytes)) fail_bad_alloc();
    return new_bytes;
}

inline slice_t pure_slice_t::copy() const {
    if (buf == nullptr)
        return null_slice;
    void* copied = new_bytes(size);
    ::memcpy(copied, buf, size);
    return slice_t(copied, size);
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

inline void slice_t::shorten(size_t s) {
    set_size(check(s));
}

inline void slice_t::set_start(const void *s) noexcept {
    check(s);
    set(s, diff_pointer(end(), s));
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

inline alloc_slice_t alloc_slice_t::null_padded_string(pure_slice_t s) {
    alloc_slice_t a(s.size + 1);
    s.copy_to((void*)a.buf);
    ((char*)a.buf)[s.size] = '\0';
    a.shorten(s.size);
    return a;
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

inline void alloc_slice_t::shorten(size_t sz) {
    pure_slice_t::set_size(check(sz));
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
