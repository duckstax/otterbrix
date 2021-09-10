#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include "base.hpp"


namespace storage {

struct alloc_slice_t;

}

extern "C" {

struct slice_t_c
{
    const void *buf;
    size_t size;

    explicit operator bool() const noexcept PURE  { return buf != nullptr; }
    explicit operator std::string() const         { return std::string(static_cast<char*>(const_cast<void*>(buf)), size); }
};


struct slice_result_t_c
{
    const void *buf;
    size_t size;

    explicit operator bool() const noexcept PURE  { return buf != nullptr; }
    explicit operator slice_t_c () const          { return {buf, size}; }
    inline explicit operator std::string() const;
};


struct heap_slice_t_c : public slice_t_c
{
    constexpr heap_slice_t_c() noexcept                              : slice_t_c{nullptr, 0} { }
private:
    constexpr heap_slice_t_c(const void *b, size_t s) noexcept  : slice_t_c{b, s} { }
    friend struct storage::alloc_slice_t;
};


#define slice_null_c ((slice_t_c) {NULL, 0})

bool is_equal_slice_c(slice_t_c a, slice_t_c b) noexcept PURE;
int compare_slice_c(slice_t_c, slice_t_c) noexcept PURE;
uint32_t hash_slice_c(slice_t_c s) noexcept PURE;
bool slice_c_to_cstring(slice_t_c s, char* buffer NONNULL, size_t capacity) noexcept;
slice_result_t_c create_slice_result_c(size_t) noexcept;
slice_result_t_c copy_slice_c(slice_t_c) noexcept;

void retain_buf_c(const void*) noexcept;
void release_buf_c(const void*) noexcept;

static inline slice_result_t_c retain_slice_result_c(slice_result_t_c s) noexcept {
    retain_buf_c(s.buf);
    return s;
}

static inline void release_slice_result_c(slice_result_t_c s) noexcept {
    release_buf_c(s.buf);
}

void wipe_memory(void *dst, size_t size) noexcept;

}

PURE static inline bool operator== (slice_t_c s1, slice_t_c s2) { return is_equal_slice_c(s1, s2); }
PURE static inline bool operator!= (slice_t_c s1, slice_t_c s2) { return !(s1 == s2); }

PURE static inline bool operator== (slice_result_t_c sr, slice_t_c s) { return static_cast<slice_t_c>(sr) == s; }
PURE static inline bool operator!= (slice_result_t_c sr, slice_t_c s) { return !(sr == s); }

slice_result_t_c::operator std::string () const {
    auto str = std::string(static_cast<char*>(const_cast<void*>(buf)), size);
    release_slice_result_c(*this);
    return str;
}
