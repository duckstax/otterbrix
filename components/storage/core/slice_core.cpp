#include "slice_core.hpp"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include "better_assert.hpp"

namespace storage::wyhash {
    #include "wyhash.hpp"
}
namespace storage::wyhash32 {
    #include "wyhash32.hpp"
}

#ifdef _MSC_VER
#include <Windows.h>
#endif


bool is_equal_slice_c(slice_t_c a, slice_t_c b) noexcept {
    return a.size==b.size && (a.size == 0 || memcmp(a.buf, b.buf, a.size) == 0);
}

int compare_slice_c(slice_t_c a, slice_t_c b) noexcept {
    if (a.size == b.size) {
        if (a.size == 0)
            return 0;
        return memcmp(a.buf, b.buf, a.size);
    } else if (a.size < b.size) {
        if (a.size == 0)
            return -1;
        int result = memcmp(a.buf, b.buf, a.size);
        return result ? result : -1;
    } else {
        if (b.size == 0)
            return 1;
        int result = memcmp(a.buf, b.buf, b.size);
        return result ? result : 1;
    }
}

bool slice_c_to_cstring(slice_t_c s, char* buffer, size_t capacity) noexcept {
    precondition(capacity > 0);
    size_t n = std::min(s.size, capacity - 1);
    if (n > 0)
        memcpy(buffer, s.buf, n);
    buffer[n] = '\0';
    return (n == s.size);
}

uint32_t hash_slice_c(slice_t_c s) noexcept {
    if (sizeof(void*) >= 8) {
        return (uint32_t) storage::wyhash::wyhash(s.buf, s.size, 0, storage::wyhash::_wyp);
    } else {
        static constexpr unsigned seed = 0x91BAC172;
        return storage::wyhash32::wyhash32(s.buf, s.size, seed);
    }
}


namespace storage {

static constexpr size_t heap_alignment_mask =
#if FL_EMBEDDED
    0x03;
#else
    0x07;
#endif

    LITECORE_UNUSED PURE static inline bool is_heap_aligned(const void *p) {
        return ((size_t)p & heap_alignment_mask) == 0;
    }

#ifndef FL_DETECT_COPIES
#define FL_DETECT_COPIES 0
#endif

    struct shared_buffer_t
    {
        std::atomic<uint32_t> _ref_count {1};
#if FL_DETECT_COPIES
        static constexpr uint32_t _magic = 0xdecade55;
#endif
        uint8_t _buf[4];

        static inline void* operator new(size_t basicSize, size_t bufferSize) noexcept {
            return malloc(basicSize - sizeof(shared_buffer_t::_buf) + bufferSize);
        }

        static inline void operator delete(void *self) {
            assert_precondition(is_heap_aligned(self));
            free(self);
        }

        inline void retain() noexcept {
            assert_precondition(is_heap_aligned(this));
            ++_ref_count;
        }

        inline void release() noexcept {
            assert_precondition(is_heap_aligned(this));
            if (--_ref_count == 0)
                delete this;
        }
    };

    PURE
    static shared_buffer_t* to_shared_buffer(const void *buf) noexcept {
        return (shared_buffer_t*)((uint8_t*)buf - offsetof(shared_buffer_t, _buf));
    }
}


using namespace storage;


slice_result_t_c create_slice_result_c(size_t size) noexcept {
    auto sb = new (size) shared_buffer_t;
    if (!sb)
        return {};
    return {&sb->_buf, size};
}

slice_result_t_c copy_slice_c(slice_t_c s) noexcept {
    if (!s.buf)
        return {};
#if FL_DETECT_COPIES
    if (s.buf && is_heap_aligned(s.buf)
        && ((size_t)s.buf & 0xFFF) >= 4
        && ((const uint32_t*)s.buf)[-1] == _magic) {
        fprintf(stderr, "$$$$$ Copying existing alloc_slice_t at {%p, %zu}\n", s.buf, s.size);
    }
#endif
    auto sb = new (s.size) shared_buffer_t;
    if (!sb)
        return {};
    memcpy(&sb->_buf, s.buf, s.size);
    return {&sb->_buf, s.size};
}

void retain_buf_c(const void *buf) noexcept {
    if (buf)
        to_shared_buffer(buf)->retain();
}

void release_buf_c(const void *buf) noexcept {
    if (buf)
        to_shared_buffer(buf)->release();
}

void wipe_memory(void *buf, size_t size) noexcept {
    if (size > 0) {
        volatile unsigned char* p = (unsigned char *)buf;
        for (auto s = size; s > 0; --s)
            *p++ = 0;
    }
}
