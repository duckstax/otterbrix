#pragma once

#include "platform_compat.hpp"
#include "better_assert.hpp"
#include <atomic>
#include <memory>

namespace storage {

class concurrent_arena_t {
public:
    concurrent_arena_t(size_t capacity);
    concurrent_arena_t();
    concurrent_arena_t(concurrent_arena_t &&other);
    concurrent_arena_t& operator=(concurrent_arena_t &&other);

    size_t capacity() const PURE;
    size_t allocated() const PURE;
    size_t available() const PURE;

    void* alloc(size_t size);
    void* calloc(size_t size);
    bool free(void *allocated_block, size_t size);
    void free_all();

    size_t to_offset(const void *ptr) const PURE;
    void* to_pointer(size_t off) const PURE;

private:
    std::unique_ptr<uint8_t[]> _heap;
    uint8_t *_heap_end;
    std::atomic<uint8_t*> _next_block;
};


template <class T, bool Zeroing = false>
class concurrent_arena_allocator_t {
public:
    typedef T value_type;

    concurrent_arena_allocator_t(concurrent_arena_t &arena) : _arena(arena) {}

    [[nodiscard]] T* allocate(size_t n) {
        if (Zeroing)
            return (T*) _arena.calloc(n * sizeof(T));
        else
            return (T*) _arena.alloc(n * sizeof(T));
    }

    void deallocate(T* p, size_t n) noexcept  { return _arena.free(p, n * sizeof(T)); }

private:
    concurrent_arena_t &_arena;
};

}
