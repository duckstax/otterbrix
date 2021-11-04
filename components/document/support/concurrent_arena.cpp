#include "concurrent_arena.hpp"
#include <string.h>

using namespace std;

namespace document {

concurrent_arena_t::concurrent_arena_t()
    : _heap_end(nullptr)
    , _next_block(nullptr)
{}

concurrent_arena_t::concurrent_arena_t(size_t capacity)
    : _heap(new uint8_t[capacity])
    , _heap_end(&_heap[capacity])
    , _next_block(&_heap[0])
{}

concurrent_arena_t::concurrent_arena_t(concurrent_arena_t &&other) {
    *this = move(other);
}

concurrent_arena_t& concurrent_arena_t::operator=(concurrent_arena_t &&other) {
    _heap = move(other._heap);
    _heap_end = other._heap_end;
    _next_block = other._next_block.load();
    return *this;
}

size_t concurrent_arena_t::capacity() const {
    return _heap_end - _heap.get();
}

size_t concurrent_arena_t::allocated() const {
    return _next_block - _heap.get();
}

size_t concurrent_arena_t::available() const {
    return _heap_end - _next_block;
}

void* concurrent_arena_t::alloc(size_t size) {
    uint8_t *result, *new_next, *next_block = _next_block;
    do {
        result = next_block;
        new_next = next_block + size;
        if (new_next > _heap_end)
            return nullptr;
    } while (!_next_block.compare_exchange_weak(next_block, new_next, memory_order_acq_rel));
    return result;
}

void* concurrent_arena_t::calloc(size_t size) {
    auto block = alloc(size);
    if (_usually_true(block != nullptr))
        memset(block, 0, size);
    return block;
}

bool concurrent_arena_t::free(void *allocated_block, size_t size) {
    uint8_t *next_block = (uint8_t*)allocated_block + size;
    uint8_t *new_next = (uint8_t*)allocated_block;
    return _next_block.compare_exchange_weak(next_block, new_next, memory_order_acq_rel);
}

void concurrent_arena_t::free_all() {
    _next_block = _heap.get();
}

size_t concurrent_arena_t::to_offset(const void *ptr) const {
    assert(ptr >= _heap.get() && ptr < _heap_end);
    return (uint8_t*)ptr - _heap.get();
}

void* concurrent_arena_t::to_pointer(size_t off) const {
    void *ptr = _heap.get() + off;
    assert(ptr < _heap_end);
    return ptr;
}

}
