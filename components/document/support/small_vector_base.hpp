#pragma once

#include <components/document/support/platform_compat.hpp>
#include <components/document/support/better_assert.hpp>
#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string.h>
#include <utility>

namespace document {

class alignas(void*) small_vector_base_t
{
public:
    size_t capacity() const PURE { return _capacity; }
    size_t size() const PURE     { return _size; }
    bool empty() const PURE           { return _size == 0; }

    static constexpr size_t max_size = (1ul<<31) - 1;

protected:
    small_vector_base_t() noexcept              {}
    small_vector_base_t(uint32_t cap) noexcept  : _size(0), _capacity(cap), _is_big(false) {}

    ~small_vector_base_t() {
        if (_is_big)
            ::free(_data_pointer);
    }

    static uint32_t check_range(size_t n) {
        if (_usually_false(n > max_size))
            throw std::domain_error("small_vector_t size/capacity too large");
        return uint32_t(n);
    }

    void* _begin() PURE                       { return _is_big ? _data_pointer : _inline_data; }
    const void* _begin() const PURE           { return _is_big ? _data_pointer : _inline_data; }

    void _move_from(small_vector_base_t &&sv, size_t size) noexcept {
        ::memcpy(this, &sv, size);
        std::move(sv).release();
    }

    void release() && noexcept {
        _size = 0;
        _data_pointer = nullptr;
    }

    void _embiggen(size_t cap, size_t item_size) {
        precondition(cap >= _size);
        uint32_t new_cap = check_range(cap);
        void *pointer = _is_big ? _data_pointer : nullptr;
        pointer = ::realloc(pointer, new_cap * item_size);
        if (_usually_false(!pointer))
            throw std::bad_alloc();
        if (!_is_big) {
            if (_size > 0)
                ::memcpy(pointer, _inline_data, _size * item_size);
            _is_big = true;
        }
        _data_pointer = pointer;
        _capacity = new_cap;
    }

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    void _emsmallen(uint32_t new_cap, size_t item_size) {
        assert_precondition(_is_big);
        void *pointer = _data_pointer;
        if (_size > 0)
            ::memcpy(_inline_data, pointer, _size * item_size);
        ::free(pointer);
        _is_big = false;
        _capacity = new_cap;
    }

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

    void* _grow_to(uint32_t new_size, size_t item_size) {
        auto old_size = _size;
        assert_precondition(new_size >= old_size);
        if (_usually_false(new_size > _capacity)) {
            check_range(new_size);
            uint32_t new_capacity;
            if (old_size == 0) {
                new_capacity = new_size;
            } else if (new_size >= (max_size / 3) * 2) {
                new_capacity = max_size;
            } else {
                new_capacity = _capacity;
                do {
                    new_capacity += new_capacity / 2;
                } while (new_capacity < new_size);
            }
            _embiggen(new_capacity, item_size);
        }
        _size = new_size;
        return (uint8_t*)_begin() + old_size * item_size;
    }

    void* _insert(void *where, uint32_t n_items, size_t item_size) {
        auto begin = (uint8_t*)_begin();
        if (_size + n_items <= _capacity) {
            _size += n_items;
        } else {
            size_t offset = (uint8_t*)where - begin;
            _grow_to(_size + n_items, item_size);
            begin = (uint8_t*)_begin();
            where = begin + offset;
        }
        _move_items((uint8_t*)where + n_items * item_size, where, begin + (_size - n_items) * item_size);
        return where;
    }

    static void _move_items(void *dst, void *start, void *end) noexcept {
        assert(start <= end);
        auto n = (uint8_t*)end - (uint8_t*)start;
        if (n > 0)
            ::memmove(dst, start, n);
    }

    static constexpr size_t base_inline_cap = sizeof(void*);

    uint32_t    _size;
    uint32_t    _capacity {31};
    bool        _is_big   {true};
    void*   _data_pointer = nullptr;
    uint8_t _inline_data[base_inline_cap];
};

}
