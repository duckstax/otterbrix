#pragma once

#include "small_vector_base.hpp"

namespace storage {

template <class T, size_t N>
class small_vector_t : public small_vector_base_t
{
public:
    static constexpr size_t max_size = (1ul<<31) - 1;

    small_vector_t() noexcept                          : small_vector_base_t(N) {}
    small_vector_t(size_t size)                   : small_vector_t()       { resize(size); }

    template <size_t M>
    small_vector_t(const small_vector_t<T, M> &sv)     : small_vector_t(sv.begin(), sv.end()) {}
    small_vector_t(const small_vector_t &sv)           : small_vector_t(sv.begin(), sv.end()) {}

    small_vector_t(small_vector_t &&sv) noexcept       : small_vector_base_t() { _move_from(std::move(sv), sizeof(*this)); }

    template <class ITER>
    small_vector_t(ITER b, ITER e)
        : small_vector_t()
    {
        set_capacity(e - b);
        while (b != e)
            heedless_push_back(*b++);
    }

    ~small_vector_t() {
        if (_size > 0) {
            auto item = begin();
            for (auto i = 0; i < _size; ++i)
                (item++)->T::~T();
        }
    }

    small_vector_t& operator=(small_vector_t &&sv) noexcept {
        clear();
        _move_from(std::move(sv), sizeof(*this));
        return *this;
    }

    small_vector_t& operator=(const small_vector_t &sv) noexcept {
        return operator=<N>(sv);
    }

    template <size_t M>
    small_vector_t& operator=(const small_vector_t<T, M> &sv) {
        erase(begin(), end());
        set_capacity(sv.size());
        for (const auto &val : sv)
            heedless_push_back(val);
        return *this;
    }

    template <size_t M>
    bool operator== (const small_vector_t<T, M> &v) const
#if defined(__clang__) || !defined(__GNUC__)
    PURE
#endif
    {
        if (_size != v._size)
            return false;
        auto vi = v.begin();
        for (auto &e : *this) {
            if (!(e == *vi))
                return false;
            ++vi;
        }
        return true;
    }

    template <size_t M>
    bool operator!= (const small_vector_t<T, M> &v) const
#if defined(__clang__) || !defined(__GNUC__)
    PURE
#endif
    {
        return !( *this == v);
    }

    void clear()                                    { shrink_to(0); }
    void reserve(size_t cap)                   { if (cap>_capacity) set_capacity(cap); }

    const T& get(size_t i) const PURE {
        assert_precondition(i < _size);
        return _get(i);
    }

    T& get(size_t i) PURE {
        assert_precondition(i < _size);
        return _get(i);
    }

    const T& operator[] (size_t i) const PURE  { return get(i); }
    T& operator[] (size_t i) PURE              { return get(i); }
    const T& back() const PURE                      { return get(_size - 1); }
    T& back() PURE                                  { return get(_size - 1); }

    using iterator = T*;
    using const_iterator = const T*;

    iterator begin() PURE                           { return &_get(0); }
    iterator end() PURE                             { return &_get(_size); }
    const_iterator begin() const PURE               { return &_get(0); }
    const_iterator end() const PURE                 { return &_get(_size); }

    T& push_back(const T& t)                        { return * new(grow()) T(t); }
    T& push_back(T&& t)                             { return * new(grow()) T(std::move(t)); }

    void pop_back()                                 { get(_size - 1).~T(); --_size; }

    template <class... Args>
    T& emplace_back(Args&&... args) {
        return * new(grow()) T(std::forward<Args>(args)...);
    }

    void insert(iterator where, T item) {
        assert_precondition(begin() <= where && where <= end());
        void *dst = _insert(where, 1, item_size);
        *(T*)dst = std::move(item);
    }

    template <class ITER>
    void insert(iterator where, ITER b, ITER e) {
        assert_precondition(begin() <= where && where <= end());
        auto n = e - b;
        assert_precondition(n >= 0 && n <= max_size);
        T *dst = (T*)_insert(where, uint32_t(n), item_size);
        while (b != e)
            *dst++ = *b++;
    }

    void erase(iterator first) {
        erase(first, first+1);
    }

    void erase(iterator first, iterator last) {
        assert_precondition(begin() <= first && first <= last && last <= end());
        if (first == last)
            return;
        for (auto i = first; i != last; ++i)
            i->T::~T();
        _move_items(first, last, end());
        _size -= last - first;
    }

    void resize(size_t sz) {
        auto old_size = _size;
        if (sz > old_size) {
            uint32_t new_size = check_range(sz);
            auto i = (iterator)_grow_to(new_size, item_size);
            for (; old_size < new_size; ++old_size)
                (void) new (i++) T();
        } else {
            shrink_to(sz);
        }
    }

    void set_capacity(size_t cap) {
        if (cap != _capacity) {
            if (cap > N)
                _embiggen(cap, item_size);
            else if (_capacity != N)
                _emsmallen(N, item_size);
        }
    }

    void* push_back_new()                   { return grow(); }

private:
    T& _get(size_t i) PURE             { return ((T*)_begin())[i]; }
    const T& _get(size_t i) const PURE { return ((T*)_begin())[i]; }

    T* grow()                               { return (T*)_grow_to(_size + 1, item_size); }
    T* heedless_grow()                      { assert(_size < _capacity); return &_get(_size++); }
    T& heedless_push_back(const T& t)       { return * new(heedless_grow()) T(t); }

    void shrink_to(size_t sz) {
        if (sz < _size) {
            auto item = end();
            for (auto i = sz; i < _size; ++i)
                (--item)->T::~T();
            _size = (uint32_t)sz;

            if (_is_big && sz <= N)
                _emsmallen(N, item_size);
        }
    }

    static constexpr size_t item_size = ((sizeof(T) + alignof(T) - 1) / alignof(T)) * alignof(T);
    static constexpr size_t inline_cap = std::max(N * item_size, base_inline_cap);

    uint8_t _padding[inline_cap - base_inline_cap];
};

}
