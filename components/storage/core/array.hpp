#pragma once

#include "value.hpp"

namespace storage { namespace impl {

class dict_t;
class mutable_array_t;
class array_iterator_t;


class array_t : public value_t
{

    struct impl
    {
        const value_t* _first;
        uint32_t _count;
        uint8_t _width;

        impl(const value_t*) noexcept;
        const value_t* second() const noexcept PURE;
        const value_t* first_value() const noexcept PURE;
        const value_t* deref(const value_t*) const noexcept PURE;
        const value_t* operator[] (unsigned index) const noexcept PURE;
        size_t index_of(const value_t *v) const noexcept PURE;
        void offset(uint32_t n);
        bool is_mutable() const noexcept PURE;
    };

public:
    constexpr array_t();

    uint32_t count() const noexcept PURE;
    bool empty() const noexcept PURE;
    const value_t* get(uint32_t index) const noexcept PURE;
    mutable_array_t* as_mutable() const PURE;
    static const array_t* const empty_array;

    using iterator = array_iterator_t;
    inline iterator begin() const noexcept;

protected:
    internal::heap_array_t* heap_array() const;

private:
    friend class value_t;
    friend class array_iterator_t;
    friend class dict_t;
    friend class dict_iterator_t;
    template <bool WIDE> friend struct dict_impl_t;
    friend class internal::heap_array_t;
};


class array_iterator_t : private array_t::impl
{
public:
    array_iterator_t(const array_t* a) noexcept;
    uint32_t count() const noexcept PURE                        { return _count; }
    const value_t* value() const noexcept PURE                  { return _value; }
    explicit operator const value_t* () const noexcept PURE     { return _value; }
    const value_t* operator-> () const noexcept PURE            { return _value; }
    const value_t* read() noexcept                              { auto v = _value; ++(*this); return v; }
    const value_t* operator[] (unsigned i) const noexcept PURE  { return ((impl&)*this)[i]; }
    explicit operator bool() const noexcept PURE                { return _count > 0; }
    array_iterator_t& operator++();
    array_iterator_t& operator += (uint32_t);

private:
    const value_t* raw_value() noexcept                         { return _first; }

    const value_t *_value;

    friend class value_t;
    friend class value_dumper_t;
};


inline array_iterator_t array_t::begin() const noexcept         { return iterator(this); }

} }
