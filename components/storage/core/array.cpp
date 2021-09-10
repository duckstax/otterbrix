#include "array.hpp"
#include "mutable_array.h"
#include "mutable_dict.hpp"
#include "internal.hpp"
#include "platform_compat.hpp"
#include "varint.hpp"


namespace storage { namespace impl {

using namespace internal;


array_t::impl::impl(const value_t* v) noexcept {
    if (_usually_false(v == nullptr)) {
        _first = nullptr;
        _width = size_narrow;
        _count = 0;
    } else if (_usually_true(!v->is_mutable())) {
        _first = (const value_t*)(&v->_byte[2]);
        _width = v->is_wide_array() ? size_wide : size_narrow;
        _count = v->count_value();
        if (_usually_false(_count == long_array_count)) {
            uint32_t extra_count;
            size_t count_size = get_uvar_int32(slice_t(_first, 10), &extra_count);
            if (_usually_true(count_size > 0))
                _count += extra_count;
            else
                _count = 0;
            _first = offsetby(_first, count_size + (count_size & 1));
        }
    } else {
        auto mcoll = (heap_collection_t*)heap_value_t::as_heap_value(v);
        heap_array_t *mut_array;
        if (v->tag() == tag_array) {
            mut_array = (heap_array_t*)mcoll;
            _count = mut_array->count();
        } else {
            mut_array = ((heap_dict_t*)mcoll)->array_key_value();
            _count = mut_array->count() / 2;
        }
        _first = _count ? (const value_t*)mut_array->first() : nullptr;
        _width = sizeof(value_slot_t);
    }
}

const value_t* array_t::impl::deref(const value_t *v) const noexcept {
    if (_usually_false(is_mutable()))
        return ((value_slot_t*)v)->as_value();
    return v->deref(_width == size_wide);
}

const value_t* array_t::impl::operator[] (unsigned index) const noexcept {
    if (_usually_false(index >= _count))
        return nullptr;
    if (_width == size_narrow)
        return offsetby(_first, size_narrow * index)->deref<false>();
    else if (_usually_true(_width == size_wide))
        return offsetby(_first, size_wide   * index)->deref<true>();
    else
        return ((value_slot_t*)_first + index)->as_value();
}

const value_t* array_t::impl::second() const noexcept {
    return offsetby(_first, _width);
}

const value_t* array_t::impl::first_value() const noexcept {
    if (_usually_false(_count == 0))
        return nullptr;
    return deref(_first);
}

size_t array_t::impl::index_of(const value_t *v) const noexcept {
    return ((size_t)v - (size_t)_first) / _width;
}

void array_t::impl::offset(uint32_t n) {
    _throw_if(n > _count, error_code::out_of_range, "iterating past end of array");
    _count -= n;
    if (_usually_true(_count > 0))
        _first = offsetby(_first, _width*n);
}

bool array_t::impl::is_mutable() const noexcept {
    return _width > 4;
}



constexpr array_t::array_t()
    : value_t(internal::tag_array, 0, 0)
{}

uint32_t array_t::count() const noexcept {
    if (_usually_false(is_mutable()))
        return heap_array()->count();
    return impl(this)._count;
}

bool array_t::empty() const noexcept {
    if (_usually_false(is_mutable()))
        return heap_array()->empty();
    return count_is_zero();
}

const value_t* array_t::get(uint32_t index) const noexcept {
    if (_usually_false(is_mutable()))
        return heap_array()->get(index);
    return impl(this)[index];
}

heap_array_t* array_t::heap_array() const {
    return (heap_array_t*)internal::heap_collection_t::as_heap_value(this);
}

mutable_array_t* array_t::as_mutable() const {
    return is_mutable() ? (mutable_array_t*)this : nullptr;
}

array_iterator_t::array_iterator_t(const array_t *a) noexcept
    :impl(a),
      _value(first_value())
{}

array_iterator_t& array_iterator_t::operator++() {
    offset(1);
    _value = first_value();
    return *this;
}

array_iterator_t& array_iterator_t::operator += (uint32_t n) {
    offset(n);
    _value = first_value();
    return *this;
}

EVEN_ALIGNED static constexpr array_t empty_array_instance;
const array_t* const array_t::empty_array = &empty_array_instance;

} }
