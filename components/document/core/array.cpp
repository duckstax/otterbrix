#include "array.hpp"
#include <components/document/core/internal.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.hpp>
#include <components/document/support/varint.hpp>

namespace document::impl {

    using namespace internal;

    array_t::iterator::iterator(const value_t* a) noexcept {
        if (_usually_false(a == nullptr)) {
            first_ = nullptr;
            width_ = size_narrow;
            count_ = 0;
        } else if (_usually_true(!a->is_mutable())) {
            first_ = reinterpret_cast<const value_t*>(&a->byte_[2]);
            width_ = a->is_wide_array() ? size_wide : size_narrow;
            count_ = a->count_value();
            if (_usually_false(count_ == long_array_count)) {
                uint32_t extra_count;
                size_t count_size = get_uvar_int32(std::string_view(reinterpret_cast<const char*>(first_), 10), &extra_count);
                if (_usually_true(count_size > 0))
                    count_ += extra_count;
                else
                    count_ = 0;
                first_ = offsetby(first_, static_cast<std::ptrdiff_t>(count_size + (count_size & 1)));
            }
        } else {
            auto mcoll = heap_value_t::as_heap_value(a);
            heap_array_t* mut_array;
            if (a->tag() == tag_array) {
                mut_array = dynamic_cast<heap_array_t*>(mcoll);
                count_ = mut_array->count();
            } else {
                mut_array = dynamic_cast<heap_dict_t*>(mcoll)->array_key_value();
                count_ = mut_array->count() / 2;
            }
            first_ = count_ ? reinterpret_cast<const value_t*>(mut_array->first()) : nullptr;
            width_ = sizeof(value_slot_t);
        }
        value_ = first_value();
    }

    const value_t* array_t::iterator::deref(const value_t* v) const noexcept {
        if (_usually_false(is_mutable()))
            return reinterpret_cast<const value_slot_t*>(v)->as_value();
        return v->deref(width_ == size_wide);
    }

    const value_t* array_t::iterator::operator[](unsigned index) const noexcept {
        if (_usually_false(index >= count_))
            return nullptr;
        if (width_ == size_narrow)
            return offsetby(first_, size_narrow * index)->deref<false>();
        else if (_usually_true(width_ == size_wide))
            return offsetby(first_, size_wide * index)->deref<true>();
        else
            return (reinterpret_cast<const value_slot_t*>(first_) + index)->as_value();
    }

    const value_t* array_t::iterator::second() const noexcept {
        return offsetby(first_, width_);
    }

    const value_t* array_t::iterator::first_value() const noexcept {
        if (_usually_false(count_ == 0))
            return nullptr;
        return deref(first_);
    }

    size_t array_t::iterator::index_of(const value_t* v) const noexcept {
        return (reinterpret_cast<size_t>(v) - reinterpret_cast<size_t>(first_)) / width_;
    }

    void array_t::iterator::offset(uint32_t n) {
        _throw_if(n > count_, error_code::out_of_range, "iterating past end of array");
        count_ -= n;
        if (_usually_true(count_ > 0))
            first_ = offsetby(first_, width_ * n);
    }

    uint32_t array_t::iterator::count() const noexcept {
        return count_;
    }

    const value_t* array_t::iterator::value() const noexcept {
        return value_;
    }

    array_t::iterator::operator const value_t*() const noexcept {
        return value_;
    }

    const value_t* array_t::iterator::operator->() const noexcept {
        return value_;
    }

    const value_t* array_t::iterator::read() noexcept {
        auto v = value_;
        ++(*this);
        return v;
    }

    array_t::iterator::operator bool() const noexcept { return count_ > 0; }

    bool array_t::iterator::is_mutable() const noexcept {
        return width_ > 4;
    }

    array_iterator_t& array_iterator_t::operator++() {
        offset(1);
        value_ = first_value();
        return *this;
    }

    array_iterator_t& array_iterator_t::operator+=(uint32_t n) {
        offset(n);
        value_ = first_value();
        return *this;
    }



    constexpr array_t::array_t()
        : value_t(internal::tag_array, 0, 0) {}

    uint32_t array_t::count() const noexcept {
        if (_usually_false(is_mutable()))
            return heap_array()->count();
        return iterator(this).count_;
    }

    bool array_t::empty() const noexcept {
        if (_usually_false(is_mutable()))
            return heap_array()->empty();
        return count_is_zero();
    }

    const value_t* array_t::get(uint32_t index) const noexcept {
        if (_usually_false(is_mutable()))
            return heap_array()->get(index);
        return iterator(this)[index];
    }

    heap_array_t* array_t::heap_array() const {
        return dynamic_cast<heap_array_t*>(internal::heap_collection_t::as_heap_value(this));
    }

    mutable_array_t* array_t::as_mutable() const {
        return is_mutable() ? reinterpret_cast<mutable_array_t*>(const_cast<array_t*>(this)) : nullptr;
    }

    array_iterator_t array_t::begin() const noexcept {
        return iterator(this);
    }

    EVEN_ALIGNED static constexpr array_t empty_array_instance;
    const array_t* const array_t::empty_array = &empty_array_instance;

} // namespace document::impl
