#include "pointer.hpp"

#include <cstdio>

#include <tuple>

#include <components/document/support/better_assert.hpp>

namespace document::impl::internal {

    pointer_t::pointer_t(size_t offset, int width, bool external)
        : value_t(tag_pointer, 0) {
        assert_precondition((offset & 1) == 0);
        offset >>= 1;
        if (width < internal::size_wide) {
            _throw_if(offset >= 0x4000, error_code::internal_error, "offset too large");
            if (_usually_false(external))
                offset |= 0x4000;
            set_narrow_bytes(endian::enc16(uint16_t(offset | 0x8000)));
        } else {
            if (_usually_false(offset >= 0x40000000))
                exception_t::_throw(error_code::out_of_range, "data too large");
            if (_usually_false(external))
                offset |= 0x40000000;
            set_wide_bytes(endian::enc32(uint32_t(offset | 0x80000000)));
        }
    }

    const value_t* pointer_t::deref_wide() const noexcept { return deref<true>(); }

    const value_t* pointer_t::deref(bool wide) const noexcept { return wide ? deref<true>() : deref<false>(); }

    void pointer_t::set_narrow_bytes(uint16_t b) { *reinterpret_cast<uint16_t*>(byte_) = b; }

    void pointer_t::set_wide_bytes(uint32_t b) { *reinterpret_cast<uint32_t*>(byte_) = b; }

    uint16_t pointer_t::narrow_bytes() const { return *reinterpret_cast<const uint16_t*>(byte_); }

    uint32_t pointer_t::wide_bytes() const { return *reinterpret_cast<const uint32_t*>(byte_); }

    const value_t* pointer_t::careful_deref(bool wide, const void*& start, const void*& end) const noexcept {
        uint32_t off = wide ? offset<true>() : offset<false>();
        if (off == 0)
            return nullptr;
        const value_t* target = offsetby(this, -static_cast<std::ptrdiff_t>(off));

        if (_usually_false(target < start) || _usually_false(target >= end)) {
            return nullptr;
        }

        end = this;

        if (_usually_false(target->is_pointer()))
            return target->as_pointer()->careful_deref(true, start, end);
        else
            return target;
    }

} // namespace document::impl::internal
