#include "pointer.hpp"
#include "doc.hpp"
#include <tuple>
#include <stdio.h>
#include "better_assert.hpp"

namespace document { namespace impl { namespace internal {

pointer_t::pointer_t(size_t offset, int width, bool external)
    : value_t(tag_pointer, 0)
{
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

bool pointer_t::is_external() const {
    return (_byte[0] & 0x40) != 0;
}

const value_t* pointer_t::deref_wide() const noexcept {
    return deref<true>();
}

const value_t* pointer_t::deref(bool wide) const noexcept {
    return wide ? deref<true>() : deref<false>();
}

const value_t* pointer_t::deref_extern(bool wide, const value_t *dst) const noexcept {
    dst = doc_t::resolve_pointer_from(this, dst);
    if (_usually_true(dst != nullptr))
        return dst;
    if (!wide) {
        dst = offsetby(this, -(std::ptrdiff_t)legacy_offset<false>());
        auto scope = scope_t::containing(this);
        if (scope && scope->data().is_contains_address(dst))
            return dst;
    }

    auto off = wide ? offset<true>() : offset<false>();
    fprintf(stderr, "FATAL: Fleece extern pointer at %p, offset -%u,"
                        " did not resolve to any address\n", this, off);
    return nullptr;
}

void pointer_t::set_narrow_bytes(uint16_t b) {
    *(uint16_t*)_byte = b;
}

void pointer_t::set_wide_bytes(uint32_t b) {
    *(uint32_t*)_byte = b;
}

uint16_t pointer_t::narrow_bytes() const {
    return *(uint16_t*)_byte;
}

uint32_t pointer_t::wide_bytes() const {
    return *(uint32_t*)_byte;
}

const value_t* pointer_t::careful_deref(bool wide, const void* &start, const void* &end) const noexcept {
    uint32_t off = wide ? offset<true>() : offset<false>();
    if (off == 0)
        return nullptr;
    const value_t *target = offsetby(this, -(std::ptrdiff_t)off);

    if (_usually_false(is_external())) {
        slice_t destination;
        std::tie(target, destination) = doc_t::resolve_pointer_from_with_range(this, target);
        if (_usually_false(!target)) {
            if (wide)
                return nullptr;
            target = offsetby(this, -(std::ptrdiff_t)legacy_offset<false>());
            if (_usually_false(target < start) || _usually_false(target >= end))
                return nullptr;
            end = this;
        } else {
            assert_always((size_t(target) & 1) == 0);
            start = destination.buf;
            end = destination.end();
        }
    } else {
        if (_usually_false(target < start) || _usually_false(target >= end))
            return nullptr;
        end = this;
    }

    if (_usually_false(target->is_pointer()))
        return target->as_pointer()->careful_deref(true, start, end);
    else
        return target;
}

bool pointer_t::validate(bool wide, const void *start) const noexcept {
    const void *data_end = this;
    const value_t *target = careful_deref(wide, start, data_end);
    return target && target->validate(start, data_end);
}


} } }
