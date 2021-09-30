#pragma once

#include "value.hpp"
#include "better_assert.hpp"

namespace storage { namespace impl { namespace internal {

using namespace storage::impl;


class pointer_t : public value_t
{
public:
    static constexpr size_t max_narrow_offset = 0x7FFE;

    pointer_t(size_t offset, int width, bool external = false);

    bool is_external() const PURE;
    template <bool WIDE> PURE uint32_t offset() const noexcept;
    template <bool WIDE> inline const value_t* deref() const noexcept;
    const value_t* deref_wide() const noexcept;
    const value_t* deref(bool wide) const noexcept;
    const value_t* careful_deref(bool wide, const void* &start, const void* &end) const noexcept;
    bool validate(bool wide, const void *start) const noexcept PURE;

private:
    template <bool WIDE> PURE uint32_t legacy_offset() const noexcept;
    const value_t* deref_extern(bool wide, const value_t *dst) const noexcept;

    void set_narrow_bytes(uint16_t b);
    void set_wide_bytes(uint32_t b);
    uint16_t narrow_bytes() const PURE;
    uint32_t wide_bytes() const PURE;
};


template <bool WIDE>
PURE uint32_t pointer_t::offset() const noexcept {
    if (WIDE)
        return (endian::dec32(wide_bytes()) & ~0xC0000000) << 1;
    else
        return (endian::dec16(narrow_bytes()) & ~0xC000) << 1;
}

template <bool WIDE>
inline const value_t* pointer_t::deref() const noexcept {
    auto off = offset<WIDE>();
    assert(off > 0);
    const value_t *dst = offsetby(this, -static_cast<std::ptrdiff_t>(off));
    if (_usually_false(is_external()))
        dst = deref_extern(WIDE, dst);
    return dst;
}

template <bool WIDE>
PURE uint32_t pointer_t::legacy_offset() const noexcept {
    if (WIDE)
        return (endian::dec32(wide_bytes()) & ~0x80000000) << 1;
    else
        return (endian::dec16(narrow_bytes()) & ~0x8000) << 1;
}

} } }
