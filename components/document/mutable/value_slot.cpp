#include "value_slot.hpp"
#include <components/document/mutable/mutable_array.hpp>
#include <components/document/mutable/mutable_dict.hpp>
#include <components/document/support/varint.hpp>
#include <algorithm>
#include <cfloat>
#include <cmath>

namespace document { namespace impl {

using namespace internal;

static_assert(sizeof(value_slot_t) == 8);

value_slot_t::value_slot_t()
    : _pointer(0)
{}

value_slot_t::value_slot_t(null_value_t)
    : _tag(inline_tag)
{
    _inline_value[0] = ((tag_special << 4) | special_value_null);
}

value_slot_t::value_slot_t(heap_collection_t *md)
    : _pointer( uint64_t(retain(md)->as_value()) )
{}

value_slot_t::value_slot_t(const value_slot_t &other) noexcept
    : _pointer(other._pointer)
{
    if (is_pointer())
        retain(pointer());
}

value_slot_t& value_slot_t::operator= (const value_slot_t &other) noexcept {
    release_value();
    _pointer = other._pointer;
    if (is_pointer())
        retain(pointer());
    return *this;
}

value_slot_t::value_slot_t(value_slot_t &&other) noexcept {
    _pointer = other._pointer;
    other._pointer = 0;
}

value_slot_t& value_slot_t::operator= (value_slot_t &&other) noexcept {
    release(as_pointer());
    _pointer = other._pointer;
    other._pointer = 0;
    return *this;
}

bool value_slot_t::empty() const {
    return _pointer == 0;
}

const value_t *value_slot_t::as_value() const {
    return is_pointer() ? pointer() : inline_pointer();
}

value_slot_t::operator bool() const {
    return !empty();
}

value_slot_t::~value_slot_t() {
    if (is_pointer())
        release(pointer());
}

void value_slot_t::release_value() {
    if (is_pointer()) {
        release(pointer());
        _pointer = 0;
    }
}

const value_t* value_slot_t::as_value_or_undefined() const {
    return _pointer ? as_value() : value_t::undefined_value;
}

void value_slot_t::set_pointer(const value_t *v) {
    precondition((intptr_t(v) & 0xFF) != inline_tag);
    precondition(v != nullptr);
    if (_usually_false(v == pointer()))
        return;
    release_value();
    _pointer = uint64_t(size_t(retain(v)));
    assert(is_pointer());
}

void value_slot_t::set_inline(internal::tags tag, int tiny) {
    release_value();
    _tag = inline_tag;
    _inline_value[0] = uint8_t((tag << 4) | tiny);
}

void value_slot_t::set(null_value_t) {
    set_inline(tag_special, special_value_null);
}

void value_slot_t::set(bool b) {
    set_inline(tag_special, b ? special_value_true : special_value_false);
}

void value_slot_t::set(int i) {
    set_int(i);
}

void value_slot_t::set(unsigned i) {
    set_int(i);
}

void value_slot_t::set(int64_t i) {
    set_int(i);
}

void value_slot_t::set(uint64_t i) {
    set_int(i);
}

template <class INT>
void value_slot_t::set_int(INT i) {
    if (i < 2048 && (!std::numeric_limits<INT>::is_signed || int64_t(i) > -2048)) {
        set_inline(tag_short, (i >> 8) & 0x0F);
        _inline_value[1] = (uint8_t)(i & 0xFF);
    } else {
        uint8_t buf[8];
        auto size = put_int_of_length(buf, i, !std::numeric_limits<INT>::is_signed);
        set_value(tag_int,
                 (int)(size-1) | (std::numeric_limits<INT>::is_signed ? 0 : 0x08),
                 {buf, size});
    }
}

void value_slot_t::set(float f) {
    struct {
        uint8_t filler = 0;
        endian::little_float le;
    } data;
    data.le = f;
    set_value(tag_float, 0, {(char*)&data.le - 1, sizeof(data.le) + 1});
    assert_postcondition(as_value()->as_float() == f);
}

void value_slot_t::set(double d) {
    if (fabs(d) <= FLT_MAX && d == static_cast<float>(d)) {
        set((float)d);
    } else {
        set_pointer(heap_value_t::create(d)->as_value());
    }
    assert_postcondition(as_value()->as_double() == d);
}

void value_slot_t::set(slice_t s) {
    set_string_or_data(tag_string, s);
}

void value_slot_t::set_data(slice_t s) {
    set_string_or_data(tag_binary, s);
}

void value_slot_t::set_value(const value_t *v) {
    if (v && v->tag() < tag_array) {
        auto size = v->data_size();
        if (size <= inline_capacity) {
            release_value();
            _tag = inline_tag;
            memcpy(&_inline_value, v, size);
            return;
        }
    }
    set_pointer(v);
}

void value_slot_t::set_value(tags ag, int tiny, slice_t bytes) {
    if (1 + bytes.size <= inline_capacity) {
        set_inline(ag, tiny);
        bytes.copy_to(&_inline_value[1]);
    } else {
        set_pointer(heap_value_t::create(ag, tiny, bytes)->as_value());
    }
}

void value_slot_t::set_string_or_data(tags tag, slice_t s) {
    if (s.size + 1 <= inline_capacity) {
        set_inline(tag, (int)s.size);
        s.copy_to(&_inline_value[1]);
    } else {
        set_pointer(heap_value_t::create_str(tag, s)->as_value());
    }
}

heap_collection_t* value_slot_t::as_mutable_collection() const {
    const value_t *ptr = as_pointer();
    if (ptr && ptr->is_mutable())
        return (heap_collection_t*)heap_value_t::as_heap_value(ptr);
    return nullptr;
}

heap_collection_t* value_slot_t::make_mutable(tags if_type) {
    if (is_inline())
        return nullptr;
    retained_t<heap_collection_t> mval = heap_collection_t::mutable_copy(pointer(), if_type);
    if (mval)
        set(mval->as_value());
    return mval;
}

bool value_slot_t::is_pointer() const noexcept {
    return _tag != inline_tag;
}

bool value_slot_t::is_inline() const noexcept {
    return !is_pointer();
}

const value_t *value_slot_t::pointer() const noexcept {
    return (const value_t*)_pointer;
}

const value_t *value_slot_t::as_pointer() const noexcept {
    return (is_pointer() ? pointer() : nullptr);
}

const value_t *value_slot_t::inline_pointer() const noexcept {
    return (const value_t*)&_inline_value;
}

void value_slot_t::copy_value(copy_flags flags) {
    const value_t *value = as_pointer();
    if (value && ((flags & copy_immutables) || value->is_mutable())) {
        bool recurse = (flags & deep_copy);
        retained_t<heap_collection_t> copy;
        switch (value->tag()) {
        case tag_array:
            copy = new heap_array_t((array_t*)value);
            if (recurse)
                ((heap_array_t*)copy.get())->copy_children(flags);
            set(copy->as_value());
            break;
        case tag_dict:
            copy = new heap_dict_t((dict_t*)value);
            if (recurse)
                ((heap_dict_t*)copy.get())->copy_children(flags);
            set(copy->as_value());
            break;
        case tag_string:
            set(value->as_string());
            break;
        case tag_float:
            set(value->as_double());
            break;
        default:
            assert(false);
        }
    }
}

} }
