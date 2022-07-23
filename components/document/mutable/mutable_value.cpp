#include "mutable_value.hpp"
#include <components/document/mutable/mutable_array.hpp>
#include <components/document/mutable/mutable_dict.hpp>
#include <components/document/core/doc.hpp>
#include <components/document/support/exception.hpp>
#include <components/document/support/varint.hpp>
#include <components/document/support/better_assert.hpp>
#include <algorithm>

namespace document { namespace impl { namespace internal {

void* heap_value_t::operator new(size_t size, size_t extra_size) {
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
    static_assert(offsetof(heap_value_t, _header) & 1, "_header must be at odd address");
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
    return ::operator new(size + extra_size);
}

heap_value_t::heap_value_t(tags tag, int tiny) {
    _pad = 0xFF;
    _header = uint8_t((tag << 4) | tiny);
}

tags heap_value_t::tag() const {
    return tags(_header >> 4);
}

heap_value_t* heap_value_t::create(tags tag, int tiny, slice_t extra_data) {
    auto hv = new (extra_data.size) heap_value_t(tag, tiny);
    extra_data.copy_to(&hv->_header + 1);
    return hv;
}

heap_value_t* heap_value_t::create(null_value_t) {
    return new (0) heap_value_t(tag_special, special_value_null);
}

heap_value_t* heap_value_t::create(bool b) {
    return new (0) heap_value_t(tag_special, b ? special_value_true : special_value_false);
}

heap_value_t *heap_value_t::create(int i) {
    return create_int(i, false);
}

heap_value_t *heap_value_t::create(unsigned i) {
    return create_int(i, true);
}

heap_value_t *heap_value_t::create(int64_t i) {
    return create_int(i, false);
}

heap_value_t *heap_value_t::create(uint64_t i) {
    return create_int(i, true);
}

template <class INT>
heap_value_t* heap_value_t::create_int(INT i, bool is_unsigned) {
    if (i < 2048 && (is_unsigned || -i < 2048)) {
        uint8_t extra = (uint8_t)(i & 0xFF);
        return create(tag_short, (i >> 8) & 0x0F, {&extra, 1});
    } else {
        uint8_t buf[8];
        auto size = put_int_of_length(buf, i, is_unsigned);
        return create(tag_int, (int)(size-1) | (is_unsigned ? 0x08 : 0), {buf, size});
    }
}

template heap_value_t* heap_value_t::create_int<int>(int i, bool is_unsigned);
template heap_value_t* heap_value_t::create_int<unsigned>(unsigned i, bool is_unsigned);
template heap_value_t* heap_value_t::create_int<int64_t>(int64_t i, bool is_unsigned);
template heap_value_t* heap_value_t::create_int<uint64_t>(uint64_t i, bool is_unsigned);

heap_value_t* heap_value_t::create(float f) {
    struct {
        uint8_t filler = 0;
        endian::little_float le;
    } data;
    data.le = f;
    return create(tag_float, 0, {(char*)&data.le - 1, sizeof(data.le) + 1});
}

heap_value_t* heap_value_t::create(double d) {
    struct {
        uint8_t filler = 0;
        endian::little_double le;
    } data;
    data.le = d;
    return create(tag_float, 8, {(char*)&data.le - 1, sizeof(data.le) + 1});
}

heap_value_t *heap_value_t::create(slice_t s) {
    return create_str(internal::tag_string, s);
}

heap_value_t* heap_value_t::create_str(tags tag, slice_t s) {
    uint8_t size_buf[max_varint_len32];
    size_t size_byte_count = 0;
    int tiny;
    if (s.size < 0x0F) {
        tiny = (int)s.size;
    } else {
        tiny = 0x0F;
        size_byte_count = put_uvar_int(&size_buf, s.size);
    }
    auto hv = new (size_byte_count + s.size) heap_value_t(tag, tiny);
    uint8_t *str_data = &hv->_header + 1;
    memcpy(str_data, size_buf, size_byte_count);
    memcpy(str_data + size_byte_count, s.buf, s.size);
    return hv;
}

heap_value_t* heap_value_t::create(const value_t *v) {
    assert_precondition(v->tag() < tag_array);
    size_t size = v->data_size();
    auto hv = new (size - 1) heap_value_t();
    memcpy(&hv->_header, v, size);
    return hv;
}

const value_t *heap_value_t::as_value(heap_value_t *v) {
    return v ? v->as_value() : nullptr;
}

const value_t *heap_value_t::as_value() const {
    return (const value_t*)&_header;
}

bool heap_value_t::is_heap_value(const value_t *v) {
    return ((size_t)v & 1) != 0;
}

heap_value_t* heap_value_t::as_heap_value(const value_t *v) {
    if (!is_heap_value(v))
        return nullptr;
    auto ov = (offset_value_t*)(size_t(v) & ~1);
    assert_postcondition(ov->_pad == 0xFF);
    return (heap_value_t*)ov;
}

static bool is_hardwired_value(const value_t *v) {
    return v == value_t::null_value || v == value_t::undefined_value
            || v == value_t::true_value || v == value_t::false_value
            || v == array_t::empty_array || v == dict_t::empty_dict;
}

const value_t* heap_value_t::retain(const value_t *v) {
    if (internal::heap_value_t::is_heap_value(v)) {
        document::retain(heap_value_t::as_heap_value(v));
    } else if (v) {
        retained_const_t<doc_t> doc = doc_t::containing(v);
        if (_usually_true(doc != nullptr))
            document::retain(std::move(doc));
        else if (!is_hardwired_value(v))
            exception_t::_throw(error_code::invalid_data,
                                "Can't retain immutable value_t %p that's not part of a doc_t",
                                v);
    }
    return v;
}

void heap_value_t::release(const value_t *v) {
    if (internal::heap_value_t::is_heap_value(v)) {
        document::release(heap_value_t::as_heap_value(v));
    } else if (v) {
        retained_const_t<doc_t> doc = doc_t::containing(v);
        if (_usually_true(doc != nullptr))
            document::release(doc.get());
        else if (!is_hardwired_value(v))
            exception_t::_throw(error_code::invalid_data,
                                "Can't release immutable value_t %p that's not part of a doc_t",
                                v);
    }
}

void *heap_value_t::operator new(size_t size) {
    return ::operator new(size);
}

void heap_value_t::operator delete(void *ptr) {
    ::operator delete(ptr);
}

void heap_value_t::operator delete(void *ptr, size_t) {
    ::operator delete(ptr);
}


retained_t<heap_collection_t> heap_collection_t::mutable_copy(const value_t *v, tags if_type) {
    if (!v || v->tag() != if_type)
        return nullptr;
    if (v->is_mutable())
        return (heap_collection_t*)as_heap_value(v);
    switch (if_type) {
    case tag_array: return new heap_array_t((const array_t*)v);
    case tag_dict:  return new heap_dict_t((const dict_t*)v);
    default:        return nullptr;
    }
}

bool heap_collection_t::is_changed() const {
    return _changed;
}

heap_collection_t::heap_collection_t(tags tag)
    : heap_value_t(tag, 0)
    , _changed(false)
{}

void heap_collection_t::set_changed(bool c) {
    _changed = c;
}

} } }
