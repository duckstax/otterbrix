#include "value.hpp"
#include <components/document/core/pointer.hpp>
#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/core/internal.hpp>
#include <components/document/core/doc.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/support/endian.hpp>
#include <components/document/support/varint.hpp>
#include <components/document/support/better_assert.hpp>
#include <components/document/support/num_conversion.hpp>


namespace document { namespace impl {

using namespace internal;

static const value_type value_types[] = {
    value_type::number,
    value_type::number,
    value_type::number,
    value_type::null,
    value_type::string,
    value_type::data,
    value_type::array,
    value_type::dict,
    value_type::null
};


class const_value_t : public value_t {
public:
    constexpr const_value_t(internal::tags tag, int tiny, int byte1 = 0)
        :value_t(tag, tiny, byte1) { }
};

EVEN_ALIGNED static constexpr const const_value_t
    null_instance           {tag_special, special_value_null},
    undefined_instance      {tag_special, special_value_undefined},
    true_instance           {tag_special, special_value_true},
    false_instance          {tag_special, special_value_false};

const value_t* const value_t::null_value      = &null_instance;
const value_t* const value_t::undefined_value = &undefined_instance;
const value_t* const value_t::true_value      = &true_instance;
const value_t* const value_t::false_value     = &false_instance;


value_type value_t::type() const noexcept {
    auto t = tag();
    if (_usually_false(t == tag_special)) {
        switch (tiny_value()) {
        case special_value_false:
        case special_value_true:
            return value_type::boolean;
        case special_value_undefined:
            return value_type::undefined;
        case special_value_null:
        default:
            return value_type::null;
        }
    } else {
        return value_types[t];
    }
}

bool value_t::as_bool() const noexcept {
    switch (tag()) {
    case tag_special:
        return tiny_value() == special_value_true;
    case tag_short:
    case tag_int:
    case tag_float:
        return as_int() != 0;
    default:
        return true;
    }
}

int64_t value_t::as_int() const noexcept {
    switch (tag()) {
    case tag_special:
        return tiny_value() == special_value_true;
    case tag_short: {
        uint16_t i = short_value();
        if (_usually_false(i & 0x0800))
            return (int16_t)(i | 0xF000);
        else
            return i;
    }
    case tag_int: {
        int64_t n = 0;
        unsigned byteCount = tiny_value();
        if ((byteCount & 0x8) == 0) {
            if (((uint8_t*)&_byte)[1+byteCount] & 0x80)
                n = -1;
        } else {
            byteCount &= 0x7;
        }
        memcpy(&n, &_byte[1], ++byteCount);
        return endian::little_dec64(n);
    }
    case tag_float:
        return (int64_t)as_double();
    default:
        return 0;
    }
}

template float value_t::as_float_of_type<float>() const noexcept;
template double value_t::as_float_of_type<double>() const noexcept;

template<typename T>
T value_t::as_float_of_type() const noexcept {
    switch (tag()) {
    case tag_float: {
        if (_byte[0] & 0x8) {
            endian::little_double d;
            memcpy(&d, &_byte[2], sizeof(d));
            return (T)d;
        } else {
            endian::little_float f;
            memcpy(&f, &_byte[2], sizeof(f));
            return (T)f;
        }
    }
    default:
        if (is_unsigned())
            return (T)as_unsigned();
        else
            return (T)as_int();
    }
}

slice_t value_t::get_string_bytes() const noexcept {
    slice_t s(&_byte[1], tiny_value());
    if (_usually_false(s.size == 0x0F)) {
        uint32_t length;
        size_t lengthBytes = get_uvar_int32(s, &length);
        return slice_t(&s[lengthBytes], length);
    }
    return s;
}

alloc_slice_t value_t::to_string() const {
    char buf[32], *str = buf;
    switch (tag()) {
    case tag_short:
    case tag_int: {
        int64_t i = as_int();
        if (is_unsigned())
            sprintf(str, "%llu", (unsigned long long)i);
        else
            sprintf(str, "%lld", (long long)i);
        break;
    }
    case tag_special: {
        switch (tiny_value()) {
        case special_value_null:
            str = (char*)"null";
            break;
        case special_value_undefined:
            str = (char*)"undefined";
            break;
        case special_value_false:
            str = (char*)"false";
            break;
        case special_value_true:
            str = (char*)"true";
            break;
        default:
            str = (char*)"{?special?}";
            break;
        }
        break;
    }
    case tag_float: {
        if (_byte[0] & 0x8)
            write_double(as_double(), str, 32);
        else
            write_float(as_float(), str, 32);
        break;
    }
    default:
        return alloc_slice_t(as_string());
    }
    return alloc_slice_t(str);
}

slice_t value_t::as_string() const noexcept {
    return _usually_true(tag() == tag_string) ? get_string_bytes() : slice_t();
}

slice_t value_t::as_data() const noexcept {
    return _usually_true(tag() == tag_binary) ? get_string_bytes() : slice_t();
}

const array_t* value_t::as_array() const noexcept {
    if (_usually_false(tag() != tag_array))
        return nullptr;
    return (const array_t*)this;
}

const dict_t* value_t::as_dict() const noexcept {
    if (_usually_false(tag() != tag_dict))
        return nullptr;
    return (const dict_t*)this;
}

shared_keys_t* value_t::shared_keys() const noexcept {
    return doc_t::shared_keys(this);
}

bool value_t::is_equal(const value_t *v) const {
    if (!v || _byte[0] != v->_byte[0])
        return false;
    if (_usually_false(this == v))
        return true;
    switch (tag()) {
    case tag_short:
    case tag_int:
        return as_int() == v->as_int();
    case tag_float:
        if (is_double())
            return as_double() == v->as_double();
        else
            return as_float() == v->as_float();
    case tag_special:
        return _byte[1] == v->_byte[1];
    case tag_string:
    case tag_binary:
        return get_string_bytes() == v->get_string_bytes();
    case tag_array: {
        array_t::iterator i((const array_t*)this);
        array_t::iterator j((const array_t*)v);
        if (i.count() != j.count())
            return false;
        for (; i; ++i, ++j)
            if (!i.value()->is_equal(j.value()))
                return false;
        return true;
    }
    case tag_dict:
        return ((const dict_t*)this)->is_equals((const dict_t*)v);
    default:
        return false;
    }
}

bool value_t::is_lt(const value_t* rhs) const {
    if (tag() < rhs->tag())
        return true;
    if (tag() > rhs->tag())
        return false;

    switch (tag()) {
        case tag_short:
        case tag_int:
            return as_int() < rhs->as_int();
        case tag_float:
            if (is_double())
                return as_double() < rhs->as_double();
            else
                return as_float() < rhs->as_float();
        case tag_special:
            return _byte[1] < rhs->_byte[1];
        case tag_string:
        case tag_binary:
            return get_string_bytes() < rhs->get_string_bytes();
        case tag_array: {
            array_t::iterator i((const array_t*) this);
            array_t::iterator j((const array_t*) rhs);
            if (i.count() != j.count())
                return false;
            for (; i; ++i, ++j)
                if (!i.value()->is_lt(j.value()))
                    return false;
            return true;
        }
        case tag_dict:
            return ((const dict_t*) this)->is_lt((const dict_t*) rhs);
        default:
            return false;
    }
}

bool value_t::is_lte(const document::impl::value_t* rhs) const {
    if (tag() < rhs->tag())
        return true;
    if (tag() > rhs->tag())
        return false;

    if (!rhs || _byte[0] != rhs->_byte[0])
        return false;
    if (_usually_false(this == rhs))
        return true;
    switch (tag()) {
        case tag_short:
        case tag_int:
            return as_int() <= rhs->as_int();
        case tag_float:
            if (is_double())
                return as_double() <= rhs->as_double();
            else
                return as_float() <= rhs->as_float();
        case tag_special:
            return _byte[1] <= rhs->_byte[1];
        case tag_string:
        case tag_binary:
            return get_string_bytes() <= rhs->get_string_bytes();
        case tag_array: {
            array_t::iterator i((const array_t*) this);
            array_t::iterator j((const array_t*) rhs);
            if (i.count() != j.count())
                return false;
            for (; i; ++i, ++j)
                if (!i.value()->is_lte(j.value()))
                    return false;
            return true;
        }
        case tag_dict:
            return ((const dict_t*) this)->is_lte((const dict_t*) rhs);
        default:
            return false;
    }
}

const value_t* value_t::from_trusted_data(slice_t s) noexcept {
#ifndef NDEBUG
    assert_precondition(from_data(s) != nullptr);
#endif
    return find_root(s);
}

const value_t* value_t::from_data(slice_t s) noexcept {
    auto root = find_root(s);
    if (root && _usually_false(!root->validate(s.buf, s.end())))
        root = nullptr;
    return root;
}

const value_t* value_t::find_root(slice_t s) noexcept {
    precondition(((size_t)s.buf & 1) == 0);

    if (_usually_false((size_t)s.buf & 1) || _usually_false(s.size < size_narrow)
            || _usually_false(s.size % size_narrow))
        return nullptr;
    auto root = (const value_t*)offsetby(s.buf, s.size - internal::size_narrow);
    if (_usually_true(root->is_pointer())) {
        const void *dataStart = s.buf, *dataEnd = root;
        return root->as_pointer()->careful_deref(false, dataStart, dataEnd);
    } else {
        if (_usually_false(s.size != size_narrow))
            return nullptr;
    };
    return root;
}

bool value_t::validate(const void *data_start, const void *data_end) const noexcept {
    auto t = tag();
    if (t == tag_array || t == tag_dict) {
        array_t::impl array(this);
        if (_usually_true(array._count > 0)) {
            size_t itemCount = array._count;
            if (_usually_true(t == tag_dict))
                itemCount *= 2;
            auto itemsSize = itemCount * array._width;
            if (_usually_false(offsetby(array._first, itemsSize) > data_end))
                return false;
            auto item = array._first;
            while (itemCount-- > 0) {
                auto nextItem = offsetby(item, array._width);
                if (item->is_pointer()) {
                    if (_usually_false(!item->as_pointer()->validate(array._width == size_wide, data_start)))
                        return false;
                } else {
                    if (_usually_false(!item->validate(data_start, nextItem)))
                        return false;
                }
                item = nextItem;
            }
            return true;
        }
    }
    return offsetby(this, data_size()) <= data_end;
}

size_t value_t::data_size() const noexcept {
    switch(tag()) {
    case tag_short:
    case tag_special:   return 2;
    case tag_float:     return is_double() ? 10 : 6;
    case tag_int:       return 2 + (tiny_value() & 0x07);
    case tag_string:
    case tag_binary:    return (uint8_t*)get_string_bytes().end() - (uint8_t*)this;
    case tag_array:
    case tag_dict:      return (uint8_t*)array_t::impl(this)._first - (uint8_t*)this;
    case tag_pointer:
    default:            return 2;
    }
}

const value_t* value_t::deref(bool wide) const {
    if (!is_pointer())
        return this;
    auto v = as_pointer()->deref(wide);
    while (_usually_false(v->is_pointer()))
        v = v->as_pointer()->deref_wide();
    return v;
}

template <bool WIDE>
const value_t* value_t::deref() const {
    if (!is_pointer())
        return this;
    auto v = as_pointer()->deref<WIDE>();
    while (!WIDE && _usually_false(v->is_pointer()))
        v = v->as_pointer()->deref_wide();
    return v;
}

template const value_t* value_t::deref<false>() const;
template const value_t* value_t::deref<true>() const;

void value_t::_retain() const     {heap_value_t::retain(this);}
void value_t::_release() const    {heap_value_t::release(this);}

template<> bool value_t::as<bool>() const {
    return as_bool();
}

template<> uint64_t value_t::as<uint64_t>() const {
    return as_unsigned();
}

template<> int64_t value_t::as<int64_t>() const {
    return as_int();
}

template<> double value_t::as<double>() const {
    return as_double();
}

template<> std::string value_t::as<std::string>() const {
    return static_cast<std::string>(as_string());
}


void release(const value_t *val) noexcept {
    heap_value_t::release(val);
}

} }
