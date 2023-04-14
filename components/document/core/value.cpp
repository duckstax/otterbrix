#include "value.hpp"
#include <cstring>

#include <boost/container/small_vector.hpp>

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/core/internal.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/support/better_assert.hpp>
#include <components/document/support/endian.hpp>
#include <components/document/support/num_conversion.hpp>
#include <components/document/support/varint.hpp>
#include <components/document/core/pointer.hpp>

#include "utils.hpp"


namespace document::impl {

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
            : value_t(tag, tiny, byte1) {}
    };

    EVEN_ALIGNED static constexpr const const_value_t
        null_instance{tag_special, special_value_null},
        undefined_instance{tag_special, special_value_undefined},
        true_instance{tag_special, special_value_true},
        false_instance{tag_special, special_value_false};

    const value_t* const value_t::null_value = &null_instance;
    const value_t* const value_t::undefined_value = &undefined_instance;
    const value_t* const value_t::true_value = &true_instance;
    const value_t* const value_t::false_value = &false_instance;

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
                    return int16_t(i | 0xF000);
                else
                    return i;
            }
            case tag_int: {
                int64_t n = 0;
                unsigned byteCount = tiny_value();
                if ((byteCount & 0x8) == 0) {
                    if (_byte[1 + byteCount] & 0x80)
                        n = -1;
                } else {
                    byteCount &= 0x7;
                }
                ::memcpy(&n, &_byte[1], ++byteCount);
                return int64_t(endian::little_dec64(uint64_t(n)));
            }
            case tag_float:
                return int64_t(as_double());
            default:
                return 0;
        }
    }

    uint64_t value_t::as_unsigned() const noexcept {
        return uint64_t(as_int());
    }

    float value_t::as_float() const noexcept {
        return as_float_of_type<float>();
    }

    double value_t::as_double() const noexcept {
        return as_float_of_type<double>();
    }

    bool value_t::is_int() const noexcept {
        return tag() <= internal::tag_int;
    }

    bool value_t::is_unsigned() const noexcept {
        return tag() == internal::tag_int && (_byte[0] & 0x08) != 0;
    }

    bool value_t::is_double() const noexcept {
        return tag() == internal::tag_float && (_byte[0] & 0x8);
    }

    bool value_t::is_undefined() const noexcept {
        return _byte[0] == ((internal::tag_special << 4) |
                            internal::special_value_undefined);
    }

    bool value_t::is_pointer() const noexcept {
        return (_byte[0] & 0x80) != 0;
    }

    const internal::pointer_t* value_t::as_pointer() const {
        return reinterpret_cast<const internal::pointer_t*>(this);
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
                    return T(d);
                } else {
                    endian::little_float f;
                    memcpy(&f, &_byte[2], sizeof(f));
                    return T(f);
                }
            }
            default:
                if (is_unsigned())
                    return T(as_unsigned());
                else
                    return T(as_int());
        }
    }

    std::string_view value_t::get_string_bytes() const noexcept {
        std::string_view s(reinterpret_cast<const char*>(&_byte[1]), tiny_value());
        if (_usually_false(s.size() == 0x0F)) {
            uint32_t length;
            std::string_view tmp(reinterpret_cast<const char*>(&_byte[1]), tiny_value());
            size_t lengthBytes = get_uvar_int32(tmp, &length);
            if (_usually_false(lengthBytes == 0))
                return {};
            return {reinterpret_cast<const char*>(&tmp[lengthBytes]),length};
        }
        return s;
    }

    bool value_t::is_mutable() const {
        return (reinterpret_cast<size_t>(this) & 1) != 0;
    }

    std::string_view value_t::as_string() const noexcept {
        return _usually_true(tag() == tag_string) ? get_string_bytes() : std::string_view();
    }

    std::string_view value_t::as_data() const noexcept {
        return _usually_true(tag() == tag_binary) ? get_string_bytes() : std::string_view();
    }

    const array_t* value_t::as_array() const noexcept {
        if (_usually_false(tag() != tag_array))
            return nullptr;
        return reinterpret_cast<const array_t*>(this);
    }

    const dict_t* value_t::as_dict() const noexcept {
        if (_usually_false(tag() != tag_dict))
            return nullptr;
        return reinterpret_cast<const dict_t*>(this);
    }

    const array_t* value_t::as_array(const value_t* v) {
        return v ? v->as_array() : nullptr;
    }

    const dict_t* value_t::as_dict(const value_t* v) {
        return v ? v->as_dict() : nullptr;
    }

    shared_keys_t* value_t::shared_keys() const noexcept {
        return nullptr; ///_containing(this);
    }

    bool value_t::is_equal(const value_t* v) const {
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
                    return is_equals(as_double(), v->as_double());
                else
                    return is_equals(as_float(), v->as_float());
            case tag_special:
                return _byte[1] == v->_byte[1];
            case tag_string:
            case tag_binary:
                return get_string_bytes() == v->get_string_bytes();
            case tag_array: {
                array_t::iterator i(reinterpret_cast<const array_t*>(this));
                array_t::iterator j(reinterpret_cast<const array_t*>(v));
                if (i.count() != j.count())
                    return false;
                for (; i; ++i, ++j)
                    if (!i.value()->is_equal(j.value()))
                        return false;
                return true;
            }
            case tag_dict:
                return (reinterpret_cast<const dict_t*>(this))->is_equals(reinterpret_cast<const dict_t*>(v));
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
                array_t::iterator i(reinterpret_cast<const array_t*>(this));
                array_t::iterator j(reinterpret_cast<const array_t*>(rhs));
                if (i.count() != j.count())
                    return false;
                for (; i; ++i, ++j)
                    if (!i.value()->is_lt(j.value()))
                        return false;
                return true;
            }
            case tag_dict:
                return (reinterpret_cast<const dict_t*>(this))->is_lt(reinterpret_cast<const dict_t*>(rhs));
            default:
                return false;
        }
    }

    bool value_t::is_lte(const document::impl::value_t* rhs) const {
        return !rhs->is_lt(this);
    }


    internal::tags value_t::tag() const noexcept {
        return internal::tags(_byte[0] >> 4);
    }

    unsigned value_t::tiny_value() const noexcept {
        return _byte[0] & 0x0F;
    }

    bool value_t::big_float() const noexcept {
        return _byte[0] & 0x8;
    }

    uint16_t value_t::short_value() const noexcept {
        return ((uint16_t(_byte[0]) << 8) | _byte[1]) & 0x0FFF;
    }

    size_t value_t::data_size() const noexcept {
        switch (tag()) {
            case tag_short:
            case tag_special:
                return 2;
            case tag_float:
                return is_double() ? 10 : 6;
            case tag_int:
                return 2 + (tiny_value() & 0x07);
            case tag_string:
            case tag_binary:
                return size_t(reinterpret_cast<const uint8_t*>(get_string_bytes().end()) - reinterpret_cast<const uint8_t*>(this));
            case tag_array:
            case tag_dict:
                return size_t(reinterpret_cast<const uint8_t*>(array_t::impl(this)._first) - reinterpret_cast<const uint8_t*>(this));
            case tag_pointer:
            default:
                return 2;
        }
    }

    bool value_t::is_wide_array() const noexcept {
        return (_byte[0] & 0x08) != 0;
    }

    uint32_t value_t::count_value() const noexcept {
        return ((uint32_t(_byte[0]) << 8) | _byte[1]) & 0x07FF;
    }

    bool value_t::count_is_zero() const noexcept {
        return _byte[1] == 0 && (_byte[0] & 0x7) == 0;
    }

    const value_t* value_t::deref(bool wide) const {
        if (!is_pointer())
            return this;
        auto v = as_pointer()->deref(wide);
        while (_usually_false(v->is_pointer()))
            v = v->as_pointer()->deref_wide();
        return v;
    }

    template<bool WIDE>
    const value_t* value_t::deref() const {
        if (!is_pointer())
            return this;
        auto v = as_pointer()->deref<WIDE>();
        if (!WIDE)
            while (_usually_false(v->is_pointer()))
                v = v->as_pointer()->deref_wide();
        return v;
    }

    template const value_t* value_t::deref<false>() const;
    template const value_t* value_t::deref<true>() const;

    const value_t* value_t::next(bool wide) const noexcept {
        return offsetby(this, wide ? internal::size_wide : internal::size_narrow);
    }

    void value_t::_retain() const {
        heap_value_t::retain(this);
    }

    void value_t::_release() const {
        heap_value_t::release(this);
    }

    template<>
    bool value_t::as<bool>() const {
        return as_bool();
    }

    template<>
    uint64_t value_t::as<uint64_t>() const {
        return as_unsigned();
    }

    template<>
    int64_t value_t::as<int64_t>() const {
        return as_int();
    }

    template<>
    double value_t::as<double>() const {
        return as_double();
    }

    template<>
    std::string value_t::as<std::string>() const {
        return static_cast<std::string>(as_string());
    }

    void release(const value_t* val) noexcept {
        heap_value_t::release(val);
    }

    std::string to_string(const value_t* value) {
        switch (value->tag()) {
            case tag_short:
            case tag_int: {
                char buf[32];
                int64_t i = value->as_int();
                if (value->is_unsigned()) {
                    sprintf(buf, "%llu", static_cast<unsigned long long>(i));
                } else {
                    sprintf(buf, "%lld", static_cast<long long>(i));
                }
                return {buf};
            }
            case tag_special: {
                switch (value->tiny_value()) {
                    case special_value_null:
                        return "null";
                    case special_value_undefined:
                        return "undefined";
                    case special_value_false:
                        return "false";
                    case special_value_true:
                        return "true";
                    default:
                        return "{?special?}";
                }
            }
            case tag_float: {
                char buf[32];
                if (value->big_float()) {
                    write_double(value->as_double(), buf, 32);
                } else {
                    write_float(value->as_float(), buf, 32);
                }
                return {buf};
            }
            default: {
                auto s = value->as_string();
                return {s.data(), s.size()};
            }
        }
        return {};
    }

} // namespace document::impl
