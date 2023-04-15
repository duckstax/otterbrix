#pragma once

#include <cassert>
#include <string_view>

#include <components/document/core/internal.hpp>
#include <components/document/support/endian.hpp>
#include <components/document/support/exception.hpp>
#include <components/document/support/ref_counted.hpp>

namespace document::impl {

    class array_t;
    class dict_t;
    class shared_keys_t;

    enum class value_type : int8_t {
        undefined = -1,
        null = 0,
        boolean,
        number,
        string,
        data,
        array,
        dict
    };

    enum copy_flags {
        default_copy = 0,
        deep_copy = 1,
        copy_immutables = 2,
        deep_copy_immutables = (deep_copy | copy_immutables),
    };

    class value_t {
    public:
        static const value_t* const true_value;
        static const value_t* const false_value;
        static const value_t* const null_value;
        static const value_t* const undefined_value;

        value_type type() const noexcept PURE;
        bool is_equal(const value_t*) const PURE;
        bool is_lt(const value_t*) const PURE;
        bool is_lte(const value_t*) const PURE;

        bool as_bool() const noexcept PURE;
        int64_t as_int() const noexcept PURE;
        uint64_t as_unsigned() const noexcept PURE;
        float as_float() const noexcept PURE;
        double as_double() const noexcept PURE;
        const internal::pointer_t* as_pointer() const PURE;
        std::string_view as_string() const noexcept PURE;
        std::string_view as_data() const noexcept PURE;
        const array_t* as_array() const noexcept PURE;
        const dict_t* as_dict() const noexcept PURE;

        static const array_t* as_array(const value_t* v) PURE;
        static const dict_t* as_dict(const value_t* v) PURE;

        bool is_int() const noexcept PURE;
        bool is_unsigned() const noexcept PURE;
        bool is_double() const noexcept PURE;
        bool is_undefined() const noexcept PURE;
        bool is_pointer() const noexcept PURE;
        bool is_mutable() const PURE;

        shared_keys_t* shared_keys() const noexcept PURE;

        void retain_() const;
        void release_() const;

        internal::tags tag() const noexcept PURE;
        unsigned tiny_value() const noexcept PURE;
        bool big_float() const noexcept PURE;

        template<class TValue>
        TValue as() const;

    protected:
        uint8_t byte_[internal::size_wide];

        constexpr value_t(internal::tags tag, int tiny, int byte1 = 0)
            : byte_{static_cast<uint8_t>((tag << 4) | tiny), static_cast<uint8_t>(byte1)} {}

        uint16_t short_value() const noexcept PURE;
        template<typename T>
        T as_float_of_type() const noexcept PURE;

        std::string_view get_string_bytes() const noexcept PURE;

        bool is_wide_array() const noexcept PURE;
        uint32_t count_value() const noexcept PURE;
        bool count_is_zero() const noexcept PURE;

        const value_t* deref(bool wide) const PURE;

        template<bool WIDE>
        const value_t* deref() const PURE;

        const value_t* next(bool wide) const noexcept PURE;
        template<bool WIDE>
        PURE const value_t* next() const noexcept { return next(WIDE); }

        size_t data_size() const noexcept PURE;

        friend class internal::pointer_t;
        friend class value_slot_t;
        friend class internal::heap_collection_t;
        friend class internal::heap_value_t;
        friend class array_t;
        friend class dict_t;
        template<bool WIDE>
        friend struct dict_impl_t;
    };


    void release(const value_t* val) noexcept;

    template<class TValue>
    TValue value_t::as() const {
        assert("value_t: not valid type for as");
        return TValue();
    }

    std::string to_string(const value_t* value);

    retained_const_t<value_t> new_value(nullptr_t data);
    retained_const_t<value_t> new_value(bool data);
    retained_const_t<value_t> new_value(int data);
    retained_const_t<value_t> new_value(unsigned data);
    retained_const_t<value_t> new_value(int64_t data);
    retained_const_t<value_t> new_value(uint64_t data);
    retained_const_t<value_t> new_value(float data);
    retained_const_t<value_t> new_value(double data);
    retained_const_t<value_t> new_value(std::string_view data);
    retained_const_t<value_t> new_value(const value_t* data);

} // namespace document::impl
