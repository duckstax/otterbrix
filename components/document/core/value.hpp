#pragma once

#include <iosfwd>
#include <stdint.h>
#include <components/document/support/exception.hpp>
#include <components/document/support/endian.hpp>
#include <components/document/core/internal.hpp>
#include <components/document/core/slice.hpp>

namespace document {
    class writer_t;
}

namespace document { namespace impl {
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
        default_copy        = 0,
        deep_copy           = 1,
        copy_immutables     = 2,
        deep_copy_immutables = (deep_copy | copy_immutables),
    };


    class null_value_t {
    };

    constexpr null_value_t null_value;


    class value_t {
    public:
        using time_stamp = int64_t;
        static constexpr time_stamp time_stamp_none = INT64_MIN;

        static const value_t* const true_value;
        static const value_t* const false_value;
        static const value_t* const null_value;
        static const value_t* const undefined_value;

        static const value_t* from_data(slice_t) noexcept;
        static const value_t* from_trusted_data(slice_t s) noexcept;
        value_type type() const noexcept PURE;
        bool is_equal(const value_t*) const PURE;

        bool as_bool() const noexcept PURE;
        int64_t as_int() const noexcept PURE;
        uint64_t as_unsigned() const noexcept PURE             {return (uint64_t)as_int();}
        float as_float() const noexcept PURE      {return as_float_of_type<float>();}
        double as_double() const noexcept PURE    {return as_float_of_type<double>();}
        bool is_int() const noexcept PURE         {return tag() <= internal::tag_int;}
        bool is_unsigned() const noexcept PURE    {return tag() == internal::tag_int && (_byte[0] & 0x08) != 0;}
        bool is_double() const noexcept PURE      {return tag() == internal::tag_float && (_byte[0] & 0x8);}
        bool is_undefined() const noexcept PURE   {return _byte[0] == ((internal::tag_special << 4) |
                                                                internal::special_value_undefined);}
        bool is_pointer() const noexcept PURE             {return (_byte[0] & 0x80) != 0;}
        const internal::pointer_t* as_pointer() const PURE {return (const internal::pointer_t*)this;}
        slice_t as_string() const noexcept PURE;
        slice_t as_data() const noexcept PURE;

        time_stamp as_time_stamp() const noexcept PURE;
        const array_t* as_array() const noexcept PURE;
        const dict_t* as_dict() const noexcept PURE;

        static const array_t* as_array(const value_t *v) PURE     {return v ? v->as_array() : nullptr;}
        static const dict_t* as_dict(const value_t *v) PURE      {return v ? v->as_dict()  : nullptr;}

        alloc_slice_t to_string() const;

        bool is_mutable() const PURE              {return ((size_t)this & 1) != 0;}

        shared_keys_t* shared_keys() const noexcept PURE;

        void _retain() const;
        void _release() const;

        template<class TValue> TValue as() const;

    protected:
        constexpr value_t(internal::tags tag, int tiny, int byte1 = 0)
            : _byte {(uint8_t)((tag<<4) | tiny), (uint8_t)byte1}
        { }

        static const value_t* find_root(slice_t) noexcept PURE;
        bool validate(const void* data_start, const void *data_end) const noexcept PURE;

        internal::tags tag() const noexcept PURE   {return (internal::tags)(_byte[0] >> 4);}
        unsigned tiny_value() const noexcept PURE  {return _byte[0] & 0x0F;}
        uint16_t short_value() const noexcept PURE {return (((uint16_t)_byte[0] << 8) | _byte[1]) & 0x0FFF;}
        template<typename T> T as_float_of_type() const noexcept PURE;

        slice_t get_string_bytes() const noexcept PURE;

        bool is_wide_array() const noexcept PURE     {return (_byte[0] & 0x08) != 0;}
        uint32_t count_value() const noexcept PURE   {return (((uint32_t)_byte[0] << 8) | _byte[1]) & 0x07FF;}
        bool count_is_zero() const noexcept PURE     {return _byte[1] == 0 && (_byte[0] & 0x7) == 0;}

        const value_t* deref(bool wide) const PURE;

        template <bool WIDE>
        const value_t* deref() const PURE;

        const value_t* next(bool wide) const noexcept PURE
                                {return offsetby(this, wide ? internal::size_wide : internal::size_narrow);}
        template <bool WIDE>
        PURE const value_t* next() const noexcept         {return next(WIDE);}

        size_t data_size() const noexcept PURE;

        uint8_t _byte[internal::size_wide];

        friend class internal::pointer_t;
        friend class value_slot_t;
        friend class internal::heap_collection_t;
        friend class internal::heap_value_t;
        friend class array_t;
        friend class dict_t;
        friend class encoder_t;
        friend class value_dumper_t;
        template <bool WIDE> friend struct dict_impl_t;
    };


    void release(const value_t *val) noexcept;

    NOINLINE void assign_ref(const value_t* &holder, const value_t *new_value) noexcept;

    template <typename T>
    static inline void assign_ref(T* &holder, const value_t *new_value) noexcept {
        assign_ref((const value_t*&)holder, new_value);
    }


    template<class TValue> TValue value_t::as() const {
        assert("value_t: not valid type for as");
        return TValue();
    }

    template<> bool value_t::as<bool>() const;
    template<> uint64_t value_t::as<uint64_t>() const;
    template<> int64_t value_t::as<int64_t>() const;
    template<> double value_t::as<double>() const;
    template<> std::string value_t::as<std::string>() const;

} }
