#pragma once

#include <climits>

#include <components/document/core/internal.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/support/endian.hpp>
#include <components/document/support/utils.hpp>
#include <components/document/support/varint.hpp>

namespace document::impl {

    namespace internal {
        class heap_array_t;
        class heap_dict_t;
    }

    class value_slot_t {
    public:
        value_slot_t();
        value_slot_t(nullptr_t);
        ~value_slot_t();
        value_slot_t(internal::heap_collection_t *md);
        value_slot_t(const value_slot_t &other) noexcept;
        value_slot_t& operator= (const value_slot_t &other) noexcept;
        value_slot_t(value_slot_t &&other) noexcept;
        value_slot_t& operator= (value_slot_t &&other) noexcept;

        bool empty() const PURE;
        explicit operator bool() const PURE;

        const value_t* as_value() const PURE;

        void set(nullptr_t);
        void set(bool b);
        void set(int32_t i);
        void set(uint32_t i);
        void set(int64_t i);
        void set(uint64_t i);
        void set(float f);
        void set(double d);
        void set(std::string_view s);
        void set_value(const value_t* v);

        template <class T> void set(const T* t)                    { set_value(t); }
        template <class T> void set(const retained_t<T> &t)        { set_value(t); }
        template <class T> void set(const retained_const_t<T> &t)  { set_value(t); }

        void copy_value(copy_flags flags);

    protected:
        friend class internal::heap_array_t;
        friend class internal::heap_dict_t;

        internal::heap_collection_t* as_mutable_collection() const;
        internal::heap_collection_t* make_mutable(internal::tags if_type);

    private:
        bool is_pointer() const noexcept PURE;
        bool is_inline() const noexcept PURE;
        const value_t* pointer() const noexcept PURE;
        const value_t* as_pointer() const noexcept PURE;
        const value_t* inline_pointer() const noexcept PURE;
        void set_pointer(const value_t *v);
        void set_inline(internal::tags tag, int tiny);

        void release_value();
        void set_value(internal::tags ag, int tiny, std::string_view bytes);
        template <class INT>
        void set_int(INT i) {
            if (i < 2048 && (!std::numeric_limits<INT>::is_signed || int64_t(i) > -2048)) {
                set_inline(internal::tag_short, (i >> 8) & 0x0F);
                _inline._value[1] = uint8_t(i & 0xFF);
            } else {
                uint8_t buf[8];
                auto size = put_int_of_length(buf, int64_t(i), !std::numeric_limits<INT>::is_signed);
                set_value(internal::tag_int,
                          int(size-1) | (std::numeric_limits<INT>::is_signed ? 0 : 0x08),
                          std::string_view(reinterpret_cast<char*>(buf), size));
            }
        }
        void set_string_or_data(internal::tags tag, std::string_view s);

        static constexpr uint8_t inline_capacity = 7;
        static constexpr uint8_t inline_tag = 0xFF;

        struct inline_t {
#ifdef __LITTLE_ENDIAN__
            uint8_t _tag;
#endif
            uint8_t _value[inline_capacity];
#ifdef __BIG_ENDIAN__
            uint8_t _tag;
#endif
        };

        union {
            uint64_t _pointer;
            inline_t _inline;
        };
    };

}
