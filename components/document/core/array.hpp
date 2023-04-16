#pragma once

#include <components/document/core/value.hpp>

namespace document::impl {

    class mutable_array_t;

    class array_t : public value_t {
    public:

        struct iterator {
            explicit iterator(const value_t* a) noexcept; //todo: replace to array_t

            uint32_t count() const noexcept PURE;
            const value_t* value() const noexcept PURE;
            explicit operator const value_t*() const noexcept;
            const value_t* operator->() const noexcept PURE;
            const value_t* read() noexcept;
            explicit operator bool() const noexcept PURE;
            iterator& operator++();
            iterator& operator+=(uint32_t);

            const value_t* second() const noexcept PURE;
            const value_t* first_value() const noexcept PURE;
            const value_t* deref(const value_t*) const noexcept PURE;
            const value_t* operator[](unsigned index) const noexcept PURE;
            size_t index_of(const value_t* v) const noexcept PURE;
            void offset(uint32_t n);
            bool is_mutable() const noexcept PURE;

        protected:
            //todo: move to private
            const value_t* first_;
            uint32_t count_;
            uint8_t width_;

            iterator() = default;

        private:
            const value_t* value_;

            friend class array_t;
            friend class dict_iterator_t; //todo: delete
        };


        constexpr array_t();

        uint32_t count() const noexcept PURE;
        bool empty() const noexcept PURE;
        const value_t* get(uint32_t index) const noexcept PURE;
        mutable_array_t* as_mutable() const PURE;
        static const array_t* const empty_array;

        iterator begin() const noexcept;

    protected:
        internal::heap_array_t* heap_array() const;

    private:
        friend class value_t;
        friend class dict_t;
        friend class dict_iterator_t;
        template<bool WIDE>
        friend struct dict_impl_t;
        friend class internal::heap_array_t;
    };


    using array_iterator_t = array_t::iterator;

} // namespace document::impl
