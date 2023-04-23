#pragma once

#include <components/document/core/value.hpp>
#include <components/document/internal/value_slot.hpp>

namespace document::impl {

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


        uint32_t count() const noexcept PURE;
        bool empty() const noexcept PURE;
        const value_t* get(uint32_t index) const noexcept PURE;
        iterator begin() const noexcept;

        static retained_t<array_t> new_array(uint32_t initial_count = 0);
        static retained_t<array_t> new_array(const array_t *a, copy_flags flags = default_copy);
        static const array_t* empty_array();

        retained_t<array_t> copy(copy_flags f = default_copy);
        retained_t<internal::heap_collection_t> mutable_copy() const;
        void copy_children(copy_flags flags = default_copy);

        const array_t* source() const;
        bool is_changed() const;

        void resize(uint32_t new_size);
        void insert(uint32_t where, uint32_t n);
        void remove(uint32_t where, uint32_t n);

        template <typename T>
        void set(uint32_t index, T t);

        template <typename T>
        void append(const T &t);

    private:
        array_t();

        internal::value_slot_t& slot(uint32_t index);
        internal::value_slot_t& appending();

        friend class value_t;
        friend class dict_t;
        friend class dict_iterator_t;
        template<bool WIDE>
        friend struct dict_impl_t;
    };


    template <typename T>
    void array_t::set(uint32_t index, T t) {
        slot(index).set(t);
    }

    template <typename T>
    void array_t::append(const T &t) {
        appending().set(t);
    }


    using array_iterator_t = array_t::iterator;

} // namespace document::impl
