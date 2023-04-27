#pragma once

#include <components/document/core/value.hpp>
#include <components/document/internal/value_slot.hpp>

namespace document::impl {

    class array_t : public value_t {
    public:
        struct iterator {
            explicit iterator(const array_t* a) noexcept;

            uint32_t count() const noexcept PURE;
            const value_t* value() const noexcept PURE;
            explicit operator const value_t*() const noexcept;
            const value_t* operator->() const noexcept PURE;
            explicit operator bool() const noexcept PURE;
            iterator& operator++();
            iterator& operator+=(uint32_t n);

        private:
            const array_t* source_;
            const value_t* value_;
            uint32_t count_;
            uint32_t pos_;

            void set_value_();
        };

        static retained_t<array_t> new_array(uint32_t initial_count = 0);
        static retained_t<array_t> new_array(const array_t* a, copy_flags flags = default_copy);
        static const array_t* empty_array();

        uint32_t count() const noexcept PURE;
        bool empty() const noexcept PURE;
        const value_t* get(uint32_t index) const noexcept PURE;
        iterator begin() const noexcept;

        retained_t<array_t> copy(copy_flags f = default_copy) const;
        retained_t<internal::heap_collection_t> mutable_copy() const;
        void copy_children(copy_flags flags = default_copy) const;

        bool is_changed() const;

        void resize(uint32_t new_size);
        void insert(uint32_t where, uint32_t n);
        void remove(uint32_t where, uint32_t n);

        template<typename T>
        void set(uint32_t index, T t);

        template<typename T>
        void append(const T& t);

        internal::value_slot_t& slot(uint32_t index);

    private:
        array_t();

        internal::value_slot_t& appending();
    };

    template<typename T>
    void array_t::set(uint32_t index, T t) {
        slot(index).set(t);
    }

    template<typename T>
    void array_t::append(const T& t) {
        appending().set(t);
    }

    using array_iterator_t = array_t::iterator;

} // namespace document::impl
