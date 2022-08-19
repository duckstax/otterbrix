#pragma once

#include "core/pmr_unique.hpp"
#include "forward.hpp"
#include <components/ql/index.hpp>

namespace components::index {

    class index_t {
    public:
        index_t() = delete;
        index_t(const index_t&) = delete;
        index_t& operator=(const index_t&) = delete;
        using pointer = index_t*;

        virtual ~index_t();

        class iterator_t final {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = const document_ptr;
            using difference_type = std::ptrdiff_t;
            using pointer = const document_ptr;
            using reference = const document_ptr&;

            class iterator_impl_t;

            explicit iterator_t(iterator_impl_t* );
            virtual ~iterator_t();

            reference operator*() const;
            iterator_t& operator++();

            class iterator_impl_t {
            public:
                virtual ~iterator_impl_t() = default;
                virtual reference value_ref() const = 0;
                virtual iterator_impl_t* next() = 0;
            };

        private:
            iterator_impl_t* impl_;
        };

        using iterator = iterator_t;
        using range = std::pair<iterator, uint>;

        void insert(value_t, doc_t);
        range find(const value_t& value) const;
        range lower_bound(const value_t& value) const;
        range upper_bound(const value_t& value) const;
        auto keys() -> std::pair<keys_base_storage_t::iterator, keys_base_storage_t::iterator>;
        std::pmr::memory_resource* resource() const noexcept;
        ql::index_type type() const noexcept;

    protected:
        index_t(std::pmr::memory_resource* resource, index_type type, const keys_base_storage_t& keys);

        virtual void insert_impl(value_t value_key, doc_t) = 0;
        virtual range find_impl(const value_t& value) const = 0;
        virtual range lower_bound_impl(const value_t& value) const = 0;
        virtual range upper_bound_impl(const value_t& value) const = 0;

    private:
        std::pmr::memory_resource* resource_;
        index_type type_;
        keys_base_storage_t keys_;
    };

    using index_ptr = core::pmr::unique_ptr<index_t>;

} // namespace components::index
