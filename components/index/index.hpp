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

            iterator_t(iterator_impl_t* ptr)
                : impl_(ptr) {}
            virtual ~iterator_t();

            reference operator*() const;
            iterator_t& operator++();
            bool operator==(const iterator_t& other) const;
            bool operator!=(const iterator_t& other) const;

            class iterator_impl_t {
            public:
                virtual ~iterator_impl_t() = default;
                virtual reference value_ref() const = 0;
                virtual iterator_t& next() = 0;
                virtual bool equals(const iterator_t& other) const = 0;
                virtual bool not_equals(const iterator_t& other) const = 0;
            };

        private:
            iterator_impl_t* impl_;
        };

        using iterator = iterator_t;
        using range = std::pair<iterator, iterator>;

        void insert(value_t, doc_t);
        iterator lower_bound(const query_t& values) const;
        iterator upper_bound(const query_t& values) const;
        iterator cbegin() const;
        iterator cend() const;
        auto keys() -> std::pair<keys_base_storage_t::iterator, keys_base_storage_t::iterator>;
        std::pmr::memory_resource* resource() const noexcept;
        ql::index_type type() const noexcept;

    protected:
        index_t(std::pmr::memory_resource* resource, index_type type, const keys_base_storage_t& keys);

        virtual void insert_impl(value_t value_key, doc_t) = 0;
        virtual iterator lower_bound_impl(const query_t& values) const  = 0;
        virtual iterator upper_bound_impl(const query_t& values) const  = 0;
        virtual iterator cbegin_impl() const  = 0;
        virtual iterator cend_impl() const  = 0;

    private:
        std::pmr::memory_resource* resource_;
        index_type type_;
        keys_base_storage_t keys_;
    };

    using index_ptr = core::pmr::unique_ptr<index_t>;

} // namespace components::index
