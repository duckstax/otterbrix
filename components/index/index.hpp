#pragma once

#include "forward.hpp"
#include <components/ql/index.hpp>

namespace components::index {

    class index_t {
    public:
        index_t() = delete;
        index_t(const index_t&) = delete;
        index_t& operator=(const index_t&) = delete;

        class iterator_t {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = const document_ptr;
            using difference_type = std::ptrdiff_t;
            using pointer = const document_ptr;
            using reference = const document_ptr&;

            virtual ~iterator_t();

            reference operator*() const;
            iterator_t& operator++();
            bool operator==(const iterator_t& other) const;
            bool operator!=(const iterator_t& other) const;

        private:
            virtual reference value_ref() const;
            virtual iterator_t& next();
            virtual bool equals(const iterator_t& other) const;
            virtual bool not_equals(const iterator_t& other) const;
        };

        using iterator = iterator_t;

        void insert(value_t, doc_t);
        iterator_t lower_bound(const query_t& values) const;
        iterator_t upper_bound(const query_t& values) const;
        iterator_t cbegin() const;
        iterator_t cend() const;
        [[nodiscard]] auto keys() -> std::pair<keys_base_storage_t::iterator, keys_base_storage_t::iterator>;
        std::pmr::memory_resource* resource() const;
        ql::index_type type() const;

    protected:
        index_t(std::pmr::memory_resource* resource, index_type type, const keys_base_storage_t& keys);
        virtual ~index_t();

        virtual void insert_impl(value_t value_key, doc_t) = 0;
        virtual iterator_t lower_bound_impl(const query_t& values) const;
        virtual iterator_t upper_bound_impl(const query_t& values) const;
        virtual iterator_t cbegin_impl() const;
        virtual iterator_t cend_impl() const;

    private:
        std::pmr::memory_resource* resource_;
        index_type type_;
        keys_base_storage_t keys_;
    };

    using index_raw_ptr = index_t*;
    using index_ptr = std::unique_ptr<index_t>;
} // namespace components::index
