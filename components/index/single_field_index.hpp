#pragma once

#include <memory>

#include <core/btree/btree.hpp>

#include "forward.hpp"
#include "index.hpp"

namespace components::index {

    class single_field_index_t final : public index_t {
    public:
        using comparator_t = std::less<value_t>;
        using storage_t = core::btree::pmr::btree_t<value_t, document_ptr, comparator_t>;

        single_field_index_t(std::pmr::memory_resource* resource, const keys_base_storage_t& keys);
        ~single_field_index_t() override;

    private:
        class iterator_t final : public index_t::iterator {
        public:
            reference value_ref() const {}
            iterator_t& next() {}
            bool equals(const iterator_t& other) const {}
            bool not_equals(const iterator_t& other) const {}
        };

        auto insert_impl(value_t key, document_ptr value) -> void override;
        virtual index_t::iterator lower_bound_impl(const query_t& values) const {}
        virtual index_t::iterator upper_bound_impl(const query_t& values) const {}
        virtual index_t::iterator cbegin_impl() const {}
        virtual index_t::iterator cend_impl() const {}

    private:
        storage_t data_;
    };

} // namespace components::index