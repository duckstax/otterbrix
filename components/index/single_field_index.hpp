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

        class iterator final : public index_t::iterator {
            reference value_ref() const {}
            iterator_t& next() {}
            bool equals(const iterator_t& other) const {}
            bool not_equals(const iterator_t& other) const {}
        };

    private:
        single_field_index_t(std::pmr::memory_resource* resource, const keys_base_storage_t& keys);
        ~single_field_index_t() override;
        auto insert_impl(value_t key, document_ptr value) -> void override;
        virtual iterator_t lower_bound_impl(const query_t& values) const {}
        virtual iterator_t upper_bound_impl(const query_t& values) const {}
        virtual iterator_t cbegin_impl() const {}
        virtual iterator_t cend_impl() const {}

    private:
        storage_t data_;
    };

} // namespace components::index