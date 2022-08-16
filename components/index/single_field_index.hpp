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
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            index_t::iterator::reference value_ref() const override {}
            iterator_t& next() override {}
            bool equals(const iterator_t& other) const {}
            bool not_equals(const iterator_t& other) const {}
        };

        auto insert_impl(value_t key, document_ptr value) -> void override;
        index_t::iterator lower_bound_impl(const query_t& values) const override;
        index_t::iterator upper_bound_impl(const query_t& values) const override;
        index_t::iterator cbegin_impl() const override;
        index_t::iterator cend_impl() const override;

    private:
        storage_t data_;
    };

} // namespace components::index