#pragma once

#include <memory>

#include <core/btree/btree.hpp>

#include "forward.hpp"
#include "index.hpp"

namespace components::index {

    class single_field_index_t final : public index_t {
    public:
        using comparator_t = std::less<value_t>;
        using storage_t = core::pmr::btree::btree_t<value_t, document_ptr, comparator_t>;
        using const_iterator = storage_t::const_iterator;

        single_field_index_t(std::pmr::memory_resource*, const keys_base_storage_t&);
        ~single_field_index_t() override;

    private:
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            impl_t(const_iterator iterator);
            index_t::iterator::reference value_ref() const override;
            iterator_impl_t* next() override;
            bool equals(const iterator_impl_t* other) const override;
            bool not_equals(const iterator_impl_t* other) const override;

        private:
            const_iterator iterator_;
        };

        auto insert_impl(value_t key, document_ptr value) -> void override;
        range find_impl(const value_t& value) const override;
        range lower_bound_impl(const value_t& value) const override;
        range upper_bound_impl(const value_t& value) const override;
        iterator cbegin_impl() const override;
        iterator cend_impl() const override;

    private:
        storage_t storage_;
    };

} // namespace components::index