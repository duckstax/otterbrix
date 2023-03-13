#pragma once

#include "core/btree/btree.hpp"
#include "core/excutor.hpp"
#include "index/forward.hpp"
#include "index/index.hpp"
#include <memory>

namespace components::index {

    class disk_single_field_index_t final : public index_t, private actor_zeta::basic_async_actor {
    public:
        using buffer_t = std::vector<index_value_t>;
        using const_iterator = buffer_t::const_iterator;

        disk_single_field_index_t(actor_zeta::base::supervisor_abstract* disk_manager,
                                  std::pmr::memory_resource* resource,
                                  std::string name,
                                  const keys_base_storage_t& keys);
        ~disk_single_field_index_t() final;

    private:
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            explicit impl_t(const_iterator iterator);
            index_t::iterator::reference value_ref() const final;
            iterator_impl_t* next() final;
            bool equals(const iterator_impl_t* other) const final;
            bool not_equals(const iterator_impl_t* other) const final;
            iterator_impl_t *copy() const final;

        private:
            const_iterator iterator_;
        };

        auto insert_impl(value_t key, index_value_t value) -> void final;
        auto remove_impl(value_t key) -> void final;
        range find_impl(const value_t& value) const final;
        range lower_bound_impl(const value_t& value) const final;
        range upper_bound_impl(const value_t& value) const final;
        iterator cbegin_impl() const final;
        iterator cend_impl() const final;

    private:
        actor_zeta::base::supervisor_abstract* disk_manager_;
        buffer_t buffer_;
    };

} // namespace components::index
