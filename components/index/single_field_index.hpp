#pragma once

#include <map>
#include <memory>
#include <scoped_allocator>

#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/session/session.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>

#include "index.hpp"

namespace components::index {

    class single_field_index_t final : public index_t {
    public:
        using comparator_t = std::less<key_t>;
        //using allocator_t = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
        using storage_t = std::pmr::map<key_t, value_t, comparator_t>;

        single_field_index_t(actor_zeta::detail::pmr::memory_resource* resource, const keys_base_t& keys)
            : index_t(resource, keys)
            , data_(resource) {}

        auto insert_impl(key_t key, document_ptr value) -> void {
            data_.emplace(key, value);
        }

        auto find_impl(query_t query, result_set_t* set) -> void {
            for (auto& i : query) {
                auto it = data_.find(i);
                set->append(it->second);
            }
        }


    private:
        storage_t data_;
    };

} // namespace components::index