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

    class multikey_index_t final : public index_t {
    public:
        using key_t = ::document::impl::value_t;
        using value_t = components::document::document_ptr;
        using comparator = std::less<key_t>;
        using allocator_type = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
        using storage = std::multimap<key_t, value_t, comparator, allocator_type>;

        multikey_index_t(actor_zeta::detail::pmr::memory_resource* resource,const keys_base_t&keys)
            : index_t(resource,keys)
            , data_(resource) {}

        auto insert_impl(key_t key, value_t) -> void  {

        }

        void find_impl(query_t query,result_set_t*)  {

        }

    private:
        storage data_;
    };
} // namespace components::index