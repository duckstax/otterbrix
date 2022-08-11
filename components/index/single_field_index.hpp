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
        using comparator_t = std::less<value_t>;
        using storage_t = std::pmr::map<value_t, document_ptr, comparator_t>;

        single_field_index_t(actor_zeta::detail::pmr::memory_resource* resource, const keys_base_t& keys);
        ~single_field_index_t() override;
        auto insert_impl(value_t key, document_ptr value) -> void override;
        auto find_impl(query_t query, result_set_t* set) -> void override;

    private:
        storage_t data_;
    };

} // namespace components::index