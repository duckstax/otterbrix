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


class single_field_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator_t = std::less<key_t>;
    using allocator_t = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage_t = std::map<key_t, value_t, comparator_t, allocator_t>;

    single_field_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : index_t(resource)
        , data_(resource) {}

    auto insert_impl() {
    }

    auto find_impl() {
    }

private:
    storage_t data_;
};