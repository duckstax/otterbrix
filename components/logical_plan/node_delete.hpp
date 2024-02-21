#pragma once

#include "node.hpp"
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/statements/delete_many.hpp>
#include <components/ql/statements/delete_one.hpp>

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const components::ql::aggregate::match_t& match,
                               const components::ql::limit_t& limit);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_delete(std::pmr::memory_resource* resource, ql::delete_many_t* ql);

    node_ptr make_node_delete(std::pmr::memory_resource* resource, ql::delete_one_t* ql);

} // namespace components::logical_plan
