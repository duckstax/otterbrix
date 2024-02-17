#pragma once

#include "node.hpp"
#include <components/ql/aggregate/group.hpp>

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_group(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             const ql::aggregate::group_t& group);

} // namespace components::logical_plan
