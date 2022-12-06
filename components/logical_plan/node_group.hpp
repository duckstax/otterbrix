#pragma once

#include <components/ql/aggregate/group.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource *resource);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_group(std::pmr::memory_resource *resource, const ql::aggregate::group_t& group);

}
