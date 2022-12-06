#pragma once

#include <components/ql/aggregate/group.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        node_group_t();

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_group(const ql::aggregate::group_t& group);

}
