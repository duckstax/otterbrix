#pragma once

#include <components/ql/aggregate.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(const ql::aggregate_statement& aggregate);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_aggregate(const ql::aggregate_statement& aggregate);

}
