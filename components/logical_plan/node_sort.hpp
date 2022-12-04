#pragma once

#include <components/ql/aggregate/sort.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_sort_t final : public node_t {
    public:
        explicit node_sort_t(const ql::aggregate::sort_t& sort);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_sort(const ql::aggregate::sort_t& sort);

}
