#pragma once

#include <components/ql/aggregate/match.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource *resource);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_match(std::pmr::memory_resource *resource, const ql::aggregate::match_t& match);

}
