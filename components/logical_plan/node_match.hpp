#pragma once

#include <components/ql/aggregate/match.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(const ql::aggregate::match_t& match);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_match(const ql::aggregate::match_t& match);

}
