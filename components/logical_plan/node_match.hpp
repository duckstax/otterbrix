#pragma once

#include "node.hpp"
#include <components/ql/aggregate/match.hpp>

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_match(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             const ql::aggregate::match_t& match);

} // namespace components::logical_plan
