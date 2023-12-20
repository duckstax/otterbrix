#pragma once

#include "node.hpp"
#include <components/ql/aggregate.hpp>

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

} // namespace components::logical_plan
