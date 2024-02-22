#pragma once

#include "node.hpp"
#include <components/ql/aggregate/sort.hpp>

namespace components::logical_plan {

    class node_sort_t final : public node_t {
    public:
        explicit node_sort_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_sort(std::pmr::memory_resource* resource,
                            const collection_full_name_t& collection,
                            const ql::aggregate::sort_t& sort);

} // namespace components::logical_plan
