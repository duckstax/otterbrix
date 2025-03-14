#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::vector<expression_ptr>& expressions);

} // namespace components::logical_plan
