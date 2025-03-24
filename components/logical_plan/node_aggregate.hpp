#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        void serialize(node_base_serializer_t* serializer) const final;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection);

} // namespace components::logical_plan
