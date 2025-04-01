#include "node_aggregate.hpp"

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_aggregate_t::node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::aggregate_t, collection) {}

    hash_t node_aggregate_t::hash_impl() const { return 0; }

    std::string node_aggregate_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$aggregate: {";
        bool is_first = true;
        for (const auto& child : children_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << child->to_string();
        }
        stream << "}";
        return stream.str();
    }

    void node_aggregate_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_map(logical_plan::to_string(type_), 2);
        serializer->append("collection", collection_);
        serializer->append("child nodes", children_);
        serializer->end_map();
    }

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection) {
        return {new node_aggregate_t(resource, collection)};
    }

} // namespace components::logical_plan