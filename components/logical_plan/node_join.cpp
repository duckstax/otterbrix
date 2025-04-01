#include "node_join.hpp"

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_join_t::node_join_t(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             join_type type)
        : node_t(resource, node_type::join_t, collection)
        , type_(type) {}

    join_type node_join_t::type() const { return type_; }

    hash_t node_join_t::hash_impl() const { return 0; }

    std::string node_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$join: {";
        stream << "$type: " << magic_enum::enum_name(type_);
        for (const auto& child : children_) {
            stream << ", " << child->to_string();
        }
        for (const auto& expr : expressions()) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    void node_join_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_map(logical_plan::to_string(node_t::type_), 3);
        serializer->append("node type", type_);
        serializer->append("collection", collection_);
        serializer->append("child nodes", children_);
        serializer->end_map();
    }

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, const collection_full_name_t& collection, join_type type) {
        return {new node_join_t{resource, collection, type}};
    }

} // namespace components::logical_plan
