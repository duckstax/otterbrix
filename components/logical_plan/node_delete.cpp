#include "node_delete.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_delete_t::node_delete_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit)
        : node_t(resource, node_type::delete_t, collection) {
        append_child(match);
        append_child(limit);
    }

    node_ptr node_delete_t::deserialize(serializer::base_deserializer_t* deserializer) {
        collection_full_name_t collection = deserializer->deserialize_collection(1);
        auto children = deserializer->deserialize_nodes(2);
        return make_node_delete(deserializer->resource(),
                                collection,
                                reinterpret_cast<const node_match_ptr&>(children.at(0)),
                                reinterpret_cast<const node_limit_ptr&>(children.at(1)));
    }

    hash_t node_delete_t::hash_impl() const { return 0; }

    std::string node_delete_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$delete: {";
        bool is_first = true;
        for (auto child : children()) {
            if (!is_first) {
                stream << ", ";
            } else {
                is_first = false;
            }
            stream << child;
        }
        stream << "}";
        return stream.str();
    }

    void node_delete_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append("type", serializer::serialization_type::logical_node_delete);
        serializer->append("collection", collection_);
        serializer->append("child nodes", children_);
        serializer->end_array();
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match) {
        return {
            new node_delete_t{resource, collection, match, make_node_limit(resource, collection, limit_t::unlimit())}};
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match) {
        return {new node_delete_t{resource,
                                  collection,
                                  match,
                                  make_node_limit(resource, collection, limit_t::limit_one())}};
    }
    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        return {new node_delete_t{resource, collection, match, limit}};
    }

} // namespace components::logical_plan
