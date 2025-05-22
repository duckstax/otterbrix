#include "node_insert.hpp"

#include "node_data.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::insert_t, collection) {}

    node_ptr node_insert_t::deserialize(serializer::base_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);
        auto children = deserializer->deserialize_nodes(2);
        auto res = make_node_insert(deserializer->resource(), collection);
        for (const auto& child : children) {
            res->append_child(child);
        }
        return res;
    }

    hash_t node_insert_t::hash_impl() const { return 0; }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: {";
        stream << children_.front()->to_string();
        stream << "}";
        return stream.str();
    }

    void node_insert_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append("type", serializer::serialization_type::logical_node_insert);
        serializer->append("collection", collection_);
        serializer->append("child nodes", children_);
        serializer->end_array();
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection) {
        return {new node_insert_t{resource, collection}};
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, documents));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(documents)));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     document_ptr document) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, {std::move(document)}));
        return res;
    }

} // namespace components::logical_plan
