#include "node_create_collection.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       const types::complex_logical_type& schema)
        : node_t(resource, node_type::create_collection_t, collection)
        , schema_(schema) {}

    node_ptr node_create_collection_t::deserialize(serializer::base_deserializer_t* deserializer) {
        return make_node_create_collection(deserializer->resource(), deserializer->deserialize_collection(1));
    }

    types::complex_logical_type node_create_collection_t::schema() const { return schema_; }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_create_collection_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append("type", serializer::serialization_type::logical_node_create_collection);
        serializer->append("collection", collection_);
        serializer->end_array();
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           const types::complex_logical_type& schema) {
        return {new node_create_collection_t{resource, collection, schema}};
    }

} // namespace components::logical_plan
