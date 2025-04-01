#include "node_create_database.hpp"

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_database_t::node_create_database_t(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& collection)
        : node_t(resource, node_type::create_database_t, collection) {}

    hash_t node_create_database_t::hash_impl() const { return 0; }

    std::string node_create_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_database: " << database_name();
        return stream.str();
    }

    void node_create_database_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append("type", std::string("node_create_database_t"));
        serializer->append("collection", collection_);
        serializer->end_array();
    }

    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection) {
        return {new node_create_database_t{resource, collection}};
    }

} // namespace components::logical_plan
