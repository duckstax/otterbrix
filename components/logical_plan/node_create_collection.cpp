#include "node_create_collection.hpp"

#include "node_serializer.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection)
        : node_t(resource, node_type::create_collection_t, collection) {}

    void node_create_collection_t::serialize(node_base_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append(type_);
        serializer->append(collection_);
        serializer->end_array();
    }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection) {
        return {new node_create_collection_t{resource, collection}};
    }

} // namespace components::logical_plan
