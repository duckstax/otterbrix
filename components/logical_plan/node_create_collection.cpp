#include "node_create_collection.hpp"
#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection)
        : node_t(resource, node_type::create_collection_t, collection) {}

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
