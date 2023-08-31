#include "node_drop_database.hpp"
#include <sstream>

namespace components::logical_plan {

    node_drop_database_t::node_drop_database_t(std::pmr::memory_resource* resource,
                                               const collection_full_name_t& collection)
        : node_t(resource, node_type::drop_database_t, collection) {
    }

    hash_t node_drop_database_t::hash_impl() const {
        return 0;
    }

    std::string node_drop_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_database: " << database_name();
        return stream.str();
    }

    node_ptr make_node_drop_database(std::pmr::memory_resource* resource,
                                     ql::drop_database_t *ql) {
        auto node = new node_drop_database_t{resource, {ql->database_, ql->collection_}};
        return node;
    }

} // namespace components::logical_plan
