#include "node_drop_index.hpp"
#include <sstream>

namespace components::logical_plan {

    node_drop_index_t::node_drop_index_t(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         components::ql::drop_index_t* ql)
        : node_t(resource, node_type::drop_index_t, collection)
        , ql_{ql} {}

    components::ql::drop_index_t* node_drop_index_t::get_ql() const { return ql_; }

    hash_t node_drop_index_t::hash_impl() const { return 0; }

    std::string node_drop_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_index: " << database_name() << "." << collection_name() << " name:" << ql_->name();
        return stream.str();
    }

    node_ptr make_node_drop_index(std::pmr::memory_resource* resource, components::ql::drop_index_t* ql) {
        auto node = new node_drop_index_t{resource, {ql->database_, ql->collection_}, ql};
        return node;
    }

} // namespace components::logical_plan
