#include "node_limit.hpp"
#include <sstream>

namespace components::logical_plan {

    node_limit_t::node_limit_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const components::ql::limit_t& limit)
        : node_t(resource, node_type::limit_t, collection)
        , limit_(limit) {
    }

    const ql::limit_t& node_limit_t::limit() const {
        return limit_;
    }

    hash_t node_limit_t::hash_impl() const {
        return 0;
    }

    std::string node_limit_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$limit: " << limit_.limit();
        return stream.str();
    }


    node_ptr make_node_limit(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             const ql::limit_t& limit) {
        auto node = new node_limit_t{resource, collection, limit};
        return node;
    }

} // namespace components::logical_plan
