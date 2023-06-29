#include "node_delete.hpp"
#include <sstream>
#include "node_limit.hpp"
#include "node_match.hpp"

namespace components::logical_plan {

    node_delete_t::node_delete_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const components::ql::aggregate::match_t& match,
                                 const components::ql::limit_t& limit)
        : node_t(resource, node_type::delete_t, collection) {
        append_child(make_node_match(resource, collection, match));
        append_child(make_node_limit(resource, collection, limit));
    }

    hash_t node_delete_t::hash_impl() const {
        return 0;
    }

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

    node_ptr make_node_delete(std::pmr::memory_resource* resource,
                              ql::delete_many_t *ql) {
        auto node = new node_delete_t{resource, {ql->database_, ql->collection_}, ql->match_, components::ql::limit_t::unlimit()};
        return node;
    }

    node_ptr make_node_delete(std::pmr::memory_resource* resource,
                              ql::delete_one_t *ql) {
        auto node = new node_delete_t{resource, {ql->database_, ql->collection_}, ql->match_, components::ql::limit_t::limit_one()};
        return node;
    }

} // namespace components::logical_plan
