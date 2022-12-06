#include "node_aggregate.hpp"
#include <sstream>

namespace components::logical_plan {

    node_aggregate_t::node_aggregate_t(std::pmr::memory_resource *resource)
        : node_t(resource, node_type::aggregate_t) {
    }

    hash_t node_aggregate_t::hash_impl() const {
        return 0;
    }

    std::string node_aggregate_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$aggregate: {";
        bool is_first = true;
        for (const auto& child : children_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << child->to_string();
        }
        stream << "}";
        return stream.str();
    }

}