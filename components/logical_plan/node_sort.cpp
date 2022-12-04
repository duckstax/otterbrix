#include "node_sort.hpp"
#include <sstream>

namespace components::logical_plan {

    node_sort_t::node_sort_t(const ql::aggregate::sort_t& sort)
        : node_t(node_type::sort_t) {
        append_expressions(sort.values);
    }

    hash_t node_sort_t::hash_impl() const {
        return 0;
    }

    std::string node_sort_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$sort: {";
        bool is_first = true;
        for (const auto& expr : expressions_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }


    node_ptr make_node_sort(const ql::aggregate::sort_t& sort) {
        return new node_sort_t(sort);
    }

}