#include "node_match.hpp"
#include <sstream>

namespace components::logical_plan {

    node_match_t::node_match_t(const ql::aggregate::match_t& match)
        : node_t(node_type::match_t) {
        append_expression(match.query);
    }

    hash_t node_match_t::hash_impl() const {
        return 0;
    }

    std::string node_match_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$match: ";
        for (const auto& expr : expressions_) {
            stream << expr->to_string();
        }
        return stream.str();
    }


    node_ptr make_node_match(const ql::aggregate::match_t& match) {
        return new node_match_t(match);
    }

}