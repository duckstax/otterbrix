#include "node_aggregate.hpp"
#include <sstream>
#include "node_match.hpp"
#include "node_group.hpp"
#include "node_sort.hpp"

namespace components::logical_plan {

    node_aggregate_t::node_aggregate_t(const ql::aggregate_statement& aggregate)
        : node_t(node_type::aggregate_t) {

        using components::ql::aggregate::operator_type;

        auto count = aggregate.count_operators();
        reserve_child(count);
        for (std::size_t i = 0; i < count; ++i) {
            auto type = aggregate.type_operator(i);
            switch (type) {
                case operator_type::match:
                    append_child(make_node_match(aggregate.get_operator<ql::aggregate::match_t>(i)));
                    break;
                case operator_type::group:
                    append_child(make_node_group(aggregate.get_operator<ql::aggregate::group_t>(i)));
                    break;
                case operator_type::sort:
                    append_child(make_node_sort(aggregate.get_operator<ql::aggregate::sort_t>(i)));
                    break;
                default:
                    break;
            }
        }
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


    node_ptr make_node_aggregate(const ql::aggregate_statement& aggregate) {
        return new node_aggregate_t(aggregate);
    }

}