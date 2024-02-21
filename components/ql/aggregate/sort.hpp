#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/key.hpp>
#include <components/ql/aggregate/forward.hpp>
#include <vector>

namespace components::ql::aggregate {

    struct sort_t final {
        static constexpr operator_type type = operator_type::sort;
        std::vector<expressions::expression_ptr> values;
    };

    void append_sort(sort_t& sort, const expressions::key_t& key, expressions::sort_order order);

    template<class OStream>
    OStream& operator<<(OStream& stream, const sort_t& sort) {
        stream << "$sort: {";
        bool is_first = true;
        for (const auto& v : sort.values) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << v->to_string();
        }
        stream << "}";
        return stream;
    }

} // namespace components::ql::aggregate