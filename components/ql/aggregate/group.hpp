#pragma once

#include <components/expressions/expression.hpp>
#include <components/ql/aggregate/forward.hpp>

namespace components::ql::aggregate {

    struct group_t final {
        static constexpr operator_type type = operator_type::group;
        std::vector<expressions::expression_ptr> fields;
    };

    void append_expr(group_t& group, expressions::expression_ptr&& expr);

    template<class OStream>
    OStream& operator<<(OStream& stream, const group_t& group) {
        stream << "$group: {";
        bool is_first = true;
        for (const auto& field : group.fields) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << field->to_string();
        }
        stream << "}";
        return stream;
    }

} // namespace components::ql::aggregate