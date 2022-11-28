#pragma once

#include <components/ql/aggregate/forward.hpp>
#include <components/ql/experimental/project_expr.hpp>

namespace components::ql::aggregate {

    using project_expr_ptr = experimental::project_expr_ptr;

    struct group_t final {
        static constexpr operator_type type = operator_type::group;
        std::vector<project_expr_ptr> fields;
    };

    void append_expr(group_t& group, project_expr_ptr&& expr);


    template <class OStream>
    OStream &operator<<(OStream &stream, const group_t &group) {
        stream << "$group: {";
        bool is_first = true;
        for (const auto &field : group.fields) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << field;
        }
        stream << "}";
        return stream;
    }

} // namespace components::ql::aggregate