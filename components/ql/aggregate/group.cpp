#include "group.hpp"

namespace components::ql::aggregate {

    void append_expr(group_t& group, project_expr_ptr&& expr) {
        if (expr) {
            group.fields.push_back(std::move(expr));
        }
    }

#ifdef DEV_MODE
    std::string debug(const group_t &group) {
        std::string value = "$group: {";
        bool is_first = true;
        for (const auto &field : group.fields) {
            if (is_first) {
                is_first = false;
            } else {
                value += ", ";
            }
            value += to_string(field);
        }
        value += "}";
        return value;
    }
#endif

} // namespace components::ql::aggregate
