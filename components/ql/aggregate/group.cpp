#include "group.hpp"

namespace components::ql::aggregate {

    void append_expr(group_t& group, expressions::expression_ptr&& expr) {
        if (expr) {
            group.fields.push_back(std::move(expr));
        }
    }

} // namespace components::ql::aggregate
