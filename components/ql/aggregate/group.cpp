#include "group.hpp"

namespace components::ql::aggregate {

    void append_expr(group_t& group, project_expr_ptr&& expr) {
        if (expr) {
            group.fields.push_back(std::move(expr));
        }
    }

} // namespace components::ql::aggregate
