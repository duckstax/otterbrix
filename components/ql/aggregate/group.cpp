#include "group.hpp"
#include <sstream>

namespace components::ql::aggregate {

    void append_expr(group_t& group, project_expr_ptr&& expr) {
        if (expr) {
            group.fields.push_back(std::move(expr));
        }
    }

#ifdef DEV_MODE
    std::string debug(const group_t &group) {
        std::stringstream stream;
        stream << group;
        return stream.str();
    }
#endif

} // namespace components::ql::aggregate
