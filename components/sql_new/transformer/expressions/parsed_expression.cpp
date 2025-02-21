#include "parsed_expression.hpp"

namespace components::sql_new::transform::expressions {
    bool parsed_expression::is_same(const components::sql_new::transform::expressions::base_expression& other) const {
        if (!base_expression::is_same(other)) {
            return false;
        }

        // calling is_same for all types...
        return true;
    }

    void parsed_expression::copy_properties(const parsed_expression &other) {
        type = other.type;
        expression_class = other.expression_class;
        alias = other.alias;
        location = other.location;
    }
} // namespace components::sql_new::transform::expressions
