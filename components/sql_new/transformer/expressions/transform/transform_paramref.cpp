#include "sql_new/transformer/expressions/parameter_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_paramref(ParamRef& node) {
        auto expr = std::make_unique<parameter_expression>();

        if (node.number < 0) {
            throw std::runtime_error("Parameter numbers cannot be negative");
        }
        size_t param_id = node.number;

        size_t known_param_index = -1;
        // This is a named parameter, try to find an entry for it
        if (!get_param(param_id, known_param_index)) {
            // We have not seen this parameter before
            if (node.number != 0) {
                // Preserve the parameter number
                known_param_index = node.number;
            } else {
                known_param_index = get_param_count() + 1;
                param_id = known_param_index;
            }

            if (!named_param_map.count(param_id)) {
                // Add it to the named parameter map so we can find it next time it's referenced
                set_param(param_id, known_param_index);
            }
        }

        expr->id = param_id;
        size_t new_param_count = std::max(get_param_count(), known_param_index);
        set_param_count(new_param_count);
        return expr;
    }
} // namespace components::sql_new::transform