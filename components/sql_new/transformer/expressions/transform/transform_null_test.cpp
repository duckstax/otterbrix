#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_null_test(NullTest& node) {
        auto arg = transform_expression(pg_ptr_cast<Node>(node.arg));
        if (node.argisrow) {
            throw std::runtime_error("IS NULL argisrow");
        }
        ExpressionType expr_type =
            (node.nulltesttype == IS_NULL) ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

        auto result = std::make_unique<operator_expression>(expr_type, std::move(arg));
        return std::move(result);
    }
} // namespace components::sql_new::transform
