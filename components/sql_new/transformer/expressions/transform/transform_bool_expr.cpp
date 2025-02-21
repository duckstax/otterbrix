#include "sql_new/transformer/expressions/conjunction_expression.hpp"
#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    std::unique_ptr<parsed_expression> transformer::transform_bool_expr(BoolExpr& expr) {
        std::unique_ptr<parsed_expression> result;
        for (const auto& node : expr.args->lst) {
            auto next = transform_expression(pg_ptr_cast<Node>(node.data));

            switch (expr.boolop) {
                case AND_EXPR: {
                    if (!result) {
                        result = std::move(next);
                    } else {
                        result = std::make_unique<conjunction_expression>(ExpressionType::CONJUNCTION_AND,
                                                                          std::move(result),
                                                                          std::move(next));
                    }
                    break;
                }
                case OR_EXPR: {
                    if (!result) {
                        result = std::move(next);
                    } else {
                        result = std::make_unique<conjunction_expression>(ExpressionType::CONJUNCTION_OR,
                                                                          std::move(result),
                                                                          std::move(next));
                    }
                    break;
                }
                case NOT_EXPR: {
                    if (next->type == ExpressionType::COMPARE_IN) {
                        // convert COMPARE_IN to COMPARE_NOT_IN
                        next->type = ExpressionType::COMPARE_NOT_IN;
                        result = std::move(next);
                    } else if (next->type >= ExpressionType::COMPARE_EQUAL &&
                               next->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
                        // NOT on a comparison: we can negate the comparison
                        // e.g. NOT(x > y) is equivalent to x <= y
                        next->type = negate_comparison_type(next->type); //
                        result = std::move(next);
                    } else {
                        result = std::make_unique<operator_expression>(ExpressionType::OPERATOR_NOT, std::move(next));
                    }
                    break;
                }
            }
        }
        result->location = expr.location;
        return result;
    }
} // namespace components::sql_new::transform
