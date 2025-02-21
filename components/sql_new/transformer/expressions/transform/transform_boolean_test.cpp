#include "sql_new/transformer/expressions/cast_expression.hpp"
#include "sql_new/transformer/expressions/comparison_expression.hpp"
#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/expressions/value_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    static std::unique_ptr<parsed_expression>
    transform_boolean_test_internal(std::unique_ptr<parsed_expression> argument,
                                    ExpressionType comparison_type,
                                    bool comparison_value) {
        auto bool_value = std::make_unique<value_expression>(comparison_value);
        // we cast the argument to bool to remove ambiguity wrt function binding on the comparison
        auto cast_argument = std::make_unique<cast_expression>(logical_type::BOOLEAN, std::move(argument));
        return std::make_unique<comparison_expression>(comparison_type,
                                                       std::move(cast_argument),
                                                       std::move(bool_value));
    }

    std::unique_ptr<expressions::parsed_expression> transformer::transform_boolean_test(BooleanTest& node) {
        auto argument = transform_expression(pg_ptr_cast<Node>(node.arg));

        switch (node.booltesttype) {
            case BoolTestType::IS_TRUE:
                return transform_boolean_test_internal(std::move(argument),
                                                       ExpressionType::COMPARE_NOT_DISTINCT_FROM,
                                                       true);
            case BoolTestType::IS_NOT_TRUE:
                return transform_boolean_test_internal(std::move(argument),
                                                       ExpressionType::COMPARE_DISTINCT_FROM,
                                                       true);
            case BoolTestType::IS_FALSE:
                return transform_boolean_test_internal(std::move(argument),
                                                       ExpressionType::COMPARE_NOT_DISTINCT_FROM,
                                                       false);
            case BoolTestType::IS_NOT_FALSE:
                return transform_boolean_test_internal(std::move(argument),
                                                       ExpressionType::COMPARE_DISTINCT_FROM,
                                                       false);
            case BoolTestType::IS_UNKNOWN: // IS NULL
                return std::make_unique<operator_expression>(ExpressionType::OPERATOR_IS_NULL, std::move(argument));
            case BoolTestType::IS_NOT_UNKNOWN: // IS NOT NULL
                return std::make_unique<operator_expression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(argument));
            default:
                throw std::runtime_error("Unknown boolean test type" + node.booltesttype);
        }
    }
} // namespace components::sql_new::transform
