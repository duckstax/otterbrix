#include "sql_new/transformer/expressions/case_expression.hpp"
#include "sql_new/transformer/expressions/comparison_expression.hpp"
#include "sql_new/transformer/expressions/value_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_case_expr(CaseExpr& node) {
        auto case_node = std::make_unique<expressions::case_expression>();
        auto root_arg = transform_expression(pg_ptr_cast<Node>(node.arg));
        for (const auto& cell : node.args->lst) {
            expressions::case_check case_check;

            auto w = pg_ptr_cast<CaseWhen>(cell.data);
            auto test_raw = transform_expression(pg_ptr_cast<Node>(w->expr));
            std::unique_ptr<expressions::parsed_expression> test;
            if (root_arg) {
                case_check.when_expr =
                    std::make_unique<expressions::comparison_expression>(expressions::ExpressionType::COMPARE_EQUAL,
                                                                         root_arg->make_copy(),
                                                                         std::move(test_raw));
            } else {
                case_check.when_expr = std::move(test_raw);
            }
            case_check.then_expr = transform_expression(pg_ptr_cast<Node>(w->result));
            case_node->case_checks.push_back(std::move(case_check));
        }

        if (node.defresult) {
            case_node->else_expr = transform_expression(pg_ptr_cast<Node>(node.defresult));
        } else {
            case_node->else_expr = std::make_unique<expressions::value_expression>(true);
        }
        case_node->location = node.location;
        return case_node;
    }
} // namespace components::sql_new::transform