#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_coalesce(A_Expr& node) {
        auto coalesce_args = pg_ptr_cast<PGList>(node.lexpr);
        //        D_ASSERT(coalesce_args->lst.size() > 0); // parser ensures this already

        auto coalesce_op =
            std::make_unique<expressions::operator_expression>(expressions::ExpressionType::OPERATOR_COALESCE);
        for (const auto& cell : coalesce_args->lst) {
            // get the value of the COALESCE
            auto value_expr = transform_expression(pg_ptr_cast<Node>(cell.data));
            coalesce_op->children.push_back(std::move(value_expr));
        }
        return std::move(coalesce_op);
    }
} // namespace components::sql_new::transform