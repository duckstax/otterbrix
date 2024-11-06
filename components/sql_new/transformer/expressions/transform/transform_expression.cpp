#include "sql_new/parser/nodes/parsenodes.h"
#include "sql_new/transformer/expressions/collate_expression.hpp"
#include "sql_new/transformer/expressions/default_expression.hpp"
#include "sql_new/transformer/expressions/operator_expression.hpp"
#include "sql_new/transformer/expressions/star_expression.hpp"
#include "sql_new/transformer/expressions/value_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

using namespace components::sql_new::transform::expressions;

namespace components::sql_new::transform {
    std::unique_ptr<expressions::parsed_expression> transformer::transform_a_const(A_Const& node) {
        auto constant = transform_value(node.val);
        constant->location = node.location;
        return constant;
    }

    std::unique_ptr<expressions::parsed_expression> transformer::transform_res_target(ResTarget& node) {
        auto expr = transform_expression(node.val);
        if (!expr) {
            return nullptr;
        }
        if (node.name) {
            expr->alias = node.name;
        }
        return expr;
    }

    std::unique_ptr<expressions::parsed_expression> transformer::transform_named_arg(NamedArgExpr& node) {
        auto expr = transform_expression(pg_ptr_cast<Node>(node.arg));
        if (node.name) {
            expr->alias = node.name;
        }
        return expr;
    }

    std::unique_ptr<expressions::parsed_expression> transformer::transform_collate_clause(CollateClause& node) {
        auto child = transform_expression(node.arg);
        auto collation = transform_collation(&node);
        return std::make_unique<expressions::collate_expression>(collation, std::move(child));
    }

    std::unique_ptr<parsed_expression> transformer::transform_grouping_func(GroupingFunc& grouping) {
        auto op = std::make_unique<operator_expression>(ExpressionType::GROUPING_FUNCTION);
        for (const auto& node : grouping.args->lst) {
            auto n = pg_ptr_cast<Node>(node.data);
            op->children.push_back(transform_expression(n));
        }

        return op;
    }

    std::unique_ptr<parsed_expression> transformer::transform_expression(Node& node) {
        switch (node.type) {
            case T_ColumnRef:
                return transform_columnref(pg_cast<ColumnRef>(node));
            case T_A_Const:
                return transform_a_const(pg_cast<A_Const>(node));
                //            case T_A_Expr:
                //                return TransformAExpr(pg_cast<A_Expr>(node));
                //            case T_FuncCall:
                //                return TransformFuncCall(pg_cast<FuncCall>(node));
            case T_BoolExpr:
                return transform_bool_expr(pg_cast<BoolExpr>(node));
                //            case T_TypeCast:
                //                return TransformTypeCast(pg_cast<TypeCast>(node));
            case T_CaseExpr:
                return transform_case_expr(pg_cast<CaseExpr>(node));
                //            case T_SubLink:
                //                return TransformSubquery(pg_cast<SubLink>(node));
            case T_CoalesceExpr:
                return transform_coalesce(pg_cast<A_Expr>(node));
            case T_NullTest:
                return transform_null_test(pg_cast<NullTest>(node));
            case T_ResTarget:
                return transform_res_target(pg_cast<ResTarget>(node));
            case T_ParamRef:
                return transform_paramref(pg_cast<ParamRef>(node));
            case T_NamedArgExpr:
                return transform_named_arg(pg_cast<NamedArgExpr>(node));
            case T_SetToDefault:
                return std::make_unique<default_expression>();
            case T_CollateClause:
                return transform_collate_clause(pg_cast<CollateClause>(node));
            case T_A_Indirection:
                return transform_array_indirection(pg_cast<A_Indirection>(node));
            case T_GroupingFunc:
                return transform_grouping_func(pg_cast<GroupingFunc>(node));
            case T_A_Star:
                return std::make_unique<star_expression>();
            case T_BooleanTest:
                return transform_boolean_test(pg_cast<BooleanTest>(node));
            default:
                throw std::runtime_error("Unimplemented expression type " + node_tag_to_string(node.type));
        }
    }

    std::unique_ptr<parsed_expression> transformer::transform_expression(Node* node) {
        if (!node) {
            return nullptr;
        }
        return transform_expression(*node);
    }
} // namespace components::sql_new::transform
