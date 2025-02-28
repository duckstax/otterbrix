#include "sql_new/transformer/impl/transfrom_common.hpp"

using namespace components::expressions;

namespace components::sql_new::transform::impl {
    std::pair<document::value_t, std::string> get_value(Node* node, document::impl::base_document* tape) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                bool is_true = (strVal(&pg_ptr_cast<A_Const>(cast->arg)->val) == "t");
                return {document::value_t(tape, is_true), (is_true ? "true" : "false")};
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        auto str = strVal(value);
                        return {document::value_t(tape, str), str};
                    }
                    case T_Integer: {
                        int int_value = intVal(value);
                        return {document::value_t(tape, int_value), std::to_string(int_value)};
                    }
                    case T_Float: {
                        return {document::value_t(tape, floatVal(value)), strVal(value)};
                    }
                }
            }
        }
        return {document::value_t(tape, nullptr), {}};
    }

    compare_expression_ptr transform_a_expr(std::pmr::memory_resource* resource, A_Expr* node) {
        switch (node->kind) {
            case AEXPR_AND: // fall-through
            case AEXPR_OR: {
                assert(nodeTag(node->lexpr) == T_A_Expr);
                assert(nodeTag(node->rexpr) == T_A_Expr);
                auto left = transform_a_expr(resource, pg_ptr_cast<A_Expr>(node->lexpr));
                auto right = transform_a_expr(resource, pg_ptr_cast<A_Expr>(node->rexpr));
                auto expr = make_compare_union_expression(resource,
                                                          node->kind == AEXPR_AND ? compare_type::union_and
                                                                                  : compare_type::union_or);
                auto append = [&expr](compare_expression_ptr& e) {
                    if (expr->type() == e->type()) {
                        for (auto& child : e->children()) {
                            expr->append_child(child);
                        }
                    } else {
                        expr->append_child(e);
                    }
                };
                append(left);
                append(right);
                return expr;
            }
            case AEXPR_OP: {
                assert(nodeTag(node->lexpr) == T_ColumnRef || nodeTag(node->rexpr) == T_ColumnRef);
                assert(nodeTag(node->lexpr) == T_A_Const || nodeTag(node->lexpr) == T_A_Const ||
                       nodeTag(node->rexpr) == T_TypeCast || nodeTag(node->rexpr) == T_TypeCast);
                if (nodeTag(node->lexpr) == T_ColumnRef) {
                    auto key = strVal(pg_ptr_cast<ColumnRef>(node->lexpr)->fields->lst.front().data);
                    //                    commented: no statement to get tape from & add_parameter to
                    //                    return make_compare_expression(
                    //                        resource,
                    //                        get_compare_type(strVal(node->name->lst.front().data)),
                    //                        components::expressions::key_t{key},
                    //                        statement.add_parameter(get_value(node->rexpr, statement.parameters().tape()).first))
                } else {
                    auto key = strVal(pg_ptr_cast<ColumnRef>(node->rexpr)->fields->lst.front().data);
                    //                    commented: no statement to get tape from & add_parameter to
                    //                    return make_compare_expression(
                    //                        resource,
                    //                        get_compare_type(strVal(node->name->lst.front().data)),
                    //                        components::expressions::key_t{key},
                    //                        statement.add_parameter(get_value(node->lexpr, statement.parameters().tape()).first))
                }
            } break;
        }
    }
} // namespace components::sql_new::transform::impl
