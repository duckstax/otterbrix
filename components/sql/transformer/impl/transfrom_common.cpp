#include <components/sql/transformer/impl/transfrom_common.hpp>

#include <components/logical_plan/node_function.hpp>

using namespace components::expressions;

namespace components::sql::transform::impl {
    std::pair<document::value_t, std::string> get_value(Node* node, document::impl::base_document* tape) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(cast->arg)->val)) == "t";
                return {document::value_t(tape, is_true), (is_true ? "true" : "false")};
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        return {document::value_t(tape, str), str};
                    }
                    case T_Integer: {
                        int64_t int_value = intVal(value);
                        return {document::value_t(tape, int_value), std::to_string(int_value)};
                    }
                    case T_Float: {
                        return {document::value_t(tape, floatVal(value)), strVal(value)};
                    }
                }
            }
            case T_ColumnRef: {
                std::string str = strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data);
                return {document::value_t(tape, str), str};
            }
        }
        return {document::value_t(tape, nullptr), {}};
    }

    compare_expression_ptr transform_a_expr(logical_plan::parameter_node_t* statement, A_Expr* node) {
        switch (node->kind) {
            case AEXPR_AND: // fall-through
            case AEXPR_OR: {
                assert(nodeTag(node->lexpr) == T_A_Expr || nodeTag(node->lexpr) == T_A_Indirection);
                assert(nodeTag(node->rexpr) == T_A_Expr || nodeTag(node->rexpr) == T_A_Indirection);
                compare_expression_ptr left;
                compare_expression_ptr right;
                if (nodeTag(node->lexpr) == T_A_Expr) {
                    left = transform_a_expr(statement, pg_ptr_cast<A_Expr>(node->lexpr));
                } else {
                    left = transform_a_indirection(statement, pg_ptr_cast<A_Indirection>(node->lexpr));
                }
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(statement, pg_ptr_cast<A_Expr>(node->rexpr));
                } else {
                    right = transform_a_indirection(statement, pg_ptr_cast<A_Indirection>(node->rexpr));
                }
                auto expr = make_compare_union_expression(statement->parameters().resource(),
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
                assert(nodeTag(node->lexpr) == T_ColumnRef || nodeTag(node->rexpr) == T_ColumnRef ||
                       nodeTag(node->lexpr) == T_A_Const || nodeTag(node->rexpr) == T_A_Const ||
                       nodeTag(node->lexpr) == T_TypeCast || nodeTag(node->rexpr) == T_TypeCast);
                if (nodeTag(node->lexpr) == T_ColumnRef) {
                    auto key_left = strVal(pg_ptr_cast<ColumnRef>(node->lexpr)->fields->lst.back().data);
                    if (nodeTag(node->rexpr) == T_ColumnRef) {
                        auto key_right = strVal(pg_ptr_cast<ColumnRef>(node->rexpr)->fields->lst.back().data);
                        return make_compare_expression(statement->parameters().resource(),
                                                       get_compare_type(strVal(node->name->lst.front().data)),
                                                       components::expressions::key_t{key_left},
                                                       components::expressions::key_t{key_right});
                    }
                    return make_compare_expression(
                        statement->parameters().resource(),
                        get_compare_type(strVal(node->name->lst.front().data)),
                        components::expressions::key_t{key_left},
                        statement->add_parameter(get_value(node->rexpr, statement->parameters().tape()).first));
                } else {
                    auto key = strVal(pg_ptr_cast<ColumnRef>(node->rexpr)->fields->lst.back().data);
                    return make_compare_expression(
                        statement->parameters().resource(),
                        get_compare_type(strVal(node->name->lst.back().data)),
                        components::expressions::key_t{key},
                        statement->add_parameter(get_value(node->lexpr, statement->parameters().tape()).first));
                }
            }
            case AEXPR_NOT: {
                assert(nodeTag(node->rexpr) == T_A_Expr || nodeTag(node->rexpr) == T_A_Indirection);
                compare_expression_ptr right;
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(statement, pg_ptr_cast<A_Expr>(node->rexpr));
                } else {
                    right = transform_a_indirection(statement, pg_ptr_cast<A_Indirection>(node->rexpr));
                }
                auto expr = make_compare_union_expression(statement->parameters().resource(), compare_type::union_not);
                if (expr->type() == right->type()) {
                    for (auto& child : right->children()) {
                        expr->append_child(child);
                    }
                } else {
                    expr->append_child(right);
                }
                return expr;
            }
            default:
                throw std::runtime_error("Unsupported node type: " + expr_kind_to_string(node->kind));
        }
    }
    components::expressions::compare_expression_ptr transform_a_indirection(logical_plan::parameter_node_t* statement,
                                                                            A_Indirection* node) {
        if (node->arg->type == T_A_Expr) {
            return transform_a_expr(statement, pg_ptr_cast<A_Expr>(node->arg));
        } else {
            throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node->type));
        }
    }

    logical_plan::node_ptr transform_function(RangeFunction& node, logical_plan::parameter_node_t* statement) {
        auto func_call = pg_ptr_cast<FuncCall>(pg_ptr_cast<List>(node.functions->lst.front().data)->lst.front().data);
        return transform_function(*func_call, statement);
    }

    logical_plan::node_ptr transform_function(FuncCall& node, logical_plan::parameter_node_t* statement) {
        std::string funcname = strVal(node.funcname->lst.front().data);
        std::pmr::vector<core::parameter_id_t> args;
        args.reserve(node.args->lst.size());
        for (const auto& arg : node.args->lst) {
            auto v = impl::get_value(pg_ptr_cast<Node>(arg.data), statement->parameters().tape());
            args.emplace_back(statement->add_parameter(v.first));
        }
        return logical_plan::make_node_function(statement->parameters().resource(),
                                                std::move(funcname),
                                                std::move(args));
    }

} // namespace components::sql::transform::impl
