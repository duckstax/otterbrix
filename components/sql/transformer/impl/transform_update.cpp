#include "transfrom_common.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    update_expr_ptr transform_update_expr(Node* node,
                                          const collection_full_name_t& to,
                                          const collection_full_name_t& from,
                                          logical_plan::parameter_node_t* params) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto value = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(value->arg)->val)) == "t";
                core::parameter_id_t id =
                    params->add_parameter(document::value_t(params->parameters().tape(), is_true));
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                core::parameter_id_t id;
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        id = params->add_parameter(document::value_t(params->parameters().tape(), str));
                        break;
                    }
                    case T_Integer: {
                        int64_t int_value = intVal(value);
                        id = params->add_parameter(document::value_t(params->parameters().tape(), int_value));
                        break;
                    }
                    case T_Float: {
                        float float_value = floatVal(value);
                        id = params->add_parameter(document::value_t(params->parameters().tape(), float_value));
                        break;
                    }
                    default:
                        assert(false);
                }
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_Expr: {
                auto expr = pg_ptr_cast<A_Expr>(node);
                switch (expr->kind) {
                    case AEXPR_OP: {
                        update_expr_ptr res;
                        auto t = pg_ptr_cast<ResTarget>(expr->name->lst.front().data);
                        switch (*t->name) {
                            case '+':
                                res = new update_expr_calculate_t(update_expr_type::add);
                                break;
                            case '-':
                                res = new update_expr_calculate_t(update_expr_type::sub);
                                break;
                            case '*':
                                res = new update_expr_calculate_t(update_expr_type::mult);
                                break;
                            case '/':
                                res = new update_expr_calculate_t(update_expr_type::div);
                                break;
                            case '%':
                                res = new update_expr_calculate_t(update_expr_type::mod);
                                break;
                            case '!':
                                res = new update_expr_calculate_t(update_expr_type::neg);
                                break;
                        }
                        assert(res);
                        res->left() = transform_update_expr(expr->lexpr, to, from, params);
                        res->right() = transform_update_expr(expr->rexpr, to, from, params);
                        return res;
                    }
                    default:
                        assert(false);
                }
            }
            case T_A_Indirection: {
                auto n = pg_ptr_cast<A_Indirection>(node);
                return transform_update_expr(n->arg, to, from, params);
            }
            case T_ColumnRef: {
                // by default it asigns column ref to "to" side of the update
                // TODO: handle undefined case (requires schema check later)
                // TODO: handle table selection ("to" and "from" if defined or an error)
                auto ref = pg_ptr_cast<ColumnRef>(node);
                std::string key = strVal(ref->fields->lst.back().data);
                if (ref->fields->lst.size() == 1) {
                    // can`t check table here
                    return {new update_expr_get_value_t(expressions::key_t{key},
                                                        update_expr_get_value_t::side_t::undefined)};
                } else if (ref->fields->lst.size() == 2) {
                    // just table
                    std::string table = strVal(ref->fields->lst.front().data);
                    if (table == to.collection) {
                        return {
                            new update_expr_get_value_t(expressions::key_t{key}, update_expr_get_value_t::side_t::to)};
                    } else if (table == from.collection) {
                        return {new update_expr_get_value_t(expressions::key_t{key},
                                                            update_expr_get_value_t::side_t::from)};
                    } else {
                        throw parser_exception_t("incorrect column path in UPDATE call", "");
                    }
                } else if (ref->fields->lst.size() == 3) {
                    // database + table
                    // TODO: technically this is a valid syntax and will fail here:
                    // UPDATE Table.Column SET DB.Table.Column
                    collection_full_name_t check_table{strVal(ref->fields->lst.front().data),
                                                       strVal((++ref->fields->lst.begin())->data)};
                    if (check_table == to) {
                        return {
                            new update_expr_get_value_t(expressions::key_t{key}, update_expr_get_value_t::side_t::to)};
                    } else if (check_table == from) {
                        return {new update_expr_get_value_t(expressions::key_t{key},
                                                            update_expr_get_value_t::side_t::from)};
                    } else {
                        throw parser_exception_t("incorrect column path in UPDATE call", "");
                    }
                } else {
                    throw parser_exception_t("incorrect column path in UPDATE call", "");
                }
            }
        }
    }

    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node, logical_plan::parameter_node_t* params) {
        logical_plan::node_match_ptr match;
        std::pmr::vector<update_expr_ptr> updates(resource);
        collection_full_name_t to = rangevar_to_collection(node.relation);
        collection_full_name_t from;

        if (!node.fromClause->lst.empty()) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                from = rangevar_to_collection(pg_ptr_cast<RangeVar>(from_first));
            } else {
                throw parser_exception_t{"undefined token in UPDATE FROM", ""};
            }
        }
        // set
        {
            for (auto target : node.targetList->lst) {
                // TODO: SET is hardcoded to be from a const value here:
                auto res = pg_ptr_cast<ResTarget>(target.data);
                updates.emplace_back(new update_expr_set_t(expressions::key_t{res->name}));
                updates.back()->left() = transform_update_expr(res->val, to, from, params);
            }
        }

        // where
        if (node.whereClause) {
            match =
                logical_plan::make_node_match(resource,
                                              to,
                                              impl::transform_a_expr(params, pg_ptr_cast<A_Expr>(node.whereClause)));
        } else {
            match =
                logical_plan::make_node_match(resource, to, make_compare_expression(resource, compare_type::all_true));
        }

        if (from.empty()) {
            return logical_plan::make_node_update_many(resource, to, match, updates, false);
        } else {
            return logical_plan::make_node_update_many(resource, to, from, match, updates, false);
        }
    }
} // namespace components::sql::transform
