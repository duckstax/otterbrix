#include "transfrom_common.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    namespace {

        void join_dfs(std::pmr::memory_resource* resource,
                      JoinExpr* join,
                      logical_plan::node_join_ptr& node_join,
                      logical_plan::parameter_node_t* statement) {
            if (nodeTag(join->larg) == T_JoinExpr) {
                join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_join, statement);
                auto prev = node_join;
                node_join = logical_plan::make_node_join(resource,
                                                         {database_name_t(), collection_name_t()},
                                                         jointype_to_ql(join));
                node_join->append_child(prev);
                if (nodeTag(join->rarg) == T_RangeVar) {
                    auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                    node_join->append_child(
                        logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
                } else if (nodeTag(join->rarg) == T_RangeFunction) {
                    auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                    node_join->append_child(impl::transform_function(*func, statement));
                }
            } else if (nodeTag(join->larg) == T_RangeVar) {
                // bamboo end
                auto table_l = pg_ptr_cast<RangeVar>(join->larg);
                assert(!node_join);
                node_join = logical_plan::make_node_join(resource, {}, jointype_to_ql(join));
                node_join->append_child(logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_l)));
                if (nodeTag(join->rarg) == T_RangeVar) {
                    auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                    node_join->append_child(
                        logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
                } else if (nodeTag(join->rarg) == T_RangeFunction) {
                    auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                    node_join->append_child(impl::transform_function(*func, statement));
                }
            } else if (nodeTag(join->larg) == T_RangeFunction) {
                assert(!node_join);
                node_join = logical_plan::make_node_join(resource,
                                                         {database_name_t(), collection_name_t()},
                                                         jointype_to_ql(join));
                node_join->append_child(impl::transform_function(*pg_ptr_cast<RangeFunction>(join->larg), statement));
                if (nodeTag(join->rarg) == T_RangeVar) {
                    auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                    node_join->append_child(
                        logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
                } else if (nodeTag(join->rarg) == T_RangeFunction) {
                    auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                    node_join->append_child(impl::transform_function(*func, statement));
                }
            } else {
                throw parser_exception_t{"incorrect type for join join->larg node",
                                         node_tag_to_string(nodeTag(join->larg))};
            }
            // on
            if (join->quals) {
                // should always be A_Expr
                assert(nodeTag(join->quals) == T_A_Expr);
                auto a_expr = pg_ptr_cast<A_Expr>(join->quals);
                node_join->append_expression(impl::transform_a_expr(statement, a_expr));
            } else {
                node_join->append_expression(make_compare_expression(resource, compare_type::all_true));
            }
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, logical_plan::parameter_node_t* statement) {
        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;
        // temp solution for custom function
        logical_plan::node_ptr overlying_func = nullptr;

        if (node.fromClause) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                agg = logical_plan::make_node_aggregate(resource, rangevar_to_collection(table));
            } else if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                agg = logical_plan::make_node_aggregate(resource, {});
                join_dfs(resource, pg_ptr_cast<JoinExpr>(from_first), join, statement);
                agg->append_child(join);
            } else if (nodeTag(from_first) == T_RangeFunction) {
                agg = logical_plan::make_node_aggregate(resource, {});
                agg->append_child(impl::transform_function(*pg_ptr_cast<RangeFunction>(from_first), statement));
            }
        } else {
            throw parser_exception_t{"otterbrix currently does not support SELECT without FROM", ""};
        }

        auto group = logical_plan::make_node_group(resource, agg->collection_full_name());
        // fields
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                switch (nodeTag(res->val)) {
                    case T_FuncCall: {
                        // group
                        auto func = pg_ptr_cast<FuncCall>(res->val);
                        auto funcname = std::string{strVal(func->funcname->lst.front().data)};
                        auto type = get_aggregate_type(funcname);

                        if (type == aggregate_type::invalid) {
                            // TODO: parse custom function here
                            overlying_func = impl::transform_function(*func, statement);
                        } else {
                            auto arg = std::string{
                                strVal(pg_ptr_cast<ColumnRef>(func->args->lst.front().data)->fields->lst.front().data)};

                            std::string expr_name;
                            if (res->name) {
                                expr_name = res->name;
                            } else {
                                expr_name.reserve(funcname.size() + arg.size() + 2);
                                expr_name.append(funcname).append("(").append(arg).append(")");
                            }

                            group->append_expression(
                                make_aggregate_expression(resource,
                                                          type,
                                                          components::expressions::key_t{std::move(expr_name)},
                                                          components::expressions::key_t{std::move(arg)}));
                        }
                        break;
                    }
                    case T_ColumnRef: {
                        // field
                        auto table = pg_ptr_cast<ColumnRef>(res->val)->fields->lst;

                        if (nodeTag(table.front().data) == T_A_Star) {
                            // ???
                            break;
                        }
                        if (res->name) {
                            group->append_expression(
                                make_scalar_expression(resource,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{res->name},
                                                       components::expressions::key_t{strVal(table.back().data)}));
                        } else {
                            group->append_expression(
                                make_scalar_expression(resource,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{strVal(table.back().data)}));
                        }
                        break;
                    }
                    case T_TypeCast: // fall-through
                    case T_A_Const: {
                        // constant
                        auto value = impl::get_value(res->val, statement->parameters().tape());
                        auto expr = make_scalar_expression(
                            resource,
                            scalar_type::get_field,
                            components::expressions::key_t{res->name ? res->name : value.second});
                        expr->append_param(statement->add_parameter(value.first));
                        group->append_expression(expr);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unknown node type: " + node_tag_to_string(nodeTag(res->val)));
                }
            }
        }

        // where
        if (node.whereClause) {
            auto where = pg_ptr_cast<A_Expr>(node.whereClause);
            auto expr = impl::transform_a_expr(statement, where);
            if (expr) {
                agg->append_child(logical_plan::make_node_match(resource, agg->collection_full_name(), expr));
            }
        }

        // group by
        if (node.groupClause) {
            // commented: current parser does not translate this clause to any logical plan
            // todo: add groupClause correctness check
        }

        if (!group->expressions().empty()) {
            agg->append_child(group);
        }

        // order by
        if (node.sortClause) {
            std::vector<expression_ptr> expressions;
            expressions.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                assert(nodeTag(sortby->node) == T_ColumnRef);
                auto field = strVal(pg_ptr_cast<ColumnRef>(sortby->node)->fields->lst.back().data);
                expressions.emplace_back(
                    make_sort_expression(components::expressions::key_t{field},
                                         sortby->sortby_dir == SORTBY_DESC ? sort_order::desc : sort_order::asc));
            }
            agg->append_child(logical_plan::make_node_sort(resource, agg->collection_full_name(), expressions));
        }

        if (overlying_func) {
            overlying_func->append_child(agg);
            return overlying_func;
        }
        return agg;
    }
} // namespace components::sql::transform
