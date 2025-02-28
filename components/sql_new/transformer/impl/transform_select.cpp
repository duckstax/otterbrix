#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include "transfrom_common.hpp"
#include <expressions/aggregate_expression.hpp>
#include <expressions/expression.hpp>
#include <expressions/join_expression.hpp>
#include <expressions/sort_expression.hpp>
#include <logical_plan/node_aggregate.hpp>
#include <logical_plan/node_group.hpp>
#include <logical_plan/node_join.hpp>
#include <logical_plan/node_match.hpp>
#include <logical_plan/node_sort.hpp>

using namespace components::expressions;

namespace components::sql_new::transform {
    namespace {
        void join_dfs(std::pmr::memory_resource* resource,
                      JoinExpr* join,
                      logical_plan::node_aggregate_t*& node_agg,
                      logical_plan::node_join_t*& node_join) {
            if (nodeTag(join->larg) == T_JoinExpr) {
                join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_agg, node_join);
                // right nodes are always tables
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                auto prev = node_join;
                node_join = new logical_plan::node_join_t{resource,
                                                          {database_name_t(), collection_name_t()},
                                                          jointype_to_ql(join)};
                node_join->append_child(prev);
                node_join->append_child(new logical_plan::node_aggregate_t{resource, rangevar_to_collection(table_r)});
            } else if (nodeTag(join->larg) == T_RangeVar) {
                // bamboo end
                auto table_l = pg_ptr_cast<RangeVar>(join->larg);
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                assert(!node_join);
                assert(!node_agg);
                node_join = new logical_plan::node_join_t{resource,
                                                          {database_name_t(), collection_name_t()},
                                                          jointype_to_ql(join)};
                node_agg = new logical_plan::node_aggregate_t{resource, rangevar_to_collection(table_l)};
                node_join->append_child(node_agg);
                node_join->append_child(new logical_plan::node_aggregate_t{resource, rangevar_to_collection(table_r)});
            }
            // on
            if (join->quals) {
                // should always be A_Expr
                assert(nodeTag(join->quals) == T_A_Expr);
                auto a_expr = pg_ptr_cast<A_Expr>(join->quals);
                join_expression_field left, right;
                auto columnref_to_expr = [&](ColumnRef* cref, join_expression_field& expr) {
                    const auto& lst = cref->fields->lst;
                    assert(lst.size() == 2 || lst.size() == 3);
                    if (lst.size() == 2) {
                        expr.collection.collection = strVal(lst.front().data);
                    } else {
                        auto it = lst.begin();
                        expr.collection.database = strVal((it++)->data);
                        expr.collection.collection = strVal((it)->data);
                    }

                    expr.expr = make_scalar_expression(resource,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{strVal(lst.back().data)});
                };

                columnref_to_expr(pg_ptr_cast<ColumnRef>(a_expr->lexpr), left);
                columnref_to_expr(pg_ptr_cast<ColumnRef>(a_expr->rexpr), right);
                node_join->append_expression(
                    make_join_expression(resource,
                                         get_compare_type(strVal(a_expr->name->lst.front().data)),
                                         std::move(left),
                                         std::move(right)));
            }
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, document::impl::base_document* tape) {
        logical_plan::node_aggregate_t* agg = nullptr;
        logical_plan::node_join_t* join = nullptr;

        if (node.fromClause) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                agg = new logical_plan::node_aggregate_t{resource, rangevar_to_collection(table)};
            }
            if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                join_dfs(resource, pg_ptr_cast<JoinExpr>(from_first), agg, join);
            }
        }

        auto group = new logical_plan::node_group_t{resource, agg->collection_full_name()};
        // fields
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                switch (nodeTag(res->val)) {
                    case T_FuncCall: {
                        // group
                        auto func = pg_ptr_cast<FuncCall>(res->val);
                        auto arg = std::string{
                            strVal(pg_ptr_cast<ColumnRef>(func->args->lst.front().data)->fields->lst.front().data)};
                        auto funcname = std::string{strVal(func->funcname->lst.front().data)};

                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name.reserve(funcname.size() + arg.size() + 2);
                            expr_name.append(funcname).append("(").append(arg).append(")");
                        }

                        group->append_expression(
                            make_aggregate_expression(resource,
                                                      get_aggregate_type(funcname),
                                                      components::expressions::key_t{std::move(expr_name)},
                                                      components::expressions::key_t{std::move(arg)}));
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
                                                       components::expressions::key_t{strVal(table.front().data)}));
                        } else {
                            group->append_expression(
                                make_scalar_expression(resource,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{strVal(table.front().data)}));
                        }
                        break;
                    }
                    case T_TypeCast: // fall-through
                    case T_A_Const: {
                        // constant
                        auto value = impl::get_value(res->val, tape);
                        auto expr = make_scalar_expression(
                            resource,
                            scalar_type::get_field,
                            components::expressions::key_t{res->name ? res->name : value.second});
                        // commented: no statement to add_parameter to
                        // expr->append_param(agg->add_parameter(value.first));
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
            auto match = new logical_plan::node_match_t{resource, agg->collection_full_name()};
            auto where = pg_ptr_cast<A_Expr>(node.whereClause);
            auto expr = impl::transform_a_expr(resource, where);
            if (expr) {
                match->append_expression(expr);
            }
            agg->append_child(match);
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
            auto sort = new logical_plan::node_sort_t{resource, agg->collection_full_name()};
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                assert(nodeTag(sortby->node) == T_ColumnRef);
                auto field = strVal(pg_ptr_cast<ColumnRef>(sortby->node)->fields->lst.front().data);
                sort->append_expression(
                    new sort_expression_t{components::expressions::key_t{field},
                                          sortby->sortby_dir == SORTBY_DESC ? sort_order::desc : sort_order::asc});
            }
        }

        if (join) {
            return join;
        }
        return agg;
    }
} // namespace components::sql_new::transform
