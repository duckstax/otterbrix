#include "transfrom_common.hpp"
#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* params) {
        if (!node.whereClause) {
            return logical_plan::make_node_delete_many(
                resource,
                rangevar_to_collection(node.relation),
                logical_plan::make_node_match(resource,
                                              rangevar_to_collection(node.relation),
                                              make_compare_expression(resource, compare_type::all_true)));
        }
        collection_full_name_t collection = rangevar_to_collection(node.relation);
        collection_full_name_t collection_from = {};
        bool function_flag = false;
        if (!node.usingClause->lst.empty()) {
            collection_from = rangevar_to_collection(pg_ptr_cast<RangeVar>(node.usingClause->lst.front().data));

            auto using_first = node.usingClause->lst.front().data;
            if (nodeTag(using_first) == T_RangeVar) {
                collection_from = rangevar_to_collection(pg_ptr_cast<RangeVar>(using_first));
            } else if (nodeTag(using_first) == T_RangeFunction) {
                function_flag = true;
                collection_from =
                    alias_to_collection(pg_ptr_cast<RangeFunction>(node.usingClause->lst.front().data)->alias);
            } else {
                throw parser_exception_t{"undefined token in DELETE USING", ""};
            }
        }
        logical_plan::node_ptr res = logical_plan::make_node_delete_many(
            resource,
            collection,
            collection_from,
            logical_plan::make_node_match(resource,
                                          collection,
                                          impl::transform_a_expr(params, pg_ptr_cast<A_Expr>(node.whereClause))));
        if (function_flag) {
            res->append_child(
                impl::transform_function(*pg_ptr_cast<RangeFunction>(node.usingClause->lst.front().data), params));
        }
        return res;
    }
} // namespace components::sql::transform
