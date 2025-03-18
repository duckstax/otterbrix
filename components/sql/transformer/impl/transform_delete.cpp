#include "transfrom_common.hpp"
#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* statement) {
        if (!node.whereClause) {
            return logical_plan::make_node_delete_many(
                resource,
                rangevar_to_collection(node.relation),
                logical_plan::make_node_match(resource,
                                              rangevar_to_collection(node.relation),
                                              make_compare_expression(resource, compare_type::all_true)));
        }
        auto collection = rangevar_to_collection(node.relation);
        return logical_plan::make_node_delete_many(
            resource,
            collection,
            logical_plan::make_node_match(resource,
                                          collection,
                                          impl::transform_a_expr(statement, pg_ptr_cast<A_Expr>(node.whereClause))));
    }
} // namespace components::sql::transform
