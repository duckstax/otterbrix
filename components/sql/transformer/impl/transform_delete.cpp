#include "sql/transformer/transformer.hpp"
#include "sql/transformer/utils.hpp"
#include "transfrom_common.hpp"
#include <logical_plan/node_delete.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node,
                                                         logical_plan::ql_param_statement_t* statement) {
        if (!node.whereClause) {
            return logical_plan::make_node_delete_many(resource, rangevar_to_collection(node.relation), {});
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
