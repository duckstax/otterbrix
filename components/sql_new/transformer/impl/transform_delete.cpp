#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include "transfrom_common.hpp"
#include <expressions/aggregate_expression.hpp>
#include <logical_plan/node_delete.hpp>

using namespace components::expressions;

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, ql::ql_param_statement_t* statement) {
        components::ql::aggregate::match_t match;

        if (node.whereClause) {
            match.query = impl::transform_a_expr(statement, pg_ptr_cast<A_Expr>(node.whereClause));
        }

        return new logical_plan::node_delete_t{resource,
                                               rangevar_to_collection(node.relation),
                                               match,
                                               components::ql::limit_t::unlimit()};
    }
} // namespace components::sql_new::transform
