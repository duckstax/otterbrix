#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include "transfrom_common.hpp"
#include <expressions/aggregate_expression.hpp>
#include <logical_plan/node_update.hpp>

using namespace components::expressions;

namespace components::sql_new::transform {
    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node) {
        components::ql::aggregate::match_t match;
        components::document::document_ptr update = document::make_document(resource);
        // set
        {
            auto doc_value = document::make_document(update->get_allocator());
            auto tape = std::make_unique<document::impl::base_document>(update->get_allocator());

            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                doc_value->set(res->name, impl::get_value(res->val, tape.get()).first);
            }
            update->set("$set", doc_value);
        }

        // where
        if (node.whereClause) {
            // (not working due to tape & expr problem)
            match.query = impl::transform_a_expr(resource, pg_ptr_cast<A_Expr>(node.whereClause));
        } else {
            match.query = make_compare_expression(resource, compare_type::all_true);
        };

        return new logical_plan::node_update_t{resource,
                                               rangevar_to_collection(node.relation),
                                               match,
                                               components::ql::limit_t::unlimit(),
                                               update,
                                               false};
    }
} // namespace components::sql_new::transform
