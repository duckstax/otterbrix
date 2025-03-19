#include "transfrom_common.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node, logical_plan::parameter_node_t* statement) {
        logical_plan::node_match_ptr match;
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
            match =
                logical_plan::make_node_match(resource,
                                              rangevar_to_collection(node.relation),
                                              impl::transform_a_expr(statement, pg_ptr_cast<A_Expr>(node.whereClause)));
        } else {
            match = logical_plan::make_node_match(resource,
                                                  rangevar_to_collection(node.relation),
                                                  make_compare_expression(resource, compare_type::all_true));
        };

        return logical_plan::make_node_update_many(resource,
                                                   rangevar_to_collection(node.relation),
                                                   match,
                                                   update,
                                                   false);
    }
} // namespace components::sql::transform
