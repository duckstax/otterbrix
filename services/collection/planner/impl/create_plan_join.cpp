#include "create_plan_join.hpp"

#include <collection/planner/create_plan.hpp>
#include <components/expressions/join_expression.hpp>
#include <components/ql/aggregate/limit.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_join.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_join(const context_storage_t& context,
                                             const components::logical_plan::node_ptr& node,
                                             components::ql::limit_t limit) {
        // assign left collection as actor for join
        auto join = boost::intrusive_ptr(new operators::operator_join_t(
            context.at(node->children().front()->collection_full()),
            static_cast<components::expressions::join_expression_t*>(node->expressions().front().get())));

        operators::operator_ptr left;
        operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, node->children().front(), limit);
        }
        if (node->children().back()) {
            right = create_plan(context, node->children().back(), limit);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::collection::planner::impl
