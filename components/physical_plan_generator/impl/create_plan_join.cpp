#include "create_plan_join.hpp"

#include <components/expressions/join_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/collection/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/ql/aggregate/limit.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_join(const context_storage_t& context,
                                             const components::logical_plan::node_ptr& node,
                                             components::ql::limit_t limit) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        std::pmr::vector<components::expressions::join_expression_ptr> expressions(node->expressions().get_allocator());
        expressions.reserve(node->expressions().size());
        for (const auto& expr : node->expressions()) {
            expressions.emplace_back(static_cast<components::expressions::join_expression_t*>(expr.get()));
        }
        // assign left collection as actor for join
        auto join = boost::intrusive_ptr(
            new operators::operator_join_t(context.at(node->children().front()->collection_full_name()),
                                           join_node->type(),
                                           std::move(expressions)));

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
