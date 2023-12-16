#include "create_plan_sort.hpp"
#include <components/expressions/sort_expression.hpp>
#include <services/collection/operators/operator_sort.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_sort(context_collection_t* context,
                                             const components::logical_plan::node_ptr& node) {
        auto sort = std::make_unique<operators::operator_sort_t>(context);
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&sort](const components::expressions::expression_ptr& expr) {
                          const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
                          sort->add(sort_expr->key().as_string(),
                                    operators::operator_sort_t::order(sort_expr->order()));
                      });
        return sort;
    }

} // namespace services::collection::planner::impl
