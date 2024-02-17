#include "create_plan_match.hpp"
#include <services/collection/operators/aggregation.hpp>
#include <services/collection/planner/create_plan.hpp>

namespace services::collection::planner::impl {

    using components::logical_plan::node_type;

    operators::operator_ptr create_plan_aggregate(context_collection_t* context,
                                                  const components::logical_plan::node_ptr& node,
                                                  components::ql::limit_t limit) {
        auto op = std::make_unique<operators::aggregation>(context);
        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    op->set_match(create_plan(context, child, limit));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, child, limit));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, child, limit));
                    break;
                default:
                    break;
            }
        }
        return op;
    }

} // namespace services::collection::planner::impl
