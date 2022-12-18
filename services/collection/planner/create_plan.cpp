#include "create_plan.hpp"

#include "impl/create_plan_match.hpp"

namespace services::collection::planner {

    using components::logical_plan::node_type;

    operators::operator_ptr create_plan(context_collection_t* context, const components::logical_plan::node_ptr& node) {
        switch (node->type()) {
            case node_type::aggregate_t:
                break;
            case node_type::match_t:
                return impl::create_plan_match(context, node);
            case node_type::group_t:
                break;
            case node_type::sort_t:
                break;
            default:
                break;
        }
        return nullptr;
    }

}
