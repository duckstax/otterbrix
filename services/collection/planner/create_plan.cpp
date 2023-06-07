#include "create_plan.hpp"

#include "impl/create_plan_aggregate.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_match.hpp"

namespace services::collection::planner {

    using components::logical_plan::node_type;

    operators::operator_ptr create_plan(
            context_collection_t* context,
            const components::logical_plan::node_ptr& node,
            operators::predicates::limit_t limit) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, node, std::move(limit));
            case node_type::insert_t:
                return impl::create_plan_insert(context, node);
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
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
