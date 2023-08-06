#include "create_plan.hpp"

#include "impl/create_plan_aggregate.hpp"
#include "impl/create_plan_delete.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_sort.hpp"
#include "impl/create_plan_update.hpp"

namespace services::collection::planner {

    using components::logical_plan::node_type;

    operators::operator_ptr create_plan(
            context_collection_t* context,
            const components::logical_plan::node_ptr& node,
            components::ql::limit_t limit) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, node, std::move(limit));
            case node_type::delete_t:
                return impl::create_plan_delete(context, node);
            case node_type::insert_t:
                return impl::create_plan_insert(context, node);
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::group_t:
                break;
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, node);
            default:
                break;
        }
        return nullptr;
    }

}
