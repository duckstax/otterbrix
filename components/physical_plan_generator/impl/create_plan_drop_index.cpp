#include "create_plan_drop_index.hpp"
#include <components/logical_plan/node_drop_index.hpp>
#include <components/physical_plan/collection/operators/operator_drop_index.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_drop_index(const context_storage_t& context,
                                                   const components::logical_plan::node_ptr& node) {
        auto* node_drop_index = static_cast<components::logical_plan::node_drop_index_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new operators::operator_drop_index(context.at(node->collection_full_name()), node_drop_index));
        return plan;
    }

} // namespace services::collection::planner::impl
