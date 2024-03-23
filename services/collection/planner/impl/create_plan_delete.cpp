#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <services/collection/operators/operator_delete.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_delete(const context_storage_t& context,
                                               const components::logical_plan::node_ptr& node) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        for (auto child : node_delete->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            }
        }

        auto plan = boost::intrusive_ptr(new operators::operator_delete(context.at(node->collection_full_name())));
        plan->set_children(
            create_plan_match(context,
                              node_match,
                              static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit()));

        return plan;
    }

} // namespace services::collection::planner::impl
