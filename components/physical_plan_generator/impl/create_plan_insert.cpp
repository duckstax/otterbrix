#include "create_plan_insert.hpp"
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_insert(const context_storage_t& context,
                                               const components::logical_plan::node_ptr& node,
                                               components::logical_plan::limit_t limit) {
        const auto* insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new operators::operator_insert(context.at(node->collection_full_name()), insert->key_translation()));

        auto checker = boost::intrusive_ptr(new operators::primary_key_scan(context.at(node->collection_full_name())));
        checker->set_children(create_plan(context, node->children().front(), std::move(limit)));
        plan->set_children(std::move(checker));

        return plan;
    }

} // namespace services::collection::planner::impl
