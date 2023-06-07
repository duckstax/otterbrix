#include "create_plan_insert.hpp"
#include <components/logical_plan/node_insert.hpp>
#include <services/collection/operators/operator_insert.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_insert(
            context_collection_t* context,
            const components::logical_plan::node_ptr& node) {
        const auto *insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        return std::make_unique<operators::operator_insert>(context, insert->documents());
    }

}
