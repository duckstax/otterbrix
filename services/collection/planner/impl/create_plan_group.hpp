#pragma once

#include <components/logical_plan/node.hpp>
#include <services/collection/operators/operator.hpp>
#include <services/memory_storage/context_storage.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_group(const context_storage_t& context,
                                              const components::logical_plan::node_ptr& node);

}
