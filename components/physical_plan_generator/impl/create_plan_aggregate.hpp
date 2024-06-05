#pragma once

#include <components/ql/aggregate/limit.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>
#include <services/memory_storage/context_storage.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_aggregate(const context_storage_t& context,
                                                  const components::logical_plan::node_ptr& node,
                                                  components::ql::limit_t limit);

}
