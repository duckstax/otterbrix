#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <services/memory_storage/context_storage.hpp>

namespace services::collection::planner::impl {

    components::collection::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit);

}

namespace services::table::planner::impl {

    components::base::operators::operator_ptr create_plan_aggregate(const context_storage_t& context,
                                                                    const components::logical_plan::node_ptr& node,
                                                                    components::logical_plan::limit_t limit);

}
