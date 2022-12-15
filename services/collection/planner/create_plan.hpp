#pragma once

#include <components/logical_plan/node.hpp>
#include <services/collection/operators/operator.hpp>

namespace services::collection::planner {

    operators::operator_ptr create_plan(const components::logical_plan::node_ptr& node);

}
