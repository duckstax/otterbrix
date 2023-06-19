#pragma once

#include <components/logical_plan/node.hpp>
#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::planner {

    operators::operator_ptr create_plan(context_collection_t* context,
            const components::logical_plan::node_ptr& node,
            operators::predicates::limit_t limit);

}
