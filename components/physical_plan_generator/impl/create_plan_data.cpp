#include "create_plan_data.hpp"
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan/collection/operators/operator_raw_data.hpp>

namespace services::collection::planner::impl {

    operators::operator_ptr create_plan_data(const components::logical_plan::node_ptr& node) {
        const auto* data = static_cast<const components::logical_plan::node_data_t*>(node.get());
        return boost::intrusive_ptr(new operators::operator_raw_data_t(data->documents()));
    }

} // namespace services::collection::planner::impl
