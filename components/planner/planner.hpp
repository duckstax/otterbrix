#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner {

    class planner_t {
    public:
        auto create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node) -> logical_plan::node_ptr;
    };

} // namespace components::planner
