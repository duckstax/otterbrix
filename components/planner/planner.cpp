#include "planner.hpp"

namespace components::planner {

    auto planner_t::create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node)
        -> logical_plan::node_ptr {
        assert(resource && node);
        return node;
    }

} // namespace components::planner
