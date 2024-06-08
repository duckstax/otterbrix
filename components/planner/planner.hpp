#pragma once

#include <components/logical_plan/node.hpp>
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/ql_statement.hpp>

namespace components::planner {

    /**
    class planner_t {
    public:
        planner_t() = default;
        ~planner_t() = default;

    void create_plan(std::pmr::memory_resource* resource, ql::ql_statement_t* statement);
    void verify_plan();
    logical_plan::node_ptr release_plan();

    private:
    logical_plan::node_ptr plan_;
    };
     * */

    class planner_t {
    public:
        auto create_plan(std::pmr::memory_resource* resource, ql::ql_statement_t* statement) -> logical_plan::node_ptr;
    };

} // namespace components::planner