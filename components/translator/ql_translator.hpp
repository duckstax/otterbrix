#pragma once

#include <components/logical_plan/node.hpp>
#include <components/ql/ql_statement.hpp>

namespace components::translator {

    auto ql_translator(std::pmr::memory_resource* resource, ql::ql_statement_t* statement) -> logical_plan::node_ptr;

}