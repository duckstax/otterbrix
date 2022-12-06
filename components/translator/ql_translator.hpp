#pragma once

#include <components/ql/ql_statement.hpp>
#include <components/logical_plan/node.hpp>

namespace components::translator {

    auto ql_translator(ql::ql_statement_t* statement) -> logical_plan::node_ptr;

}