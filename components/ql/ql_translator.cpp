#include "ql_translator.hpp"

#include "aggregate.hpp"
/*
 * abstract_lqp_node.cpp
abstract_lqp_node.hpp
abstract_non_query_node.cpp
abstract_non_query_node.hpp
aggregate_node.cpp
aggregate_node.hpp


create_prepared_plan_node.cpp
create_prepared_plan_node.hpp
create_table_node.cpp
create_table_node.hpp
create_view_node.cpp
create_view_node.hpp
enable_make_for_lqp_node.hpp

functional_dependency.cpp
functional_dependency.hpp
import_node.cpp
import_node.hpp
insert_node.cpp
insert_node.hpp
intersect_node.cpp
intersect_node.hpp
join_node.cpp
join_node.hpp
limit_node.cpp
limit_node.hpp
logical_plan_root_node.cpp
logical_plan_root_node.hpp
lqp_translator.cpp
lqp_translator.hpp
lqp_unique_constraint.cpp
lqp_unique_constraint.hpp
lqp_utils.cpp
lqp_utils.hpp
mock_node.cpp
mock_node.hpp
predicate_node.cpp
predicate_node.hpp
projection_node.cpp
projection_node.hpp
sort_node.cpp
sort_node.hpp
static_table_node.cpp
static_table_node.hpp
stored_table_node.cpp
stored_table_node.hpp
union_node.cpp
union_node.hpp
update_node.cpp
update_node.hpp
validate_node.cpp
validate_node.hpp
 */

namespace components::ql {
    auto ql_translator(const ql_statement_t&) -> result_translator_ptr {
        auto* result = new result_translator;
        return {result};
    }

    auto translator_aggregate(const aggregate_t&aggregate) -> {

    }
} // namespace components::ql