#pragma once

#include <cstdint>

namespace components::logical_plan {

    enum class node_type : uint8_t
    {
        aggregate_t,
        alias_t,
        create_collection_t,
        create_database_t,
        create_index_t,
        data_t,
        delete_t,
        drop_collection_t,
        drop_database_t,
        drop_index_t,
        insert_t,
        join_t,
        intersect_t,
        limit_t,
        match_t,
        group_t,
        sort_t,
        update_t,
        union_t
    };

    enum class visitation : uint8_t
    {
        visit_inputs,
        do_not_visit_inputs
    };

    enum class input_side : uint8_t
    {
        left,
        right
    };

    enum class expression_iteration : uint8_t
    {
        continue_t,
        break_t
    };

} // namespace components::logical_plan
