#pragma once

namespace components::logical_plan {

    enum class node_type : char {
        aggregate_t,
        alias_t,
        create_table_t,
        delete_t,
        drop_table_t,
        insert_t,
        join_t,
        intersect_t,
        limit_t,
        predicate_t,
        projection_t,
        sort_t,
        update_t,
        union_t,

    };

    enum class visitation {
        visit_inputs,
        do_not_visit_inputs
    };

    enum class input_side : char {
        left,
        right
    };

    enum class expression_iteration {
        continue_t,
        break_t
    };

} // namespace components::logical_plan