#pragma once

namespace components::logical_plan {

    enum class expression_type : uint8_t {
        novalid,

        compare_eq,
        compare_ne,
        compare_gt,
        compare_lt,
        compare_gte,
        compare_lte,
        compare_regex,
        compare_any,
        compare_all,
        compare_and,
        compare_or,
        compare_not,

        aggregate_count,
        aggregate_sum,
        aggregate_min,
        aggregate_max,
        aggregate_avg,

        scalar_get_field,
        scalar_add,
        scalar_subtract,
        scalar_multiply,
        scalar_divide,
        scalar_round,
        scalar_ceil,
        scalar_floor,
        scalar_abs,
        scalar_mod,
        scalar_pow,
        scalar_sqrt
    };

    enum class node_type : uint8_t {
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
        union_t
    };

    enum class visitation : uint8_t {
        visit_inputs,
        do_not_visit_inputs
    };

    enum class input_side : uint8_t {
        left,
        right
    };

    enum class expression_iteration : uint8_t {
        continue_t,
        break_t
    };

    using hash_t = std::size_t;

} // namespace components::logical_plan