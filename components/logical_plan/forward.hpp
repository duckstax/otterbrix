#pragma once

namespace components::logical_plan {

    enum class logical_plan_type : char {
        Aggregate,
        Alias,
        CreateTable,
        Delete,
        DropTable,
        Insert,
        Join,
        Limit,
        Predicate,
        Projection,
        Sort,
        Update,
        Union

    };

    enum class logical_plan_input_side : char {
        Left,
        Right
    };
} // namespace components::logical_query_plan