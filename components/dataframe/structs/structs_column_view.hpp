#pragma once

#include <dataframe/column/column.hpp>
#include <dataframe/column/column_view.hpp>

namespace components::dataframe::structs {
    class structs_column_view : public column::column_view {
    public:
        structs_column_view(structs_column_view const&) = default;
        structs_column_view(structs_column_view&&) = default;
        ~structs_column_view() = default;
        structs_column_view& operator=(structs_column_view const&) = default;
        structs_column_view& operator=(structs_column_view&&) = default;

        explicit structs_column_view(column::column_view const& col);
        [[nodiscard]] column_view parent() const;

        using column_view::child_begin;
        using column_view::child_end;
        using column_view::has_nulls;
        using column_view::null_count;
        using column_view::null_mask;
        using column_view::num_children;
        using column_view::offset;
        using column_view::size;

        [[nodiscard]] column_view get_sliced_child(int index) const;
    };

} // namespace components::dataframe::structs
