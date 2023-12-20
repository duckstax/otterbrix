#pragma once

#include "dataframe/column/column_view.hpp"

namespace components::dataframe::lists {

    class lists_column_view : private column::column_view {
    public:
        lists_column_view(column_view const& lists_column);
        lists_column_view(lists_column_view&&) = default;
        lists_column_view(const lists_column_view&) = default;
        ~lists_column_view() = default;

        lists_column_view& operator=(lists_column_view const&) = default;
        lists_column_view& operator=(lists_column_view&&) = default;

        static constexpr size_type offsets_column_index{0};
        static constexpr size_type child_column_index{1};

        using column_view::child_begin;
        using column_view::child_end;
        using column_view::has_nulls;
        using column_view::is_empty;
        using column_view::null_count;
        using column_view::null_mask;
        using column_view::offset;
        using column_view::size;
        static_assert(std::is_same_v<offset_type, size_type>, "offset_type is expected to be the same as size_type.");
        using offset_iterator = offset_type const*;

        [[nodiscard]] column_view parent() const;
        [[nodiscard]] column_view offsets() const;
        [[nodiscard]] column_view child() const;
        [[nodiscard]] column_view get_sliced_child() const;
        [[nodiscard]] offset_iterator offsets_begin() const noexcept {
            return offsets().begin<offset_type>() + offset();
        }
        [[nodiscard]] offset_iterator offsets_end() const noexcept { return offsets_begin() + size() + 1; }
    };
} // namespace components::dataframe::lists
