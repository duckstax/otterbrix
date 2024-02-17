#pragma once

#include <dataframe/column/column_view.hpp>

namespace components::dataframe::column {

    class strings_column_view : private column_view {
    public:
        strings_column_view(column_view strings_column);
        strings_column_view(strings_column_view&&) = default;
        strings_column_view(strings_column_view const&) = default;
        ~strings_column_view() = default;

        strings_column_view& operator=(strings_column_view const&) = default;
        strings_column_view& operator=(strings_column_view&&) = default;

        static constexpr size_type offsets_column_index{0};
        static constexpr size_type chars_column_index{1};

        using column_view::has_nulls;
        using column_view::is_empty;
        using column_view::null_count;
        using column_view::null_mask;
        using column_view::offset;
        using column_view::size;

        using offset_iterator = offset_type const*;
        using chars_iterator = char const*;

        [[nodiscard]] column_view parent() const;
        [[nodiscard]] column_view offsets() const;
        [[nodiscard]] offset_iterator offsets_begin() const;
        [[nodiscard]] offset_iterator offsets_end() const;
        [[nodiscard]] column_view chars() const;
        [[nodiscard]] size_type chars_size() const noexcept;
        [[nodiscard]] chars_iterator chars_begin() const;
        [[nodiscard]] chars_iterator chars_end() const;
    };

} // namespace components::dataframe::column
