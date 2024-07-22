#pragma once

#include <dataframe/column/column.hpp>
#include <dataframe/column/column_view.hpp>

namespace components::dataframe::dictionary {

    class dictionary_column_view : private column::column_view {
    public:
        dictionary_column_view(column_view const& dictionary_column);
        dictionary_column_view(dictionary_column_view&&) = default;
        dictionary_column_view(dictionary_column_view const&) = default;
        ~dictionary_column_view() = default;

        dictionary_column_view& operator=(dictionary_column_view const&) = default;
        dictionary_column_view& operator=(dictionary_column_view&&) = default;

        static constexpr size_type indices_column_index{0};
        static constexpr size_type keys_column_index{1};

        using column_view::has_nulls;
        using column_view::is_empty;
        using column_view::null_count;
        using column_view::null_mask;
        using column_view::offset;
        using column_view::size;

        [[nodiscard]] column_view parent() const noexcept;
        [[nodiscard]] column_view indices() const noexcept;
        [[nodiscard]] column_view get_indices_annotated(std::pmr::memory_resource* resource) const noexcept;
        [[nodiscard]] column_view keys() const noexcept;
        [[nodiscard]] data_type keys_type() const noexcept;
        [[nodiscard]] size_type keys_size() const noexcept;
    };

} // namespace components::dataframe::dictionary
