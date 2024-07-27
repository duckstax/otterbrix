#pragma once

#include <algorithm>
#include <vector>

#include <dataframe/column/column_view.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::table {
    class table_view;
    namespace detail {

        template<typename ColumnView>
        class table_view_base {
            static_assert(std::is_same_v<ColumnView, column::column_view> or
                              std::is_same_v<ColumnView, column::mutable_column_view>,
                          "table_view_base can only be instantiated with column_view or "
                          "column_view_base.");

        private:
            std::vector<ColumnView> _columns{}; ///< ColumnViews to column of equal size
            size_type _num_rows{};              ///< The numbers of elements in every column

        public:
            using iterator = decltype(std::begin(_columns));        ///< Iterator type for the table
            using const_iterator = decltype(std::cbegin(_columns)); ///< const iterator type for the table

            explicit table_view_base(std::vector<ColumnView> const& cols)
                : _columns{cols} {
                if (num_columns() > 0) {
                    std::for_each(_columns.begin(), _columns.end(), [this](ColumnView col) {
                        assertion_exception_msg(col.size() == _columns.front().size(), "Column size mismatch.");
                    });
                    _num_rows = _columns.front().size();
                } else {
                    _num_rows = 0;
                }
            }

            iterator begin() noexcept { return std::begin(_columns); }
            [[nodiscard]] const_iterator begin() const noexcept { return std::begin(_columns); }
            iterator end() noexcept { return std::end(_columns); }
            [[nodiscard]] const_iterator end() const noexcept { return std::end(_columns); }
            ColumnView const& column(size_type column_index) const;
            [[nodiscard]] size_type num_columns() const noexcept { return _columns.size(); }
            [[nodiscard]] size_type num_rows() const noexcept { return _num_rows; }
            [[nodiscard]] size_type is_empty() const noexcept { return num_columns() == 0; }
            table_view_base() = default;
            ~table_view_base() = default;
            table_view_base(table_view_base const&) = default;
            table_view_base(table_view_base&&) = default;
            table_view_base& operator=(table_view_base const&) = default;
            table_view_base& operator=(table_view_base&&) = default;
        };

        bool has_nested_columns(table::table_view const& table);
    } // namespace detail

    class table_view : public detail::table_view_base<column::column_view> {
        using detail::table_view_base<column::column_view>::table_view_base;

    public:
        using ColumnView = column::column_view;

        table_view() = default;

        table_view(std::vector<table_view> const& views);

        template<typename InputIterator>
        table_view select(InputIterator begin, InputIterator end) const {
            std::vector<column::column_view> columns(std::distance(begin, end));
            std::transform(begin, end, columns.begin(), [this](auto index) { return this->column(index); });
            return table_view(columns);
        }

        [[nodiscard]] table_view select(std::vector<size_type> const& column_indices) const;
    };

    class mutable_table_view : public detail::table_view_base<column::mutable_column_view> {
        using detail::table_view_base<column::mutable_column_view>::table_view_base;

    public:
        using ColumnView = column::mutable_column_view;

        mutable_table_view() = default;

        [[nodiscard]] column::mutable_column_view& column(size_type column_index) const {
            return const_cast<column::mutable_column_view&>(table_view_base::column(column_index));
        }

        operator table_view();

        mutable_table_view(std::vector<mutable_table_view> const& views);
    };

    inline bool nullable(table_view const& view) {
        return std::any_of(view.begin(), view.end(), [](auto const& col) { return col.nullable(); });
    }

    inline bool has_nulls(table_view const& view, std::pmr::memory_resource* resource) {
        return std::any_of(view.begin(), view.end(), [resource](auto const& col) { return col.has_nulls(resource); });
    }

    inline bool has_nested_nulls(table_view const& input, std::pmr::memory_resource* resource) {
        return std::any_of(input.begin(), input.end(), [resource](auto const& col) {
            return col.has_nulls(resource) ||
                   std::any_of(col.child_begin(), col.child_end(), [resource](auto const& child_col) {
                       return has_nested_nulls(table_view{{child_col}}, resource);
                   });
        });
    }

    std::vector<column::column_view> get_nullable_columns(table_view const& table);

    inline bool have_same_types(table_view const& lhs, table_view const& rhs) {
        return std::equal(lhs.begin(),
                          lhs.end(),
                          rhs.begin(),
                          rhs.end(),
                          [](column::column_view const& lcol, column::column_view const& rcol) {
                              return (lcol.type() == rcol.type());
                          });
    }

    table_view scatter_columns(table_view const& source, std::vector<size_type> const& map, table_view const& target);

    namespace detail {

        template<typename TableView>
        bool is_relationally_comparable(TableView const& lhs, TableView const& rhs);
        extern template bool is_relationally_comparable<table_view>(table_view const& lhs, table_view const& rhs);
        extern template bool is_relationally_comparable<mutable_table_view>(mutable_table_view const& lhs,
                                                                            mutable_table_view const& rhs);

    } // namespace detail
} // namespace components::dataframe::table
