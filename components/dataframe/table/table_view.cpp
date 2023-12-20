#include <dataframe/column/column_view.hpp>
#include <dataframe/table/table_view.hpp>
#include <dataframe/types.hpp>

#include <algorithm>
#include <cassert>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>

namespace components::dataframe::table {

    namespace detail {
        template<typename ViewType>
        auto concatenate_column_views(std::vector<ViewType> const& views) {
            using ColumnView = typename ViewType::ColumnView;
            std::vector<ColumnView> concat_cols;
            for (auto& view : views) {
                concat_cols.insert(concat_cols.end(), view.begin(), view.end());
            }
            return concat_cols;
        }

        template<typename ColumnView>
        ColumnView const& table_view_base<ColumnView>::column(size_type column_index) const {
            return _columns.at(column_index);
        }

        // Explicit instantiation for a table of `column_view`s
        template class table_view_base<column::column_view>;

        // Explicit instantiation for a table of `mutable_column_view`s
        template class table_view_base<column::mutable_column_view>;
    } // namespace detail

    // Returns a table_view with set of specified columns
    table_view table_view::select(std::vector<size_type> const& column_indices) const {
        return select(column_indices.begin(), column_indices.end());
    }

    // Convert mutable view to immutable view
    mutable_table_view::operator table_view() {
        std::vector<column::column_view> cols{begin(), end()};
        return table_view{cols};
    }

    table_view::table_view(std::vector<table_view> const& views)
        : table_view{concatenate_column_views(views)} {}

    mutable_table_view::mutable_table_view(std::vector<mutable_table_view> const& views)
        : mutable_table_view{concatenate_column_views(views)} {}

    table_view scatter_columns(table_view const& source, std::vector<size_type> const& map, table_view const& target) {
        std::vector<column::column_view> updated_columns(target.begin(), target.end());
        // scatter(updated_table.begin(),updated_table.end(),indices.begin(),updated_columns.begin());
        for (size_type idx = 0; idx < source.num_columns(); ++idx) updated_columns[map[idx]] = source.column(idx);
        return table_view{updated_columns};
    }

    std::vector<column::column_view> get_nullable_columns(table_view const& table) {
        std::vector<column::column_view> result;
        for (auto const& col : table) {
            if (col.nullable()) {
                result.push_back(col);
            }
            for (auto it = col.child_begin(); it != col.child_end(); ++it) {
                auto const& child = *it;
                if (child.size() == col.size()) {
                    auto const child_result = get_nullable_columns(table_view{{child}});
                    result.insert(result.end(), child_result.begin(), child_result.end());
                }
            }
        }
        return result;
    }

    namespace detail {

        template<typename TableView>
        bool is_relationally_comparable(TableView const& lhs, TableView const& rhs) {
            return std::all_of(boost::iterators::make_counting_iterator<size_type>(0),
                               boost::iterators::make_counting_iterator<size_type>(lhs.num_columns()),
                               [lhs, rhs](auto const i) {
                                   return lhs.column(i).type() == rhs.column(i).type() and
                                          is_relationally_comparable(lhs.column(i).type());
                               });
        }

        // Explicit template instantiation for a table of immutable views
        template bool is_relationally_comparable<table_view>(table_view const& lhs, table_view const& rhs);

        // Explicit template instantiation for a table of mutable views
        template bool is_relationally_comparable<mutable_table_view>(mutable_table_view const& lhs,
                                                                     mutable_table_view const& rhs);

        bool has_nested_columns(table_view const& table) {
            return std::any_of(table.begin(), table.end(), [](column::column_view const& col) {
                return is_nested(col.type());
            });
        }

    } // namespace detail
} // namespace components::dataframe::table
