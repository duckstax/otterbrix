#include <core/assert/assert.hpp>
#include <dataframe/column/slice.hpp>
#include <dataframe/detail/get_value.hpp>
#include <dataframe/lists/lists_column_view.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::lists {

    lists_column_view::lists_column_view(column_view const& lists_column)
        : column_view(lists_column) {
        assertion_exception_msg(type().id() == type_id::list, "lists_column_view only supports lists");
    }

    column::column_view lists_column_view::parent() const { return static_cast<column_view>(*this); }

    column::column_view lists_column_view::offsets() const {
        assertion_exception_msg(num_children() == 2, "lists column has an incorrect numbers of children");
        return column_view::child(offsets_column_index);
    }

    column::column_view lists_column_view::child() const {
        assertion_exception_msg(num_children() == 2, "lists column has an incorrect numbers of children");
        return column_view::child(child_column_index);
    }

    column::column_view lists_column_view::get_sliced_child() const {
        if (offset() > 0) {
            auto child_offset_start = dataframe::detail::get_value<size_type>(offsets(), offset());
            auto child_offset_end = dataframe::detail::get_value<size_type>(offsets(), offset() + size());
            return slice(child(), {child_offset_start, child_offset_end}).front();
        }

        if (size() < offsets().size() - 1) {
            auto child_offset = dataframe::detail::get_value<size_type>(offsets(), size());
            return slice(child(), {0, child_offset}).front();
        }

        return child();
    }

} // namespace components::dataframe::lists
