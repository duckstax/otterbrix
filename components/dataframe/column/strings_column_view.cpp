#include <core/assert/assert.hpp>
#include <dataframe/column/strings_column_view.hpp>

namespace components::dataframe::column {

    strings_column_view::strings_column_view(column_view strings_column)
        : column_view(strings_column) {
        assertion_exception_msg(type().id() == type_id::string, "strings_column_view only supports strings");
    }

    column_view strings_column_view::parent() const { return static_cast<column_view>(*this); }

    column_view strings_column_view::offsets() const {
        assertion_exception_msg(num_children() > 0, "strings column has no children");
        return child(offsets_column_index);
    }

    strings_column_view::offset_iterator strings_column_view::offsets_begin() const {
        return offsets().begin<offset_type>() + offset();
    }

    strings_column_view::offset_iterator strings_column_view::offsets_end() const {
        return offsets_begin() + size() + 1;
    }

    column_view strings_column_view::chars() const {
        assertion_exception_msg(num_children() > 0, "strings column has no children");
        return child(chars_column_index);
    }

    size_type strings_column_view::chars_size() const noexcept {
        if (size() == 0) {
            return 0;
        }
        return chars().size();
    }

    strings_column_view::chars_iterator strings_column_view::chars_begin() const { return chars().begin<char>(); }

    strings_column_view::chars_iterator strings_column_view::chars_end() const { return chars_begin() + chars_size(); }

} // namespace components::dataframe::column
