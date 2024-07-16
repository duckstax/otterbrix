#include <core/assert/assert.hpp>
#include <dataframe/dictionary/dictionary_column_view.hpp>

namespace components::dataframe::dictionary {

    dictionary_column_view::dictionary_column_view(column_view const& dictionary_column)
        : column_view(dictionary_column) {
        assertion_exception_msg(type().id() == type_id::dictionary32,
                                "dictionary_column_view only supports DICTIONARY type");

        if (size() > 0) {
            assertion_exception_msg(num_children() == 2, "dictionary column has no children");
        }
    }

    column::column_view dictionary_column_view::parent() const noexcept { return static_cast<column_view>(*this); }

    column::column_view dictionary_column_view::indices() const noexcept { return child(0); }

    column::column_view
    dictionary_column_view::get_indices_annotated(std::pmr::memory_resource* resource) const noexcept {
        return column_view(indices().type(), size(), indices().head(), null_mask(), null_count(resource), offset());
    }

    column::column_view dictionary_column_view::keys() const noexcept { return child(1); }

    size_type dictionary_column_view::keys_size() const noexcept { return (size() == 0) ? 0 : keys().size(); }

    data_type dictionary_column_view::keys_type() const noexcept {
        return (size() == 0) ? data_type{type_id::empty} : keys().type();
    }

} // namespace components::dataframe::dictionary
