#include <core/assert/assert.hpp>
#include <dataframe/column/column.hpp>
#include <dataframe/structs/structs_column_view.hpp>

namespace components::dataframe::structs {

    structs_column_view::structs_column_view(column_view const& rhs)
        : column_view{rhs} {
        assertion_exception_msg(type().id() == type_id::STRUCT, "structs_column_view only supports struct columns");
    }

    column::column_view structs_column_view::parent() const { return *this; }

    column::column_view structs_column_view::get_sliced_child(int index) const {
        std::vector<column_view> children;
        children.reserve(child(index).num_children());
        for (size_type i = 0; i < child(index).num_children(); i++) {
            children.push_back(child(index).child(i));
        }
        return column_view{child(index).type(),
                           size(),
                           child(index).head<uint8_t>(),
                           child(index).null_mask(),
                           UNKNOWN_NULL_COUNT,
                           offset(),
                           children};
    }

} // namespace components::dataframe::structs
