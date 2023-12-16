#include <memory>
#include <memory_resource>
#include <numeric>

#include <dataframe/column/make.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/detail/util.hpp>
#include <dataframe/traits.hpp>
#include <dataframe/type_dispatcher.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::column {

    // Empty column of specified type
    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, data_type type) {
        assertion_exception_msg(type.id() == type_id::empty || !is_nested(type),
                                "make_empty_column is invalid to call on nested types");
        return std::make_unique<column_t>(resource, type, 0, core::buffer(resource), core::buffer(resource));
    }

    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, type_id id) {
        return make_empty_column(resource, data_type{id});
    }

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  std::unique_ptr<column_t> offsets_column,
                                                  std::unique_ptr<column_t> chars_column,
                                                  size_type null_count,
                                                  core::buffer&& null_mask) {
        //todo: impl
    }

    std::unique_ptr<column_t> make_structs_column(std::pmr::memory_resource* resource,
                                                  size_type num_rows,
                                                  std::vector<std::unique_ptr<column_t>>&& child_columns,
                                                  size_type null_count,
                                                  core::buffer&& null_mask) {
        assertion_exception_msg(null_count <= 0 || !null_mask.is_empty(), "Struct column with nulls must be nullable.");

        assertion_exception_msg(std::all_of(child_columns.begin(),
                                            child_columns.end(),
                                            [&](auto const& child_col) { return num_rows == child_col->size(); }),
                                "Child columns must have the same number of rows as the Struct column.");

        if (!null_mask.is_empty()) {
            for (auto& child : child_columns) {
                child = dataframe::detail::superimpose_nulls(resource,
                                                             static_cast<bitmask_type const*>(null_mask.data()),
                                                             null_count,
                                                             std::move(child));
            }
        }

        return std::make_unique<column::column_t>(resource,
                                                  data_type{type_id::structs},
                                                  num_rows,
                                                  core::buffer{resource},
                                                  std::move(null_mask),
                                                  null_count,
                                                  std::move(child_columns));
    }

} // namespace components::dataframe::column
