#include "make.hpp"

#include <memory>
#include <memory_resource>

#include <core/assert/assert.hpp>

#include <dataframe/column/column.hpp>

namespace components::dataframe::lists {
    std::unique_ptr<column::column_t> make_lists_column(std::pmr::memory_resource* resource,
                                                        size_type num_rows,
                                                        std::unique_ptr<column::column_t> offsets_column,
                                                        std::unique_ptr<column::column_t> child_column,
                                                        size_type null_count,
                                                        core::buffer&& null_mask) {
        if (null_count > 0) {
            assertion_exception_msg(null_mask.size() > 0, "Column with nulls must be nullable.");
        }
        assertion_exception_msg((num_rows == 0 && offsets_column->size() == 0) ||
                                    num_rows == offsets_column->size() - 1,
                                "Invalid offsets column size for lists column.");
        assertion_exception_msg(offsets_column->null_count() == 0, "Offsets column should not contain nulls");
        assertion_exception_msg(child_column != nullptr, "Must pass a valid child column");

        std::vector<std::unique_ptr<column::column_t>> children;
        children.emplace_back(std::move(offsets_column));
        children.emplace_back(std::move(child_column));
        return std::make_unique<column::column_t>(resource,
                                                  data_type{type_id::list},
                                                  num_rows,
                                                  core::buffer(resource),
                                                  std::move(null_mask),
                                                  null_count,
                                                  std::move(children));
    }
} // namespace components::dataframe::lists