#pragma once

#include <cassert>

#include <memory>
#include <memory_resource>
#include <type_traits>
#include <utility>
#include <vector>

#include "column.hpp"
#include "column_view.hpp"
#include "dataframe/traits.hpp"
#include "dataframe/types.hpp"

#include "core/buffer.hpp"
#include "core/uvector.hpp"

namespace components::dataframe::column {

    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, data_type type);
    std::unique_ptr<column_t> make_empty_column(std::pmr::memory_resource* resource, type_id id);

    std::unique_ptr<column_t> make_strings_column(std::pmr::memory_resource* resource,
                                                  size_type num_strings,
                                                  std::unique_ptr<column_t> offsets_column,
                                                  std::unique_ptr<column_t> chars_column,
                                                  size_type null_count,
                                                  core::buffer&& null_mask);

    std::unique_ptr<column_t> make_structs_column(std::pmr::memory_resource* resource,
                                                  size_type num_rows,
                                                  std::vector<std::unique_ptr<column_t>>&& child_columns,
                                                  size_type null_count,
                                                  core::buffer&& null_mask);

} // namespace components::dataframe::column
