#pragma once

#include <core/buffer.hpp>
#include <dataframe/forward.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::lists {

    std::unique_ptr<column::column_t> make_lists_column(std::pmr::memory_resource* resource,
                                                        size_type num_rows,
                                                        std::unique_ptr<column::column_t> offsets_column,
                                                        std::unique_ptr<column::column_t> child_column,
                                                        size_type null_count,
                                                        core::buffer&& null_mask);

}
