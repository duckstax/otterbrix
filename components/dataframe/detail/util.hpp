#pragma once

#include <dataframe/column/column.hpp>

namespace components::dataframe::detail {

    std::unique_ptr<column::column_t> superimpose_nulls(std::pmr::memory_resource* resource,
                                                        const bitmask_type* null_mask,
                                                        size_type null_count,
                                                        std::unique_ptr<column::column_t>&& input);

} // namespace components::dataframe::detail