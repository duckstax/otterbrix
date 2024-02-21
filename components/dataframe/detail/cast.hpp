#pragma once

#include <memory>
#include <memory_resource>

#include <dataframe/column/column_view.hpp>

namespace components::dataframe::detail {

    std::unique_ptr<column::column_t>
    cast(std::pmr::memory_resource* resource, column::column_view const& input, data_type type);

} // namespace components::dataframe::detail