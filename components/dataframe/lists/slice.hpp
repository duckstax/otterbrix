#pragma once

#include <memory>
#include <memory_resource>

#include <dataframe/column/column.hpp>
#include <dataframe/lists/lists_column_view.hpp>

namespace components::dataframe::lists {

    std::unique_ptr<column::column_t>
    copy_slice(std::pmr::memory_resource* resource, lists_column_view const& lists, size_type start, size_type end);

} // namespace components::dataframe::lists