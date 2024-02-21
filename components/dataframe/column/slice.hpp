#pragma once

#include <memory>
#include <memory_resource>

#include "column.hpp"
#include "strings_column_view.hpp"

namespace components::dataframe::column {

    std::vector<column::column_view> slice(column::column_view const& input, core::span<size_type const> indices);
    std::vector<column::column_view> slice(column::column_view const& input, std::initializer_list<size_type> indices);
    std::unique_ptr<column_t>
    copy_slice(std::pmr::memory_resource*, strings_column_view const& strings, size_type start, size_type end);

} // namespace components::dataframe::column