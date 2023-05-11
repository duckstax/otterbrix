#pragma once

#include <vector>

#include "table_view.hpp"

namespace components::dataframe::table {

    std::pmr::vector<table::table_view> slice(table::table_view const& input, std::initializer_list<size_type> indices);
    std::pmr::vector<table::table_view> slice(table::table_view const& input, core::span<size_type const> indices);

} // namespace components::dataframe::table