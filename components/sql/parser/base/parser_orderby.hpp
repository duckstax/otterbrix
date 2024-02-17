#pragma once

#include "parser_result.hpp"
#include <components/ql/aggregate/sort.hpp>
#include <components/sql/lexer/lexer.hpp>
#include <memory_resource>

namespace components::sql::impl {

    parser_result
    parse_orderby(std::pmr::memory_resource* resource, lexer_t& lexer, components::ql::aggregate::sort_t& sort);

} // namespace components::sql::impl
