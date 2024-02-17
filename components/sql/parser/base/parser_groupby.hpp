#pragma once

#include "parser_result.hpp"
#include <components/ql/aggregate/group.hpp>
#include <components/sql/lexer/lexer.hpp>
#include <memory_resource>
#include <set>

namespace components::sql::impl {

    parser_result parse_groupby(lexer_t& lexer, std::pmr::set<token_t>& group_fields);

    parser_result check_groupby(std::pmr::memory_resource* resource,
                                const ql::aggregate::group_t& group,
                                const std::pmr::set<token_t>& group_fields_select,
                                const std::pmr::set<token_t>& group_fields);

} // namespace components::sql::impl
