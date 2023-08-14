#pragma once

#include <set>
#include <components/ql/aggregate/group.hpp>
#include <components/sql/lexer/lexer.hpp>
#include "parser_result.hpp"

namespace components::sql::impl {

    parser_result parse_groupby(lexer_t& lexer, std::pmr::set<std::string_view>& group_fields);

    parser_result check_groupby(const ql::aggregate::group_t& group, const std::pmr::set<std::string_view>& group_fields);

} // namespace components::sql::impl
