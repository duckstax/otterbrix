#pragma once

#include "parser_result.hpp"
#include <components/ql/aggregate/match.hpp>
#include <components/ql/join/join.hpp>
#include <components/ql/ql_param_statement.hpp>
#include <components/sql/lexer/lexer.hpp>

namespace components::sql::impl {

    parser_result parse_where(std::pmr::memory_resource* resource,
                              lexer_t& lexer,
                              components::ql::aggregate::match_t& match,
                              components::ql::ql_param_statement_t& statement);

    parser_result parse_join_on(std::pmr::memory_resource* resource, lexer_t& lexer, components::ql::join_t& join);

} // namespace components::sql::impl
