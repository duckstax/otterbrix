#pragma once

#include <components/ql/aggregate/group.hpp>
#include <components/ql/statements.hpp>
#include <components/sql/lexer/lexer.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::select::impl {

    components::sql::impl::parser_result parse_select_fields(std::pmr::memory_resource* resource,
                                                             components::sql::lexer_t& lexer,
                                                             components::ql::aggregate::group_t& group,
                                                             components::ql::ql_param_statement_t& statement,
                                                             std::pmr::set<token_t>& group_fields_select);

} // namespace components::sql::select::impl
