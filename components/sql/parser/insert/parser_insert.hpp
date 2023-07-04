#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::insert {

    components::sql::impl::parser_result parse(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::insert
