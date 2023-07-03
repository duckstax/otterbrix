#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::database::impl {

    parser_result parse_drop(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::database::impl
