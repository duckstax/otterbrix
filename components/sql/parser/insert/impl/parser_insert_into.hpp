#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::insert::impl {

    parser_result parse_insert_into(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::insert::impl
