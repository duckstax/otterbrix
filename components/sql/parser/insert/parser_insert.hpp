#pragma once

#include <components/ql/statements.hpp>

namespace components::sql::insert {

    bool parse(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::insert
