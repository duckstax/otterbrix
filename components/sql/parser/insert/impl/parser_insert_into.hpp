#pragma once

#include <components/ql/statements.hpp>

namespace components::sql::insert::impl {

    bool parseInsertInto(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::insert::impl
