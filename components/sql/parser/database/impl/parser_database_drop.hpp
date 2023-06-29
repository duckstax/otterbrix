#pragma once

#include <components/ql/statements.hpp>

namespace components::sql::database::impl {

    bool parseDrop(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::database::impl
