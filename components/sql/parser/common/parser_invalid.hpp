#pragma once

#include <components/ql/statements.hpp>

namespace components::sql::invalid {

    bool parse(std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::invalid
