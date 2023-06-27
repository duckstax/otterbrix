#pragma once

#include <components/ql/statements.hpp>

namespace components::sql {

    ql::variant_statement_t parse(std::string_view query);
    ql::variant_statement_t parse(const std::string& query);
    ql::variant_statement_t parse(const char* query);

} // namespace components::sql
