#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::database::impl {

    components::sql::impl::parser_result parse_create(std::string_view query,
                                                      ql::variant_statement_t& statement,
                                                      std::pmr::memory_resource* resource);

} // namespace components::sql::database::impl
