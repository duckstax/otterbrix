#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::select::impl {

    components::sql::impl::parser_result parse_select_without_from(std::pmr::memory_resource* resource,
                                                                   std::string_view query,
                                                                   ql::variant_statement_t& statement);

} // namespace components::sql::select::impl
