#pragma once

#include <components/ql/statements.hpp>
#include <components/sql/parser/base/parser_result.hpp>

namespace components::sql::delete_::impl {

    components::sql::impl::parser_result
    parse_delete_base(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement);

} // namespace components::sql::delete_::impl
