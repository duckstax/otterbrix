#pragma once

#include <components/ql/statements.hpp>
#include "parser/base/parse_error.hpp"

namespace components::sql {

    struct parse_result {
        ql::variant_statement_t ql;
        error_t error;

        explicit parse_result(const ql::variant_statement_t& ql);
        explicit parse_result(const error_t& error);
    };

    parse_result parse(std::string_view query, std::pmr::memory_resource* resource);
    parse_result parse(const std::string& query, std::pmr::memory_resource* resource);
    parse_result parse(const char* query, std::pmr::memory_resource* resource);

} // namespace components::sql
