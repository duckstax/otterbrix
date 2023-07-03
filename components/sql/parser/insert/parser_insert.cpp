#include "parser_insert.hpp"
#include "impl/parser_insert_into.hpp"

namespace components::sql::insert {

    parser_result parse(std::string_view query, ql::variant_statement_t& statement) {
        return impl::parse_insert_into(query, statement);
    }

} // namespace components::sql::insert
