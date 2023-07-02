#include "parser_insert.hpp"
#include "impl/parser_insert_into.hpp"

namespace components::sql::insert {

    bool parse(std::string_view query, ql::variant_statement_t& statement) {
        return impl::parseInsertInto(query, statement);
    }

} // namespace components::sql::insert
