#include "parser_database.hpp"
#include "impl/parser_database_create.hpp"
#include "impl/parser_database_drop.hpp"

namespace components::sql::database {

    bool parse(std::string_view query, ql::variant_statement_t& statement) {
        return impl::parseCreate(query, statement)
            || impl::parseDrop(query, statement);
    }

} // namespace components::sql::database
