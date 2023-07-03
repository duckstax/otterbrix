#include "parser_database.hpp"
#include "impl/parser_database_create.hpp"
#include "impl/parser_database_drop.hpp"

namespace components::sql::database {

    parser_result parse(std::string_view query, ql::variant_statement_t& statement) {
        return impl::parse_create(query, statement)
            || impl::parse_drop(query, statement);
    }

} // namespace components::sql::database
