#include "parser_database.hpp"
#include "impl/parser_database_create.hpp"
#include "impl/parser_database_drop.hpp"

#define PARSE(F) if (!ok) ok = impl::F(query, statement)

namespace components::sql::database {

    parser_result parse(std::string_view query, ql::variant_statement_t& statement) {
        parser_result ok{false};
        PARSE(parse_create);
        PARSE(parse_drop);
        return ok;
    }

} // namespace components::sql::database
