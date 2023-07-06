#include "parser_insert.hpp"
#include "impl/parser_insert_into.hpp"

#define PARSE(F) if (!ok) ok = impl::F(query, statement, resource)

namespace components::sql::insert {

    components::sql::impl::parser_result parse(std::string_view query,
                                               ql::variant_statement_t& statement,
                                               std::pmr::memory_resource* resource) {
        components::sql::impl::parser_result ok{false};
        PARSE(parse_insert_into);
        return ok;
    }

} // namespace components::sql::insert
